// cache/redis_test.go
//go:build integration

package cache_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/cache" // Assumed import path
	"github.com/illmade-knight/go-test/emulators"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type redisTestValue struct {
	ID   string
	Data []byte
}

// newRawRedisClient is a test helper to connect directly to the emulator for verification.
func newRawRedisClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestRedisCache_Fetch_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// Setup Redis emulator once for all sub-tests
	rc := emulators.GetDefaultRedisImageContainer()
	redisConn := emulators.SetupRedisContainer(t, ctx, rc)

	baseCfg := &cache.RedisConfig{
		Addr:     redisConn.EmulatorAddress,
		CacheTTL: 1 * time.Minute,
	}

	t.Run("Miss with no fallback", func(t *testing.T) {
		// Arrange
		c, err := cache.NewRedisCache[string, redisTestValue](ctx, baseCfg, zerolog.Nop(), nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close() })

		// Act
		_, err = c.Fetch(ctx, "non-existent-key")

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in cache and no fallback is configured")
	})

	t.Run("Fallback success and cache write-back", func(t *testing.T) {
		// Arrange
		var fetcherCallCount atomic.Int32
		expectedValue := redisTestValue{ID: "source-id", Data: []byte("source data")}
		mockSource := &mockFetcher[string, redisTestValue]{
			FetchFunc: func(ctx context.Context, key string) (redisTestValue, error) {
				fetcherCallCount.Add(1)
				if key == "source-key" {
					return expectedValue, nil
				}
				return redisTestValue{}, errors.New("source not found")
			},
		}

		c, err := cache.NewRedisCache[string, redisTestValue](ctx, baseCfg, zerolog.Nop(), mockSource)
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close() })

		// Act 1: First fetch misses Redis, hits the fallback source, and triggers async write-back.
		val1, err1 := c.Fetch(ctx, "source-key")

		// Assert 1: Check that the value is correct and the fallback was called.
		require.NoError(t, err1)
		assert.Equal(t, expectedValue, val1)
		assert.Equal(t, int32(1), fetcherCallCount.Load())

		// Assert 2: Wait for the async write-back to complete by polling Redis directly.
		rawClient := newRawRedisClient(t, redisConn.EmulatorAddress)
		require.Eventually(t, func() bool {
			res, err := rawClient.Exists(ctx, "source-key").Result()
			return err == nil && res == 1
		}, 2*time.Second, 50*time.Millisecond, "Key was not written back to Redis cache in time")

		// Act 3: Second fetch should now be a definitive cache hit.
		val2, err2 := c.Fetch(ctx, "source-key")

		// Assert 3: Verify the value is correct and the fallback was NOT called again.
		require.NoError(t, err2)
		assert.Equal(t, expectedValue, val2)
		assert.Equal(t, int32(1), fetcherCallCount.Load(), "Fallback should not be called again on a cache hit")
	})
}

func TestRedisCache_TTL_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	rc := emulators.GetDefaultRedisImageContainer()
	redisConn := emulators.SetupRedisContainer(t, ctx, rc)
	rawClient := newRawRedisClient(t, redisConn.EmulatorAddress)

	// Arrange
	var fetcherCallCount atomic.Int32
	mockSource := &mockFetcher[string, string]{
		FetchFunc: func(ctx context.Context, key string) (string, error) {
			fetcherCallCount.Add(1)
			return "fresh-data", nil
		},
	}

	shortTTLCfg := &cache.RedisConfig{Addr: redisConn.EmulatorAddress, CacheTTL: 150 * time.Millisecond}
	c, err := cache.NewRedisCache[string, string](ctx, shortTTLCfg, zerolog.Nop(), mockSource)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// Act 1: Populate the cache via fallback.
	_, err = c.Fetch(ctx, "ttl-key")
	require.NoError(t, err)
	assert.Equal(t, int32(1), fetcherCallCount.Load(), "Fallback should be called on the first fetch")

	// Wait for the key to appear in Redis.
	require.Eventually(t, func() bool {
		res, err := rawClient.Exists(ctx, "ttl-key").Result()
		return err == nil && res == 1
	}, 2*time.Second, 50*time.Millisecond, "Key did not appear in Redis after first fetch")

	// Act 2: Verify it's a cache hit immediately after.
	_, err = c.Fetch(ctx, "ttl-key")
	require.NoError(t, err)
	assert.Equal(t, int32(1), fetcherCallCount.Load(), "Fallback should not be called on immediate second fetch")

	// Act 3: Wait for TTL to expire and fetch again.
	time.Sleep(200 * time.Millisecond)
	_, err = c.Fetch(ctx, "ttl-key")
	require.NoError(t, err)

	// Assert 3: The fallback should have been called a second time.
	assert.Equal(t, int32(2), fetcherCallCount.Load(), "Fallback should be called again after TTL expires")
}
