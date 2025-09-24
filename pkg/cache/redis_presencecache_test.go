//go:build integration

package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type presenceTestValue struct {
	ServerID    string `json:"serverId"`
	ConnectedAt int64  `json:"connectedAt"`
}

func TestRedisPresenceCache_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// Setup Redis emulator once for all sub-tests
	rc := emulators.GetDefaultRedisImageContainer()
	redisConn := emulators.SetupRedisContainer(t, ctx, rc)
	rawClient := newRawRedisClient(t, redisConn.EmulatorAddress)

	baseCfg := &cache.RedisConfig{
		Addr:     redisConn.EmulatorAddress,
		CacheTTL: 1 * time.Minute,
	}

	const testKey = "user:pres:123"
	testValue := presenceTestValue{ServerID: "instance-1", ConnectedAt: time.Now().Unix()}

	// Create the cache instance to be tested
	presenceCache, err := cache.NewRedisPresenceCache[string, presenceTestValue](ctx, baseCfg, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = presenceCache.Close() })

	t.Run("Set, Fetch, and Delete cycle", func(t *testing.T) {
		// Act 1: Set a value
		err := presenceCache.Set(ctx, testKey, testValue)
		require.NoError(t, err)

		// Assert 1: Verify directly in Redis that the key exists
		exists, err := rawClient.Exists(ctx, testKey).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), exists, "Key should exist in Redis after Set")

		// Act 2: Fetch the value back
		retrieved, err := presenceCache.Fetch(ctx, testKey)
		require.NoError(t, err)
		assert.Equal(t, testValue, retrieved)

		// Act 3: Delete the value
		err = presenceCache.Delete(ctx, testKey)
		require.NoError(t, err)

		// Assert 3: Verify directly in Redis that the key is gone
		exists, err = rawClient.Exists(ctx, testKey).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), exists, "Key should not exist in Redis after Delete")
	})

	t.Run("TTL causes key expiration", func(t *testing.T) {
		// Arrange: Use a very short TTL
		shortTTLCfg := &cache.RedisConfig{Addr: redisConn.EmulatorAddress, CacheTTL: 150 * time.Millisecond}
		c, err := cache.NewRedisPresenceCache[string, presenceTestValue](ctx, shortTTLCfg, zerolog.Nop())
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close() })

		// Act 1: Set the value
		err = c.Set(ctx, "ttl-key", testValue)
		require.NoError(t, err)

		// Assert 1: Verify it exists immediately after
		exists, err := rawClient.Exists(ctx, "ttl-key").Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), exists)

		// Act 2: Wait for the TTL to expire
		time.Sleep(200 * time.Millisecond)

		// Assert 2: The key should no longer exist
		exists, err = rawClient.Exists(ctx, "ttl-key").Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), exists, "Key should have expired and been removed from Redis")
	})
}
