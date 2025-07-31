// cache/redis_test.go
//go:build integration

package cache_test

import (
	"context"
	"errors"
	"strings"
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

func TestRedisCache_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// Assumes a helper that sets up a Redis Docker container for testing.
	rc := emulators.GetDefaultRedisImageContainer()
	redisConn := emulators.SetupRedisContainer(t, ctx, rc)

	cfg := &cache.RedisConfig{
		Addr:     redisConn.EmulatorAddress,
		CacheTTL: 1 * time.Minute,
	}

	c, err := cache.NewRedisCache[string, redisTestValue](ctx, cfg, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	t.Run("Set and Get", func(t *testing.T) {
		key := "test-key-1"
		value := redisTestValue{ID: "test-id", Data: []byte("hello world")}

		err := c.WriteToCache(ctx, key, value)
		require.NoError(t, err)

		retrieved, err := c.Fetch(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, retrieved)
	})

	t.Run("Get Miss", func(t *testing.T) {
		_, err := c.Fetch(ctx, "non-existent-key")
		require.Error(t, err)
		assert.True(t, errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "key not found"), "Error should indicate a cache miss")
	})

	t.Run("TTL Expires", func(t *testing.T) {
		// Create a new cache with a very short TTL for this specific test.
		shortTTLCfg := &cache.RedisConfig{Addr: redisConn.EmulatorAddress, CacheTTL: 100 * time.Millisecond}
		shortCache, err := cache.NewRedisCache[string, redisTestValue](ctx, shortTTLCfg, zerolog.Nop())
		require.NoError(t, err)
		t.Cleanup(func() { _ = shortCache.Close() })

		key := "ttl-key"
		value := redisTestValue{ID: "ttl-id"}
		err = shortCache.WriteToCache(ctx, key, value)
		require.NoError(t, err)

		// This is one of the few acceptable uses of time.Sleep in a test,
		// as we are explicitly verifying a time-based feature.
		time.Sleep(150 * time.Millisecond)

		_, err = shortCache.Fetch(ctx, key)
		require.Error(t, err, "Should get an error after TTL expires")
	})
}
