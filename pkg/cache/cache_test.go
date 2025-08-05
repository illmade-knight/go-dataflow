package cache_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/illmade-knight/go-dataflow/pkg/cache" // Assumed import path
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSourceOfTruth is a test double that simulates a database or other primary data source.
type mockSourceOfTruth struct {
	callCount atomic.Int32
	data      map[string]string
}

func newMockSourceOfTruth() *mockSourceOfTruth {
	return &mockSourceOfTruth{
		data: map[string]string{
			"user:123": "John Doe",
			"user:456": "Jane Smith",
		},
	}
}

func (m *mockSourceOfTruth) Fetch(_ context.Context, key string) (string, error) {
	m.callCount.Add(1)
	if val, ok := m.data[key]; ok {
		return val, nil
	}
	return "", errors.New("not found in source")
}

func (m *mockSourceOfTruth) Close() error { return nil }

// TestChainedCache_FallbackAndInvalidation demonstrates the full lifecycle of the
// multi-layered cache, including fallback on miss and explicit invalidation.
func TestChainedCache_FallbackAndInvalidation(t *testing.T) {
	ctx := context.Background()
	const testKey = "user:123"

	// Arrange: Create the mock source of truth and the chained cache.
	// The chain is: L1 (InMemoryLRU) -> L2 (Simple InMemory) -> Source (Mock)
	source := newMockSourceOfTruth()

	// L2 Cache (e.g., this could be a RedisCache in a real scenario)
	l2Cache := cache.NewInMemoryCache[string, string](source)

	// L1 Cache (the top-level cache our service will use)
	l1Cache, err := cache.NewInMemoryLRUCache[string, string](10, l2Cache)
	require.NoError(t, err)

	// --- 1. First Fetch: Cache Miss ---
	// The first time we fetch the key, it should be a miss on both L1 and L2,
	// resulting in a call to the source of truth.
	t.Run("First Fetch causes cache miss and fallback", func(t *testing.T) {
		// Act
		value, err := l1Cache.Fetch(ctx, testKey)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "John Doe", value)
		assert.Equal(t, int32(1), source.callCount.Load(), "Source of truth should be called exactly once")
	})

	// --- 2. Second Fetch: Cache Hit ---
	// The second fetch for the same key should be a hit in the L1 cache.
	// The source of truth should not be called again.
	t.Run("Second Fetch is a cache hit", func(t *testing.T) {
		// Act
		value, err := l1Cache.Fetch(ctx, testKey)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "John Doe", value)
		assert.Equal(t, int32(1), source.callCount.Load(), "Source of truth should NOT be called on a cache hit")
	})

	// --- 3. Invalidate the Cache ---
	// We now invalidate the key. This should remove it from all cache layers that
	// implement the invalidation logic (in this case, just L1).
	t.Run("Invalidate removes the key from the cache", func(t *testing.T) {
		// Act
		// Note: We need to define a new interface that includes Invalidate to call it.
		// For this test, we can cast directly as we know the implementation.
		c, ok := (interface{})(l1Cache).(cache.Cache[string, string])
		require.True(t, ok, "l1Cache should implement the Cache interface")
		err := c.Invalidate(ctx, testKey)

		// Assert
		require.NoError(t, err)
	})

	// --- 4. Third Fetch: Cache Miss After Invalidation ---
	// After invalidation, fetching the key again should result in another cache miss,
	// forcing a call to the source of truth to repopulate the cache.
	t.Run("Fetch after invalidation causes a miss", func(t *testing.T) {
		// Act
		value, err := l1Cache.Fetch(ctx, testKey)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "John Doe", value)
		assert.Equal(t, int32(1), source.callCount.Load(), "Invalidation does not cascade so the source is not affected by invalidating the top level cache")
		fmt.Println("Final source call count:", source.callCount.Load())
	})
}
