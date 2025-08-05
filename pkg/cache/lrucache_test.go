package cache_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryLRUCache_Fetch(t *testing.T) {
	ctx := context.Background()

	t.Run("Eviction policy works correctly", func(t *testing.T) {
		// Arrange
		var fetcherCallCount atomic.Int32
		mockSource := &mockFetcher[string, int]{
			FetchFunc: func(ctx context.Context, key string) (int, error) {
				fetcherCallCount.Add(1)
				switch key {
				case "key1":
					return 1, nil
				case "key2":
					return 2, nil
				case "key3":
					return 3, nil
				default:
					return 0, errors.New("not found")
				}
			},
		}

		// Create a cache with a max size of 2.
		lru, err := cache.NewInMemoryLRUCache[string, int](2, mockSource)
		require.NoError(t, err)

		// Act 1: Fill the cache. This will call the fallback for key1 and key2.
		val1, _ := lru.Fetch(ctx, "key1")
		val2, _ := lru.Fetch(ctx, "key2")

		// Assert 1
		assert.Equal(t, 1, val1)
		assert.Equal(t, 2, val2)
		assert.Equal(t, int32(2), fetcherCallCount.Load(), "Fallback should be called twice to fill the cache")

		// Act 2: Access key1 again. This should be a cache hit and make key1 the most recently used.
		_, _ = lru.Fetch(ctx, "key1")

		// Assert 2
		assert.Equal(t, int32(2), fetcherCallCount.Load(), "Fallback should NOT be called for a cache hit")

		// Act 3: Fetch a new key, key3. This should be a miss, and should evict key2 (the least recently used).
		val3, _ := lru.Fetch(ctx, "key3")

		// Assert 3
		assert.Equal(t, 3, val3)
		assert.Equal(t, int32(3), fetcherCallCount.Load(), "Fallback should be called for the new key")

		// Act 4: Fetch key2 again. It should have been evicted, so this must trigger the fallback again.
		_, _ = lru.Fetch(ctx, "key2")

		// Assert 4
		assert.Equal(t, int32(4), fetcherCallCount.Load(), "Fallback should be called for the evicted key2")

		// Act 5: Fetch key1 again. It should still be in the cache.
		_, _ = lru.Fetch(ctx, "key1")

		// Assert 5
		assert.Equal(t, int32(5), fetcherCallCount.Load(), "Fallback should NOT be called for key1, which should still be cached")
	})

	t.Run("Miss with no fallback", func(t *testing.T) {
		// Arrange
		lru, err := cache.NewInMemoryLRUCache[string, int](5, nil)
		require.NoError(t, err)

		// Act
		_, err = lru.Fetch(ctx, "miss")

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in LRU cache and no fallback is configured")
	})
}
