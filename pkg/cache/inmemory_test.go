package cache_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockFetcher is a test double for the cache.Fetcher interface.
type mockFetcher[K comparable, V any] struct {
	FetchFunc func(ctx context.Context, key K) (V, error)
	CloseFunc func() error
}

func (m *mockFetcher[K, V]) Fetch(ctx context.Context, key K) (V, error) {
	if m.FetchFunc != nil {
		return m.FetchFunc(ctx, key)
	}
	var zero V
	return zero, fmt.Errorf("mock fetcher not implemented")
}

func (m *mockFetcher[K, V]) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

func TestInMemoryCache_Fetch(t *testing.T) {
	ctx := context.Background()

	t.Run("Miss with no fallback", func(t *testing.T) {
		// Arrange
		c := cache.NewInMemoryCache[string, int](nil)

		// Act
		_, err := c.Fetch(ctx, "miss")

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in cache and no fallback is configured")
	})

	t.Run("Fallback failure", func(t *testing.T) {
		// Arrange
		expectedErr := errors.New("source is down")
		mockSource := &mockFetcher[string, int]{
			FetchFunc: func(ctx context.Context, key string) (int, error) {
				return 0, expectedErr
			},
		}
		c := cache.NewInMemoryCache[string, int](mockSource)

		// Act
		_, err := c.Fetch(ctx, "any-key")

		// Assert
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("Fallback success and cache write-back", func(t *testing.T) {
		// Arrange
		var fetcherCallCount atomic.Int32
		mockSource := &mockFetcher[string, string]{
			FetchFunc: func(ctx context.Context, key string) (string, error) {
				fetcherCallCount.Add(1)
				if key == "hit-key" {
					return "this-is-the-data", nil
				}
				return "", errors.New("source not found")
			},
		}
		c := cache.NewInMemoryCache[string, string](mockSource)

		// Act 1: First fetch should be a cache miss, triggering the fallback.
		val1, err1 := c.Fetch(ctx, "hit-key")

		// Assert 1
		require.NoError(t, err1)
		assert.Equal(t, "this-is-the-data", val1)
		assert.Equal(t, int32(1), fetcherCallCount.Load(), "Fallback fetcher should be called once on the first miss")

		// Act 2: Second fetch should be a cache hit. The fallback should not be called again.
		val2, err2 := c.Fetch(ctx, "hit-key")

		// Assert 2
		require.NoError(t, err2)
		assert.Equal(t, "this-is-the-data", val2)
		assert.Equal(t, int32(1), fetcherCallCount.Load(), "Fallback fetcher should NOT be called on a cache hit")
	})
}
