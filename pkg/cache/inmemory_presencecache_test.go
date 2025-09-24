// Package cache_test provides tests for the cache implementations.
package cache_test

import (
	"context"
	"testing"

	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInMemoryPresenceCache provides unit tests for the simple in-memory presence cache.
func TestInMemoryPresenceCache(t *testing.T) {
	ctx := context.Background()
	const testKey = "user:123"
	testValue := "connection-info"

	// Arrange
	c := cache.NewInMemoryPresenceCache[string, string]()

	// 1. Test Fetch on a non-existent key
	t.Run("Fetch miss", func(t *testing.T) {
		_, err := c.Fetch(ctx, "non-existent-key")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in presence cache")
	})

	// 2. Test the full Set -> Fetch -> Delete cycle
	t.Run("Set, Fetch, and Delete cycle", func(t *testing.T) {
		// Act: Set a value
		err := c.Set(ctx, testKey, testValue)
		require.NoError(t, err)

		// Assert: Fetch the value back
		retrieved, err := c.Fetch(ctx, testKey)
		require.NoError(t, err)
		assert.Equal(t, testValue, retrieved)

		// Act: Delete the value
		err = c.Delete(ctx, testKey)
		require.NoError(t, err)

		// Assert: Fetching again should result in an error
		_, err = c.Fetch(ctx, testKey)
		require.Error(t, err)
	})
}
