// cache/inmemory_test.go
package cache_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/illmade-knight/go-dataflow/pkg/cache" // Assumed import path
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testStruct struct {
	Name  string
	Value int
}

func TestInMemoryCache_SetAndGet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := cache.NewInMemoryCache[string, testStruct]()

	key := "test-key"
	value := testStruct{Name: "test-name", Value: 123}

	err := c.WriteToCache(ctx, key, value)
	require.NoError(t, err)

	retrieved, err := c.FetchFromCache(ctx, key)
	require.NoError(t, err)

	assert.Equal(t, value, retrieved)
}

func TestInMemoryCache_GetMiss(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := cache.NewInMemoryCache[string, testStruct]()

	_, err := c.FetchFromCache(ctx, "non-existent-key")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found in cache")
}

func TestInMemoryCache_Concurrency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := cache.NewInMemoryCache[string, int]()

	var wg sync.WaitGroup
	numGoroutines := 100
	writesPerGoroutine := 10

	// Run concurrent writers
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				key := fmt.Sprintf("key-%d-%d", gID, j)
				_ = c.WriteToCache(ctx, key, gID*100+j)
			}
		}(i)
	}

	// Run concurrent readers and writers
	for i := numGoroutines / 2; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				// Write a new key
				writeKey := fmt.Sprintf("key-%d-%d", gID, j)
				_ = c.WriteToCache(ctx, writeKey, gID*100+j)

				// Read a key that should exist
				readKey := fmt.Sprintf("key-%d-%d", gID-numGoroutines/2, j)
				_, _ = c.FetchFromCache(ctx, readKey)
			}
		}(i)
	}

	wg.Wait()

	// Final verification
	val, err := c.FetchFromCache(ctx, "key-1-1")
	require.NoError(t, err)
	assert.Equal(t, 101, val)
}
