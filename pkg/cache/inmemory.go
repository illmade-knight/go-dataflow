package cache

import (
	"context"
	"fmt"
	"sync"
)

// InMemoryCache is a generic, thread-safe, in-memory cache.
// It implements the Fetcher interface and can be configured with a fallback Fetcher
// to use on a cache miss.
type InMemoryCache[K comparable, V any] struct {
	mu       sync.RWMutex
	data     map[K]V
	fallback Fetcher[K, V]
}

// NewInMemoryCache creates a new in-memory cache.
// It can optionally be provided with a fallback Fetcher, which will be
// used to populate the cache on a miss.
func NewInMemoryCache[K comparable, V any](fallback Fetcher[K, V]) *InMemoryCache[K, V] {
	return &InMemoryCache[K, V]{
		data:     make(map[K]V),
		fallback: fallback,
	}
}

// Fetch retrieves an item. It first checks its internal in-memory map.
// If the key is not found (a cache miss), and a fallback Fetcher is configured,
// it will attempt to fetch the data from the fallback. If the fallback fetch
// is successful, it writes the result to its own cache for future requests.
func (c *InMemoryCache[K, V]) Fetch(ctx context.Context, key K) (V, error) {
	c.mu.RLock()
	value, ok := c.data[key]
	c.mu.RUnlock()

	if ok {
		// Cache hit
		return value, nil
	}

	// Cache miss
	var zero V
	if c.fallback == nil {
		return zero, fmt.Errorf("key '%v' not found in cache and no fallback is configured", key)
	}

	// Fallback to source
	sourceValue, err := c.fallback.Fetch(ctx, key)
	if err != nil {
		return zero, err // Return the error from the source
	}

	// Write back to cache for next time.
	_ = c.write(ctx, key, sourceValue)

	return sourceValue, nil
}

func (c *InMemoryCache[K, V]) Invalidate(_ context.Context, key K) error {
	c.mu.Lock()
	delete(c.data, key)
	c.mu.Unlock()
	return nil
}

// write adds an item to the cache. This method is unexported as it's an
// internal implementation detail of the Fetch-with-fallback logic.
func (c *InMemoryCache[K, V]) write(_ context.Context, key K, value V) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = value
	return nil
}

// Close is a no-op for the in-memory cache but satisfies the Fetcher interface.
func (c *InMemoryCache[K, V]) Close() error {
	return nil
}
