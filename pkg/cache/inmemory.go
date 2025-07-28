// cache/inmemory.go
package cache

import (
	"context"
	"fmt"
	"sync"
)

// InMemoryCache is a generic, thread-safe, in-memory cache implementation.
// It satisfies the Cache interface.
type InMemoryCache[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]V
}

// NewInMemoryCache creates a new in-memory cache.
func NewInMemoryCache[K comparable, V any]() *InMemoryCache[K, V] {
	return &InMemoryCache[K, V]{
		data: make(map[K]V),
	}
}

// FetchFromCache retrieves an item from the cache.
func (c *InMemoryCache[K, V]) FetchFromCache(ctx context.Context, key K) (V, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	value, ok := c.data[key]
	if !ok {
		var zero V
		return zero, fmt.Errorf("key '%v' not found in cache", key)
	}
	return value, nil
}

// WriteToCache adds an item to the cache.
func (c *InMemoryCache[K, V]) WriteToCache(ctx context.Context, key K, value V) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = value
	return nil
}
