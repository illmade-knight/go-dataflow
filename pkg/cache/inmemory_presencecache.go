// Package cache provides generic caching components for data pipelines.
package cache

import (
	"context"
	"fmt"
	"sync"
)

// InMemoryPresenceCache is a thread-safe, in-memory implementation of PresenceCache.
// It is primarily intended for local development and testing.
type InMemoryPresenceCache[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]V
}

// NewInMemoryPresenceCache creates a new in-memory presence cache.
func NewInMemoryPresenceCache[K comparable, V any]() *InMemoryPresenceCache[K, V] {
	return &InMemoryPresenceCache[K, V]{
		data: make(map[K]V),
	}
}

// Set stores a value for a key.
func (c *InMemoryPresenceCache[K, V]) Set(_ context.Context, key K, value V) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = value
	return nil
}

// Fetch retrieves a value by its key.
func (c *InMemoryPresenceCache[K, V]) Fetch(_ context.Context, key K) (V, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, ok := c.data[key]
	if !ok {
		var zero V
		return zero, fmt.Errorf("key '%v' not found in presence cache", key)
	}
	return value, nil
}

// Delete removes a key.
func (c *InMemoryPresenceCache[K, V]) Delete(_ context.Context, key K) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, key)
	return nil
}

// Close is a no-op for the in-memory implementation.
func (c *InMemoryPresenceCache[K, V]) Close() error {
	return nil
}
