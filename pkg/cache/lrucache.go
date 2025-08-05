package cache

import (
	"container/list"
	"context"
	"fmt"
	"sync"
)

// lruCacheItem is the internal structure stored in the linked list.
type lruCacheItem[K comparable, V any] struct {
	key   K
	value V
}

// InMemoryLRUCache is a generic, thread-safe, in-memory cache with a fixed size
// and a Least Recently Used (LRU) eviction policy.
// It implements the Fetcher interface and can be configured with a fallback Fetcher
// to use on a cache miss.
type InMemoryLRUCache[K comparable, V any] struct {
	maxSize  int
	fallback Fetcher[K, V]

	mu    sync.Mutex
	ll    *list.List          // Used to track the order of items (recency).
	cache map[K]*list.Element // Used for fast key lookups.
}

// NewInMemoryLRUCache creates a new size-limited, in-memory LRU cache.
// - maxSize: The maximum number of items to store in the cache. Must be > 0.
// - fallback: An optional Fetcher to use to populate the cache on a miss.
func NewInMemoryLRUCache[K comparable, V any](maxSize int, fallback Fetcher[K, V]) (*InMemoryLRUCache[K, V], error) {
	if maxSize <= 0 {
		return nil, fmt.Errorf("maxSize must be greater than 0")
	}
	return &InMemoryLRUCache[K, V]{
		maxSize:  maxSize,
		fallback: fallback,
		ll:       list.New(),
		cache:    make(map[K]*list.Element),
	}, nil
}

// Fetch retrieves an item. It first checks its internal in-memory map.
// If the key is found (a cache hit), it moves the item to the front of the recency list.
// If the key is not found (a cache miss), and a fallback is configured, it fetches
// the data from the fallback, adds it to the cache, and potentially evicts the
// least recently used item if the cache is full.
func (c *InMemoryLRUCache[K, V]) Fetch(ctx context.Context, key K) (V, error) {
	c.mu.Lock()
	if elem, ok := c.cache[key]; ok {
		// Cache hit: Move the accessed item to the front of the list.
		c.ll.MoveToFront(elem)
		c.mu.Unlock()
		return elem.Value.(*lruCacheItem[K, V]).value, nil
	}
	c.mu.Unlock()

	// Cache miss
	var zero V
	if c.fallback == nil {
		return zero, fmt.Errorf("key '%v' not found in LRU cache and no fallback is configured", key)
	}

	// Fallback to the source.
	sourceValue, err := c.fallback.Fetch(ctx, key)
	if err != nil {
		return zero, err // Return the error from the source.
	}

	// Write the newly fetched value back to the cache.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check if another goroutine populated the cache while we were fetching.
	if elem, ok := c.cache[key]; ok {
		c.ll.MoveToFront(elem)
		return elem.Value.(*lruCacheItem[K, V]).value, nil
	}

	// Add the new item to the front of the list and to the map.
	newItem := &lruCacheItem[K, V]{key: key, value: sourceValue}
	element := c.ll.PushFront(newItem)
	c.cache[key] = element

	// If the cache is over capacity, evict the least recently used item.
	if c.ll.Len() > c.maxSize {
		c.evict()
	}

	return sourceValue, nil
}

func (c *InMemoryLRUCache[K, V]) Invalidate(_ context.Context, key K) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.cache[key]; ok {
		c.ll.Remove(elem)
		delete(c.cache, key)
	}
	return nil
}

// evict removes the least recently used item from the cache.
// This method is unexported and must be called within a locked mutex.
func (c *InMemoryLRUCache[K, V]) evict() {
	elementToRemove := c.ll.Back()
	if elementToRemove != nil {
		itemToRemove := c.ll.Remove(elementToRemove).(*lruCacheItem[K, V])
		delete(c.cache, itemToRemove.key)
	}
}

// Close is a no-op for the in-memory cache but satisfies the Fetcher interface.
func (c *InMemoryLRUCache[K, V]) Close() error {
	return nil
}
