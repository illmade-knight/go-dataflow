package cache

import "context"

// Cache is a generic interface for a caching layer.
type Cache[K any, V any] interface {
	// FetchFromCache retrieves an item from the cache.
	FetchFromCache(ctx context.Context, key K) (V, error)
	// WriteToCache adds an item to the cache.
	WriteToCache(ctx context.Context, key K, value V) error
}
