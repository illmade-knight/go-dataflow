package cache

import (
	"context"
	"io"
)

// Fetcher defines the public, read-only contract for any component
// that can retrieve data by a key. This is the primary interface
// that consuming services should depend on.
type Fetcher[K comparable, V any] interface {
	// Fetch retrieves a value by its key. The implementation is responsible
	// for its own retrieval logic, which may include falling back to another
	// Fetcher.
	Fetch(ctx context.Context, key K) (V, error)
	io.Closer
}

// Cache read/write/invalidate interface
type Cache[K comparable, V any] interface {
	Fetcher[K, V] // Embeds Fetch and Close methods
	Invalidate(ctx context.Context, key K) error
	// We could also add a Write method for direct cache writes, at the moment we let the implementation decide
	// Write(ctx context.Context, key K, value V) error
}
