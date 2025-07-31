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
