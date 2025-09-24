package cache

import (
	"context"
	"io"
)

// PresenceCache defines the contract for managing ephemeral, real-time state,
// such as a user's online status. It requires explicit Set and Delete operations,
// as this type of data has no persistent source of truth to fall back on.
type PresenceCache[K comparable, V any] interface {
	// Set explicitly stores a value for a key.
	Set(ctx context.Context, key K, value V) error
	// Fetch retrieves a value by its key.
	Fetch(ctx context.Context, key K) (V, error)
	// Delete explicitly removes a key.
	Delete(ctx context.Context, key K) error
	// Closer is included for implementations that manage network connections.
	io.Closer
}
