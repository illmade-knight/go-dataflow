// Package cache provides generic caching components for data pipelines.
package cache

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FirestorePresenceCache is a production-ready implementation of PresenceCache using Firestore.
// It is suitable for smaller deployments where a dedicated Redis instance may be overkill.
type FirestorePresenceCache[K comparable, V any] struct {
	client     *firestore.Client
	collection string
}

// NewFirestorePresenceCache creates a new FirestorePresenceCache.
func NewFirestorePresenceCache[K comparable, V any](
	client *firestore.Client,
	collectionName string,
) (*FirestorePresenceCache[K, V], error) {
	if client == nil {
		return nil, errors.New("firestore client cannot be nil")
	}
	return &FirestorePresenceCache[K, V]{
		client:     client,
		collection: collectionName,
	}, nil
}

// Set creates or overwrites a document with the presence information.
func (c *FirestorePresenceCache[K, V]) Set(ctx context.Context, key K, value V) error {
	stringKey := fmt.Sprintf("%v", key)
	_, err := c.client.Collection(c.collection).Doc(stringKey).Set(ctx, value)
	if err != nil {
		return fmt.Errorf("failed to set presence in firestore for key %s: %w", stringKey, err)
	}
	return nil
}

// Fetch retrieves a document and maps it to the value type.
func (c *FirestorePresenceCache[K, V]) Fetch(ctx context.Context, key K) (V, error) {
	var zero V
	stringKey := fmt.Sprintf("%v", key)
	docSnap, err := c.client.Collection(c.collection).Doc(stringKey).Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return zero, fmt.Errorf("key '%v' not found in presence cache: %w", key, err)
		}
		return zero, fmt.Errorf("firestore get failed for key %s: %w", stringKey, err)
	}
	var value V
	if err := docSnap.DataTo(&value); err != nil {
		return zero, fmt.Errorf("failed to unmarshal presence data for key %s: %w", stringKey, err)
	}
	return value, nil
}

// Delete removes the document from Firestore.
func (c *FirestorePresenceCache[K, V]) Delete(ctx context.Context, key K) error {
	stringKey := fmt.Sprintf("%v", key)
	_, err := c.client.Collection(c.collection).Doc(stringKey).Delete(ctx)
	if err != nil {
		// It's often acceptable to ignore "not found" errors on delete.
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("firestore delete failed for key %s: %w", stringKey, err)
	}
	return nil
}

// Close is a no-op as the Firestore client's lifecycle is managed externally.
func (c *FirestorePresenceCache[K, V]) Close() error {
	return nil
}
