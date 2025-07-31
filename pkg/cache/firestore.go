package cache

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FirestoreConfig holds configuration for the Firestore client.
type FirestoreConfig struct {
	ProjectID      string
	CollectionName string
}

// Firestore is a generic data fetcher for a specific Firestore collection.
// It implements the Fetcher interface and acts as a "source of truth"
// that a caching Fetcher can use as a fallback.
type Firestore[K comparable, V any] struct {
	client         *firestore.Client
	collectionName string
	logger         zerolog.Logger
}

// NewFirestore creates a new generic Firestore Fetcher.
func NewFirestore[K comparable, V any](
	_ context.Context, // Included for API consistency, though not used in this constructor.
	cfg *FirestoreConfig,
	client *firestore.Client,
	logger zerolog.Logger,
) (*Firestore[K, V], error) {
	if client == nil {
		return nil, fmt.Errorf("firestore client cannot be nil")
	}

	logger.Info().Str("project_id", cfg.ProjectID).Str("collection", cfg.CollectionName).Msg("Firestore Fetcher initialized.")

	return &Firestore[K, V]{
		client:         client,
		collectionName: cfg.CollectionName,
		logger:         logger.With().Str("component", "FirestoreFetcher").Logger(),
	}, nil
}

// Fetch retrieves a single document from Firestore by its key.
func (f *Firestore[K, V]) Fetch(ctx context.Context, key K) (V, error) {
	var zero V
	stringKey := fmt.Sprintf("%v", key)
	docRef := f.client.Collection(f.collectionName).Doc(stringKey)
	docSnap, err := docRef.Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			f.logger.Warn().Str("key", stringKey).Msg("Document not found in Firestore.")
			return zero, fmt.Errorf("document not found: %w", err)
		}
		f.logger.Error().Err(err).Str("key", stringKey).Msg("Failed to get document from Firestore.")
		return zero, fmt.Errorf("firestore get for %s: %w", stringKey, err)
	}

	var value V
	if err := docSnap.DataTo(&value); err != nil {
		f.logger.Error().Err(err).Str("key", stringKey).Msg("Failed to map Firestore document data.")
		return zero, fmt.Errorf("firestore DataTo for %s: %w", stringKey, err)
	}

	f.logger.Debug().Str("key", stringKey).Msg("Successfully fetched data from Firestore.")
	return value, nil
}

// Close is a no-op as the Firestore client's lifecycle is managed externally by the
// service that creates and injects it. This satisfies the io.Closer part of the Fetcher interface.
func (f *Firestore[K, V]) Close() error {
	f.logger.Info().Msg("Firestore Fetcher does not close the injected Firestore client.")
	return nil
}
