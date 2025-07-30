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

// FirestoreSource is a generic data fetcher for a specific Firestore collection.
// It acts as a "source of truth" that a cache can pull from.
type FirestoreSource[K comparable, V any] struct {
	client         *firestore.Client
	collectionName string
	logger         zerolog.Logger
}

// NewFirestoreSource creates a new generic FirestoreSource.
// REFACTOR: Reordered parameters for consistency (config, dependencies, logger).
func NewFirestoreSource[K comparable, V any](
	cfg *FirestoreConfig,
	client *firestore.Client,
	logger zerolog.Logger,
) (*FirestoreSource[K, V], error) {
	if client == nil {
		return nil, fmt.Errorf("firestore client cannot be nil")
	}

	logger.Info().Str("project_id", cfg.ProjectID).Str("collection", cfg.CollectionName).Msg("FirestoreSource initialized.")

	return &FirestoreSource[K, V]{
		client:         client,
		collectionName: cfg.CollectionName,
		logger:         logger.With().Str("component", "FirestoreSource").Logger(),
	}, nil
}

// Fetch retrieves a single document from Firestore by its key.
func (s *FirestoreSource[K, V]) Fetch(ctx context.Context, key K) (V, error) {
	var zero V
	stringKey := fmt.Sprintf("%v", key)
	docRef := s.client.Collection(s.collectionName).Doc(stringKey)
	docSnap, err := docRef.Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			s.logger.Warn().Str("key", stringKey).Msg("Document not found in Firestore.")
			return zero, fmt.Errorf("document not found: %w", err)
		}
		s.logger.Error().Err(err).Str("key", stringKey).Msg("Failed to get document from Firestore.")
		return zero, fmt.Errorf("firestore get for %s: %w", stringKey, err)
	}

	var value V
	if err := docSnap.DataTo(&value); err != nil {
		s.logger.Error().Err(err).Str("key", stringKey).Msg("Failed to map Firestore document data.")
		return zero, fmt.Errorf("firestore DataTo for %s: %w", stringKey, err)
	}

	s.logger.Debug().Str("key", stringKey).Msg("Successfully fetched data from Firestore.")
	return value, nil
}

// Close is a no-op as the Firestore client's lifecycle is managed externally.
func (s *FirestoreSource[K, V]) Close() error {
	s.logger.Info().Msg("FirestoreSource does not close the injected Firestore client.")
	return nil
}

// ALLOW FIRESTORE TO BE USED IN LOW VOLUME DEPLOYMENTS
// don't use it like this in high volume deployments - that's what redis is for.

// FetchFromCache satisfies the Cache interface by calling the existing Fetch method.
func (s *FirestoreSource[K, V]) FetchFromCache(ctx context.Context, key K) (V, error) {
	return s.Fetch(ctx, key)
}

// WriteToCache satisfies the Cache interface by writing the document to Firestore.
func (s *FirestoreSource[K, V]) WriteToCache(ctx context.Context, key K, value V) error {
	stringKey := fmt.Sprintf("%v", key)
	_, err := s.client.Collection(s.collectionName).Doc(stringKey).Set(ctx, value)
	if err != nil {
		s.logger.Error().Err(err).Str("key", stringKey).Msg("Failed to write document to Firestore.")
		return fmt.Errorf("firestore set for %s: %w", stringKey, err)
	}
	s.logger.Debug().Str("key", stringKey).Msg("Successfully wrote data to Firestore.")
	return nil
}
