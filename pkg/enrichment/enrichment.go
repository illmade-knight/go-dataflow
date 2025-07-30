package enrichment

import (
	"context"
	"fmt"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
)

// --- Generic Enrichment Client ---

// KeyExtractor defines a function to get an enrichment key from a message.
type KeyExtractor[K comparable] func(msg *messagepipeline.Message) (K, bool)

// EnrichmentApplier defines a function to apply fetched data to a message's enrichment map.
type EnrichmentApplier[V any] func(msg *messagepipeline.Message, data V)

// Enricher is a client that modifies a message in-place with external data.
type Enricher[K comparable, V any] struct {
	fetcher Fetcher[K, V]
	keyEx   KeyExtractor[K]
	applier EnrichmentApplier[V]
	logger  zerolog.Logger
}

// NewEnricher creates a new enrichment client.
func NewEnricher[K comparable, V any](
	fetcher Fetcher[K, V],
	keyEx KeyExtractor[K],
	applier EnrichmentApplier[V],
	logger zerolog.Logger,
) (*Enricher[K, V], error) {
	if fetcher == nil || keyEx == nil || applier == nil {
		return nil, fmt.Errorf("fetcher, keyExtractor, and applier cannot be nil")
	}
	return &Enricher[K, V]{
		fetcher: fetcher,
		keyEx:   keyEx,
		applier: applier,
		logger:  logger.With().Str("component", "Enricher").Logger(),
	}, nil
}

// Enrich fetches data based on a key from the message and applies it to the
// message's EnrichmentData map. It modifies the message pointer directly.
func (e *Enricher[K, V]) Enrich(ctx context.Context, msg *messagepipeline.Message) error {
	key, ok := e.keyEx(msg)
	if !ok {
		e.logger.Debug().Str("msg_id", msg.ID).Msg("Key not found in message, skipping enrichment.")
		return nil // Not an error, just nothing to do.
	}

	data, err := e.fetcher(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to fetch enrichment data for key '%v': %w", key, err)
	}

	if msg.EnrichmentData == nil {
		msg.EnrichmentData = make(map[string]interface{})
	}
	e.applier(msg, data)
	e.logger.Debug().Str("msg_id", msg.ID).Msg("Message enriched successfully.")
	return nil
}
