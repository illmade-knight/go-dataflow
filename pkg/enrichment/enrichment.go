package enrichment

import (
	"context"
	"fmt"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
)

// --- Generic Enrichment Components ---

// KeyExtractor defines a function to get an enrichment key from a message.
type KeyExtractor[K comparable] func(msg *messagepipeline.Message) (K, bool)

// Applier defines a function to apply fetched data to a message.
type Applier[V any] func(msg *messagepipeline.Message, data V)

// NewEnricherFunc is a factory that creates and returns a `MessageEnricher` function.
// This function encapsulates the full enrichment logic, including handling fetch failures.
func NewEnricherFunc[K comparable, V any](
	fetcher Fetcher[K, V],
	keyEx KeyExtractor[K],
	applier Applier[V],
	logger zerolog.Logger,
) (messagepipeline.MessageEnricher, error) {
	if fetcher == nil || keyEx == nil || applier == nil {
		return nil, fmt.Errorf("fetcher, keyExtractor, and applier cannot be nil")
	}

	enrichLogger := logger.With().Str("component", "EnricherFunc").Logger()

	// This is the function that will be used as the enricher in the pipeline.
	return func(ctx context.Context, msg *messagepipeline.Message) (skip bool, err error) {
		key, ok := keyEx(msg)
		if !ok {
			enrichLogger.Debug().Str("msg_id", msg.ID).Msg("Key not found in message, skipping enrichment.")
			return false, nil // Continue pipeline without enrichment.
		}

		data, err := fetcher(ctx, key)
		if err != nil {
			// On fetch failure, we log the error and skip the message so it's not
			// processed further, but we don't return an error so the pipeline acks it.
			// This prevents a message with a bad key from causing a processing loop.
			enrichLogger.Error().Err(err).Str("msg_id", msg.ID).Msgf("Failed to fetch enrichment data for key '%v'", key)
			return true, nil
		}

		if msg.EnrichmentData == nil {
			msg.EnrichmentData = make(map[string]interface{})
		}
		applier(msg, data)
		enrichLogger.Debug().Str("msg_id", msg.ID).Msg("Message enriched successfully.")
		return false, nil
	}, nil
}
