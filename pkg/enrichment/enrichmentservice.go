// enrichment/service.go
package enrichment

import (
	"context"
	"fmt"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
)

// KeyExtractor defines a function to get the enrichment key from a message.
type KeyExtractor[K any] func(msg types.ConsumedMessage) (K, bool)

// Enricher defines a function to apply fetched data to a message's enrichment map.
type Enricher[V any] func(msg *types.PublishMessage, data V)

// NewEnrichmentTransformer creates a generic message transformer for data enrichment.
// It uses a fetcher to get external data and an enricher to apply it to the message.
func NewEnrichmentTransformer[K comparable, V any](
	fetcher Fetcher[K, V],
	keyExtractor KeyExtractor[K],
	enricher Enricher[V],
	deadLetterPublisher messagepipeline.SimplePublisher,
	logger zerolog.Logger,
) messagepipeline.MessageTransformer[types.PublishMessage] {
	// REFACTOR: The returned function now accepts a context.
	return func(ctx context.Context, msg types.ConsumedMessage) (*types.PublishMessage, bool, error) {
		handleFailure := func(reason string, key K) {
			if deadLetterPublisher != nil {
				attributes := map[string]string{
					"error":          reason,
					"enrichment_key": fmt.Sprintf("%v", key),
					"msg_id":         msg.ID,
				}
				// REFACTOR: Use the provided context for the dead-letter publish call.
				if err := deadLetterPublisher.Publish(ctx, msg.Payload, attributes); err != nil {
					logger.Error().Err(err).Msg("Failed to publish to dead-letter topic.")
				}
			}
			msg.Ack() // Ack the message even on failure to prevent reprocessing loops.
		}

		key, ok := keyExtractor(msg)
		if !ok {
			handleFailure("key_extraction_failed", key)
			return nil, true, nil // Skip message
		}

		// REFACTOR: Use the provided context for the fetch call.
		data, err := fetcher(ctx, key)
		if err != nil {
			logger.Error().Err(err).Str("key", fmt.Sprintf("%v", key)).Msg("Failed to fetch enrichment data.")
			handleFailure("data_fetch_failed", key)
			return nil, true, nil // Skip message
		}

		enrichedMsg := &types.PublishMessage{
			ID:             msg.ID,
			Payload:        msg.Payload,
			PublishTime:    msg.PublishTime,
			EnrichmentData: make(map[string]interface{}), // Initialize the generic map
		}

		enricher(enrichedMsg, data)

		logger.Debug().Str("msg_id", msg.ID).Msg("Message enriched successfully.")
		return enrichedMsg, false, nil
	}
}
