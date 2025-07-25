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

// Enricher defines a function to apply the fetched data to a message.
type Enricher[V any] func(msg *types.PublishMessage, data V)

// NewEnrichmentTransformer creates a generic message transformer for data enrichment.
func NewEnrichmentTransformer[K comparable, V any](
	fetcher Fetcher[K, V],
	keyExtractor KeyExtractor[K],
	enricher Enricher[V],
	deadLetterPublisher messagepipeline.SimplePublisher,
	logger zerolog.Logger,
) messagepipeline.MessageTransformer[types.PublishMessage] {
	return func(msg types.ConsumedMessage) (*types.PublishMessage, bool, error) {
		handleFailure := func(reason string, key K) {
			if deadLetterPublisher != nil {
				// This could be improved to handle non-string keys
				attributes := map[string]string{
					"error":          reason,
					"enrichment_key": fmt.Sprintf("%v", key),
					"msg_id":         msg.ID,
				}
				_ = deadLetterPublisher.Publish(context.Background(), msg.Payload, attributes)
			}
			msg.Ack()
		}

		key, ok := keyExtractor(msg)
		if !ok {
			handleFailure("key_extraction_failed", key)
			return nil, true, nil // Skip message
		}

		// Fetch the enrichment data using the generic fetcher
		data, err := fetcher(context.Background(), key)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to fetch enrichment data.")
			handleFailure("data_fetch_failed", key)
			return nil, true, nil // Skip message
		}

		// Create the base message to be published
		enrichedMsg := &types.PublishMessage{
			ID:          msg.ID,
			Payload:     msg.Payload,
			PublishTime: msg.PublishTime,
			DeviceInfo:  msg.DeviceInfo, // Copy original info
		}

		// Apply the enrichment
		enricher(enrichedMsg, data)

		logger.Debug().Str("msg_id", msg.ID).Msg("Message enriched successfully.")
		return enrichedMsg, false, nil
	}
}
