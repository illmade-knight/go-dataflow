package messagepipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/rs/zerolog"
)

// GooglePubsubProducerConfig holds configuration for the Google Pub/Sub producer.
type GooglePubsubProducerConfig struct {
	TopicID string
	// BatchDelay is the maximum time the client will wait before sending a batch.
	BatchDelay time.Duration
	// BatchSize is the maximum number of messages in a batch.
	BatchSize int
	// PublishGoroutines is the number of goroutines used for publishing messages.
	PublishGoroutines int
}

// NewGooglePubsubProducerDefaults provides a config with sensible defaults.
func NewGooglePubsubProducerDefaults(topicID string) *GooglePubsubProducerConfig {
	return &GooglePubsubProducerConfig{
		TopicID:           topicID,
		BatchDelay:        100 * time.Millisecond,
		BatchSize:         100,
		PublishGoroutines: 5,
	}
}

// GooglePubsubProducer is a client for publishing messages to a
// Google Cloud Pub/Sub topic. It wraps the underlying client, providing a
// simple Publish method and handling topic validation and batching configuration.
type GooglePubsubProducer struct {
	publisher *pubsub.Publisher
	logger    zerolog.Logger
}

// NewGooglePubsubProducer creates a new GooglePubsubProducer.
// REFACTOR: Removed the topic existence check to adhere to the principle of least
// privilege. The producer should only need 'publish' permissions, not 'get'.
// A failure to publish to a non-existent topic will be caught at runtime.
func NewGooglePubsubProducer(
	cfg *GooglePubsubProducerConfig,
	client *pubsub.Client,
	logger zerolog.Logger,
) (*GooglePubsubProducer, error) {
	if client == nil {
		return nil, fmt.Errorf("pubsub client cannot be nil")
	}

	publisher := client.Publisher(cfg.TopicID)
	publisher.PublishSettings.DelayThreshold = cfg.BatchDelay
	publisher.PublishSettings.CountThreshold = cfg.BatchSize
	publisher.PublishSettings.NumGoroutines = cfg.PublishGoroutines

	logger.Info().Str("topic_id", cfg.TopicID).Msg("GooglePubsubProducer initialized successfully.")
	producer := &GooglePubsubProducer{
		publisher: publisher,
		logger:    logger.With().Str("component", "GooglePubsubProducer").Str("topic_id", cfg.TopicID).Logger(),
	}
	return producer, nil
}

// Publish marshals the provided MessageData and sends it to the Pub/Sub topic.
func (p *GooglePubsubProducer) Publish(ctx context.Context, data MessageData) (string, error) {
	var err error
	var payload []byte
	payload, err = json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal message data for publishing: %w", err)
	}

	res := p.publisher.Publish(ctx, &pubsub.Message{
		Data: payload,
	})

	var msgID string
	msgID, err = res.Get(ctx)
	if err != nil {
		p.logger.Error().Err(err).Str("original_msg_id", data.ID).Msg("Failed to get publish result.")
		return "", fmt.Errorf("failed to publish message ID %s: %w", data.ID, err)
	}

	p.logger.Debug().Str("original_msg_id", data.ID).Str("pubsub_msg_id", msgID).Msg("Message published successfully.")
	return msgID, nil
}

// Stop flushes any buffered messages.
func (p *GooglePubsubProducer) Stop(ctx context.Context) error {
	if p.publisher == nil {
		return nil
	}

	p.logger.Info().Msg("Flushing messages and stopping Pub/Sub publisher...")
	stopDone := make(chan struct{})
	go func() {
		p.publisher.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		p.logger.Info().Msg("Pub/Sub publisher stopped.")
		return nil
	case <-ctx.Done():
		p.logger.Error().Err(ctx.Err()).Msg("Timeout waiting for Pub/Sub publisher to stop gracefully.")
		return ctx.Err()
	}
}
