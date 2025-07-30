package messagepipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
)

// GooglePubsubProducerConfig holds configuration for the Google Pub/Sub producer.
type GooglePubsubProducerConfig struct {
	TopicID string
	// Timeout for the initial check to see if the topic exists.
	TopicExistsTimeout time.Duration
	// Batching settings for the underlying Google Pub/Sub client.
	BatchDelay        time.Duration
	BatchSize         int
	PublishGoroutines int
}

// NewGooglePubsubProducerDefaults provides a config with sensible defaults.
func NewGooglePubsubProducerDefaults(projectID, topicID string) *GooglePubsubProducerConfig {
	return &GooglePubsubProducerConfig{
		TopicID:            topicID,
		TopicExistsTimeout: 20 * time.Second,
		BatchDelay:         100 * time.Millisecond,
		BatchSize:          100,
		PublishGoroutines:  5,
	}
}

// GooglePubsubProducer is a lightweight client for publishing messages to a
// Google Cloud Pub/Sub topic. It wraps the underlying client, providing a
// simple Publish method.
type GooglePubsubProducer struct {
	topic  *pubsub.Topic
	logger zerolog.Logger
}

// NewGooglePubsubProducer creates a new GooglePubsubProducer. It validates the
// topic's existence before returning a functional producer.
func NewGooglePubsubProducer(
	ctx context.Context,
	cfg *GooglePubsubProducerConfig,
	client *pubsub.Client,
	logger zerolog.Logger,
) (*GooglePubsubProducer, error) {
	if client == nil {
		return nil, fmt.Errorf("pubsub client cannot be nil")
	}

	topic := client.Topic(cfg.TopicID)

	// Check for topic existence using a derived context with a timeout.
	existsCtx, cancel := context.WithTimeout(ctx, cfg.TopicExistsTimeout)
	defer cancel()
	exists, err := topic.Exists(existsCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to check for topic %s: %w", cfg.TopicID, err)
	}
	if !exists {
		return nil, fmt.Errorf("pubsub topic %s does not exist", cfg.TopicID)
	}

	// Configure the Pub/Sub client's built-in batching.
	topic.PublishSettings.DelayThreshold = cfg.BatchDelay
	topic.PublishSettings.CountThreshold = cfg.BatchSize
	topic.PublishSettings.NumGoroutines = cfg.PublishGoroutines

	logger.Info().Str("topic_id", cfg.TopicID).Msg("GooglePubsubProducer initialized successfully.")
	return &GooglePubsubProducer{
		topic:  topic,
		logger: logger.With().Str("component", "GooglePubsubProducer").Str("topic_id", cfg.TopicID).Logger(),
	}, nil
}

// Publish marshals the provided MessageData and sends it to the Pub/Sub topic.
// It returns the server-assigned message ID and an error if the publish fails.
// This function is non-blocking; the underlying client handles publishing in the background.
func (p *GooglePubsubProducer) Publish(ctx context.Context, data MessageData) (string, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal message data for publishing: %w", err)
	}

	res := p.topic.Publish(ctx, &pubsub.Message{
		Data: payload,
		// Note: The original message attributes are not passed through by default,
		// as the enriched MessageData is the canonical source of truth at this stage.
	})

	// Get blocks until the message is acknowledged by the server or the context is done.
	msgID, err := res.Get(ctx)
	if err != nil {
		p.logger.Error().Err(err).Str("original_msg_id", data.ID).Msg("Failed to get publish result.")
		return "", fmt.Errorf("failed to publish message ID %s: %w", data.ID, err)
	}

	p.logger.Debug().Str("original_msg_id", data.ID).Str("pubsub_msg_id", msgID).Msg("Message published successfully.")
	return msgID, nil
}

// Stop flushes any buffered messages and stops the underlying topic client.
// This should be called during a graceful shutdown of the application.
func (p *GooglePubsubProducer) Stop() {
	if p.topic != nil {
		p.logger.Info().Msg("Flushing messages and stopping Pub/Sub topic...")
		p.topic.Stop()
		p.logger.Info().Msg("Pub/Sub topic stopped.")
	}
}
