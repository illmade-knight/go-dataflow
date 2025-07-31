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
	// TopicExistsTimeout is the timeout for the initial check to see if the topic exists.
	TopicExistsTimeout time.Duration
	// BatchDelay is the maximum time the client will wait before sending a batch.
	BatchDelay time.Duration
	// BatchSize is the maximum number of messages in a batch.
	BatchSize int
	// PublishGoroutines is the number of goroutines used for publishing messages.
	PublishGoroutines int
}

// NewGooglePubsubProducerDefaults provides a config with sensible defaults.
func NewGooglePubsubProducerDefaults() *GooglePubsubProducerConfig {
	return &GooglePubsubProducerConfig{
		TopicExistsTimeout: 20 * time.Second,
		BatchDelay:         100 * time.Millisecond,
		BatchSize:          100,
		PublishGoroutines:  5,
	}
}

// GooglePubsubProducer is a client for publishing messages to a
// Google Cloud Pub/Sub topic. It wraps the underlying client, providing a
// simple Publish method and handling topic validation and batching configuration.
type GooglePubsubProducer struct {
	topic  *pubsub.Topic
	logger zerolog.Logger
}

// NewGooglePubsubProducer creates a new GooglePubsubProducer. It validates the
// topic's existence before returning a functional producer. The provided context
// is used for the initial topic existence check.
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
// It returns the server-assigned message ID upon successful receipt by the server.
//
// This function blocks until the publish result is received from the Pub/Sub server
// or the provided context is cancelled.
//
// To prevent message processing issues, it is the caller's responsibility to ensure
// that the `data.Payload` field contains the raw message payload, not a
// pre-serialized `MessageData` object, which would lead to double-wrapping.
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
// It accepts a context to enforce a timeout on the shutdown process.
// This should be called during a graceful shutdown of the application.
func (p *GooglePubsubProducer) Stop(ctx context.Context) error {
	if p.topic == nil {
		return nil
	}

	p.logger.Info().Msg("Flushing messages and stopping Pub/Sub topic...")
	// Wrap topic.Stop() to respect the context timeout.
	stopDone := make(chan struct{})
	go func() {
		p.topic.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		p.logger.Info().Msg("Pub/Sub topic stopped.")
		return nil // Graceful shutdown completed.
	case <-ctx.Done():
		p.logger.Error().Err(ctx.Err()).Msg("Timeout waiting for Pub/Sub topic to stop gracefully.")
		return ctx.Err() // Shutdown timed out.
	}
}
