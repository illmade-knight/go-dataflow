package messagepipeline

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
)

// GoogleSimplePublisherConfig holds configuration for the GoogleSimplePublisher.
type GoogleSimplePublisherConfig struct {
	TopicID string
	// TopicExistsTimeout is the timeout for the initial check to see if the topic exists.
	TopicExistsTimeout time.Duration
	// PublishResultTimeout is the timeout for asynchronously waiting for a publish result
	// in the background. This is detached from the caller's context.
	PublishResultTimeout time.Duration
}

// NewGoogleSimplePublisherDefaults provides a config with sensible defaults for a given topic ID.
func NewGoogleSimplePublisherDefaults(topicID string) *GoogleSimplePublisherConfig {
	return &GoogleSimplePublisherConfig{
		TopicID:              topicID,
		TopicExistsTimeout:   20 * time.Second,
		PublishResultTimeout: 30 * time.Second,
	}
}

// SimplePublisher defines a generic, direct publisher interface.
// This is ideal for use cases like dead-lettering where batching is not required
// and a "fire-and-forget" publishing pattern is acceptable.
type SimplePublisher interface {
	Publish(ctx context.Context, payload []byte, attributes map[string]string) error
	// Stop flushes any pending messages and accepts a context for timeout control.
	Stop(ctx context.Context) error
}

// GoogleSimplePublisher implements a direct-to-Pub/Sub publisher. It is designed
// for scenarios where messages should be sent quickly without blocking the caller
// to wait for the server acknowledgment.
type GoogleSimplePublisher struct {
	topic  *pubsub.Topic
	logger zerolog.Logger
	cfg    *GoogleSimplePublisherConfig
}

// NewGoogleSimplePublisher creates a new simple, non-batching publisher.
// The provided context is used to verify that the target topic exists before returning.
func NewGoogleSimplePublisher(
	ctx context.Context,
	cfg *GoogleSimplePublisherConfig,
	client *pubsub.Client,
	logger zerolog.Logger,
) (SimplePublisher, error) {
	if client == nil {
		return nil, fmt.Errorf("pubsub client cannot be nil")
	}
	topic := client.Topic(cfg.TopicID)

	// Verify the topic exists, respecting the context's deadline.
	existsCtx, cancel := context.WithTimeout(ctx, cfg.TopicExistsTimeout)
	defer cancel()
	exists, err := topic.Exists(existsCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to check for topic %s: %w", cfg.TopicID, err)
	}
	if !exists {
		return nil, fmt.Errorf("pubsub topic %s does not exist", cfg.TopicID)
	}

	return &GoogleSimplePublisher{
		topic:  topic,
		cfg:    cfg,
		logger: logger.With().Str("component", "GoogleSimplePublisher").Str("topic_id", cfg.TopicID).Logger(),
	}, nil
}

// Publish sends a single message to Pub/Sub. It returns immediately after queueing
// the message. The final result of the publish operation (success or failure) is
// handled asynchronously in a background goroutine, where it is logged.
func (p *GoogleSimplePublisher) Publish(ctx context.Context, payload []byte, attributes map[string]string) error {
	result := p.topic.Publish(ctx, &pubsub.Message{
		Data:       payload,
		Attributes: attributes,
	})

	// Asynchronously check the result to log any publish errors without blocking the caller.
	go func() {
		// Use a new context for Get to avoid being cancelled by a short-lived publish context.
		getCtx, cancel := context.WithTimeout(context.Background(), p.cfg.PublishResultTimeout)
		defer cancel()

		msgID, err := result.Get(getCtx)
		if err != nil {
			p.logger.Error().Err(err).Msg("Failed to publish message")
			return
		}
		p.logger.Info().Str("published_msg_id", msgID).Msg("Message sent successfully.")
	}()

	return nil
}

// Stop flushes any pending messages for the topic.
// It respects the provided context for timeout/cancellation.
func (p *GoogleSimplePublisher) Stop(ctx context.Context) error {
	if p.topic == nil {
		return nil
	}

	// topic.Stop() is blocking, so we wrap it to respect the context timeout.
	stopDone := make(chan struct{})
	go func() {
		p.logger.Info().Msg("Flushing messages and stopping simple publisher topic...")
		p.topic.Stop()
		p.logger.Info().Msg("Simple publisher topic stopped.")
		close(stopDone)
	}()

	select {
	case <-stopDone:
		return nil // Graceful shutdown completed.
	case <-ctx.Done():
		p.logger.Error().Err(ctx.Err()).Msg("Timeout waiting for simple publisher to stop gracefully.")
		return ctx.Err() // Shutdown timed out.
	}
}
