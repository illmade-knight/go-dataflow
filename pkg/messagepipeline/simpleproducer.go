package messagepipeline

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/rs/zerolog"
)

// GoogleSimplePublisherConfig holds configuration for the GoogleSimplePublisher.
// REFACTOR: Removed TopicExistsTimeout as the check is no longer performed.
type GoogleSimplePublisherConfig struct {
	TopicID string
	// PublishResultTimeout is the timeout for asynchronously waiting for a publish result
	// in the background. This is detached from the caller's context.
	PublishResultTimeout time.Duration
}

// NewGoogleSimplePublisherDefaults provides a config with sensible defaults for a given topic ID.
func NewGoogleSimplePublisherDefaults(topicID string) *GoogleSimplePublisherConfig {
	return &GoogleSimplePublisherConfig{
		TopicID:              topicID,
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
// REFACTOR: This struct now holds a v2 Publisher.
type GoogleSimplePublisher struct {
	publisher *pubsub.Publisher
	logger    zerolog.Logger
	cfg       *GoogleSimplePublisherConfig
}

// NewGoogleSimplePublisher creates a new simple, non-batching publisher.
// REFACTOR: Updated to use the v2 pubsub.Publisher and removed the existence check.
func NewGoogleSimplePublisher(
	cfg *GoogleSimplePublisherConfig,
	client *pubsub.Client,
	logger zerolog.Logger,
) (SimplePublisher, error) {
	if client == nil {
		return nil, errors.New("pubsub client cannot be nil")
	}
	publisher := client.Publisher(cfg.TopicID)

	return &GoogleSimplePublisher{
		publisher: publisher,
		cfg:       cfg,
		logger:    logger.With().Str("component", "GoogleSimplePublisher").Str("topic_id", cfg.TopicID).Logger(),
	}, nil
}

// Publish sends a single message to Pub/Sub. It returns immediately after queueing
// the message. The final result of the publish operation (success or failure) is
// handled asynchronously in a background goroutine, where it is logged.
// REFACTOR: Updated to use the v2 publisher.
func (p *GoogleSimplePublisher) Publish(ctx context.Context, payload []byte, attributes map[string]string) error {
	result := p.publisher.Publish(ctx, &pubsub.Message{
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
// REFACTOR: Updated to call Stop on the v2 publisher.
func (p *GoogleSimplePublisher) Stop(ctx context.Context) error {
	if p.publisher == nil {
		return nil
	}

	// publisher.Stop() is blocking, so we wrap it to respect the context timeout.
	stopDone := make(chan struct{})
	go func() {
		p.logger.Info().Msg("Flushing messages and stopping simple publisher topic...")
		p.publisher.Stop()
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
