package messagepipeline

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
)

// SimplePublisher defines a generic, direct publisher interface.
// This is ideal for use cases like dead-lettering where batching is not required.
type SimplePublisher interface {
	Publish(ctx context.Context, payload []byte, attributes map[string]string) error
	// Stop flushes any pending messages and accepts a context for timeout control.
	Stop(ctx context.Context) error
}

// GoogleSimplePublisher implements a direct-to-Pub/Sub publisher.
type GoogleSimplePublisher struct {
	topic  *pubsub.Topic
	logger zerolog.Logger
}

// NewGoogleSimplePublisher creates a new simple, non-batching publisher.
// It accepts a context to verify that the target topic exists before returning.
func NewGoogleSimplePublisher(ctx context.Context, client *pubsub.Client, topicID string, logger zerolog.Logger) (SimplePublisher, error) {
	if client == nil {
		return nil, fmt.Errorf("pubsub client cannot be nil")
	}
	topic := client.Topic(topicID)

	// Verify the topic exists, respecting the context's deadline.
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check for topic %s: %w", topicID, err)
	}
	if !exists {
		return nil, fmt.Errorf("pubsub topic %s does not exist", topicID)
	}

	return &GoogleSimplePublisher{
		topic:  topic,
		logger: logger.With().Str("component", "GoogleSimplePublisher").Str("topic_id", topicID).Logger(),
	}, nil
}

// Publish sends a single message to Pub/Sub. It returns immediately after queueing
// the message and logs the final result of the publish operation asynchronously.
func (p *GoogleSimplePublisher) Publish(ctx context.Context, payload []byte, attributes map[string]string) error {
	result := p.topic.Publish(ctx, &pubsub.Message{
		Data:       payload,
		Attributes: attributes,
	})

	// Asynchronously check the result to log any publish errors without blocking the caller.
	go func() {
		// Use a new context for Get to avoid being cancelled by a short-lived publish context.
		getCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

// Stop flushes any pending messages for the topic, respecting the context's timeout.
func (p *GoogleSimplePublisher) Stop(ctx context.Context) error {
	if p.topic == nil {
		return nil
	}

	// topic.Stop() is blocking, so we wrap it to respect the context timeout.
	stopDone := make(chan struct{})
	go func() {
		p.topic.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		return nil // Graceful shutdown completed.
	case <-ctx.Done():
		return ctx.Err() // Shutdown timed out.
	}
}
