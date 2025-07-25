package messagepipeline

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"

	"cloud.google.com/go/pubsub"
)

// SimplePublisher defines a generic, direct publisher interface.
// This is ideal for use cases like dead-lettering where batching is not required.
type SimplePublisher interface {
	Publish(ctx context.Context, payload []byte, attributes map[string]string) error
	Stop()
}

// GoogleSimplePublisher implements a direct-to-Pub/Sub publisher.
type GoogleSimplePublisher struct {
	topic  *pubsub.Topic
	logger zerolog.Logger
}

// NewGoogleSimplePublisher creates a new simple, non-batching publisher.
func NewGoogleSimplePublisher(client *pubsub.Client, topicID string, logger zerolog.Logger) (SimplePublisher, error) {
	if client == nil {
		return nil, fmt.Errorf("pubsub client cannot be nil")
	}
	topic := client.Topic(topicID)
	// In a real implementation, you might add an Exists() check here.
	return &GoogleSimplePublisher{
		topic:  topic,
		logger: logger.With().Str("component", "GoogleSimplePublisher").Str("topic_id", topicID).Logger(),
	}, nil
}

// Publish sends a single message to Pub/Sub and does not wait for the result.
func (p *GoogleSimplePublisher) Publish(publishContext context.Context, payload []byte, attributes map[string]string) error {
	result := p.topic.Publish(publishContext, &pubsub.Message{
		Data:       payload,
		Attributes: attributes,
	})

	// Asynchronously check the result to log any publish errors without blocking the caller.
	go func() {
		msgID, err := result.Get(publishContext)
		if err != nil {
			p.logger.Error().Err(err).Msg("Failed to publish dead-letter message")
			return
		}
		p.logger.Info().Str("dlt_msg_id", msgID).Msg("Message successfully sent to dead-letter topic.")
	}()

	return nil
}

// Stop flushes any pending messages for the topic.
func (p *GoogleSimplePublisher) Stop() {
	if p.topic != nil {
		p.topic.Stop()
	}
}
