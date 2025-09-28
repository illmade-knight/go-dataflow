package messagepipeline

import (
	"context"
	"errors"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/rs/zerolog"
)

type ReceiveSettings struct {
	MaxOutstandingMessages int
	NumGoroutines          int
	MinAckDeadline         time.Duration
}

// GooglePubsubConsumerConfig holds all configuration for the GooglePubsubConsumer.
// REFACTOR: Removed SubscriptionExistsTimeout as the existence check is removed.
type GooglePubsubConsumerConfig struct {
	SubscriptionID string
	pubsub.ReceiveSettings
}

// NewGooglePubsubConsumerDefaults provides a config with sensible defaults.
func NewGooglePubsubConsumerDefaults(subscriptionID string) *GooglePubsubConsumerConfig {
	return &GooglePubsubConsumerConfig{
		SubscriptionID: subscriptionID,
		ReceiveSettings: pubsub.ReceiveSettings{
			MaxOutstandingMessages: 100,
			NumGoroutines:          5,
			// Set a default MinAckDeadline to prevent very short, aggressive retries.
			MinDurationPerAckExtension: 10 * time.Second,
		},
	}
}

// GooglePubsubConsumer is a message pipeline consumer that receives messages
// from a Google Cloud Pub/Sub subscription. It implements the MessageConsumer interface.
type GooglePubsubConsumer struct {
	subscriber         *pubsub.Subscriber
	logger             zerolog.Logger
	outputChan         chan Message
	stopOnce           sync.Once
	cancelSubscription context.CancelFunc
	doneChan           chan struct{}
}

// NewGooglePubsubConsumer creates a new consumer for a Google Cloud Pub/Sub subscription.
// REFACTOR: Removed the subscription existence check to align with v2 best practices.
// The consumer will fail at runtime via the Receive call if the subscription does not exist.
func NewGooglePubsubConsumer(
	cfg *GooglePubsubConsumerConfig,
	client *pubsub.Client,
	logger zerolog.Logger,
) (*GooglePubsubConsumer, error) {
	if client == nil {
		return nil, errors.New("pubsub client cannot be nil")
	}

	subscriber := client.Subscriber(cfg.SubscriptionID)
	subscriber.ReceiveSettings.MaxOutstandingMessages = cfg.MaxOutstandingMessages
	subscriber.ReceiveSettings.NumGoroutines = cfg.NumGoroutines

	consumer := &GooglePubsubConsumer{
		subscriber: subscriber,
		logger:     logger.With().Str("component", "GooglePubsubConsumer").Str("subscription_id", cfg.SubscriptionID).Logger(),
		outputChan: make(chan Message, cfg.MaxOutstandingMessages),
		doneChan:   make(chan struct{}),
	}
	return consumer, nil
}

// Messages returns the read-only channel where consumed messages are sent.
func (c *GooglePubsubConsumer) Messages() <-chan Message {
	return c.outputChan
}

// Start begins the message consumption process.
func (c *GooglePubsubConsumer) Start(ctx context.Context) error {
	c.logger.Info().Msg("Starting Pub/Sub message consumption...")
	receiveCtx, cancel := context.WithCancel(ctx)
	c.cancelSubscription = cancel

	go func() {
		defer close(c.outputChan)
		defer close(c.doneChan)

		c.logger.Info().Msg("Pub/Sub Receive goroutine started.")
		err := c.subscriber.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
			payloadCopy := make([]byte, len(msg.Data))
			copy(payloadCopy, msg.Data)

			consumedMsg := Message{
				MessageData: MessageData{
					ID:          msg.ID,
					Payload:     payloadCopy,
					PublishTime: msg.PublishTime,
				},
				Attributes: msg.Attributes,
				Ack:        msg.Ack,
				Nack:       msg.Nack,
			}

			select {
			case c.outputChan <- consumedMsg:
				// Message successfully sent to the processing pipeline.
			case <-receiveCtx.Done():
				// The consumer is stopping; Nack the message so it can be redelivered later.
				msg.Nack()
				c.logger.Warn().Str("msg_id", msg.ID).Msg("Consumer stopping, Nacking message.")
			}
		})

		if err != nil && !errors.Is(err, context.Canceled) {
			c.logger.Error().Err(err).Msg("Pub/Sub Receive call exited with an unexpected error.")
		}
		c.logger.Info().Msg("Pub/Sub Receive goroutine stopped.")
	}()
	return nil
}

// Stop gracefully shuts down the consumer.
func (c *GooglePubsubConsumer) Stop(ctx context.Context) error {
	c.stopOnce.Do(func() {
		c.logger.Info().Msg("Stopping Pub/Sub consumer...")
		if c.cancelSubscription != nil {
			c.cancelSubscription()
		}
	})

	select {
	case <-c.doneChan:
		c.logger.Info().Msg("Pub/Sub consumer confirmed stopped.")
		return nil
	case <-ctx.Done():
		c.logger.Error().Err(ctx.Err()).Msg("Timeout waiting for Pub/Sub consumer to stop gracefully.")
		return ctx.Err()
	}
}

// Done returns a channel that is closed when the consumer has fully stopped.
func (c *GooglePubsubConsumer) Done() <-chan struct{} {
	return c.doneChan
}
