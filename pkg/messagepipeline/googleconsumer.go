package messagepipeline

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"sync"
	"time"
)

// GooglePubsubConsumerConfig holds all configuration for the GooglePubsubConsumer.
type GooglePubsubConsumerConfig struct {
	SubscriptionID            string
	MaxOutstandingMessages    int
	NumGoroutines             int
	SubscriptionExistsTimeout time.Duration
}

// NewGooglePubsubConsumerDefaults provides a config with sensible defaults.
func NewGooglePubsubConsumerDefaults() *GooglePubsubConsumerConfig {
	return &GooglePubsubConsumerConfig{
		MaxOutstandingMessages:    100,
		NumGoroutines:             5,
		SubscriptionExistsTimeout: 20 * time.Second,
	}
}

// GooglePubsubConsumer is a message pipeline consumer that receives messages
// from a Google Cloud Pub/Sub subscription. It implements the MessageConsumer interface.
type GooglePubsubConsumer struct {
	client             *pubsub.Client
	subscription       *pubsub.Subscription
	logger             zerolog.Logger
	outputChan         chan Message
	stopOnce           sync.Once
	cancelSubscription context.CancelFunc
	doneChan           chan struct{}
}

// NewGooglePubsubConsumer creates and validates a new consumer for a Google Cloud
// Pub/Sub subscription. It checks if the subscription exists before returning.
func NewGooglePubsubConsumer(
	ctx context.Context,
	cfg *GooglePubsubConsumerConfig,
	client *pubsub.Client,
	logger zerolog.Logger,
) (*GooglePubsubConsumer, error) {
	sub := client.Subscription(cfg.SubscriptionID)

	// Use a derived context with a configurable timeout for the existence check.
	subCtx, cancel := context.WithTimeout(ctx, cfg.SubscriptionExistsTimeout)
	defer cancel()

	exists, err := sub.Exists(subCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to check for subscription %s: %w", cfg.SubscriptionID, err)
	}
	if !exists {
		return nil, fmt.Errorf("subscription %s does not exist", cfg.SubscriptionID)
	}
	logger.Info().Str("subscription_id", cfg.SubscriptionID).Msg("Pub/Sub subscription validated.")

	sub.ReceiveSettings.MaxOutstandingMessages = cfg.MaxOutstandingMessages
	sub.ReceiveSettings.NumGoroutines = cfg.NumGoroutines

	return &GooglePubsubConsumer{
		client:       client,
		subscription: sub,
		logger:       logger.With().Str("component", "GooglePubsubConsumer").Str("subscription_id", cfg.SubscriptionID).Logger(),
		outputChan:   make(chan Message, cfg.MaxOutstandingMessages),
		doneChan:     make(chan struct{}),
	}, nil
}

// Messages returns the read-only channel where consumed messages are sent.
func (c *GooglePubsubConsumer) Messages() <-chan Message {
	return c.outputChan
}

// Start begins the message consumption process. It starts a background goroutine
// to receive messages from Pub/Sub. The provided context manages the lifecycle
// of this goroutine.
func (c *GooglePubsubConsumer) Start(ctx context.Context) error {
	c.logger.Info().Msg("Starting Pub/Sub message consumption...")
	receiveCtx, cancel := context.WithCancel(ctx)
	c.cancelSubscription = cancel // Store the cancel function to be called by Stop().

	go func() {
		defer close(c.outputChan)
		defer close(c.doneChan)

		c.logger.Info().Msg("Pub/Sub Receive goroutine started.")
		err := c.subscription.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
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

		// A Canceled error is expected on graceful shutdown, so we don't log it as an error.
		if err != nil && !errors.Is(err, context.Canceled) {
			c.logger.Error().Err(err).Msg("Pub/Sub Receive call exited with an unexpected error.")
		}
		c.logger.Info().Msg("Pub/Sub Receive goroutine stopped.")
	}()
	return nil
}

// Stop gracefully shuts down the consumer. It signals the receiver to stop and
// waits for it to finish, respecting the timeout from the provided context.
func (c *GooglePubsubConsumer) Stop(ctx context.Context) error {
	c.stopOnce.Do(func() {
		c.logger.Info().Msg("Stopping Pub/Sub consumer...")
		if c.cancelSubscription != nil {
			// This cancels the context passed to the subscription.Receive loop,
			// causing it to return.
			c.cancelSubscription()
		}
	})

	// Wait for the shutdown to complete, respecting the caller's timeout.
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
// This is useful for orchestrating graceful shutdowns.
func (c *GooglePubsubConsumer) Done() <-chan struct{} {
	return c.doneChan
}
