package messagepipeline

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"fmt"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
	"sync"
	"time"
)

// --- Google Cloud Pub/Sub Consumer Implementation ---

type GooglePubsubConsumerConfig struct {
	ProjectID              string
	SubscriptionID         string
	CredentialsFile        string // Optional
	MaxOutstandingMessages int
	NumGoroutines          int
}

// LoadDefaultGooglePubsubConsumerConfig GooglePubsubConsumer will always need a sub
func LoadDefaultGooglePubsubConsumerConfig(subID string) (*GooglePubsubConsumerConfig, error) {
	cfg := &GooglePubsubConsumerConfig{
		SubscriptionID:         subID,
		MaxOutstandingMessages: 100,
		NumGoroutines:          5,
	}
	return cfg, nil
}

type GooglePubsubConsumer struct {
	client             *pubsub.Client
	subscription       *pubsub.Subscription
	logger             zerolog.Logger
	outputChan         chan types.ConsumedMessage
	stopOnce           sync.Once
	cancelSubscription context.CancelFunc
	wg                 sync.WaitGroup
	doneChan           chan struct{}
}

func NewGooglePubsubConsumer(cfg *GooglePubsubConsumerConfig, client *pubsub.Client, logger zerolog.Logger) (*GooglePubsubConsumer, error) {
	sub := client.Subscription(cfg.SubscriptionID)

	subContext, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	e, err := sub.Exists(subContext)
	if !e || err != nil {
		return nil, fmt.Errorf("subscription %s does not exist: %w", cfg.SubscriptionID, err)
	}
	logger.Info().Str("subscription_id", cfg.SubscriptionID).Msg("Listening for messages")

	sub.ReceiveSettings.MaxOutstandingMessages = cfg.MaxOutstandingMessages
	sub.ReceiveSettings.NumGoroutines = cfg.NumGoroutines

	return &GooglePubsubConsumer{
		client:       client,
		subscription: sub,
		logger:       logger.With().Str("component", "GooglePubsubConsumer").Str("subscription_id", cfg.SubscriptionID).Logger(),
		outputChan:   make(chan types.ConsumedMessage, cfg.MaxOutstandingMessages),
		doneChan:     make(chan struct{}),
	}, nil
}
func (c *GooglePubsubConsumer) Messages() <-chan types.ConsumedMessage { return c.outputChan }
func (c *GooglePubsubConsumer) Start(ctx context.Context) error {
	c.logger.Info().Msg("Starting Pub/Sub message consumption...")
	receiveCtx, cancel := context.WithCancel(ctx)
	c.cancelSubscription = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(c.outputChan)
		defer close(c.doneChan)
		defer c.logger.Info().Msg("Pub/Sub Receive goroutine stopped.")

		c.logger.Info().Msg("Pub/Sub Receive goroutine started.")
		err := c.subscription.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
			payloadCopy := make([]byte, len(msg.Data))
			copy(payloadCopy, msg.Data)

			// REFACTORED: The consumer no longer creates DeviceInfo. It just passes
			// the message and its attributes along. The enrichment service is now
			// solely responsible for interpreting attributes.
			consumedMsg := types.ConsumedMessage{
				PublishMessage: types.PublishMessage{
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
			case <-receiveCtx.Done():
				msg.Nack()
				c.logger.Warn().Str("msg_id", msg.ID).Msg("Consumer stopping, Nacking message due to receive context done.")
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			c.logger.Error().Err(err).Msg("Pub/Sub Receive call exited with error")
		}
	}()
	return nil
}
func (c *GooglePubsubConsumer) Stop() error {
	c.stopOnce.Do(func() {
		c.logger.Info().Msg("Stopping Pub/Sub consumer...")
		if c.cancelSubscription != nil {
			c.cancelSubscription()
		}
		select {
		case <-c.doneChan:
			c.logger.Info().Msg("Pub/Sub Receive goroutine confirmed stopped.")
		case <-time.After(30 * time.Second):
			c.logger.Error().Msg("Timeout waiting for Pub/Sub Receive goroutine to stop.")
		}
	})
	return nil
}
func (c *GooglePubsubConsumer) Done() <-chan struct{} { return c.doneChan }
