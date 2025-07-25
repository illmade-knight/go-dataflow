package messagepipeline

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"fmt"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
	"os"
	"sync"
	"time"
)

// --- Google Cloud Pub/Sub Consumer Implementation (Ideally in a shared package) ---
// This is a simplified version for this service.
// It assumes the topic provides messages that can be unmarshalled into ConsumedUpstreamMessage.

type GooglePubsubConsumerConfig struct {
	ProjectID              string
	SubscriptionID         string
	CredentialsFile        string // Optional
	MaxOutstandingMessages int
	NumGoroutines          int
}

// LoadGooglePubsubConsumerConfigFromEnv loads consumer configuration from environment variables.
// Renamed from LoadConsumerConfigFromEnv
func LoadGooglePubsubConsumerConfigFromEnv() (*GooglePubsubConsumerConfig, error) {
	subID := os.Getenv("PUBSUB_SUBSCRIPTION_ID_GARDEN_MONITOR_INPUT")

	cfg := &GooglePubsubConsumerConfig{
		ProjectID:              os.Getenv("GCP_PROJECT_ID"),
		SubscriptionID:         subID,
		CredentialsFile:        os.Getenv("GCP_PUBSUB_CREDENTIALS_FILE"),
		MaxOutstandingMessages: 100,
		NumGoroutines:          5,
	}
	if cfg.ProjectID == "" {
		return nil, errors.New("GCP_PROJECT_ID environment variable not set for Pub/Sub consumer")
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

// NewGooglePubsubConsumer changed so pubsub client can be shared by consumer and producer
func NewGooglePubsubConsumer(cfg *GooglePubsubConsumerConfig, client *pubsub.Client, logger zerolog.Logger) (*GooglePubsubConsumer, error) {
	sub := client.Subscription(cfg.SubscriptionID)

	// the context just makes sure we don't hang endlessly looking for subscriptions
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
	receiveCtx, cancel := context.WithCancel(ctx) // Derived from the passed context
	c.cancelSubscription = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(c.outputChan) // Close output channel when done
		defer close(c.doneChan)   // Close done channel when done
		defer c.logger.Info().Msg("Pub/Sub Receive goroutine stopped.")

		c.logger.Info().Msg("Pub/Sub Receive goroutine started.")
		err := c.subscription.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
			payloadCopy := make([]byte, len(msg.Data))
			copy(payloadCopy, msg.Data)

			consumedMsg := types.ConsumedMessage{
				PublishMessage: types.PublishMessage{
					ID:          msg.ID,
					Payload:     payloadCopy,
					PublishTime: msg.PublishTime,
				},
				Attributes: msg.Attributes,
				Ack:        msg.Ack,
				Nack:       msg.Nack}

			// Enrich message with DeviceInfo from attributes, if present.
			if uid, ok := msg.Attributes["uid"]; ok {
				consumedMsg.DeviceInfo = &types.DeviceInfo{
					UID:      uid,
					Location: msg.Attributes["location"],
				}
			}

			select {
			case c.outputChan <- consumedMsg:
				// Message sent successfully
			case <-receiveCtx.Done(): // Use receiveCtx for worker specific cancellation
				msg.Nack() // Nack because the consumer is shutting down
				c.logger.Warn().Str("msg_id", msg.ID).Msg("Consumer stopping, Nacking message due to receive context done.")
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) { // context.Canceled is expected on graceful shutdown
			c.logger.Error().Err(err).Msg("Pub/Sub Receive call exited with error")
		}
	}()
	return nil
}
func (c *GooglePubsubConsumer) Stop() error {
	c.stopOnce.Do(func() {
		c.logger.Info().Msg("Stopping Pub/Sub consumer...")
		if c.cancelSubscription != nil {
			c.cancelSubscription() // Signal receiveCtx to cancel
		}
		select {
		case <-c.doneChan: // Wait for the internal goroutine to close doneChan
			c.logger.Info().Msg("Pub/Sub Receive goroutine confirmed stopped.")
		case <-time.After(30 * time.Second):
			c.logger.Error().Msg("Timeout waiting for Pub/Sub Receive goroutine to stop.")
		}
		// REMOVED: c.client.Close() is no longer here. The client should be managed by the orchestrator.
	})
	return nil
}
func (c *GooglePubsubConsumer) Done() <-chan struct{} { return c.doneChan }
