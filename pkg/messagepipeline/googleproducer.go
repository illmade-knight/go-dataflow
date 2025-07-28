package messagepipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
)

// GooglePubsubProducerConfig holds configuration for the Google Pub/Sub producer.
type GooglePubsubProducerConfig struct {
	ProjectID              string
	TopicID                string
	BatchSize              int           // Corresponds to Pub/Sub's CountThreshold.
	BatchDelay             time.Duration // Corresponds to Pub/Sub's DelayThreshold.
	InputChannelMultiplier int           // Multiplier for the producer's input buffer size.
	AutoAckOnPublish       bool          // If true, producer calls original message's Ack/Nack callbacks.
	// REFACTOR: Added configurable timeouts for initialization and publish confirmation.
	TopicExistsTimeout         time.Duration
	PublishConfirmationTimeout time.Duration
}

// NewGooglePubsubProducerDefaults provides a config with sensible defaults.
func NewGooglePubsubProducerDefaults() *GooglePubsubProducerConfig {
	cfg := &GooglePubsubProducerConfig{
		BatchSize:              100,
		BatchDelay:             100 * time.Millisecond,
		InputChannelMultiplier: 2,
		AutoAckOnPublish:       false, // Default to false; let the ProcessingService manage Ack/Nack.
		// REFACTOR: Set defaults for the new timeout configurations.
		TopicExistsTimeout:         15 * time.Second,
		PublishConfirmationTimeout: 20 * time.Second,
	}
	// The following logic allows for overriding defaults via environment variables.
	if bs := os.Getenv("PUBSUB_PRODUCER_BATCH_SIZE"); bs != "" {
		if val, err := strconv.Atoi(bs); err == nil {
			cfg.BatchSize = val
		}
	}
	if bd := os.Getenv("PUBSUB_PRODUCER_BATCH_DELAY"); bd != "" {
		if val, err := time.ParseDuration(bd); err == nil {
			cfg.BatchDelay = val
		}
	}
	if icm := os.Getenv("PUBSUB_PRODUCER_INPUT_CHAN_MULTIPLIER"); icm != "" {
		if val, err := strconv.Atoi(icm); err == nil {
			cfg.InputChannelMultiplier = val
		}
	}
	if autoAckStr := os.Getenv("PUBSUB_PRODUCER_AUTO_ACK_ON_PUBLISH"); autoAckStr != "" {
		if val, err := strconv.ParseBool(autoAckStr); err == nil {
			cfg.AutoAckOnPublish = val
		}
	}
	return cfg
}

// GooglePubsubProducer implements MessageProcessor for publishing to Google Cloud Pub/Sub.
// It leverages the built-in batching capabilities of the official Go client.
type GooglePubsubProducer[T any] struct {
	topic            *pubsub.Topic
	logger           zerolog.Logger
	inputChan        chan *types.BatchedMessage[T]
	wg               sync.WaitGroup
	autoAckOnPublish bool
	// REFACTOR: Add field for confirmation timeout.
	publishConfirmationTimeout time.Duration
}

// NewGooglePubsubProducer creates a new GooglePubsubProducer.
// It validates the topic's existence before returning a functional producer.
// REFACTOR: The signature is now ordered consistently: ctx, cfg, dependencies, logger.
func NewGooglePubsubProducer[T any](
	ctx context.Context,
	cfg *GooglePubsubProducerConfig,
	client *pubsub.Client,
	logger zerolog.Logger,
) (*GooglePubsubProducer[T], error) {
	if client == nil {
		return nil, fmt.Errorf("pubsub client cannot be nil for producer")
	}
	if cfg.InputChannelMultiplier <= 0 {
		logger.Warn().Int("invalid_multiplier", cfg.InputChannelMultiplier).Msg("InputChannelMultiplier is non-positive; defaulting to 1.")
		cfg.InputChannelMultiplier = 1
	}

	topic := client.Topic(cfg.TopicID)
	// Configure Google Pub/Sub's built-in batching via PublishSettings.
	topic.PublishSettings.DelayThreshold = cfg.BatchDelay
	topic.PublishSettings.CountThreshold = cfg.BatchSize
	topic.PublishSettings.Timeout = 10 * time.Second // Overall timeout for a single batch to be published.
	topic.PublishSettings.NumGoroutines = 5

	// Check for topic existence using a derived context with the configured timeout.
	// REFACTOR: Use the configurable timeout from the config struct.
	existsCtx, cancel := context.WithTimeout(ctx, cfg.TopicExistsTimeout)
	defer cancel()
	exists, err := topic.Exists(existsCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to check for topic %s: %w", cfg.TopicID, err)
	}
	if !exists {
		return nil, fmt.Errorf("pubsub topic %s does not exist", cfg.TopicID)
	}

	logger.Info().Str("topic_id", cfg.TopicID).Msg("GooglePubsubProducer initialized successfully.")
	return &GooglePubsubProducer[T]{
		topic:                      topic,
		logger:                     logger.With().Str("component", "GooglePubsubProducer").Str("topic_id", cfg.TopicID).Logger(),
		inputChan:                  make(chan *types.BatchedMessage[T], cfg.BatchSize*cfg.InputChannelMultiplier),
		autoAckOnPublish:           cfg.AutoAckOnPublish,
		publishConfirmationTimeout: cfg.PublishConfirmationTimeout,
	}, nil
}

// Input returns the channel to send messages to the producer.
func (p *GooglePubsubProducer[T]) Input() chan<- *types.BatchedMessage[T] {
	return p.inputChan
}

// Start initiates the producer's internal publishing loop.
// It accepts a context to manage its lifecycle.
func (p *GooglePubsubProducer[T]) Start(shutdownCtx context.Context) {
	p.logger.Info().Msg("Starting Pub/Sub producer...")
	p.wg.Add(1)

	go func() {
		defer p.wg.Done()
		for {
			select {
			case batchedMsg, ok := <-p.inputChan:
				if !ok {
					p.logger.Info().Msg("Producer input channel closed, stopping publishing loop.")
					return
				}
				p.publishMessage(shutdownCtx, batchedMsg)
			case <-shutdownCtx.Done():
				p.logger.Info().Msg("Producer received shutdown signal, stopping publishing loop.")
				// Drain any remaining messages from the channel before exiting.
				p.drainOnShutdown(shutdownCtx)
				return
			}
		}
	}()
}

// publishMessage handles the marshalling and publishing of a single message.
// REFACTOR: This logic was extracted into a helper. It now correctly uses the shutdownCtx
// for the main Publish call, ensuring operations are cancelled during shutdown.
func (p *GooglePubsubProducer[T]) publishMessage(ctx context.Context, msg *types.BatchedMessage[T]) {
	payload, err := json.Marshal(msg.Payload)
	if err != nil {
		p.logger.Error().Err(err).Str("original_msg_id", msg.OriginalMessage.ID).Msg("Failed to marshal payload for publishing.")
		if p.autoAckOnPublish {
			msg.OriginalMessage.Nack()
		}
		return
	}

	// Publish the message. Google's client handles batching internally.
	res := p.topic.Publish(ctx, &pubsub.Message{
		Data:       payload,
		Attributes: msg.OriginalMessage.Attributes,
	})

	// Asynchronously wait for the publish result to Ack/Nack the original message.
	go p.confirmPublish(res, msg.OriginalMessage)
}

// drainOnShutdown processes any messages buffered in the input channel during a shutdown.
func (p *GooglePubsubProducer[T]) drainOnShutdown(ctx context.Context) {
	p.logger.Info().Msg("Draining producer input channel on shutdown...")
	for {
		select {
		case batchedMsg, ok := <-p.inputChan:
			if !ok {
				p.logger.Info().Msg("Finished draining producer channel.")
				return
			}
			p.publishMessage(ctx, batchedMsg)
		default:
			// Channel is empty
			p.logger.Info().Msg("Finished draining producer channel.")
			return
		}
	}
}

// confirmPublish waits for the result of a single publish operation.
func (p *GooglePubsubProducer[T]) confirmPublish(res *pubsub.PublishResult, originalMsg types.ConsumedMessage) {
	// Create a new context with a specific timeout for waiting on the result.
	// REFACTOR: Use the configurable timeout.
	getCtx, cancel := context.WithTimeout(context.Background(), p.publishConfirmationTimeout)
	defer cancel()

	msgID, err := res.Get(getCtx)
	if err != nil {
		p.logger.Error().Err(err).Str("original_msg_id", originalMsg.ID).Msg("Failed to get publish result.")
		if p.autoAckOnPublish {
			originalMsg.Nack()
		}
	} else {
		p.logger.Debug().Str("original_msg_id", originalMsg.ID).Str("pubsub_msg_id", msgID).Msg("Message published successfully.")
		if p.autoAckOnPublish {
			originalMsg.Ack()
		}
	}
}

// Stop gracefully shuts down the producer. It stops accepting new messages,
// waits for the processing loop to finish, and then flushes all outstanding
// messages to Pub/Sub, respecting the provided context's timeout.
// REFACTOR: Stop now accepts a context and returns an error.
func (p *GooglePubsubProducer[T]) Stop(ctx context.Context) error {
	p.logger.Info().Msg("Stopping Pub/Sub producer...")
	// 1. Close the input channel to stop accepting new messages.
	close(p.inputChan)
	// 2. Wait for the internal publishing goroutine to finish processing.
	p.wg.Wait()

	// 3. Stop the Pub/Sub topic client. This flushes any buffered messages.
	// Since topic.Stop() is blocking and doesn't take a context, we wrap it
	// to enforce our own timeout.
	if p.topic != nil {
		p.logger.Info().Msg("Flushing remaining messages and stopping Pub/Sub topic...")
		stopDone := make(chan struct{})
		go func() {
			p.topic.Stop()
			close(stopDone)
		}()
		select {
		case <-stopDone:
			p.logger.Info().Msg("Pub/Sub topic stopped.")
		case <-ctx.Done():
			p.logger.Error().Err(ctx.Err()).Msg("Timeout waiting for Pub/Sub topic to flush and stop.")
			return ctx.Err()
		}
	}
	p.logger.Info().Msg("Pub/Sub producer stopped gracefully.")
	return nil
}
