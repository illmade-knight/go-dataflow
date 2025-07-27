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
	"github.com/rs/zerolog/log"
)

// GooglePubsubProducerConfig holds configuration for the Google Pub/Sub producer.
type GooglePubsubProducerConfig struct {
	ProjectID              string
	TopicID                string
	BatchSize              int           // Now refers to Google's CountThreshold
	BatchDelay             time.Duration // Now refers to Google's DelayThreshold
	InputChannelMultiplier int           // Still for the producer's input buffer size
	AutoAckOnPublish       bool          // New: If true, producer calls original message's Ack/Nack callbacks based on publish result.
}

// LoadGooglePubsubProducerConfig loads producer configuration from environment variables.
// we can have multiple producers and we always need a topicID so pass it in
func LoadGooglePubsubProducerConfig(topicID string) (*GooglePubsubProducerConfig, error) {
	cfg := &GooglePubsubProducerConfig{
		TopicID:                topicID,
		BatchSize:              100,                    // Default Google Pub/Sub batch size
		BatchDelay:             100 * time.Millisecond, // Default Google Pub/Sub batch delay
		InputChannelMultiplier: 2,                      // Default multiplier for input channel
		AutoAckOnPublish:       false,                  // Default to false for production use (ProcessingService handles Ack/Nack)
	}
	if bs := os.Getenv("PUBSUB_PRODUCER_BATCH_SIZE"); bs != "" {
		if val, err := strconv.Atoi(bs); err == nil {
			cfg.BatchSize = val
		}
	}
	if bd := os.Getenv("PUBSUB_PRODUCER_BATCH_DELAY"); bd != "" {
		if val, err := time.ParseDuration(bd); err == nil {
			if val < 0 {
				log.Warn().Dur("invalid_delay", val).Msg("PUBSUB_PRODUCER_BATCH_DELAY is negative, producer will handle it.")
			}
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

	return cfg, nil
}

// GooglePubsubProducer implements MessageProcessor for publishing to Google Cloud Pub/Sub.
type GooglePubsubProducer[T any] struct {
	client           *pubsub.Client
	topic            *pubsub.Topic
	logger           zerolog.Logger
	inputChan        chan *types.BatchedMessage[T]
	doneChan         chan struct{}  // Signals when the internal publishing goroutine has stopped
	wg               sync.WaitGroup // For internal publishing goroutine
	autoAckOnPublish bool           // Determines if producer calls original message's Ack/Nack.
	// Removed batchSize and batchDelay fields as they are configured directly on topic.PublishSettings
}

// NewGooglePubsubProducer creates a new GooglePubsubProducer.
func NewGooglePubsubProducer[T any](
	client *pubsub.Client,
	cfg *GooglePubsubProducerConfig,
	logger zerolog.Logger,
) (*GooglePubsubProducer[T], error) {
	if client == nil {
		return nil, fmt.Errorf("pubsub client cannot be nil for producer")
	}

	if cfg.InputChannelMultiplier <= 0 {
		logger.Warn().Int("invalid_multiplier", cfg.InputChannelMultiplier).Msg("GooglePubsubProducerConfig.InputChannelMultiplier is non-positive. Defaulting to 1.")
		cfg.InputChannelMultiplier = 1
	}

	topic := client.Topic(cfg.TopicID)
	// Configure Google Pub/Sub's built-in PublishSettings
	topic.PublishSettings.DelayThreshold = cfg.BatchDelay
	topic.PublishSettings.CountThreshold = cfg.BatchSize
	topic.PublishSettings.Timeout = 10 * time.Second // Overall timeout for a batch to publish
	topic.PublishSettings.NumGoroutines = 5          // Number of concurrent goroutines for internal publishing

	maxRetries := 3
	retryDelay := 100 * time.Millisecond
	exists := false
	var existsErr error

	for i := 0; i < maxRetries; i++ {
		topicCtx, topicCancel := context.WithTimeout(context.Background(), 5*time.Second)
		exists, existsErr = topic.Exists(topicCtx)
		topicCancel()

		if existsErr == nil && exists {
			logger.Debug().Str("topic_id", cfg.TopicID).Int("attempt", i+1).Msg("Topic confirmed to exist by producer's internal check.")
			break
		}

		if existsErr != nil {
			logger.Warn().Err(existsErr).Str("topic_id", cfg.TopicID).Int("attempt", i+1).
				Msg("NewGooglePubsubProducer: Failed to check existence of topic, retrying...")
		} else if !exists {
			logger.Warn().Str("topic_id", cfg.TopicID).Int("attempt", i+1).
				Msg("NewGooglePubsubProducer: Topic reported as not existing by producer's internal check, retrying...")
		}

		time.Sleep(retryDelay)
		retryDelay *= 2
	}

	if existsErr != nil {
		return nil, fmt.Errorf("failed to check existence of topic %s after %d retries: %w", cfg.TopicID, maxRetries, existsErr)
	}
	if !exists {
		return nil, fmt.Errorf("pubsub topic %s does not exist after %d retries", cfg.TopicID, maxRetries)
	}

	if cfg.BatchDelay < 0 { // This check is mostly for warning, as negative will be handled by PublishSettings
		logger.Warn().Dur("invalid_delay", cfg.BatchDelay).Msg("GooglePubsubProducerConfig.BatchDelay is negative. Time-based batching will be disabled.")
	}

	logger.Info().Str("topic_id", cfg.TopicID).Msg("GooglePubsubProducer initialized successfully.")

	return &GooglePubsubProducer[T]{
		client:           client,
		topic:            topic,
		logger:           logger.With().Str("component", "GooglePubsubProducer").Str("topic_id", cfg.TopicID).Logger(),
		inputChan:        make(chan *types.BatchedMessage[T], cfg.BatchSize*cfg.InputChannelMultiplier),
		doneChan:         make(chan struct{}),
		autoAckOnPublish: cfg.AutoAckOnPublish, // Set from config
		// Removed batchSize and batchDelay fields
	}, nil
}

// NewGooglePubsubProducerWithExistingTopic creates a new GooglePubsubProducer
// by directly accepting an already created and validated *pubsub.Topic object.
// This is mainly for testing purposes.
func NewGooglePubsubProducerWithExistingTopic[T any](
	client *pubsub.Client,
	topic *pubsub.Topic,
	cfg *GooglePubsubProducerConfig,
	logger zerolog.Logger,
) (*GooglePubsubProducer[T], error) {
	if client == nil {
		return nil, fmt.Errorf("pubsub client cannot be nil for producer")
	}
	if topic == nil {
		return nil, fmt.Errorf("pubsub topic cannot be nil for producer")
	}
	if cfg.TopicID != topic.ID() {
		logger.Warn().Str("config_topic_id", cfg.TopicID).Str("provided_topic_id", topic.ID()).
			Msg("Producer config TopicID does not match provided Pub/Sub Topic object ID. Using provided topic object's ID.")
		cfg.TopicID = topic.ID()
	}

	if cfg.InputChannelMultiplier <= 0 {
		logger.Warn().Int("invalid_multiplier", cfg.InputChannelMultiplier).Msg("GooglePubsubProducerConfig.InputChannelMultiplier is non-positive. Defaulting to 1.")
		cfg.InputChannelMultiplier = 1
	}

	// Configure Google Pub/Sub's built-in PublishSettings even for existing topic
	topic.PublishSettings.DelayThreshold = cfg.BatchDelay
	topic.PublishSettings.CountThreshold = cfg.BatchSize
	topic.PublishSettings.Timeout = 10 * time.Second
	topic.PublishSettings.NumGoroutines = 5

	if cfg.BatchDelay < 0 {
		logger.Warn().Dur("invalid_delay", cfg.BatchDelay).Msg("GooglePubsubProducerConfig.BatchDelay is negative. Time-based batching will be disabled.")
	}

	logger.Info().Str("topic_id", cfg.TopicID).Msg("GooglePubsubProducer (WithExistingTopic) initialized successfully.")

	return &GooglePubsubProducer[T]{
		client:           client,
		topic:            topic,
		logger:           logger.With().Str("component", "GooglePubsubProducer").Str("topic_id", cfg.TopicID).Logger(),
		inputChan:        make(chan *types.BatchedMessage[T], cfg.BatchSize*cfg.InputChannelMultiplier),
		doneChan:         make(chan struct{}),
		autoAckOnPublish: cfg.AutoAckOnPublish, // Set from config
		// Removed batchSize and batchDelay fields
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
		defer close(p.doneChan) // Signal completion of this goroutine

		for {
			select {
			case batchedMsg, ok := <-p.inputChan:
				if !ok {
					p.logger.Info().Msg("Producer input channel closed, stopping publishing loop.")
					// All messages sent to topic.Publish are already queued internally by the client library.
					// topic.Stop() called in p.Stop() will flush them.
					return
				}

				payload, err := json.Marshal(batchedMsg.Payload)
				if err != nil {
					p.logger.Error().Err(err).
						Str("original_msg_id", batchedMsg.OriginalMessage.ID).
						Msg("Failed to marshal payload for publishing.")
					if p.autoAckOnPublish { // Conditionally Nack
						batchedMsg.OriginalMessage.Nack()
					}
					continue
				}

				// Publish the message. Google's client handles batching internally.
				res := p.topic.Publish(context.Background(), &pubsub.Message{ // Using Background context for Publish as topic manages lifecycle
					Data:       payload,
					Attributes: batchedMsg.OriginalMessage.Attributes, // Pass through original attributes
				})

				// Asynchronously wait for the publish result. This is where Ack/Nack are associated with the original message.
				// This goroutine is detached from the main producer loop and its context.
				go func(originalMsg types.ConsumedMessage, publishResult *pubsub.PublishResult) {
					publishCtx, publishCancel := context.WithTimeout(shutdownCtx, 10*time.Second) // Use new context for Get
					defer publishCancel()

					msgID, err := publishResult.Get(publishCtx)
					if err != nil {
						p.logger.Error().Err(err).
							Str("original_msg_id", originalMsg.ID).
							Msg("Failed to get publish result.")
						if p.autoAckOnPublish { // Conditionally Nack
							originalMsg.Nack()
						}
					} else {
						p.logger.Debug().
							Str("original_msg_id", originalMsg.ID).
							Str("pubsub_msg_id", msgID).
							Msg("Message published successfully and confirmed by Pub/Sub.")
						if p.autoAckOnPublish { // Conditionally Ack
							originalMsg.Ack()
						}
					}
				}(batchedMsg.OriginalMessage, res) // Pass original message and result to goroutine

			case <-shutdownCtx.Done(): // Use the passed context for shutdown signal
				p.logger.Info().Msg("Producer received shutdown signal, stopping publishing loop.")
				return
			}
		}
	}()
}

// Stop gracefully shuts down the producer.
func (p *GooglePubsubProducer[T]) Stop() {
	p.logger.Info().Msg("Stopping Pub/Sub producer...")
	// 1. Close the input channel to stop accepting new messages.
	close(p.inputChan)
	// 2. Wait for the internal publishing goroutine (started by Start) to finish processing remaining messages from inputChan.
	p.wg.Wait()
	// 3. Stop the Pub/Sub topic client. This flushes any remaining buffered messages and waits for their completion.
	if p.topic != nil {
		p.logger.Info().Msg("Flushing remaining messages and stopping Pub/Sub topic...")
		p.topic.Stop() // This blocks until all outstanding messages are published.
		p.logger.Info().Msg("Pub/Sub topic stopped.")
	}
	// 4. Wait for the doneChan to be closed. This confirms the internal publishing goroutine has fully exited.
	<-p.doneChan
	p.logger.Info().Msg("Pub/Sub producer stopped gracefully.")
}

// Done returns a channel that is closed when the producer has fully stopped.
func (p *GooglePubsubProducer[T]) Done() <-chan struct{} {
	return p.doneChan
}
