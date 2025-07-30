package messagepipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog"
)

// StreamingService orchestrates a pipeline that consumes messages, transforms them
// individually, and immediately sends them to a streaming processor function.
// It is ideal for use cases that do not require batching.
type StreamingService[T any] struct {
	numWorkers  int
	consumer    MessageConsumer
	transformer MessageTransformer[T]
	processor   StreamProcessor[T]
	logger      zerolog.Logger
	wg          sync.WaitGroup
}

// StreamingServiceConfig holds configuration for a StreamingService.
type StreamingServiceConfig struct {
	NumWorkers int
}

// NewStreamingService creates a new StreamingService.
func NewStreamingService[T any](
	cfg StreamingServiceConfig,
	consumer MessageConsumer,
	transformer MessageTransformer[T],
	processor StreamProcessor[T],
	logger zerolog.Logger,
) (*StreamingService[T], error) {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 5 // Default to a reasonable number of workers.
	}
	if consumer == nil {
		return nil, fmt.Errorf("consumer cannot be nil")
	}
	if transformer == nil {
		return nil, fmt.Errorf("transformer cannot be nil")
	}
	if processor == nil {
		return nil, fmt.Errorf("processor cannot be nil")
	}

	return &StreamingService[T]{
		numWorkers:  cfg.NumWorkers,
		consumer:    consumer,
		transformer: transformer,
		processor:   processor,
		logger:      logger.With().Str("service", "StreamingService").Logger(),
	}, nil
}

// Start begins the service operation. It starts the consumer and then spawns
// a pool of workers to process messages concurrently.
func (s *StreamingService[T]) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting streaming service...")

	if err := s.consumer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start message consumer: %w", err)
	}
	s.logger.Info().Msg("Message consumer started.")

	s.logger.Info().Int("worker_count", s.numWorkers).Msg("Starting processing workers...")
	s.wg.Add(s.numWorkers)
	for i := 0; i < s.numWorkers; i++ {
		go s.worker(ctx, i)
	}

	s.logger.Info().Msg("Streaming service started successfully.")
	return nil
}

// Stop gracefully shuts down the entire service in the correct order.
func (s *StreamingService[T]) Stop(ctx context.Context) error {
	s.logger.Info().Msg("Stopping streaming service...")

	// Stop the consumer first to prevent new messages from arriving.
	if err := s.consumer.Stop(ctx); err != nil {
		s.logger.Warn().Err(err).Msg("Error during consumer stop, continuing shutdown.")
	}

	// Wait for all workers to finish processing in-flight messages.
	workerDone := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(workerDone)
	}()

	select {
	case <-workerDone:
		s.logger.Info().Msg("All processing workers completed gracefully.")
	case <-ctx.Done():
		s.logger.Error().Err(ctx.Err()).Msg("Timeout waiting for processing workers to finish.")
		return ctx.Err()
	}

	s.logger.Info().Msg("Streaming service stopped.")
	return nil
}

// worker is the main processing loop for each concurrent worker.
func (s *StreamingService[T]) worker(ctx context.Context, workerID int) {
	defer s.wg.Done()
	s.logger.Debug().Int("worker_id", workerID).Msg("Processing worker started.")
	for {
		select {
		case <-ctx.Done():
			s.logger.Info().Int("worker_id", workerID).Msg("Processing worker shutting down due to context cancellation.")
			return
		case msg, ok := <-s.consumer.Messages():
			if !ok {
				s.logger.Info().Int("worker_id", workerID).Msg("Consumer channel closed, worker exiting.")
				return
			}
			s.processConsumedMessage(ctx, msg, workerID)
		}
	}
}

// processConsumedMessage contains the core logic for transforming and processing a single message.
func (s *StreamingService[T]) processConsumedMessage(ctx context.Context, msg Message, workerID int) {
	s.logger.Debug().Int("worker_id", workerID).Str("msg_id", msg.ID).Msg("Transforming message.")

	transformedPayload, skip, err := s.transformer(ctx, &msg)
	if err != nil {
		s.logger.Error().Err(err).Str("msg_id", msg.ID).Msg("Failed to transform message, Nacking.")
		msg.Nack()
		return
	}

	if skip {
		s.logger.Debug().Str("msg_id", msg.ID).Msg("Transformer signaled to skip message, Acking.")
		msg.Ack()
		return
	}

	if err := s.processor(ctx, msg, transformedPayload); err != nil {
		s.logger.Error().Err(err).Str("msg_id", msg.ID).Msg("Processor failed to handle message, Nacking.")
		msg.Nack()
		return
	}

	s.logger.Debug().Str("msg_id", msg.ID).Msg("Message processed successfully, Acking.")
	msg.Ack()
}
