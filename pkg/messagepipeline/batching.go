package messagepipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// BatchingServiceConfig holds the configuration for a BatchingService.
type BatchingServiceConfig struct {
	NumWorkers    int
	BatchSize     int
	FlushInterval time.Duration
}

// BatchingService orchestrates a pipeline that consumes messages, transforms them,
// collects them into batches, and sends them to a batch processor function.
type BatchingService[T any] struct {
	cfg         BatchingServiceConfig
	consumer    MessageConsumer
	transformer MessageTransformer[T]
	processor   BatchProcessor[T]
	logger      zerolog.Logger
	transformWg sync.WaitGroup
	batchWg     sync.WaitGroup
	batchChan   chan ProcessableItem[T]
}

// NewBatchingService creates a new, generic BatchingService.
func NewBatchingService[T any](
	cfg BatchingServiceConfig,
	consumer MessageConsumer,
	transformer MessageTransformer[T],
	processor BatchProcessor[T],
	logger zerolog.Logger,
) (*BatchingService[T], error) {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 5
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 1 * time.Minute
	}
	if consumer == nil || transformer == nil || processor == nil {
		return nil, fmt.Errorf("consumer, transformer, and processor cannot be nil")
	}

	return &BatchingService[T]{
		cfg:         cfg,
		consumer:    consumer,
		transformer: transformer,
		processor:   processor,
		logger:      logger.With().Str("service", "BatchingService").Logger(),
		batchChan:   make(chan ProcessableItem[T], cfg.BatchSize*cfg.NumWorkers),
	}, nil
}

// Start begins the service operation.
func (s *BatchingService[T]) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting batching service...")

	if err := s.consumer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start message consumer: %w", err)
	}
	s.logger.Info().Msg("Message consumer started.")

	// Start the single batch manager worker
	s.batchWg.Add(1)
	go s.batchWorker(ctx)

	// Start the transformation workers
	s.logger.Info().Int("worker_count", s.cfg.NumWorkers).Msg("Starting transformation workers...")
	s.transformWg.Add(s.cfg.NumWorkers)
	for i := 0; i < s.cfg.NumWorkers; i++ {
		go s.transformWorker(ctx, i)
	}

	// Start a dedicated closer goroutine. It waits for all producers (transform workers)
	// to finish, and only then closes the batchChan. This prevents a panic.
	go func() {
		s.transformWg.Wait()
		close(s.batchChan)
	}()

	s.logger.Info().Msg("Batching service started successfully.")
	return nil
}

// Stop gracefully shuts down the entire service.
func (s *BatchingService[T]) Stop(ctx context.Context) error {
	s.logger.Info().Msg("Stopping batching service...")

	if err := s.consumer.Stop(ctx); err != nil {
		s.logger.Warn().Err(err).Msg("Error during consumer stop, continuing shutdown.")
	}

	allDone := make(chan struct{})
	go func() {
		s.transformWg.Wait()
		s.batchWg.Wait()
		close(allDone)
	}()

	select {
	case <-allDone:
		s.logger.Info().Msg("All workers completed gracefully.")
	case <-ctx.Done():
		s.logger.Error().Err(ctx.Err()).Msg("Timeout waiting for workers to finish.")
		return ctx.Err()
	}

	s.logger.Info().Msg("Batching service stopped.")
	return nil
}

// transformWorker consumes messages, transforms them, and sends them to the batch channel.
func (s *BatchingService[T]) transformWorker(ctx context.Context, workerID int) {
	defer s.transformWg.Done()
	s.logger.Debug().Int("worker_id", workerID).Msg("Transform worker started.")
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-s.consumer.Messages():
			if !ok {
				return
			}

			payload, skip, err := s.transformer(ctx, &msg)
			if err != nil {
				s.logger.Error().Err(err).Str("msg_id", msg.ID).Msg("Failed to transform message, Nacking.")
				msg.Nack()
				continue
			}
			if skip {
				s.logger.Debug().Str("msg_id", msg.ID).Msg("Transformer signaled to skip message, Acking.")
				msg.Ack()
				continue
			}

			s.batchChan <- ProcessableItem[T]{Original: msg, Payload: payload}
		}
	}
}

// batchWorker collects items from the batch channel and flushes them.
func (s *BatchingService[T]) batchWorker(ctx context.Context) {
	defer s.batchWg.Done()
	s.logger.Debug().Msg("Batch worker started.")

	batch := make([]ProcessableItem[T], 0, s.cfg.BatchSize)
	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		s.logger.Info().Int("batch_size", len(batch)).Msg("Flushing batch.")
		if err := s.processor(ctx, batch); err != nil {
			s.logger.Error().Err(err).Msg("Batch processor failed. Message Ack/Nack is handled by the processor function.")
		}
		batch = make([]ProcessableItem[T], 0, s.cfg.BatchSize)
		ticker.Reset(s.cfg.FlushInterval)
	}

	for {
		select {
		case item, ok := <-s.batchChan:
			if !ok {
				flush() // Final flush
				return
			}
			batch = append(batch, item)
			if len(batch) >= s.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}
