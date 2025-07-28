package bqstore

import (
	"context"
	"sync"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
)

// DataBatchInserter is a generic interface for inserting a batch of items.
// It abstracts the destination data store (e.g., BigQuery, Postgres, etc.).
type DataBatchInserter[T any] interface {
	InsertBatch(ctx context.Context, items []*T) error
	Close() error
}

// BatchInserterConfig holds configuration for the BatchInserter.
type BatchInserterConfig struct {
	BatchSize     int
	FlushInterval time.Duration // How often to flush a partial batch.
	// REFACTOR: Added a configurable timeout for the underlying InsertBatch call.
	InsertTimeout time.Duration // The timeout for a single flush operation.
}

// BatchInserter manages batching and insertion for items of type T.
// It implements the messagepipeline.MessageProcessor[T] interface.
type BatchInserter[T any] struct {
	config    *BatchInserterConfig
	inserter  DataBatchInserter[T]
	logger    zerolog.Logger
	inputChan chan *types.BatchedMessage[T]
	wg        sync.WaitGroup
	// REFACTOR: Removed internal context management (shutdownCtx, shutdownFunc).
}

// NewBatcher creates a new generic BatchInserter.
// REFACTOR: The constructor is now simpler and does not require a context.
func NewBatcher[T any](
	config *BatchInserterConfig,
	inserter DataBatchInserter[T],
	logger zerolog.Logger,
) *BatchInserter[T] {
	return &BatchInserter[T]{
		config:    config,
		inserter:  inserter,
		logger:    logger.With().Str("component", "BatchInserter").Logger(),
		inputChan: make(chan *types.BatchedMessage[T], config.BatchSize*2),
	}
}

// Start begins the batching worker. This is part of the MessageProcessor interface.
// REFACTOR: The passed context is now used to control the worker's lifecycle.
func (b *BatchInserter[T]) Start(ctx context.Context) {
	b.logger.Info().
		Int("batch_size", b.config.BatchSize).
		Dur("flush_interval", b.config.FlushInterval).
		Msg("Starting BatchInserter worker...")
	b.wg.Add(1)
	go b.worker(ctx)
}

// Stop gracefully shuts down the BatchInserter. This is part of the MessageProcessor interface.
// REFACTOR: The method now accepts a context to enforce a shutdown timeout.
func (b *BatchInserter[T]) Stop(ctx context.Context) error {
	b.logger.Info().Msg("Stopping BatchInserter...")
	close(b.inputChan)

	// Wait for the worker to finish, but respect the timeout.
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		b.logger.Info().Msg("BatchInserter worker stopped gracefully.")
	case <-ctx.Done():
		b.logger.Error().Err(ctx.Err()).Msg("Timeout waiting for BatchInserter worker to stop.")
		return ctx.Err()
	}

	if err := b.inserter.Close(); err != nil {
		b.logger.Error().Err(err).Msg("Error closing underlying data inserter")
	}
	b.logger.Info().Msg("BatchInserter stopped.")
	return nil
}

// Input returns the channel to which payloads should be sent.
func (b *BatchInserter[T]) Input() chan<- *types.BatchedMessage[T] {
	return b.inputChan
}

// worker is the core logic that collects items into a batch and flushes it.
// REFACTOR: The worker now uses the context passed from Start for its lifecycle.
func (b *BatchInserter[T]) worker(ctx context.Context) {
	defer b.wg.Done()
	batch := make([]*types.BatchedMessage[T], 0, b.config.BatchSize)
	ticker := time.NewTicker(b.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// The service is shutting down, flush any remaining items.
			b.flush(context.Background(), batch) // Use a background context for the final flush.
			return

		case msg, ok := <-b.inputChan:
			if !ok {
				// The input channel was closed, flush remaining items and exit.
				b.flush(ctx, batch)
				return
			}
			batch = append(batch, msg)
			if len(batch) >= b.config.BatchSize {
				b.flush(ctx, batch)
				batch = make([]*types.BatchedMessage[T], 0, b.config.BatchSize)
				// Reset the ticker to prevent an immediate, unnecessary flush.
				ticker.Reset(b.config.FlushInterval)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				b.flush(ctx, batch)
				batch = make([]*types.BatchedMessage[T], 0, b.config.BatchSize)
			}
		}
	}
}

// flush sends the current batch to the inserter and handles Ack/Nack logic.
func (b *BatchInserter[T]) flush(ctx context.Context, batch []*types.BatchedMessage[T]) {
	if len(batch) == 0 {
		return
	}

	payloads := make([]*T, len(batch))
	for i, msg := range batch {
		payloads[i] = msg.Payload
	}

	// REFACTOR: Use the configured timeout for the insert operation.
	insertCtx, cancel := context.WithTimeout(ctx, b.config.InsertTimeout)
	defer cancel()

	if err := b.inserter.InsertBatch(insertCtx, payloads); err != nil {
		b.logger.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to insert batch, Nacking messages.")
		for _, msg := range batch {
			if msg.OriginalMessage.Nack != nil {
				msg.OriginalMessage.Nack()
			}
		}
	} else {
		b.logger.Info().Int("batch_size", len(batch)).Msg("Successfully flushed batch, Acking messages.")
		for _, msg := range batch {
			if msg.OriginalMessage.Ack != nil {
				msg.OriginalMessage.Ack()
			}
		}
	}
}
