package icestore

import (
	"context"
	"sync"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
)

// BatcherConfig holds configuration for the Batcher.
type BatcherConfig struct {
	BatchSize     int
	FlushInterval time.Duration
	// REFACTOR: Added a configurable timeout for the underlying UploadBatch call.
	UploadTimeout time.Duration
}

// Batcher manages the batching of ArchivalData items. It implements the
// messagepipeline.MessageProcessor interface for the ArchivalData type.
type Batcher struct {
	config    *BatcherConfig
	uploader  DataUploader
	logger    zerolog.Logger
	inputChan chan *types.BatchedMessage[ArchivalData]
	wg        sync.WaitGroup
	// REFACTOR: Removed internal context management (shutdownCtx, shutdownFunc).
}

// NewBatcher creates a new Batcher for ArchivalData.
func NewBatcher(
	config *BatcherConfig,
	uploader DataUploader,
	logger zerolog.Logger,
) *Batcher {
	if config.BatchSize <= 0 {
		config.BatchSize = 100 // Default to a safe value.
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 1 * time.Minute // Default to a safe value.
	}
	return &Batcher{
		config:    config,
		uploader:  uploader,
		logger:    logger.With().Str("component", "IceStoreBatcher").Logger(),
		inputChan: make(chan *types.BatchedMessage[ArchivalData], config.BatchSize*2),
	}
}

// Start begins the batching worker goroutine.
// REFACTOR: The passed context is now used to control the worker's lifecycle.
func (b *Batcher) Start(ctx context.Context) {
	b.logger.Info().
		Int("batch_size", b.config.BatchSize).
		Dur("flush_interval", b.config.FlushInterval).
		Msg("Starting icestore Batcher worker...")
	b.wg.Add(1)
	go b.worker(ctx)
}

// Stop gracefully shuts down the Batcher, ensuring any pending items are flushed.
// REFACTOR: The method now accepts a context to enforce a shutdown timeout.
func (b *Batcher) Stop(ctx context.Context) error {
	b.logger.Info().Msg("Stopping icestore Batcher...")
	close(b.inputChan)

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		if err := b.uploader.Close(); err != nil {
			b.logger.Error().Err(err).Msg("Error closing underlying data uploader")
		}
		close(done)
	}()

	select {
	case <-done:
		b.logger.Info().Msg("IceStore Batcher stopped gracefully.")
		return nil
	case <-ctx.Done():
		b.logger.Error().Err(ctx.Err()).Msg("Timeout waiting for icestore Batcher to stop.")
		return ctx.Err()
	}
}

// Input returns the write-only channel for ArchivalData messages.
func (b *Batcher) Input() chan<- *types.BatchedMessage[ArchivalData] {
	return b.inputChan
}

// worker contains the core key-aware batching logic.
// REFACTOR: The worker now uses the context passed from Start.
func (b *Batcher) worker(ctx context.Context) {
	defer b.wg.Done()
	batches := make(map[string][]*types.BatchedMessage[ArchivalData])
	ticker := time.NewTicker(b.config.FlushInterval)
	defer ticker.Stop()

	flushAll := func() {
		if len(batches) == 0 {
			return
		}
		b.logger.Info().Int("key_count", len(batches)).Msg("Flushing all pending batches.")
		for key, batchToFlush := range batches {
			b.flush(ctx, batchToFlush)
			delete(batches, key)
		}
	}

	for {
		select {
		case <-ctx.Done():
			flushAll()
			return
		case msg, ok := <-b.inputChan:
			if !ok {
				flushAll()
				return
			}
			key := msg.Payload.GetBatchKey()
			batches[key] = append(batches[key], msg)
			if len(batches[key]) >= b.config.BatchSize {
				b.flush(ctx, batches[key])
				delete(batches, key)
				ticker.Reset(b.config.FlushInterval)
			}
		case <-ticker.C:
			flushAll()
		}
	}
}

// flush sends the current batch to the uploader and handles Ack/Nack logic.
func (b *Batcher) flush(ctx context.Context, batch []*types.BatchedMessage[ArchivalData]) {
	if len(batch) == 0 {
		return
	}
	payloads := make([]*ArchivalData, len(batch))
	for i, msg := range batch {
		payloads[i] = msg.Payload
	}

	// REFACTOR: Use the configured timeout for the upload operation.
	uploadCtx, cancel := context.WithTimeout(ctx, b.config.UploadTimeout)
	defer cancel()

	if err := b.uploader.UploadBatch(uploadCtx, payloads); err != nil {
		b.logger.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to upload batch, Nacking messages.")
		for _, msg := range batch {
			if msg.OriginalMessage.Nack != nil {
				msg.OriginalMessage.Nack()
			}
		}
	} else {
		b.logger.Info().Int("batch_size", len(batch)).Str("batch_key", payloads[0].GetBatchKey()).Msg("Successfully uploaded batch, Acking messages.")
		for _, msg := range batch {
			if msg.OriginalMessage.Ack != nil {
				msg.OriginalMessage.Ack()
			}
		}
	}
}
