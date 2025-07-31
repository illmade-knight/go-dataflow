package icestore

import (
	"context"
	"sync"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
)

// flushFunc is the function that the KeyAwareBatcher will call with a batch of
// items that all share the same batch key.
type flushFunc func(ctx context.Context, batch []messagepipeline.ProcessableItem[ArchivalData])

// KeyAwareBatcherConfig holds configuration for the KeyAwareBatcher.
type KeyAwareBatcherConfig struct {
	// BatchSize is the number of items for a single key to accumulate before flushing.
	BatchSize int
	// FlushInterval is the maximum amount of time a batch can be held before being flushed,
	// regardless of its size.
	FlushInterval time.Duration
}

// KeyAwareBatcher is a stateful component that receives individual items, groups
// them in-memory into batches based on their `BatchKey`, and flushes these batches
// to a provided flush function. A flush is triggered for a specific key when its
// batch reaches `BatchSize`, or for all pending batches when `FlushInterval` expires.
type KeyAwareBatcher struct {
	cfg       KeyAwareBatcherConfig
	logger    zerolog.Logger
	flushFn   flushFunc
	batches   map[string][]messagepipeline.ProcessableItem[ArchivalData]
	mu        sync.Mutex
	wg        sync.WaitGroup
	inputChan chan messagepipeline.ProcessableItem[ArchivalData]
}

// NewKeyAwareBatcher creates a new batcher that groups items by key.
func NewKeyAwareBatcher(cfg KeyAwareBatcherConfig, flusher flushFunc, logger zerolog.Logger) *KeyAwareBatcher {
	return &KeyAwareBatcher{
		cfg:       cfg,
		logger:    logger,
		flushFn:   flusher,
		batches:   make(map[string][]messagepipeline.ProcessableItem[ArchivalData]),
		inputChan: make(chan messagepipeline.ProcessableItem[ArchivalData], cfg.BatchSize*2),
	}
}

// Start begins the worker goroutine that manages batching and timed flushes.
func (b *KeyAwareBatcher) Start(ctx context.Context) {
	b.wg.Add(1)
	go b.worker(ctx)
}

// Add submits a new item to be batched. This method is safe for concurrent use.
func (b *KeyAwareBatcher) Add(item messagepipeline.ProcessableItem[ArchivalData]) {
	b.inputChan <- item
}

// Stop gracefully shuts down the batcher. It closes the input channel and waits
// for the worker to process and flush all pending items.
func (b *KeyAwareBatcher) Stop() {
	close(b.inputChan)
	b.wg.Wait()
}

// worker contains the core key-aware batching logic.
func (b *KeyAwareBatcher) worker(ctx context.Context) {
	defer b.wg.Done()
	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()

	flushAll := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if len(b.batches) == 0 {
			return
		}
		b.logger.Info().Int("key_count", len(b.batches)).Msg("Flushing all pending batches due to timer or shutdown.")
		for key, batchToFlush := range b.batches {
			b.flushFn(ctx, batchToFlush)
			delete(b.batches, key)
		}
	}

	for {
		select {
		case item, ok := <-b.inputChan:
			if !ok { // Channel closed, do a final flush and exit.
				flushAll()
				return
			}

			key := item.Payload.GetBatchKey()
			b.mu.Lock()
			b.batches[key] = append(b.batches[key], item)
			batchForKey := b.batches[key]

			if len(batchForKey) >= b.cfg.BatchSize {
				b.logger.Info().Str("key", key).Int("batch_size", len(batchForKey)).Msg("Flushing batch due to size.")
				b.flushFn(ctx, batchForKey)
				delete(b.batches, key)
				ticker.Reset(b.cfg.FlushInterval)
			}
			b.mu.Unlock()

		case <-ticker.C:
			flushAll()
		}
	}
}
