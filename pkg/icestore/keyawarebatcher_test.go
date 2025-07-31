package icestore_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/icestore"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockFlusher is a test helper to inspect the batches received by the flush function.
type mockFlusher struct {
	mu      sync.Mutex
	flushed map[string][][]messagepipeline.ProcessableItem[icestore.ArchivalData]
}

func newMockFlusher() *mockFlusher {
	return &mockFlusher{
		flushed: make(map[string][][]messagepipeline.ProcessableItem[icestore.ArchivalData]),
	}
}

func (m *mockFlusher) flush(_ context.Context, batch []messagepipeline.ProcessableItem[icestore.ArchivalData]) {
	if len(batch) == 0 {
		return
	}
	key := batch[0].Payload.GetBatchKey()
	m.mu.Lock()
	m.flushed[key] = append(m.flushed[key], batch)
	m.mu.Unlock()
}

func (m *mockFlusher) getFlushCount(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.flushed[key])
}

func TestKeyAwareBatcher(t *testing.T) {
	ctx := context.Background()

	createItem := func(id, key string) messagepipeline.ProcessableItem[icestore.ArchivalData] {
		return messagepipeline.ProcessableItem[icestore.ArchivalData]{
			Original: messagepipeline.Message{MessageData: messagepipeline.MessageData{ID: id}},
			Payload:  &icestore.ArchivalData{ID: id, BatchKey: key},
		}
	}

	t.Run("flushes when batch size is reached", func(t *testing.T) {
		flusher := newMockFlusher()
		cfg := icestore.KeyAwareBatcherConfig{BatchSize: 2, FlushInterval: time.Minute}
		batcher := icestore.NewKeyAwareBatcher(cfg, flusher.flush, zerolog.Nop())
		batcher.Start(ctx)
		defer batcher.Stop()

		batcher.Add(createItem("a1", "key-a"))
		batcher.Add(createItem("b1", "key-b"))
		assert.Equal(t, 0, flusher.getFlushCount("key-a"), "Should not flush before batch is full")

		batcher.Add(createItem("a2", "key-a")) // This should trigger a flush for key-a

		require.Eventually(t, func() bool {
			return flusher.getFlushCount("key-a") == 1
		}, time.Second, 10*time.Millisecond, "Batch for key-a should have flushed")
		assert.Equal(t, 0, flusher.getFlushCount("key-b"), "Batch for key-b should not have flushed")
	})

	t.Run("flushes when interval timer fires", func(t *testing.T) {
		flusher := newMockFlusher()
		cfg := icestore.KeyAwareBatcherConfig{BatchSize: 10, FlushInterval: 100 * time.Millisecond}
		batcher := icestore.NewKeyAwareBatcher(cfg, flusher.flush, zerolog.Nop())
		batcher.Start(ctx)
		defer batcher.Stop()

		batcher.Add(createItem("a1", "key-a"))
		batcher.Add(createItem("b1", "key-b"))

		require.Eventually(t, func() bool {
			// Both keys should be flushed by the timer.
			return flusher.getFlushCount("key-a") == 1 && flusher.getFlushCount("key-b") == 1
		}, time.Second, 10*time.Millisecond, "All batches should have been flushed by the interval timer")
	})

	t.Run("flushes pending batches on stop", func(t *testing.T) {
		flusher := newMockFlusher()
		cfg := icestore.KeyAwareBatcherConfig{BatchSize: 10, FlushInterval: time.Minute}
		batcher := icestore.NewKeyAwareBatcher(cfg, flusher.flush, zerolog.Nop())
		batcher.Start(ctx)

		batcher.Add(createItem("a1", "key-a"))
		batcher.Add(createItem("b1", "key-b"))

		batcher.Stop() // Stop should trigger a final flush.

		assert.Equal(t, 1, flusher.getFlushCount("key-a"), "Pending batch for key-a should be flushed on stop")
		assert.Equal(t, 1, flusher.getFlushCount("key-b"), "Pending batch for key-b should be flushed on stop")
	})
}
