package bqstore_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/bqstore"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestBatcher is a helper to set up a batcher with a mock for testing.
func newTestBatcher(t *testing.T, batchSize int, flushInterval time.Duration) (*bqstore.BatchInserter[testPayload], *MockDataBatchInserter[testPayload]) {
	t.Helper()

	mockInserter := &MockDataBatchInserter[testPayload]{}
	config := &bqstore.BatchInserterConfig{
		BatchSize:     batchSize,
		FlushInterval: flushInterval,
		InsertTimeout: 2 * time.Second, // A short timeout for tests.
	}

	batcher := bqstore.NewBatcher[testPayload](config, mockInserter, zerolog.Nop())

	// Start the batcher and ensure it's stopped at the end of the test.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	batcher.Start(ctx)

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer stopCancel()
		assert.NoError(t, batcher.Stop(stopCtx))
	})

	return batcher, mockInserter
}

func TestBatchInserter_BatchSizeTrigger(t *testing.T) {
	batcher, mockInserter := newTestBatcher(t, 3, 10*time.Second)

	// Send 3 messages, which should trigger an immediate flush.
	for i := 0; i < 3; i++ {
		batcher.Input() <- &types.BatchedMessage[testPayload]{Payload: &testPayload{ID: i}}
	}

	require.Eventually(t, func() bool {
		return mockInserter.GetCallCount() == 1
	}, time.Second, 10*time.Millisecond, "InsertBatch should be called once")

	receivedBatches := mockInserter.GetReceivedItems()
	require.Len(t, receivedBatches, 1, "Should have received one batch")
	assert.Len(t, receivedBatches[0], 3, "The batch should contain 3 items")
}

func TestBatchInserter_FlushIntervalTrigger(t *testing.T) {
	flushInterval := 100 * time.Millisecond
	batcher, mockInserter := newTestBatcher(t, 10, flushInterval)

	// Send 2 messages, fewer than the batch size.
	for i := 0; i < 2; i++ {
		batcher.Input() <- &types.BatchedMessage[testPayload]{Payload: &testPayload{ID: i}}
	}

	require.Eventually(t, func() bool {
		return mockInserter.GetCallCount() == 1
	}, flushInterval*2, 10*time.Millisecond, "InsertBatch should be called once due to timeout")

	receivedBatches := mockInserter.GetReceivedItems()
	require.Len(t, receivedBatches, 1)
	assert.Len(t, receivedBatches[0], 2, "The batch should contain 2 items")
}

func TestBatchInserter_StopFlushesFinalBatch(t *testing.T) {
	mockInserter := &MockDataBatchInserter[testPayload]{}
	config := &bqstore.BatchInserterConfig{
		BatchSize:     10,
		FlushInterval: 5 * time.Second, // Long interval to ensure it doesn't trigger
		InsertTimeout: 2 * time.Second,
	}

	batcher := bqstore.NewBatcher[testPayload](config, mockInserter, zerolog.Nop())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	batcher.Start(ctx)

	// Send a partial batch.
	for i := 0; i < 4; i++ {
		batcher.Input() <- &types.BatchedMessage[testPayload]{Payload: &testPayload{ID: i}}
	}

	// FIX: Explicitly call Stop to trigger the final flush. Do not cancel the
	// worker context directly as that creates a race condition. The Stop method
	// is responsible for the full shutdown orchestration.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(stopCancel)
	err := batcher.Stop(stopCtx)
	require.NoError(t, err)

	assert.Equal(t, 1, mockInserter.GetCallCount(), "InsertBatch should be called on stop")
	receivedBatches := mockInserter.GetReceivedItems()
	require.Len(t, receivedBatches, 1)
	assert.Len(t, receivedBatches[0], 4, "The final batch should contain 4 items")
}

func TestBatchInserter_AckNackLogic(t *testing.T) {
	t.Run("acks messages on successful insert", func(t *testing.T) {
		batcher, mockInserter := newTestBatcher(t, 2, time.Second)
		mockInserter.InsertBatchFn = func(ctx context.Context, items []*testPayload) error {
			return nil // Success
		}

		var ackCount int32
		createMessage := func() *types.BatchedMessage[testPayload] {
			return &types.BatchedMessage[testPayload]{
				Payload: &testPayload{},
				OriginalMessage: types.ConsumedMessage{
					Ack: func() { atomic.AddInt32(&ackCount, 1) },
				},
			}
		}

		batcher.Input() <- createMessage()
		batcher.Input() <- createMessage()

		require.Eventually(t, func() bool {
			return atomic.LoadInt32(&ackCount) == 2
		}, time.Second, 10*time.Millisecond, "Should have acked 2 messages")
	})

	t.Run("nacks messages on failed insert", func(t *testing.T) {
		batcher, mockInserter := newTestBatcher(t, 2, time.Second)
		mockInserter.InsertBatchFn = func(ctx context.Context, items []*testPayload) error {
			return errors.New("bigquery insert failed") // Failure
		}

		var nackCount int32
		createMessage := func() *types.BatchedMessage[testPayload] {
			return &types.BatchedMessage[testPayload]{
				Payload: &testPayload{},
				OriginalMessage: types.ConsumedMessage{
					Nack: func() { atomic.AddInt32(&nackCount, 1) },
				},
			}
		}

		batcher.Input() <- createMessage()
		batcher.Input() <- createMessage()

		require.Eventually(t, func() bool {
			return atomic.LoadInt32(&nackCount) == 2
		}, time.Second, 10*time.Millisecond, "Should have nacked 2 messages")
	})
}
