package icestore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockFinalUploader is a mock implementation of the DataUploader interface.
type mockFinalUploader struct {
	sync.Mutex
	UploadBatchFn func(ctx context.Context, items []*ArchivalData) error
	callCount     int
	receivedItems [][]*ArchivalData
}

func (m *mockFinalUploader) UploadBatch(ctx context.Context, items []*ArchivalData) error {
	m.Lock()
	defer m.Unlock()
	m.callCount++
	m.receivedItems = append(m.receivedItems, items)
	if m.UploadBatchFn != nil {
		return m.UploadBatchFn(ctx, items)
	}
	return nil
}
func (m *mockFinalUploader) Close() error { return nil }
func (m *mockFinalUploader) GetCallCount() int {
	m.Lock()
	defer m.Unlock()
	return m.callCount
}
func (m *mockFinalUploader) GetReceivedItems() [][]*ArchivalData {
	m.Lock()
	defer m.Unlock()
	return m.receivedItems
}

// newTestIceBatcher is a helper to set up a batcher with a mock for testing.
func newTestIceBatcher(t *testing.T, batchSize int, flushInterval time.Duration) (*Batcher, *mockFinalUploader) {
	t.Helper()

	mockUploader := &mockFinalUploader{}
	config := &BatcherConfig{
		BatchSize:     batchSize,
		FlushInterval: flushInterval,
		UploadTimeout: 2 * time.Second,
	}

	batcher := NewBatcher(config, mockUploader, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	batcher.Start(ctx)

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer stopCancel()
		assert.NoError(t, batcher.Stop(stopCtx))
	})
	return batcher, mockUploader
}

func TestBatcher_BatchSizeTrigger(t *testing.T) {
	batcher, mockUploader := newTestIceBatcher(t, 3, 10*time.Second)

	for i := 0; i < 3; i++ {
		batcher.Input() <- &types.BatchedMessage[ArchivalData]{Payload: &ArchivalData{}}
	}

	// REFACTOR: Use Eventually to wait for the flush instead of time.Sleep.
	require.Eventually(t, func() bool {
		return mockUploader.GetCallCount() == 1
	}, time.Second, 10*time.Millisecond, "UploadBatch should be called once")

	receivedBatches := mockUploader.GetReceivedItems()
	require.Len(t, receivedBatches, 1)
	assert.Len(t, receivedBatches[0], 3, "The batch should contain 3 items")
}

func TestBatcher_FlushIntervalTrigger(t *testing.T) {
	flushInterval := 100 * time.Millisecond
	batcher, mockUploader := newTestIceBatcher(t, 10, flushInterval)

	for i := 0; i < 2; i++ {
		batcher.Input() <- &types.BatchedMessage[ArchivalData]{Payload: &ArchivalData{}}
	}

	// REFACTOR: Use Eventually to wait for the flush triggered by the interval.
	require.Eventually(t, func() bool {
		return mockUploader.GetCallCount() == 1
	}, flushInterval*2, 10*time.Millisecond, "UploadBatch should be called once due to timeout")

	receivedBatches := mockUploader.GetReceivedItems()
	require.Len(t, receivedBatches, 1)
	assert.Len(t, receivedBatches[0], 2, "The batch should contain 2 items")
}

func TestBatcher_StopFlushesFinalBatch(t *testing.T) {
	mockUploader := &mockFinalUploader{}
	config := &BatcherConfig{
		BatchSize:     10,
		FlushInterval: 5 * time.Second,
		UploadTimeout: 2 * time.Second,
	}

	batcher := NewBatcher(config, mockUploader, zerolog.Nop())
	batcher.Start(context.Background())

	for i := 0; i < 4; i++ {
		batcher.Input() <- &types.BatchedMessage[ArchivalData]{Payload: &ArchivalData{}}
	}

	// REFACTOR: Explicitly call Stop to trigger the final flush.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(stopCancel)
	err := batcher.Stop(stopCtx)
	require.NoError(t, err)

	assert.Equal(t, 1, mockUploader.GetCallCount(), "UploadBatch should be called on stop")
	receivedBatches := mockUploader.GetReceivedItems()
	require.Len(t, receivedBatches, 1)
	assert.Len(t, receivedBatches[0], 4, "The final batch should contain 4 items")
}

func TestBatcher_AckNackLogic(t *testing.T) {
	createMessage := func() (*types.BatchedMessage[ArchivalData], *uint32, *uint32) {
		var ackCount, nackCount uint32
		msg := &types.BatchedMessage[ArchivalData]{
			Payload: &ArchivalData{},
			OriginalMessage: types.ConsumedMessage{
				Ack:  func() { atomic.AddUint32(&ackCount, 1) },
				Nack: func() { atomic.AddUint32(&nackCount, 1) },
			},
		}
		return msg, &ackCount, &nackCount
	}

	t.Run("acks messages on successful upload", func(t *testing.T) {
		batcher, mockUploader := newTestIceBatcher(t, 1, time.Second)
		mockUploader.UploadBatchFn = func(ctx context.Context, items []*ArchivalData) error {
			return nil // Success
		}

		msg, ack, nack := createMessage()
		batcher.Input() <- msg

		require.Eventually(t, func() bool { return atomic.LoadUint32(ack) == 1 }, time.Second, 10*time.Millisecond)
		assert.Equal(t, uint32(0), atomic.LoadUint32(nack))
	})

	t.Run("nacks messages on failed upload", func(t *testing.T) {
		batcher, mockUploader := newTestIceBatcher(t, 1, time.Second)
		mockUploader.UploadBatchFn = func(ctx context.Context, items []*ArchivalData) error {
			return errors.New("gcs upload failed") // Failure
		}

		msg, ack, nack := createMessage()
		batcher.Input() <- msg

		require.Eventually(t, func() bool { return atomic.LoadUint32(nack) == 1 }, time.Second, 10*time.Millisecond)
		assert.Equal(t, uint32(0), atomic.LoadUint32(ack))
	})
}
