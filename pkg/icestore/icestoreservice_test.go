package icestore

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupIceStoreService is a helper to initialize a service with mocks for unit testing.
func setupIceStoreService(t *testing.T, transformer messagepipeline.MessageTransformer[ArchivalData]) (
	*messagepipeline.ProcessingService[ArchivalData], *MockMessageConsumer, *mockFinalUploader) {
	t.Helper()

	logger := zerolog.Nop()
	mockUploader := &mockFinalUploader{}
	mockConsumer := NewMockMessageConsumer(10)

	batcherCfg := &BatcherConfig{BatchSize: 1, FlushInterval: time.Second, UploadTimeout: time.Second}
	batcher := NewBatcher(batcherCfg, mockUploader, logger)

	service, err := NewIceStorageService(1, mockConsumer, batcher, transformer, logger)
	require.NoError(t, err)

	return service, mockConsumer, mockUploader
}

// TestIceStorageService_Success verifies the happy path.
func TestIceStorageService_Success(t *testing.T) {
	// Arrange
	service, mockConsumer, mockUploader := setupIceStoreService(t, ArchivalTransformer)

	serviceCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	err := service.Start(serviceCtx)
	require.NoError(t, err)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer stopCancel()
		service.Stop(stopCtx)
	})

	var acked int32
	msg := types.ConsumedMessage{
		PublishMessage: types.PublishMessage{
			ID:      "test-id-123",
			Payload: []byte(`{"data":"test"}`),
		},
		Ack: func() { atomic.AddInt32(&acked, 1) },
	}

	// Act
	mockConsumer.Push(msg)

	// Assert
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&acked) == 1
	}, time.Second, 10*time.Millisecond, "Message should have been acked")

	receivedBatches := mockUploader.GetReceivedItems()
	require.Len(t, receivedBatches, 1)
	require.Len(t, receivedBatches[0], 1)
	assert.Equal(t, "test-id-123", receivedBatches[0][0].ID, "ArchivalData.ID should match the original message ID")
}

// TestIceStorageService_TransformerError verifies a Nack on transformation failure.
func TestIceStorageService_TransformerError(t *testing.T) {
	// Arrange
	// FIX: The transformer now accepts a context to match the updated interface.
	errorTransformer := func(ctx context.Context, msg types.ConsumedMessage) (*ArchivalData, bool, error) {
		return nil, false, errors.New("transformation failed")
	}
	service, mockConsumer, mockUploader := setupIceStoreService(t, errorTransformer)

	serviceCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	err := service.Start(serviceCtx)
	require.NoError(t, err)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer stopCancel()
		service.Stop(stopCtx)
	})

	var nacked int32
	msg := types.ConsumedMessage{
		PublishMessage: types.PublishMessage{ID: "ice-msg-err"},
		Nack:           func() { atomic.AddInt32(&nacked, 1) },
	}

	// Act
	mockConsumer.Push(msg)

	// Assert
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&nacked) == 1
	}, time.Second, 10*time.Millisecond, "timed out waiting for message to be nacked")

	assert.Equal(t, 0, mockUploader.GetCallCount(), "Uploader should not be called")
}
