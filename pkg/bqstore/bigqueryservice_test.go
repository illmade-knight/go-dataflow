package bqstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/bqstore"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestService helper is simplified to not use t.Cleanup, giving
// tests explicit control over the service lifecycle.
func setupTestService(t *testing.T, transformer messagepipeline.MessageTransformer[testPayload]) (
	*messagepipeline.ProcessingService[testPayload], *MockMessageConsumer, *MockDataBatchInserter[testPayload]) {
	t.Helper()

	logger := zerolog.Nop()
	mockConsumer := NewMockMessageConsumer(10)
	mockInserter := &MockDataBatchInserter[testPayload]{}

	batcherCfg := &bqstore.BatchInserterConfig{BatchSize: 10, FlushInterval: 10 * time.Second, InsertTimeout: time.Second}
	batcher := bqstore.NewBatcher[testPayload](batcherCfg, mockInserter, logger)

	service, err := bqstore.NewBigQueryService[testPayload](1, mockConsumer, batcher, transformer, logger)
	require.NoError(t, err)

	return service, mockConsumer, mockInserter
}

func TestBigQueryService_ProcessesMessagesSuccessfully(t *testing.T) {
	// Arrange
	// FIX: The transformer now accepts a context to match the updated interface.
	testTransformer := func(ctx context.Context, msg types.ConsumedMessage) (*testPayload, bool, error) {
		var p testPayload
		err := json.Unmarshal(msg.Payload, &p)
		return &p, false, err
	}
	service, mockConsumer, mockInserter := setupTestService(t, testTransformer)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	err := service.Start(ctx)
	require.NoError(t, err)

	payload, err := json.Marshal(&testPayload{ID: 101, Data: "hello"})
	require.NoError(t, err)

	var acked int32
	msg := types.ConsumedMessage{
		PublishMessage: types.PublishMessage{ID: "test-msg-1", Payload: payload},
		Ack:            func() { atomic.StoreInt32(&acked, 1) },
	}

	// Act
	mockConsumer.Push(msg)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(stopCancel)
	service.Stop(stopCtx)

	// Assert
	assert.Equal(t, 1, mockInserter.GetCallCount(), "Inserter should have been called once")
	receivedBatches := mockInserter.GetReceivedItems()
	require.Len(t, receivedBatches[0], 1, "Batch should have one item")
	assert.Equal(t, 101, receivedBatches[0][0].ID)
	assert.Equal(t, int32(1), atomic.LoadInt32(&acked), "Message should have been acked")
}

func TestBigQueryService_HandlesTransformerError(t *testing.T) {
	// Arrange
	// FIX: The transformer now accepts a context to match the updated interface.
	errorTransformer := func(ctx context.Context, msg types.ConsumedMessage) (*testPayload, bool, error) {
		return nil, false, errors.New("bad data")
	}
	service, mockConsumer, mockInserter := setupTestService(t, errorTransformer)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	err := service.Start(ctx)
	require.NoError(t, err)

	var nacked int32
	msg := types.ConsumedMessage{
		PublishMessage: types.PublishMessage{ID: "test-msg-2", Payload: []byte("this is not valid json")},
		Nack:           func() { atomic.StoreInt32(&nacked, 1) },
	}

	// Act
	mockConsumer.Push(msg)

	// Assert
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&nacked) == 1
	}, time.Second, 10*time.Millisecond, "Message should have been nacked on transform failure")
	assert.Equal(t, 0, mockInserter.GetCallCount(), "Inserter should not be called for a failed message")

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(stopCancel)
	service.Stop(stopCtx)
}

func TestBigQueryService_SkipsMessage(t *testing.T) {
	// Arrange
	// FIX: The transformer now accepts a context to match the updated interface.
	skipTransformer := func(ctx context.Context, msg types.ConsumedMessage) (*testPayload, bool, error) {
		return nil, true, nil // Signal to skip
	}
	service, mockConsumer, mockInserter := setupTestService(t, skipTransformer)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	err := service.Start(ctx)
	require.NoError(t, err)

	var acked int32
	msg := types.ConsumedMessage{
		PublishMessage: types.PublishMessage{ID: "test-msg-3", Payload: []byte("some data to be skipped")},
		Ack:            func() { atomic.StoreInt32(&acked, 1) },
	}

	// Act
	mockConsumer.Push(msg)

	// Assert
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&acked) == 1
	}, time.Second, 10*time.Millisecond, "Message should have been acked on skip")
	assert.Equal(t, 0, mockInserter.GetCallCount(), "Inserter should not be called for a skipped message")

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(stopCancel)
	service.Stop(stopCtx)
}
