package messagepipeline_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test Payload ---
type processTestPayload struct {
	Data string
}

// --- Helper Functions ---

// newTestService is a helper to create a ProcessingService with standard mocks for testing.
func newTestService[T any](numWorkers, consumerBuffer, processorBuffer int) (*messagepipeline.ProcessingService[T], *MockMessageConsumer, *MockMessageProcessor[T]) {
	consumer := NewMockMessageConsumer(consumerBuffer)
	processor := NewMockMessageProcessor[T](processorBuffer)

	// FIX: The transformer now accepts a context.Context to match the updated interface.
	transformer := func(ctx context.Context, msg types.ConsumedMessage) (*T, bool, error) {
		payload := any(&processTestPayload{Data: string(msg.Payload)}).(*T)
		return payload, false, nil
	}

	service, err := messagepipeline.NewProcessingService[T](numWorkers, consumer, processor, transformer, zerolog.Nop())
	if err != nil {
		panic(err)
	}
	return service, consumer, processor
}

// --- Test Cases ---

func TestProcessingService_Lifecycle(t *testing.T) {
	service, consumer, processor := newTestService[processTestPayload](1, 10, 10)

	serviceCtx, serviceCancel := context.WithCancel(context.Background())
	t.Cleanup(serviceCancel)

	err := service.Start(serviceCtx)
	require.NoError(t, err)

	assert.Equal(t, 1, consumer.GetStartCount())
	assert.Equal(t, 1, processor.GetStartCount())

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(stopCancel)
	service.Stop(stopCtx)

	assert.Equal(t, 1, consumer.GetStopCount())
	assert.Equal(t, 1, processor.GetStopCount())
}

func TestProcessingService_ProcessMessage_Success(t *testing.T) {
	service, consumer, processor := newTestService[processTestPayload](1, 10, 10)

	serviceCtx, serviceCancel := context.WithCancel(context.Background())
	t.Cleanup(serviceCancel)

	err := service.Start(serviceCtx)
	require.NoError(t, err)

	processor.SetAckOnProcess(true)

	ackCalled := false
	var ackMu sync.Mutex
	msg := types.ConsumedMessage{
		PublishMessage: types.PublishMessage{
			ID:      "test-msg-1",
			Payload: []byte("original"),
		},
		Ack: func() {
			ackMu.Lock()
			ackCalled = true
			ackMu.Unlock()
		},
	}
	consumer.Push(msg)

	require.Eventually(t, func() bool {
		return len(processor.GetReceived()) > 0
	}, time.Second, 10*time.Millisecond, "Processor did not receive message in time")

	received := processor.GetReceived()
	assert.Equal(t, "original", received[0].Payload.Data)

	require.Eventually(t, func() bool {
		ackMu.Lock()
		defer ackMu.Unlock()
		return ackCalled
	}, time.Second, 10*time.Millisecond, "Ack was not called for successful message")

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(stopCancel)
	service.Stop(stopCtx)
}

func TestProcessingService_ProcessMessage_TransformerError(t *testing.T) {
	// Arrange
	consumer := NewMockMessageConsumer(10)
	processor := NewMockMessageProcessor[processTestPayload](10)
	// FIX: The transformer now accepts a context.Context.
	failingTransformer := func(ctx context.Context, msg types.ConsumedMessage) (*processTestPayload, bool, error) {
		return nil, false, errors.New("transformation failed")
	}

	service, err := messagepipeline.NewProcessingService[processTestPayload](1, consumer, processor, failingTransformer, zerolog.Nop())
	require.NoError(t, err)

	serviceCtx, serviceCancel := context.WithCancel(context.Background())
	t.Cleanup(serviceCancel)

	err = service.Start(serviceCtx)
	require.NoError(t, err)

	nackCalled := false
	var nackMu sync.Mutex
	msg := types.ConsumedMessage{
		PublishMessage: types.PublishMessage{ID: "test-msg-err"},
		Nack:           func() { nackMu.Lock(); nackCalled = true; nackMu.Unlock() },
	}

	// Act
	consumer.Push(msg)

	// Assert
	assert.Eventually(t, func() bool {
		nackMu.Lock()
		defer nackMu.Unlock()
		return nackCalled
	}, time.Second, 10*time.Millisecond, "Nack was not called on transformer error")

	assert.Empty(t, processor.GetReceived(), "Processor should not receive any message on transformer error")

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(stopCancel)
	service.Stop(stopCtx)
}

func TestProcessingService_ProcessMessage_Skip(t *testing.T) {
	// Arrange
	consumer := NewMockMessageConsumer(10)
	processor := NewMockMessageProcessor[processTestPayload](10)
	// FIX: The transformer now accepts a context.Context.
	skippingTransformer := func(ctx context.Context, msg types.ConsumedMessage) (*processTestPayload, bool, error) {
		return nil, true, nil // Signal to skip
	}

	service, err := messagepipeline.NewProcessingService[processTestPayload](1, consumer, processor, skippingTransformer, zerolog.Nop())
	require.NoError(t, err)

	serviceCtx, serviceCancel := context.WithCancel(context.Background())
	t.Cleanup(serviceCancel)

	err = service.Start(serviceCtx)
	require.NoError(t, err)

	ackCalled := false
	var ackMu sync.Mutex
	msg := types.ConsumedMessage{
		PublishMessage: types.PublishMessage{ID: "test-msg-skip"},
		Ack:            func() { ackMu.Lock(); ackCalled = true; ackMu.Unlock() },
	}

	// Act
	consumer.Push(msg)

	// Assert
	assert.Eventually(t, func() bool {
		ackMu.Lock()
		defer ackMu.Unlock()
		return ackCalled
	}, time.Second, 10*time.Millisecond, "Ack was not called on skip")

	assert.Empty(t, processor.GetReceived(), "Processor should not receive any message on skip")

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(stopCancel)
	service.Stop(stopCtx)
}
