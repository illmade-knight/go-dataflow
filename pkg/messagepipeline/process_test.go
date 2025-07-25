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
	// A standard transformer that succeeds.
	transformer := func(msg types.ConsumedMessage) (*T, bool, error) {
		payload := any(&processTestPayload{Data: string(msg.Payload)}).(*T)
		return payload, false, nil
	}

	service, err := messagepipeline.NewProcessingService[T](numWorkers, consumer, processor, transformer, zerolog.Nop())
	if err != nil {
		panic(err) // Should not happen with valid inputs.
	}
	return service, consumer, processor
}

// --- Test Cases ---

func TestProcessingService_Lifecycle(t *testing.T) {
	service, consumer, processor := newTestService[processTestPayload](1, 10, 10)

	// Provide a context for the service Start, which will be passed to consumer/processor
	serviceCtx, serviceCancel := context.WithCancel(context.Background())
	defer serviceCancel() // Ensure context is cancelled on test exit

	err := service.Start(serviceCtx) // Now starts with internal shutdownCtx
	require.NoError(t, err)

	// Give components a moment to start their goroutines before checking counts
	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, 1, consumer.GetStartCount())
	assert.Equal(t, 1, processor.GetStartCount())

	service.Stop()

	// Give components a moment to stop
	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, 1, consumer.GetStopCount())
	assert.Equal(t, 1, processor.GetStopCount())
}

func TestProcessingService_ProcessMessage_Success(t *testing.T) {
	service, consumer, processor := newTestService[processTestPayload](1, 10, 10)

	serviceCtx, serviceCancel := context.WithCancel(context.Background())
	defer serviceCancel()

	err := service.Start(serviceCtx)
	require.NoError(t, err)
	defer service.Stop()

	// Ensure the processor will call Ack/Nack (as the ProcessingService now manages it)
	processor.SetAckOnProcess(true) // Processor mock now responsible for Ack/Nack on messages it processes.

	ackCalled, nackCalled := false, false
	var ackNackMu sync.Mutex // Initialized correctly
	msg := types.ConsumedMessage{
		PublishMessage: types.PublishMessage{
			ID:      "test-msg-1",
			Payload: []byte("original"),
		},
		Ack:  func() { ackNackMu.Lock(); ackCalled = true; ackNackMu.Unlock() },
		Nack: func() { ackNackMu.Lock(); nackCalled = true; ackNackMu.Unlock() },
	}
	consumer.Push(msg)

	require.Eventually(t, func() bool {
		return len(processor.GetReceived()) > 0
	}, time.Second, 10*time.Millisecond, "Processor did not receive message in time")

	received := processor.GetReceived()
	assert.Equal(t, "original", received[0].Payload.Data)

	require.Eventually(t, func() bool { // Wait for Ack to be called by processor mock
		ackNackMu.Lock()
		defer ackNackMu.Unlock()
		return ackCalled
	}, time.Second, 10*time.Millisecond, "Ack was not called for successful message")
	assert.False(t, nackCalled, "Nack should not be called")
}

// CORRECTED: This test now correctly injects the failing transformer at construction time.
func TestProcessingService_ProcessMessage_TransformerError(t *testing.T) {
	// Arrange
	consumer := NewMockMessageConsumer(10)
	processor := NewMockMessageProcessor[processTestPayload](10)
	failingTransformer := func(msg types.ConsumedMessage) (*processTestPayload, bool, error) {
		return nil, false, errors.New("transformation failed")
	}

	service, err := messagepipeline.NewProcessingService[processTestPayload](1, consumer, processor, failingTransformer, zerolog.Nop())
	require.NoError(t, err)

	serviceCtx, serviceCancel := context.WithCancel(context.Background())
	defer serviceCancel()

	err = service.Start(serviceCtx)
	require.NoError(t, err)
	defer service.Stop()

	nackCalled := false
	var nackMu sync.Mutex // Initialized correctly
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
}

// CORRECTED: This test is simplified and focuses on the skip logic.
func TestProcessingService_ProcessMessage_Skip(t *testing.T) {
	// Arrange
	consumer := NewMockMessageConsumer(10)
	processor := NewMockMessageProcessor[processTestPayload](10)
	skippingTransformer := func(msg types.ConsumedMessage) (*processTestPayload, bool, error) {
		return nil, true, nil // Signal to skip
	}

	service, err := messagepipeline.NewProcessingService[processTestPayload](1, consumer, processor, skippingTransformer, zerolog.Nop())
	require.NoError(t, err)

	serviceCtx, serviceCancel := context.WithCancel(context.Background())
	defer serviceCancel()

	err = service.Start(serviceCtx)
	require.NoError(t, err)
	defer service.Stop()

	ackCalled := false
	var ackMu sync.Mutex // Initialized correctly
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
}
