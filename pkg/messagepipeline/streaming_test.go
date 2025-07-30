package messagepipeline_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test Payload & Mocks ---

type streamTestPayload struct {
	Data string
}

// newTestStreamingService is a helper to create a StreamingService with mocks for testing.
func newTestStreamingService(
	t *testing.T,
	cfg messagepipeline.StreamingServiceConfig,
	processor messagepipeline.StreamProcessor[streamTestPayload],
) (*messagepipeline.StreamingService[streamTestPayload], *MockMessageConsumer) {
	consumer := NewMockMessageConsumer(10)
	t.Cleanup(func() {
		// Ensure channel is closed to avoid test hangs if Stop isn't called.
		consumer.Close()
	})

	transformer := func(ctx context.Context, msg *messagepipeline.Message) (*streamTestPayload, bool, error) {
		if string(msg.Payload) == "skip" {
			return nil, true, nil
		}
		if string(msg.Payload) == "transform_error" {
			return nil, false, errors.New("transformation failed")
		}
		return &streamTestPayload{Data: string(msg.Payload)}, false, nil
	}

	service, err := messagepipeline.NewStreamingService[streamTestPayload](cfg, consumer, transformer, processor, zerolog.Nop())
	require.NoError(t, err)
	return service, consumer
}

// --- Test Cases ---

func TestStreamingService_Lifecycle(t *testing.T) {
	// Arrange
	var processorCalled atomic.Int32
	processor := func(ctx context.Context, original messagepipeline.Message, payload *streamTestPayload) error {
		processorCalled.Add(1)
		return nil
	}

	service, consumer := newTestStreamingService(t, messagepipeline.StreamingServiceConfig{NumWorkers: 1}, processor)

	serviceCtx, serviceCancel := context.WithCancel(context.Background())
	defer serviceCancel()

	// Act
	err := service.Start(serviceCtx)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, 1, consumer.GetStartCount())

	// Act
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	err = service.Stop(stopCtx)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, 1, consumer.GetStopCount())
}

func TestStreamingService_ProcessMessage_Success(t *testing.T) {
	// Arrange
	var processorCalled atomic.Int32
	var receivedPayload *streamTestPayload
	var mu sync.Mutex

	processor := func(ctx context.Context, original messagepipeline.Message, payload *streamTestPayload) error {
		mu.Lock()
		receivedPayload = payload
		mu.Unlock()
		processorCalled.Add(1)
		return nil
	}

	service, consumer := newTestStreamingService(t, messagepipeline.StreamingServiceConfig{NumWorkers: 1}, processor)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := service.Start(ctx)
	require.NoError(t, err)

	var ackCalled atomic.Bool
	msg := messagepipeline.Message{
		MessageData: messagepipeline.MessageData{
			ID:      "test-msg-1",
			Payload: []byte("original"),
		},
		Ack:  func() { ackCalled.Store(true) },
		Nack: func() { t.Error("Nack was called unexpectedly") },
	}

	// Act
	consumer.Push(msg)

	// Assert
	require.Eventually(t, func() bool {
		return processorCalled.Load() == 1
	}, time.Second, 10*time.Millisecond, "Processor was not called in time")

	mu.Lock()
	assert.Equal(t, "original", receivedPayload.Data)
	mu.Unlock()

	require.Eventually(t, ackCalled.Load, time.Second, 10*time.Millisecond, "Ack was not called")
}

func TestStreamingService_ProcessMessage_TransformerError(t *testing.T) {
	// Arrange
	processor := func(ctx context.Context, original messagepipeline.Message, payload *streamTestPayload) error {
		t.Error("Processor should not be called when transformer fails")
		return nil
	}

	service, consumer := newTestStreamingService(t, messagepipeline.StreamingServiceConfig{NumWorkers: 1}, processor)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := service.Start(ctx)
	require.NoError(t, err)

	var nackCalled atomic.Bool
	msg := messagepipeline.Message{
		MessageData: messagepipeline.MessageData{ID: "test-msg-err", Payload: []byte("transform_error")},
		Ack:         func() { t.Error("Ack was called unexpectedly") },
		Nack:        func() { nackCalled.Store(true) },
	}

	// Act
	consumer.Push(msg)

	// Assert
	require.Eventually(t, nackCalled.Load, time.Second, 10*time.Millisecond, "Nack was not called on transformer error")
}

func TestStreamingService_ProcessMessage_Skip(t *testing.T) {
	// Arrange
	processor := func(ctx context.Context, original messagepipeline.Message, payload *streamTestPayload) error {
		t.Error("Processor should not be called for a skipped message")
		return nil
	}

	service, consumer := newTestStreamingService(t, messagepipeline.StreamingServiceConfig{NumWorkers: 1}, processor)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := service.Start(ctx)
	require.NoError(t, err)

	var ackCalled atomic.Bool
	msg := messagepipeline.Message{
		MessageData: messagepipeline.MessageData{ID: "test-msg-skip", Payload: []byte("skip")},
		Ack:         func() { ackCalled.Store(true) },
		Nack:        func() { t.Error("Nack was called unexpectedly") },
	}

	// Act
	consumer.Push(msg)

	// Assert
	require.Eventually(t, ackCalled.Load, time.Second, 10*time.Millisecond, "Ack was not called on skip")
}

func TestStreamingService_ProcessMessage_ProcessorError(t *testing.T) {
	// Arrange
	processor := func(ctx context.Context, original messagepipeline.Message, payload *streamTestPayload) error {
		return errors.New("processing failed")
	}

	service, consumer := newTestStreamingService(t, messagepipeline.StreamingServiceConfig{NumWorkers: 1}, processor)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := service.Start(ctx)
	require.NoError(t, err)

	var nackCalled atomic.Bool
	msg := messagepipeline.Message{
		MessageData: messagepipeline.MessageData{ID: "test-msg-proc-err", Payload: []byte("process_me")},
		Ack:         func() { t.Error("Ack was called unexpectedly") },
		Nack:        func() { nackCalled.Store(true) },
	}

	// Act
	consumer.Push(msg)

	// Assert
	require.Eventually(t, nackCalled.Load, time.Second, 10*time.Millisecond, "Nack was not called on processor error")
}

// MockMessageConsumer is a mock implementation of the MessageConsumer interface for testing.
type MockMessageConsumer struct {
	msgChan    chan messagepipeline.Message
	startCount int
	stopCount  int
	mu         sync.Mutex
	closeOnce  sync.Once
}

func NewMockMessageConsumer(bufferSize int) *MockMessageConsumer {
	return &MockMessageConsumer{
		msgChan: make(chan messagepipeline.Message, bufferSize),
	}
}
func (m *MockMessageConsumer) Push(msg messagepipeline.Message) {
	m.msgChan <- msg
}
func (m *MockMessageConsumer) Close() {
	m.closeOnce.Do(func() {
		close(m.msgChan)
	})
}
func (m *MockMessageConsumer) Messages() <-chan messagepipeline.Message {
	return m.msgChan
}
func (m *MockMessageConsumer) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCount++
	return nil
}
func (m *MockMessageConsumer) Stop(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCount++
	m.Close()
	return nil
}
func (m *MockMessageConsumer) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
func (m *MockMessageConsumer) GetStartCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startCount
}
func (m *MockMessageConsumer) GetStopCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopCount
}
