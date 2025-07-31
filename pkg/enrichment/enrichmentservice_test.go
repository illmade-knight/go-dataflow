package enrichment_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/enrichment"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mocks & Test Setup ---

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
func (m *MockMessageConsumer) Push(msg messagepipeline.Message)         { m.msgChan <- msg }
func (m *MockMessageConsumer) Close()                                   { m.closeOnce.Do(func() { close(m.msgChan) }) }
func (m *MockMessageConsumer) Messages() <-chan messagepipeline.Message { return m.msgChan }
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

// newTestEnrichmentService is a helper to create an EnrichmentService with mocks for testing.
func newTestEnrichmentService(t *testing.T, enricher enrichment.MessageEnricher, processor messagepipeline.MessageProcessor) (*enrichment.EnrichmentService, *MockMessageConsumer) {
	consumer := NewMockMessageConsumer(10)
	t.Cleanup(consumer.Close)

	service, err := enrichment.NewEnrichmentService(
		enrichment.EnrichmentServiceConfig{NumWorkers: 1},
		enricher,
		consumer,
		processor,
		zerolog.Nop(),
	)
	require.NoError(t, err)
	return service, consumer
}

// --- Test Cases ---

func TestEnrichmentService_Lifecycle(t *testing.T) {
	// Arrange
	service, consumer := newTestEnrichmentService(t,
		func(ctx context.Context, msg *messagepipeline.Message) (bool, error) { return false, nil },
		func(ctx context.Context, msg *messagepipeline.Message) error { return nil },
	)
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

func TestEnrichmentService_ProcessMessage_Success(t *testing.T) {
	// Arrange
	var processorCalled atomic.Bool
	enricher := func(ctx context.Context, msg *messagepipeline.Message) (bool, error) {
		msg.EnrichmentData = map[string]interface{}{"enriched": true}
		return false, nil
	}
	processor := func(ctx context.Context, msg *messagepipeline.Message) error {
		processorCalled.Store(true)
		assert.True(t, msg.EnrichmentData["enriched"].(bool))
		return nil
	}
	service, consumer := newTestEnrichmentService(t, enricher, processor)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := service.Start(ctx)
	require.NoError(t, err)

	var ackCalled atomic.Bool
	msg := messagepipeline.Message{
		MessageData: messagepipeline.MessageData{ID: "test-msg-1"},
		Ack:         func() { ackCalled.Store(true) },
		Nack:        func() { t.Error("Nack was called unexpectedly") },
	}

	// Act
	consumer.Push(msg)

	// Assert
	require.Eventually(t, processorCalled.Load, time.Second, 10*time.Millisecond, "Processor was not called")
	require.Eventually(t, ackCalled.Load, time.Second, 10*time.Millisecond, "Ack was not called")
}

func TestEnrichmentService_ProcessMessage_EnricherSkips(t *testing.T) {
	// Arrange
	var processorCalled atomic.Bool
	enricher := func(ctx context.Context, msg *messagepipeline.Message) (bool, error) {
		return true, nil // Signal to skip
	}
	processor := func(ctx context.Context, msg *messagepipeline.Message) error {
		processorCalled.Store(true)
		return nil
	}
	service, consumer := newTestEnrichmentService(t, enricher, processor)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := service.Start(ctx)
	require.NoError(t, err)

	var ackCalled atomic.Bool
	msg := messagepipeline.Message{
		MessageData: messagepipeline.MessageData{ID: "test-msg-skip"},
		Ack:         func() { ackCalled.Store(true) },
		Nack:        func() { t.Error("Nack was called unexpectedly") },
	}

	// Act
	consumer.Push(msg)

	// Assert
	require.Eventually(t, ackCalled.Load, time.Second, 10*time.Millisecond, "Ack was not called on skip")
	assert.False(t, processorCalled.Load(), "Processor should not be called when message is skipped")
}

func TestEnrichmentService_ProcessMessage_ProcessorFails(t *testing.T) {
	// Arrange
	enricher := func(ctx context.Context, msg *messagepipeline.Message) (bool, error) {
		return false, nil
	}
	processor := func(ctx context.Context, msg *messagepipeline.Message) error {
		return errors.New("downstream failure")
	}
	service, consumer := newTestEnrichmentService(t, enricher, processor)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := service.Start(ctx)
	require.NoError(t, err)

	var nackCalled atomic.Bool
	msg := messagepipeline.Message{
		MessageData: messagepipeline.MessageData{ID: "test-msg-fail"},
		Ack:         func() { t.Error("Ack was called unexpectedly") },
		Nack:        func() { nackCalled.Store(true) },
	}

	// Act
	consumer.Push(msg)

	// Assert
	require.Eventually(t, nackCalled.Load, time.Second, 10*time.Millisecond, "Nack was not called on processor failure")
}
