package bqstore_test

import (
	"context"
	"sync"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog/log"
)

// ====================================================================================
// Test Mocks & Helpers
// ====================================================================================

type testPayload struct {
	ID   int
	Data string
}

// MockDataBatchInserter is a mock implementation of bqstore.DataBatchInserter.
type MockDataBatchInserter[T any] struct {
	mu            sync.Mutex
	receivedItems [][]*T
	callCount     int
	InsertBatchFn func(ctx context.Context, items []*T) error
}

func (m *MockDataBatchInserter[T]) InsertBatch(ctx context.Context, items []*T) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount++
	m.receivedItems = append(m.receivedItems, items)
	if m.InsertBatchFn != nil {
		return m.InsertBatchFn(ctx, items)
	}
	return nil
}

func (m *MockDataBatchInserter[T]) Close() error { return nil }
func (m *MockDataBatchInserter[T]) GetReceivedItems() [][]*T {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.receivedItems
}
func (m *MockDataBatchInserter[T]) GetCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

// MockMessageConsumer is a mock of the messagepipeline.MessageConsumer interface.
type MockMessageConsumer struct {
	msgChan  chan messagepipeline.Message
	doneChan chan struct{}
	stopOnce sync.Once
}

func NewMockMessageConsumer(bufferSize int) *MockMessageConsumer {
	return &MockMessageConsumer{
		msgChan:  make(chan messagepipeline.Message, bufferSize),
		doneChan: make(chan struct{}),
	}
}
func (m *MockMessageConsumer) Messages() <-chan messagepipeline.Message { return m.msgChan }
func (m *MockMessageConsumer) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		// Use a new context with a timeout for the stop procedure.
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = m.Stop(stopCtx)
	}()
	return nil
}
func (m *MockMessageConsumer) Stop(_ context.Context) error {
	m.stopOnce.Do(func() {
		close(m.msgChan)
		close(m.doneChan)
	})
	return nil
}
func (m *MockMessageConsumer) Done() <-chan struct{} { return m.doneChan }
func (m *MockMessageConsumer) Push(msg messagepipeline.Message) {
	defer func() {
		if r := recover(); r != nil {
			log.Warn().Msg("Recovered from panic trying to push to closed consumer channel.")
		}
	}()
	m.msgChan <- msg
}
