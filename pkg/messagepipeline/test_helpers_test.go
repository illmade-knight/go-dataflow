package messagepipeline_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog/log"
)

// receiveSingleMessage is a test helper to wait for one message from a subscription.
// It is moved here to be accessible by all tests in the package.
func receiveSingleMessage(t *testing.T, ctx context.Context, sub *pubsub.Subscription, timeout time.Duration) *pubsub.Message {
	t.Helper()
	var receivedMsg *pubsub.Message
	var mu sync.RWMutex

	receiveCtx, receiveCancel := context.WithTimeout(ctx, timeout)
	defer receiveCancel()

	err := sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()
		if receivedMsg == nil {
			receivedMsg = msg
			msg.Ack()
			receiveCancel() // Stop receiving after getting the first message.
		} else {
			// This can happen in tests if more than one message arrives.
			// Nack subsequent messages to avoid them being redelivered.
			msg.Nack()
		}
	})

	// context.Canceled is the expected error when receiveCancel() is called.
	if err != nil && err != context.Canceled {
		t.Logf("Receive loop ended with an unexpected error: %v", err)
	}

	mu.RLock()
	defer mu.RUnlock()
	return receivedMsg
}

// ====================================================================================
// This file contains mocks for the interfaces defined in this package.
// These mocks are intended for use in unit tests of services that depend on
// the consumer pipeline.
// ====================================================================================

// --- MockMessageConsumer ---

// MockMessageConsumer is a mock implementation of the MessageConsumer interface.
// It is designed to be used in unit tests to simulate a message source.
type MockMessageConsumer struct {
	msgChan    chan types.ConsumedMessage
	doneChan   chan struct{}
	stopOnce   sync.Once
	startErr   error // Error to be returned by Start()
	startMu    sync.Mutex
	startCount int
	stopCount  int
}

// NewMockMessageConsumer creates a new mock consumer with a buffered channel.
func NewMockMessageConsumer(bufferSize int) *MockMessageConsumer {
	if bufferSize < 0 {
		bufferSize = 0
	}
	return &MockMessageConsumer{
		msgChan:  make(chan types.ConsumedMessage, bufferSize),
		doneChan: make(chan struct{}),
	}
}

// Messages returns the read-only channel for consuming messages.
func (m *MockMessageConsumer) Messages() <-chan types.ConsumedMessage {
	return m.msgChan
}

// Start simulates the startup of a real consumer.
func (m *MockMessageConsumer) Start(ctx context.Context) error {
	m.startMu.Lock()
	defer m.startMu.Unlock()
	m.startCount++
	if m.startErr != nil {
		return m.startErr
	}
	// Simulate being stopped by the context.
	go func() {
		<-ctx.Done()
		_ = m.Stop(context.Background()) // Call stop when context is done.
	}()
	return nil
}

// Stop gracefully closes the message and done channels.
// It now conforms to the updated MessageConsumer interface.
func (m *MockMessageConsumer) Stop(ctx context.Context) error {
	m.stopOnce.Do(func() {
		m.startMu.Lock()
		m.stopCount++
		m.startMu.Unlock()

		close(m.doneChan)
		close(m.msgChan)

		// Nack any messages that were in the buffer but not processed.
		for msg := range m.msgChan {
			log.Warn().Str("msg_id", msg.ID).Msg("MockConsumer draining and Nacking message on shutdown.")
			if msg.Nack != nil {
				msg.Nack()
			}
		}
	})
	return nil
}

// Done returns the channel that signals when the consumer has fully stopped.
func (m *MockMessageConsumer) Done() <-chan struct{} {
	return m.doneChan
}

// Push is a test helper to inject a message into the mock consumer's channel.
func (m *MockMessageConsumer) Push(msg types.ConsumedMessage) {
	// A panic can occur if a test tries to push after Stop() has been called.
	defer func() {
		if r := recover(); r != nil {
			log.Warn().Msg("Recovered from panic trying to push to closed consumer channel.")
		}
	}()
	m.msgChan <- msg
}

// SetStartError configures the mock to return an error on Start().
func (m *MockMessageConsumer) SetStartError(err error) {
	m.startMu.Lock()
	defer m.startMu.Unlock()
	m.startErr = err
}

// GetStartCount returns the number of times Start() was called.
func (m *MockMessageConsumer) GetStartCount() int {
	m.startMu.Lock()
	defer m.startMu.Unlock()
	return m.startCount
}

// GetStopCount returns the number of times Stop() was called.
func (m *MockMessageConsumer) GetStopCount() int {
	m.startMu.Lock()
	defer m.startMu.Unlock()
	return m.stopCount
}

// --- MockMessageProcessor ---

// MockMessageProcessor is a mock implementation of the MessageProcessor interface.
type MockMessageProcessor[T any] struct {
	InputChan              chan *types.BatchedMessage[T]
	Received               []*types.BatchedMessage[T]
	mu                     sync.Mutex
	wg                     sync.WaitGroup
	startCount             int
	startMu                sync.Mutex
	stopCount              int
	processDelay           time.Duration
	processingHook         func(msg *types.BatchedMessage[T])
	ackOnProcess           bool
	messageProcessedSignal chan struct{}
}

// NewMockMessageProcessor creates a new mock processor.
func NewMockMessageProcessor[T any](bufferSize int) *MockMessageProcessor[T] {
	if bufferSize < 0 {
		bufferSize = 0
	}
	return &MockMessageProcessor[T]{
		InputChan: make(chan *types.BatchedMessage[T], bufferSize),
		Received:  []*types.BatchedMessage[T]{},
	}
}

// Input returns the write-only channel for sending messages to the processor.
func (m *MockMessageProcessor[T]) Input() chan<- *types.BatchedMessage[T] {
	return m.InputChan
}

// Start begins the processor's operations.
func (m *MockMessageProcessor[T]) Start(ctx context.Context) {
	m.startMu.Lock()
	m.startCount++
	m.startMu.Unlock()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			select {
			case msg, ok := <-m.InputChan:
				if !ok {
					return // Channel closed, exit goroutine.
				}

				if m.processDelay > 0 {
					time.Sleep(m.processDelay)
				}
				m.mu.Lock()
				m.Received = append(m.Received, msg)
				if m.ackOnProcess {
					msg.OriginalMessage.Ack()
				}
				hook := m.processingHook
				signalChan := m.messageProcessedSignal
				m.mu.Unlock()

				if hook != nil {
					hook(msg)
				}

				if signalChan != nil {
					select {
					case signalChan <- struct{}{}:
					default:
					}
				}
			case <-ctx.Done():
				// If the context is cancelled, stop processing and exit.
				return
			}
		}
	}()
}

// GetStartCount returns the number of times Start() was called.
func (m *MockMessageProcessor[T]) GetStartCount() int {
	m.startMu.Lock()
	defer m.startMu.Unlock()
	return m.startCount
}

// Stop gracefully shuts down the processor, respecting the context deadline.
func (m *MockMessageProcessor[T]) Stop(ctx context.Context) error {
	m.startMu.Lock()
	m.stopCount++
	m.startMu.Unlock()

	close(m.InputChan)

	// Wait for the processing goroutine to finish, but respect the timeout.
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil // Graceful shutdown completed.
	case <-ctx.Done():
		return ctx.Err() // Shutdown timed out.
	}
}

func (m *MockMessageProcessor[T]) GetStopCount() int {
	m.startMu.Lock()
	defer m.startMu.Unlock()
	return m.stopCount
}

// GetReceived returns a copy of the messages received by the processor.
func (m *MockMessageProcessor[T]) GetReceived() []*types.BatchedMessage[T] {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Create a copy to avoid race conditions in tests.
	receivedCopy := make([]*types.BatchedMessage[T], len(m.Received))
	copy(receivedCopy, m.Received)
	return receivedCopy
}

// SetProcessDelay introduces a delay for every message processed.
func (m *MockMessageProcessor[T]) SetProcessDelay(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processDelay = d
}

// SetProcessingHook sets a function to be called for each message processed.
func (m *MockMessageProcessor[T]) SetProcessingHook(hook func(msg *types.BatchedMessage[T])) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processingHook = hook
}

// SetAckOnProcess configures the mock processor to call Ack() on the original message.
func (m *MockMessageProcessor[T]) SetAckOnProcess(b bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ackOnProcess = b
}

// SetMessageProcessedSignal sets a channel that will be signaled each time a message is processed.
func (m *MockMessageProcessor[T]) SetMessageProcessedSignal(ch chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messageProcessedSignal = ch
}

// --- messageState ---
// messageState tracks the Ack/Nack status for individual messages in table tests.
type messageState struct {
	ID         string
	mu         sync.Mutex
	ackCalled  bool
	nackCalled bool
}

func (ms *messageState) Ack() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.ackCalled = true
}

func (ms *messageState) Nack() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.nackCalled = true
}

func (ms *messageState) IsAcked() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.ackCalled
}

func (ms *messageState) IsNacked() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.nackCalled
}
