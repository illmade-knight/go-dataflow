package icestore

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/illmade-knight/go-dataflow/pkg/types"
)

// --- Mock GCS Client Components ---

// mockGCSWriter is a mock GCSWriter that writes to an in-memory buffer.
type mockGCSWriter struct {
	buf    bytes.Buffer
	closed bool
}

func (m *mockGCSWriter) Write(p []byte) (n int, err error) {
	if m.closed {
		return 0, errors.New("write on closed writer")
	}
	return m.buf.Write(p)
}

func (m *mockGCSWriter) Close() error {
	if m.closed {
		return errors.New("already closed")
	}
	m.closed = true
	return nil
}

// mockGCSObjectHandle is a mock GCSObjectHandle.
type mockGCSObjectHandle struct {
	writer *mockGCSWriter
}

func (m *mockGCSObjectHandle) NewWriter(_ context.Context) GCSWriter {
	if m.writer == nil {
		m.writer = &mockGCSWriter{}
	}
	return m.writer
}

// mockGCSBucketHandle is a mock GCSBucketHandle that stores created objects in a map.
type mockGCSBucketHandle struct {
	sync.Mutex
	objects map[string]*mockGCSObjectHandle
}

func (m *mockGCSBucketHandle) Object(name string) GCSObjectHandle {
	m.Lock()
	defer m.Unlock()
	if m.objects == nil {
		m.objects = make(map[string]*mockGCSObjectHandle)
	}
	if _, ok := m.objects[name]; !ok {
		m.objects[name] = &mockGCSObjectHandle{}
	}
	return m.objects[name]
}

// mockGCSClient is a mock GCSClient.
type mockGCSClient struct {
	bucket *mockGCSBucketHandle
}

func newMockGCSClient() *mockGCSClient {
	return &mockGCSClient{
		bucket: &mockGCSBucketHandle{},
	}
}

func (m *mockGCSClient) Bucket(_ string) GCSBucketHandle {
	return m.bucket
}

// MockMessageConsumer is a local mock implementation of the messagepipeline.MessageConsumer interface.
type MockMessageConsumer struct {
	msgChan  chan types.ConsumedMessage
	doneChan chan struct{}
	stopOnce sync.Once
}

func NewMockMessageConsumer(bufferSize int) *MockMessageConsumer {
	return &MockMessageConsumer{
		msgChan:  make(chan types.ConsumedMessage, bufferSize),
		doneChan: make(chan struct{}),
	}
}
func (m *MockMessageConsumer) Messages() <-chan types.ConsumedMessage { return m.msgChan }
func (m *MockMessageConsumer) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		// Call Stop with a background context as the mock's shutdown is simple.
		_ = m.Stop(context.Background())
	}()
	return nil
}

// REFACTOR: The Stop method now accepts a context to conform to the updated interface.
func (m *MockMessageConsumer) Stop(_ context.Context) error {
	m.stopOnce.Do(func() {
		close(m.msgChan)
		close(m.doneChan)
	})
	return nil
}
func (m *MockMessageConsumer) Done() <-chan struct{} { return m.doneChan }
func (m *MockMessageConsumer) Push(msg types.ConsumedMessage) {
	select {
	case m.msgChan <- msg:
	default:
	}
}
