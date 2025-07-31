package icestore_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/icestore"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
)

// =============================================================================
// Mocks for Black-Box Testing
// =============================================================================

// --- Mock GCS Client with Failure Injection ---

type mockGCSWriter struct {
	buf         bytes.Buffer
	mu          sync.Mutex
	shouldError bool
	closed      bool
}

func (m *mockGCSWriter) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, errors.New("write on closed writer")
	}
	if m.shouldError {
		return 0, errors.New("mock gcs write error")
	}
	return m.buf.Write(p)
}
func (m *mockGCSWriter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("already closed")
	}
	m.closed = true
	if m.shouldError {
		return errors.New("mock gcs close error")
	}
	return nil
}
func (m *mockGCSWriter) Bytes() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.buf.Bytes()
}

type mockGCSObjectHandle struct{ writer icestore.GCSWriter }

func (m *mockGCSObjectHandle) NewWriter(_ context.Context) icestore.GCSWriter { return m.writer }

type mockGCSBucketHandle struct {
	mu                sync.Mutex
	objects           map[string]icestore.GCSObjectHandle
	writersShouldFail bool
}

func (m *mockGCSBucketHandle) Object(name string) icestore.GCSObjectHandle {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objects[name]; !ok {
		m.objects[name] = &mockGCSObjectHandle{
			writer: &mockGCSWriter{shouldError: m.writersShouldFail},
		}
	}
	return m.objects[name]
}

type mockGCSClient struct {
	bucket *mockGCSBucketHandle
}

func newMockGCSClient(writersShouldFail bool) *mockGCSClient {
	return &mockGCSClient{
		bucket: &mockGCSBucketHandle{
			objects:           make(map[string]icestore.GCSObjectHandle),
			writersShouldFail: writersShouldFail,
		},
	}
}
func (m *mockGCSClient) Bucket(_ string) icestore.GCSBucketHandle { return m.bucket }

// --- Mock Message Consumer ---

type mockMessageConsumer struct {
	msgChan  chan messagepipeline.Message
	stopOnce sync.Once
}

func newMockMessageConsumer(bufferSize int) *mockMessageConsumer {
	return &mockMessageConsumer{msgChan: make(chan messagepipeline.Message, bufferSize)}
}
func (m *mockMessageConsumer) Messages() <-chan messagepipeline.Message { return m.msgChan }
func (m *mockMessageConsumer) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = m.Stop(stopCtx)
	}()
	return nil
}
func (m *mockMessageConsumer) Stop(_ context.Context) error {
	m.stopOnce.Do(func() { close(m.msgChan) })
	return nil
}
func (m *mockMessageConsumer) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
func (m *mockMessageConsumer) Push(msg messagepipeline.Message) { m.msgChan <- msg }
