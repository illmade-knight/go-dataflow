package icestore_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/icestore"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockUploader is a test double for the DataUploader interface.
type mockUploader struct {
	mu          sync.Mutex
	UploadCount int
	UploadErr   error
	LastBatch   []*icestore.ArchivalData
}

func (m *mockUploader) UploadGroup(ctx context.Context, batchKey string, items []*icestore.ArchivalData) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.UploadCount++
	m.LastBatch = items
	return m.UploadErr
}
func (m *mockUploader) Close() error { return nil }

// testServiceSetup is a helper to assemble the service with mocks.
func testServiceSetup(t *testing.T, uploader icestore.DataUploader) (*icestore.IceStorageService, *mockMessageConsumer) {
	t.Helper()

	mockConsumer := newMockMessageConsumer(10)
	serviceCfg := icestore.IceStorageServiceConfig{
		NumWorkers:    1,
		BatchSize:     1, // Flush immediately for predictable tests
		FlushInterval: time.Minute,
	}
	// We pass a mock client because the service constructor requires one,
	// but we will mock the uploader interface that the service *actually* uses.
	mockClient := newMockGCSClient(false)
	uploaderCfg := icestore.GCSBatchUploaderConfig{BucketName: "test-bucket"}

	// Manually create the service and replace its uploader with our mock.
	// This is a form of dependency injection for testing.
	service, err := icestore.NewIceStorageService(
		serviceCfg,
		mockConsumer,
		mockClient,
		uploaderCfg,
		icestore.ArchivalTransformer,
		zerolog.Nop(),
	)
	require.NoError(t, err)
	service.UseUploaderForTest(uploader) // Assumes a test helper method exists

	return service, mockConsumer
}

// Assumes a helper method is added to IceStorageService for testing:
// func (s *IceStorageService) UseUploaderForTest(uploader DataUploader) { s.uploader = uploader }

func TestIceStorageService_Success(t *testing.T) {
	// Arrange
	uploader := &mockUploader{}
	service, consumer := testServiceSetup(t, uploader)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	err := service.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Stop(ctx) })

	var acked atomic.Bool
	msg := messagepipeline.Message{
		MessageData: messagepipeline.MessageData{ID: "test-id-123"},
		Ack:         func() { acked.Store(true) },
		Nack:        func() { t.Error("Nack should not have been called") },
	}

	// Act
	consumer.Push(msg)

	// Assert
	require.Eventually(t, func() bool {
		uploader.mu.Lock()
		defer uploader.mu.Unlock()
		return uploader.UploadCount == 1
	}, time.Second, 10*time.Millisecond, "Uploader should have been called")

	require.Eventually(t, acked.Load, time.Second, 10*time.Millisecond, "Message should have been acked")
	assert.Len(t, uploader.LastBatch, 1)
	assert.Equal(t, "test-id-123", uploader.LastBatch[0].ID)
}

func TestIceStorageService_UploadFailure(t *testing.T) {
	// Arrange
	uploader := &mockUploader{UploadErr: errors.New("gcs failed")}
	service, consumer := testServiceSetup(t, uploader)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	err := service.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Stop(ctx) })

	var nacked atomic.Bool
	msg := messagepipeline.Message{
		MessageData: messagepipeline.MessageData{ID: "fail-id-456"},
		Nack:        func() { nacked.Store(true) },
		Ack:         func() { t.Error("Ack should not be called on failure") },
	}

	// Act
	consumer.Push(msg)

	// Assert
	require.Eventually(t, func() bool {
		uploader.mu.Lock()
		defer uploader.mu.Unlock()
		return uploader.UploadCount == 1
	}, time.Second, 10*time.Millisecond, "Uploader should have been called")

	require.Eventually(t, nacked.Load, time.Second, 10*time.Millisecond, "Message should have been nacked")
}
