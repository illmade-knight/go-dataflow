package icestore_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/icestore"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Test Cases
// =============================================================================

func TestIceStorageService_Success(t *testing.T) {
	// --- Arrange ---
	mockConsumer := newMockMessageConsumer(10)
	mockClient := newMockGCSClient(false)

	serviceCfg := messagepipeline.BatchingServiceConfig{
		NumWorkers:    1,
		BatchSize:     1,
		FlushInterval: time.Minute,
	}
	uploaderCfg := icestore.GCSBatchUploaderConfig{BucketName: "test-bucket"}

	service, err := icestore.NewIceStorageService(serviceCfg, mockConsumer, mockClient, uploaderCfg, icestore.ArchivalTransformer, zerolog.Nop())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	err = service.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Stop(context.Background()) })

	var acked int32
	msg := messagepipeline.Message{
		MessageData: messagepipeline.MessageData{
			ID:          "test-id-123",
			Payload:     []byte(`{"data":"test"}`),
			PublishTime: time.Now(),
		},
		Ack: func() { atomic.AddInt32(&acked, 1) },
	}

	// --- Act ---
	mockConsumer.Push(msg)

	// --- Assert ---
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&acked) == 1
	}, 2*time.Second, 10*time.Millisecond, "Message should have been acked")
}

func TestIceStorageService_UploadFailure(t *testing.T) {
	// --- Arrange ---
	mockConsumer := newMockMessageConsumer(10)
	mockClient := newMockGCSClient(true)

	serviceCfg := messagepipeline.BatchingServiceConfig{
		NumWorkers:    1,
		BatchSize:     1,
		FlushInterval: time.Minute,
	}
	uploaderCfg := icestore.GCSBatchUploaderConfig{BucketName: "test-bucket"}

	service, err := icestore.NewIceStorageService(serviceCfg, mockConsumer, mockClient, uploaderCfg, icestore.ArchivalTransformer, zerolog.Nop())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	err = service.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Stop(context.Background()) })

	var nacked int32
	msg := messagepipeline.Message{
		MessageData: messagepipeline.MessageData{ID: "fail-id-456"},
		Nack:        func() { atomic.AddInt32(&nacked, 1) },
		Ack:         func() { t.Error("Ack should not be called on failure") },
	}

	// --- Act ---
	mockConsumer.Push(msg)

	// --- Assert ---
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&nacked) == 1
	}, 2*time.Second, 10*time.Millisecond, "Message should have been nacked")
}
