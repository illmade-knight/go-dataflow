package icestore

import (
	"fmt"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
)

// NewIceStorageService is a constructor that assembles a complete GCS archival pipeline.
// It wires together a message consumer, a batcher, and a message transformer into
// a generic processing service tailored for ArchivalData.
func NewIceStorageService(
	numWorkers int,
	consumer messagepipeline.MessageConsumer,
	batcher *Batcher,
	transformer messagepipeline.MessageTransformer[ArchivalData],
	logger zerolog.Logger,
) (*messagepipeline.ProcessingService[ArchivalData], error) {

	processingService, err := messagepipeline.NewProcessingService[ArchivalData](
		numWorkers,
		consumer,
		batcher, // The icestore.Batcher satisfies the MessageProcessor interface.
		transformer,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create processing service for icestore: %w", err)
	}

	return processingService, nil
}

// NewGCSBatchProcessor is a high-level helper that creates a complete GCS archival
// component (uploader + batcher). The returned Batcher satisfies the
// messagepipeline.MessageProcessor interface and can be passed to NewIceStorageService.
func NewGCSBatchProcessor(
	gcsClient GCSClient,
	batchCfg *BatcherConfig,
	uploaderCfg GCSBatchUploaderConfig,
	logger zerolog.Logger,
) (*Batcher, error) {
	// 1. Create the underlying GCS-specific data uploader.
	gcsUploader, err := NewGCSBatchUploader(gcsClient, uploaderCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS uploader: %w", err)
	}

	// 2. Wrap the GCS uploader with the batching logic.
	batcher := NewBatcher(batchCfg, gcsUploader, logger)

	return batcher, nil
}
