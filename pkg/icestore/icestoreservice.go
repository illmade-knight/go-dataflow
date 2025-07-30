package icestore

import (
	"context"
	"fmt"
	"sync"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
)

// NewIceStorageService assembles a complete GCS archival pipeline using the BatchingService.
func NewIceStorageService(
	cfg messagepipeline.BatchingServiceConfig,
	consumer messagepipeline.MessageConsumer,
	gcsClient GCSClient,
	uploaderCfg GCSBatchUploaderConfig,
	transformer messagepipeline.MessageTransformer[ArchivalData],
	logger zerolog.Logger,
) (*messagepipeline.BatchingService[ArchivalData], error) {
	// 1. Create the final "sink" component, the uploader.
	gcsUploader, err := NewGCSBatchUploader(gcsClient, uploaderCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS uploader: %w", err)
	}

	// 2. Define the processor function. This function is responsible for key-aware grouping.
	batchProcessor := func(ctx context.Context, batch []messagepipeline.ProcessableItem[ArchivalData]) error {
		if len(batch) == 0 {
			return nil
		}

		// Group the incoming generic batch by the application-specific BatchKey.
		groupedItems := make(map[string][]messagepipeline.ProcessableItem[ArchivalData])
		for _, item := range batch {
			key := item.Payload.GetBatchKey()
			groupedItems[key] = append(groupedItems[key], item)
		}

		var wg sync.WaitGroup
		var firstErr error
		var mu sync.Mutex

		// Process each key-specific group in parallel.
		for _, itemsInGroup := range groupedItems {
			wg.Add(1)
			go func(group []messagepipeline.ProcessableItem[ArchivalData]) {
				defer wg.Done()

				// Extract just the payloads for the uploader.
				payloads := make([]*ArchivalData, len(group))
				for i, item := range group {
					payloads[i] = item.Payload
				}

				// The gcsUploader will receive a pre-grouped batch and create a single file.
				if err := gcsUploader.UploadBatch(ctx, payloads); err != nil {
					logger.Error().Err(err).Int("batch_size", len(group)).Msg("Failed to upload grouped batch, Nacking messages.")
					for _, item := range group {
						item.Original.Nack()
					}
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				} else {
					logger.Info().Int("batch_size", len(group)).Msg("Successfully uploaded grouped batch, Acking messages.")
					for _, item := range group {
						item.Original.Ack()
					}
				}
			}(itemsInGroup)
		}
		wg.Wait()
		return firstErr // Return the first error encountered, if any.
	}

	// 3. Assemble the service with the generic BatchingService.
	batchingService, err := messagepipeline.NewBatchingService[ArchivalData](
		cfg,
		consumer,
		transformer,
		batchProcessor,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create batching service for icestore: %w", err)
	}

	return batchingService, nil
}
