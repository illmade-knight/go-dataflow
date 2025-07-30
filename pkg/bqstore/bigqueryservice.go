package bqstore

import (
	"context"
	"fmt"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
)

// NewBigQueryService is a high-level constructor that assembles and returns a
// fully configured BigQuery processing pipeline using the generic BatchingService.
func NewBigQueryService[T any](
	cfg messagepipeline.BatchingServiceConfig,
	consumer messagepipeline.MessageConsumer,
	bqInserter DataBatchInserter[T],
	transformer messagepipeline.MessageTransformer[T],
	logger zerolog.Logger,
) (*messagepipeline.BatchingService[T], error) {

	// 1. Define the BatchProcessor function. This is the final stage of the pipeline.
	batchProcessor := func(ctx context.Context, batch []messagepipeline.ProcessableItem[T]) error {
		if len(batch) == 0 {
			return nil
		}

		// Extract the typed payloads from the processable items.
		payloads := make([]*T, len(batch))
		for i, item := range batch {
			payloads[i] = item.Payload
		}

		// 2. Call the underlying data inserter.
		if err := bqInserter.InsertBatch(ctx, payloads); err != nil {
			logger.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to insert batch, Nacking all messages.")
			for _, item := range batch {
				item.Original.Nack()
			}
			return err // Propagate the error.
		}

		// 3. On success, Ack all original messages.
		logger.Info().Int("batch_size", len(batch)).Msg("Successfully inserted batch, Acking all messages.")
		for _, item := range batch {
			item.Original.Ack()
		}
		return nil
	}

	// 4. Assemble the pipeline using the generic BatchingService.
	genericService, err := messagepipeline.NewBatchingService[T](
		cfg,
		consumer,
		transformer,
		batchProcessor,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create generic batching service for bqstore: %w", err)
	}

	return genericService, nil
}
