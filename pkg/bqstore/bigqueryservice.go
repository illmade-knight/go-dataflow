package bqstore

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
)

// NewBigQueryService is a high-level constructor that assembles and returns a
// fully configured BigQuery processing pipeline service.
// It wires together a message consumer, a batch inserter, and a message transformer.
func NewBigQueryService[T any](
	numWorkers int,
	consumer messagepipeline.MessageConsumer,
	batchProcessor messagepipeline.MessageProcessor[T],
	transformer messagepipeline.MessageTransformer[T],
	logger zerolog.Logger,
) (*messagepipeline.ProcessingService[T], error) {

	// The BatchInserter (which is a MessageProcessor) is passed directly to the
	// generic service constructor.
	genericService, err := messagepipeline.NewProcessingService[T](
		numWorkers,
		consumer,
		batchProcessor,
		transformer,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create generic processing service for bqstore: %w", err)
	}

	return genericService, nil
}

// NewBigQueryBatchProcessor is a convenience constructor that creates a complete
// BigQuery batch processing component.
// It satisfies the messagepipeline.MessageProcessor interface and can be passed
// directly to NewBigQueryService.
func NewBigQueryBatchProcessor[T any](
	ctx context.Context,
	client *bigquery.Client,
	batchCfg *BatchInserterConfig,
	dsCfg *BigQueryDatasetConfig,
	logger zerolog.Logger,
) (*BatchInserter[T], error) {
	// 1. Create the underlying BigQuery-specific data inserter.
	bigQueryInserter, err := NewBigQueryInserter[T](ctx, client, dsCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery inserter: %w", err)
	}

	// 2. Wrap the BigQuery inserter with the generic batching logic.
	// REFACTOR: The call to NewBatcher no longer needs a context.
	batchInserter := NewBatcher[T](batchCfg, bigQueryInserter, logger)

	return batchInserter, nil
}
