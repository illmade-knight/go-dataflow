package bqstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"

	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// DataBatchInserter is a generic interface for inserting a batch of items into a
// data store. It abstracts the destination (e.g., BigQuery, Postgres, etc.),
// making pipeline components more modular and testable.
type DataBatchInserter[T any] interface {
	// InsertBatch inserts a slice of items into the data store.
	InsertBatch(ctx context.Context, items []*T) error
	// Close handles any necessary cleanup of the inserter's resources.
	Close() error
}

// BigQueryDatasetConfig holds configuration for a BigQuery dataset and table.
type BigQueryDatasetConfig struct {
	DatasetID       string
	TableID         string
	CredentialsFile string // Optional: Path to a service account JSON file.
}

// NewProductionBigQueryClient creates a BigQuery client suitable for production environments.
func NewProductionBigQueryClient(ctx context.Context, projectID string, credentialsFile string, logger zerolog.Logger) (*bigquery.Client, error) {
	var opts []option.ClientOption
	if credentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(credentialsFile))
		logger.Info().Str("credentials_file", credentialsFile).Msg("Using specified credentials file for BigQuery client.")
	} else {
		logger.Info().Msg("Using Application Default Credentials (ADC) for BigQuery client.")
	}

	client, err := bigquery.NewClient(ctx, projectID, opts...)
	if err != nil {
		logger.Error().Err(err).Str("project_id", projectID).Msg("Failed to create BigQuery client.")
		return nil, fmt.Errorf("bigquery.NewClient: %w", err)
	}
	logger.Info().Str("project_id", projectID).Msg("BigQuery client created successfully.")
	return client, nil
}

// BigQueryInserter implements the DataBatchInserter interface for Google BigQuery,
// providing a mechanism to stream data into a specified table.
type BigQueryInserter[T any] struct {
	client   *bigquery.Client
	table    *bigquery.Table
	inserter *bigquery.Inserter
	logger   zerolog.Logger
}

// NewBigQueryInserter creates a new inserter for a specified BigQuery table.
//
// The provided context is used for initial API calls to verify and potentially
// create the target table. If the table specified in the config does not exist,
// this function will attempt to create it by inferring a schema from the provided
// generic type T. This simplifies deployment by removing the need for manual
// table creation for new data types.
func NewBigQueryInserter[T any](
	ctx context.Context,
	client *bigquery.Client,
	cfg *BigQueryDatasetConfig,
	logger zerolog.Logger,
) (*BigQueryInserter[T], error) {
	if client == nil {
		return nil, errors.New("bigquery client cannot be nil")
	}
	if cfg == nil {
		return nil, errors.New("BigQueryDatasetConfig cannot be nil")
	}

	projectID := client.Project()
	logger = logger.With().Str("project_id", projectID).Str("dataset_id", cfg.DatasetID).Str("table_id", cfg.TableID).Logger()

	tableRef := client.Dataset(cfg.DatasetID).Table(cfg.TableID)
	_, err := tableRef.Metadata(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "notFound") {
			logger.Warn().Msg("BigQuery table not found. Attempting to create with inferred schema.")
			var zero T
			inferredSchema, inferErr := bigquery.InferSchema(zero)
			if inferErr != nil {
				return nil, fmt.Errorf("failed to infer schema for type %T: %w", zero, inferErr)
			}
			tableMetadata := &bigquery.TableMetadata{Schema: inferredSchema}
			if createErr := tableRef.Create(ctx, tableMetadata); createErr != nil {
				return nil, fmt.Errorf("failed to create BigQuery table %s.%s: %w", cfg.DatasetID, cfg.TableID, createErr)
			}
			logger.Info().Msg("BigQuery table created successfully.")
		} else {
			return nil, fmt.Errorf("failed to get BigQuery table metadata: %w", err)
		}
	} else {
		logger.Info().Msg("Successfully connected to existing BigQuery table.")
	}

	return &BigQueryInserter[T]{
		client:   client,
		table:    tableRef,
		inserter: tableRef.Inserter(),
		logger:   logger,
	}, nil
}

// InsertBatch streams a batch of items of type T to the configured BigQuery table
// using the BigQuery Storage Write API for high-throughput streaming.
//
// It handles row-level insertion errors by logging detailed information for each
// failed row, which is crucial for debugging data quality issues. If any row fails,
// the method returns an error wrapping the `bigquery.PutMultiError`.
func (i *BigQueryInserter[T]) InsertBatch(ctx context.Context, items []*T) error {
	if len(items) == 0 {
		i.logger.Info().Msg("InsertBatch called with an empty slice, nothing to do.")
		return nil
	}

	err := i.inserter.Put(ctx, items)
	if err != nil {
		i.logger.Error().Err(err).Int("batch_size", len(items)).Msg("Failed to insert rows into BigQuery.")
		if multiErr, ok := err.(bigquery.PutMultiError); ok {
			for _, rowErr := range multiErr {
				i.logger.Error().
					Int("row_index", rowErr.RowIndex).
					Msgf("BigQuery insert error for row: %v", rowErr.Errors)
			}
		}
		return fmt.Errorf("bigquery Inserter.Put failed: %w", err)
	}

	i.logger.Debug().Int("batch_size", len(items)).Msg("Successfully inserted batch into BigQuery.")
	return nil
}

// Close is a no-op for this implementation, as the underlying BigQuery client's
// lifecycle is managed externally by the service that created it.
func (i *BigQueryInserter[T]) Close() error {
	i.logger.Info().Msg("BigQueryInserter.Close() called; client lifecycle is managed externally.")
	return nil
}
