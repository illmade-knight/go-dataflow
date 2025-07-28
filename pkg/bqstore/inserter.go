package bqstore

import (
	"cloud.google.com/go/bigquery"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// BigQueryDatasetConfig holds configuration for a BigQuery dataset and table.
type BigQueryDatasetConfig struct {
	ProjectID       string
	DatasetID       string
	TableID         string
	CredentialsFile string // Optional: Path to a service account JSON file.
}

// LoadBigQueryInserterConfigFromEnv loads BigQuery configuration from environment variables.
func LoadBigQueryInserterConfigFromEnv() (*BigQueryDatasetConfig, error) {
	cfg := &BigQueryDatasetConfig{
		ProjectID:       os.Getenv("GCP_PROJECT_ID"),
		DatasetID:       os.Getenv("BQ_DATASET_ID"),
		TableID:         os.Getenv("BQ_TABLE_ID"),
		CredentialsFile: os.Getenv("GCP_BQ_CREDENTIALS_FILE"),
	}
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("GCP_PROJECT_ID environment variable not set")
	}
	if cfg.DatasetID == "" {
		return nil, fmt.Errorf("BQ_DATASET_ID environment variable not set")
	}
	if cfg.TableID == "" {
		return nil, fmt.Errorf("BQ_TABLE_ID environment variable not set")
	}
	return cfg, nil
}

// NewProductionBigQueryClient creates a BigQuery client suitable for production environments.
// It will use Application Default Credentials unless a specific credentials file is provided.
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

// BigQueryInserter implements the DataBatchInserter interface for Google BigQuery.
// It can insert batches of any struct type T that is compatible with BigQuery's schema inference.
type BigQueryInserter[T any] struct {
	client   *bigquery.Client
	table    *bigquery.Table
	inserter *bigquery.Inserter
	logger   zerolog.Logger
}

// NewBigQueryInserter creates a new generic inserter for a specified type T.
// If the target table does not exist, it attempts to create it by inferring the schema
// from the zero value of type T.
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
			logger.Info().Int("inferred_field_count", len(inferredSchema)).Msg("Successfully inferred schema from type.")

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

// InsertBatch streams a batch of items of type T to BigQuery.
func (i *BigQueryInserter[T]) InsertBatch(ctx context.Context, items []*T) error {
	if len(items) == 0 {
		i.logger.Info().Msg("InsertBatch called with an empty slice, nothing to do.")
		return nil
	}

	// The BigQuery client's Put method natively handles a slice of struct pointers.
	err := i.inserter.Put(ctx, items)
	if err != nil {
		i.logger.Error().Err(err).Int("batch_size", len(items)).Msg("Failed to insert rows into BigQuery.")
		// Log detailed row-level errors if available for easier debugging.
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

// Close is a no-op for this implementation. The BigQuery client's lifecycle is managed
// externally, allowing a single client to be shared across multiple inserters if needed.
func (i *BigQueryInserter[T]) Close() error {
	i.logger.Info().Msg("BigQueryInserter.Close() called; client lifecycle is managed externally.")
	return nil
}
