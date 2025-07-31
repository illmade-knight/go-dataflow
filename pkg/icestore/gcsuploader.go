package icestore

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// DataUploader defines the interface for the final "sink" in the icestore pipeline.
type DataUploader interface {
	UploadGroup(ctx context.Context, batchKey string, items []*ArchivalData) error
	Close() error
}

// GCSBatchUploaderConfig holds configuration specific to the GCS uploader.
type GCSBatchUploaderConfig struct {
	BucketName   string
	ObjectPrefix string
}

// GCSBatchUploader implements the DataUploader interface for ArchivalData.
// It uploads a pre-grouped batch of items to a single compressed file in GCS.
type GCSBatchUploader struct {
	client GCSClient
	config GCSBatchUploaderConfig
	logger zerolog.Logger
	wg     sync.WaitGroup
}

// NewGCSBatchUploader creates a new uploader configured for Google Cloud Storage.
func NewGCSBatchUploader(
	gcsClient GCSClient,
	config GCSBatchUploaderConfig,
	logger zerolog.Logger,
) (*GCSBatchUploader, error) {
	if gcsClient == nil {
		return nil, errors.New("GCS client cannot be nil")
	}
	if config.BucketName == "" {
		return nil, errors.New("GCS bucket name is required")
	}
	return &GCSBatchUploader{
		client: gcsClient,
		config: config,
		logger: logger.With().Str("component", "GCSBatchUploader").Logger(),
	}, nil
}

// UploadGroup takes a batch of items that already share the same batchKey and
// uploads them to a single, compressed GCS object.
func (u *GCSBatchUploader) UploadGroup(ctx context.Context, batchKey string, items []*ArchivalData) error {
	if len(items) == 0 {
		return nil
	}

	u.wg.Add(1)
	defer u.wg.Done()

	batchFileID := uuid.New().String()
	objectName := path.Join(u.config.ObjectPrefix, batchKey, fmt.Sprintf("%s.jsonl.gz", batchFileID))
	u.logger.Info().Str("object_name", objectName).Int("record_count", len(items)).Msg("Starting upload for grouped batch.")

	objHandle := u.client.Bucket(u.config.BucketName).Object(objectName)
	gcsWriter := objHandle.NewWriter(ctx)
	pr, pw := io.Pipe()

	// This goroutine encodes and compresses the data, writing it to a pipe.
	go func() {
		var err error
		defer func() { _ = pw.CloseWithError(err) }()
		gz := gzip.NewWriter(pw)
		defer func() { _ = gz.Close() }()
		enc := json.NewEncoder(gz)
		for _, rec := range items {
			if rec == nil {
				continue
			}
			if err = enc.Encode(rec); err != nil {
				err = fmt.Errorf("json encoding failed for %s: %w", objectName, err)
				return
			}
		}
	}()

	// This reads from the pipe and streams the data to the GCS writer.
	bytesWritten, pipeReadErr := io.Copy(gcsWriter, pr)
	closeErr := gcsWriter.Close() // This finalizes the GCS upload.

	if pipeReadErr != nil {
		return fmt.Errorf("failed to stream data for GCS object %s: %w", objectName, pipeReadErr)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to close GCS object writer for %s: %w", objectName, closeErr)
	}

	u.logger.Info().
		Str("object_name", objectName).
		Int64("bytes_written", bytesWritten).
		Msg("Successfully uploaded grouped batch to GCS.")
	return nil
}

// Close waits for any pending upload goroutines to complete.
func (u *GCSBatchUploader) Close() error {
	u.logger.Info().Msg("Waiting for all pending GCS uploads to complete...")
	u.wg.Wait()
	u.logger.Info().Msg("All GCS uploads completed.")
	return nil
}
