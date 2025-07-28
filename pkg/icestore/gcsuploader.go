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

// GCSBatchUploaderConfig holds configuration specific to the GCS uploader.
type GCSBatchUploaderConfig struct {
	BucketName   string
	ObjectPrefix string
}

// GCSBatchUploader implements the DataUploader interface for ArchivalData.
// It groups items by their batch key and uploads each group to a compressed file in GCS.
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

// UploadBatch takes a batch of ArchivalData items, groups them by their key,
// and uploads each group to a separate, compressed GCS object in parallel.
func (u *GCSBatchUploader) UploadBatch(ctx context.Context, items []*ArchivalData) error {
	if len(items) == 0 {
		return nil
	}

	groupedBatches := make(map[string][]*ArchivalData)
	for _, item := range items {
		if item != nil {
			key := item.GetBatchKey()
			if key != "" {
				groupedBatches[key] = append(groupedBatches[key], item)
			}
		}
	}

	if len(groupedBatches) == 0 {
		return nil // All items were nil or had empty keys.
	}

	var uploadWg sync.WaitGroup
	errs := make(chan error, len(groupedBatches))

	for key, batchData := range groupedBatches {
		uploadWg.Add(1)
		u.wg.Add(1) // Add to the main waitgroup for the Close method.

		go func(batchKey string, dataToUpload []*ArchivalData) {
			defer uploadWg.Done()
			defer u.wg.Done()
			if err := u.uploadSingleGroup(ctx, batchKey, dataToUpload); err != nil {
				errs <- err
			}
		}(key, batchData)
	}

	uploadWg.Wait()
	close(errs)

	var combinedErr error
	for err := range errs {
		if combinedErr == nil {
			combinedErr = err
		} else {
			combinedErr = fmt.Errorf("%v; %w", combinedErr, err)
		}
	}
	return combinedErr
}

// uploadSingleGroup handles writing one group of records to a GCS object.
func (u *GCSBatchUploader) uploadSingleGroup(ctx context.Context, batchKey string, batchData []*ArchivalData) error {
	batchFileID := uuid.New().String()
	objectName := path.Join(u.config.ObjectPrefix, batchKey, fmt.Sprintf("%s.jsonl.gz", batchFileID))
	u.logger.Info().Str("object_name", objectName).Int("record_count", len(batchData)).Msg("Starting upload for grouped batch.")

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
		for _, rec := range batchData {
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

// Close waits for any pending upload goroutines to complete. Its blocking nature
// is handled by the timeout context in the calling Batcher's Stop method.
func (u *GCSBatchUploader) Close() error {
	u.logger.Info().Msg("Waiting for all pending GCS uploads to complete...")
	u.wg.Wait()
	u.logger.Info().Msg("All GCS uploads completed.")
	return nil
}
