package icestore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
)

// IceStorageServiceConfig holds configuration for an IceStorageService.
type IceStorageServiceConfig struct {
	NumWorkers    int
	BatchSize     int
	FlushInterval time.Duration
}

// IceStorageService orchestrates a pipeline that archives messages to GCS.
// It uses a KeyAwareBatcher to group messages before uploading them.
type IceStorageService struct {
	cfg         IceStorageServiceConfig
	consumer    messagepipeline.MessageConsumer
	transformer messagepipeline.MessageTransformer[ArchivalData]
	uploader    DataUploader
	batcher     *KeyAwareBatcher
	logger      zerolog.Logger
	wg          sync.WaitGroup
}

// NewIceStorageService creates and configures a new GCS archival service.
func NewIceStorageService(
	cfg IceStorageServiceConfig,
	consumer messagepipeline.MessageConsumer,
	gcsClient GCSClient,
	uploaderCfg GCSBatchUploaderConfig,
	transformer messagepipeline.MessageTransformer[ArchivalData],
	logger zerolog.Logger,
) (*IceStorageService, error) {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 5
	}
	if consumer == nil || gcsClient == nil || transformer == nil {
		return nil, fmt.Errorf("consumer, gcsClient, and transformer cannot be nil")
	}

	serviceLogger := logger.With().Str("service", "IceStorageService").Logger()

	// 1. Create the final "sink" component, the uploader.
	gcsUploader, err := NewGCSBatchUploader(gcsClient, uploaderCfg, serviceLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS uploader: %w", err)
	}

	// 2. Define the flush function that will be called by the batcher.
	flushFunc := func(ctx context.Context, batch []messagepipeline.ProcessableItem[ArchivalData]) {
		if len(batch) == 0 {
			return
		}
		batchKey := batch[0].Payload.GetBatchKey()
		payloads := make([]*ArchivalData, len(batch))
		for i, item := range batch {
			payloads[i] = item.Payload
		}

		if err := gcsUploader.UploadGroup(ctx, batchKey, payloads); err != nil {
			serviceLogger.Error().Err(err).Str("batch_key", batchKey).Int("batch_size", len(batch)).Msg("Failed to upload grouped batch, Nacking all messages.")
			for _, item := range batch {
				item.Original.Nack()
			}
		} else {
			serviceLogger.Info().Str("batch_key", batchKey).Int("batch_size", len(batch)).Msg("Successfully uploaded grouped batch, Acking all messages.")
			for _, item := range batch {
				item.Original.Ack()
			}
		}
	}

	// 3. Create the key-aware batcher.
	batcherCfg := KeyAwareBatcherConfig{
		BatchSize:     cfg.BatchSize,
		FlushInterval: cfg.FlushInterval,
	}
	batcher := NewKeyAwareBatcher(batcherCfg, flushFunc, serviceLogger)

	return &IceStorageService{
		cfg:         cfg,
		consumer:    consumer,
		transformer: transformer,
		uploader:    gcsUploader,
		batcher:     batcher,
		logger:      serviceLogger,
	}, nil
}

// Start begins the service operation.
func (s *IceStorageService) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting icestore service...")
	s.batcher.Start(ctx)

	if err := s.consumer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start message consumer: %w", err)
	}
	s.logger.Info().Msg("Message consumer started.")

	s.logger.Info().Int("worker_count", s.cfg.NumWorkers).Msg("Starting processing workers...")
	s.wg.Add(s.cfg.NumWorkers)
	for i := 0; i < s.cfg.NumWorkers; i++ {
		go s.worker(ctx)
	}

	s.logger.Info().Msg("IceStorage service started successfully.")
	return nil
}

// Stop gracefully shuts down the entire service.
func (s *IceStorageService) Stop(ctx context.Context) error {
	s.logger.Info().Msg("Stopping icestore service...")

	if err := s.consumer.Stop(ctx); err != nil {
		s.logger.Warn().Err(err).Msg("Error during consumer stop, continuing shutdown.")
	}

	workerDone := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(workerDone)
	}()

	select {
	case <-workerDone:
		s.logger.Info().Msg("All processing workers completed gracefully.")
	case <-ctx.Done():
		s.logger.Error().Err(ctx.Err()).Msg("Timeout waiting for processing workers to finish.")
		return ctx.Err()
	}

	s.batcher.Stop()
	if err := s.uploader.Close(); err != nil {
		s.logger.Error().Err(err).Msg("Error closing uploader.")
	}

	s.logger.Info().Msg("IceStorage service stopped.")
	return nil
}

func (s *IceStorageService) worker(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-s.consumer.Messages():
			if !ok {
				return
			}
			s.processConsumedMessage(ctx, &msg)
		}
	}
}

func (s *IceStorageService) processConsumedMessage(ctx context.Context, msg *messagepipeline.Message) {
	payload, skip, err := s.transformer(ctx, msg)
	if err != nil {
		s.logger.Error().Err(err).Str("msg_id", msg.ID).Msg("Failed to transform message, Nacking.")
		msg.Nack()
		return
	}
	if skip {
		s.logger.Debug().Str("msg_id", msg.ID).Msg("Transformer signaled to skip message, Acking.")
		msg.Ack()
		return
	}
	s.batcher.Add(messagepipeline.ProcessableItem[ArchivalData]{Original: *msg, Payload: payload})
}

// UseUploaderForTest replaces the service's internal uploader with a mock.
// This should only be used in tests.
func (s *IceStorageService) UseUploaderForTest(uploader DataUploader) {
	s.uploader = uploader
	// Replace the batcher's flush function to use the new mock uploader
	s.batcher.flushFn = func(ctx context.Context, batch []messagepipeline.ProcessableItem[ArchivalData]) {
		if len(batch) > 0 {
			batchKey := batch[0].Payload.GetBatchKey()
			payloads := make([]*ArchivalData, len(batch))
			for i, item := range batch {
				payloads[i] = item.Payload
			}
			if err := s.uploader.UploadGroup(ctx, batchKey, payloads); err != nil {
				for _, item := range batch {
					item.Original.Nack()
				}
			} else {
				for _, item := range batch {
					item.Original.Ack()
				}
			}
		}
	}
}
