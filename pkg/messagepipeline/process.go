package messagepipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
)

// ====================================================================================
// This file contains a generic, reusable service for processing messages from any
// MessageConsumer and handing them off to any MessageProcessor.
// ====================================================================================

// ProcessingService orchestrates the pipeline of consuming, transforming, and processing messages.
type ProcessingService[T any] struct {
	numWorkers   int
	consumer     MessageConsumer
	processor    MessageProcessor[T]
	transformer  MessageTransformer[T]
	logger       zerolog.Logger
	wg           sync.WaitGroup
	shutdownCtx  context.Context
	shutdownFunc context.CancelFunc
}

// NewProcessingService creates a new, generic ProcessingService.
func NewProcessingService[T any](
	numWorkers int,
	consumer MessageConsumer,
	processor MessageProcessor[T],
	transformer MessageTransformer[T],
	logger zerolog.Logger,
) (*ProcessingService[T], error) {
	if numWorkers <= 0 {
		numWorkers = 5
	}
	return &ProcessingService[T]{
		numWorkers:  numWorkers,
		consumer:    consumer,
		processor:   processor,
		transformer: transformer,
		logger:      logger.With().Str("service", "ProcessingService").Logger(),
	}, nil
}

// Start begins the service operation.
func (s *ProcessingService[T]) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting generic ProcessingService...")
	s.shutdownCtx, s.shutdownFunc = context.WithCancel(ctx)

	s.processor.Start(s.shutdownCtx)

	if err := s.consumer.Start(s.shutdownCtx); err != nil {
		// Use a background context for cleanup stop on a startup failure.
		_ = s.processor.Stop(context.Background())
		s.shutdownFunc()
		return fmt.Errorf("failed to start message consumer: %w", err)
	}
	s.logger.Info().Msg("Message consumer started.")

	s.logger.Info().Int("worker_count", s.numWorkers).Msg("Starting processing workers...")
	for i := 0; i < s.numWorkers; i++ {
		s.wg.Add(1)
		go s.worker(i)
	}
	s.logger.Info().Msg("Generic ProcessingService started successfully.")
	return nil
}

// worker is the main loop for each concurrent worker.
func (s *ProcessingService[T]) worker(workerID int) {
	defer s.wg.Done()
	s.logger.Debug().Int("worker_id", workerID).Msg("Processing worker started.")
	for {
		select {
		case <-s.shutdownCtx.Done():
			s.logger.Info().Int("worker_id", workerID).Msg("Processing worker shutting down.")
			return
		case msg, ok := <-s.consumer.Messages():
			if !ok {
				s.logger.Info().Int("worker_id", workerID).Msg("Consumer channel closed, worker exiting.")
				return
			}
			// REFACTOR: The worker now passes its lifecycle context into the transformer.
			s.processConsumedMessage(s.shutdownCtx, msg, workerID)
		}
	}
}

// processConsumedMessage contains the core logic for each worker.
func (s *ProcessingService[T]) processConsumedMessage(ctx context.Context, msg types.ConsumedMessage, workerID int) {
	s.logger.Debug().Int("worker_id", workerID).Str("msg_id", msg.ID).Msg("Transforming message")

	// The context is passed to the transformer.
	transformedPayload, skip, err := s.transformer(ctx, msg)
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

	batchedMsg := &types.BatchedMessage[T]{
		OriginalMessage: msg,
		Payload:         transformedPayload,
	}

	select {
	case s.processor.Input() <- batchedMsg:
		s.logger.Debug().Str("msg_id", msg.ID).Msg("Payload sent to processor.")
	case <-s.shutdownCtx.Done():
		s.logger.Warn().Str("msg_id", msg.ID).Msg("Shutdown in progress, Nacking message.")
		msg.Nack()
	}
}

// Stop gracefully shuts down the entire service in the correct order.
func (s *ProcessingService[T]) Stop(ctx context.Context) {
	s.logger.Info().Msg("Stopping generic ProcessingService...")

	if s.consumer != nil {
		if err := s.consumer.Stop(ctx); err != nil {
			s.logger.Warn().Err(err).Msg("Error during consumer stop, continuing shutdown.")
		}
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
	}

	if s.processor != nil {
		if err := s.processor.Stop(ctx); err != nil {
			s.logger.Warn().Err(err).Msg("Error during processor stop.")
		}
	}

	if s.shutdownFunc != nil {
		s.shutdownFunc()
	}
	s.logger.Info().Msg("Generic ProcessingService stopped.")
}
