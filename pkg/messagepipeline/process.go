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
// It is generic and can be used with any combination of consumers and processors.
type ProcessingService[T any] struct {
	numWorkers   int
	consumer     MessageConsumer
	processor    MessageProcessor[T]
	transformer  MessageTransformer[T]
	logger       zerolog.Logger
	wg           sync.WaitGroup
	shutdownCtx  context.Context // This will now be a child of the context passed to Start()
	shutdownFunc context.CancelFunc
}

// NewProcessingService creates a new, generic ProcessingService.
// It requires a consumer to get messages, a transformer to give them structure, and a
// processor to handle the structured data.
func NewProcessingService[T any](
	numWorkers int,
	consumer MessageConsumer,
	processor MessageProcessor[T],
	transformer MessageTransformer[T],
	logger zerolog.Logger,
) (*ProcessingService[T], error) {
	// In a production library, you would add nil checks for the parameters here.
	if numWorkers <= 0 {
		numWorkers = 5 // Default to 5 workers if an invalid number is provided.
	}

	// No shutdownCtx/Func initialization here anymore; it's done in Start().

	return &ProcessingService[T]{
		numWorkers:  numWorkers,
		consumer:    consumer,
		processor:   processor,
		transformer: transformer,
		logger:      logger.With().Str("service", "ProcessingService").Logger(),
		// shutdownCtx and shutdownFunc will be set in Start()
	}, nil
}

// Start begins the service operation. It accepts a context for its own lifecycle.
// It starts the processor and the consumer, then spins up a pool of workers to process messages.
func (s *ProcessingService[T]) Start(ctx context.Context) error { // CORRECTED: Added ctx parameter
	s.logger.Info().Msg("Starting generic ProcessingService...")

	// Initialize the service's shutdown context from the provided context.
	s.shutdownCtx, s.shutdownFunc = context.WithCancel(ctx) // Now derived from passed context

	// Start the processor first, passing the service's shutdown context to it.
	s.processor.Start(s.shutdownCtx) // Already passes shutdownCtx

	// Start the consumer, passing the service's shutdown context to it.
	if err := s.consumer.Start(s.shutdownCtx); err != nil { // Already passes shutdownCtx
		// If the consumer fails to start, stop the processor to clean up.
		s.processor.Stop() // Processor doesn't need context for Stop
		return fmt.Errorf("failed to start message consumer: %w", err)
	}
	s.logger.Info().Msg("Message consumer started.")

	// Start a pool of workers to process messages concurrently.
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
	s.logger.Debug().Int("worker_id", workerID).Msg("Processing worker shutting down.")

	for {
		select {
		case <-s.shutdownCtx.Done(): // Worker listens to service's shutdown context
			s.logger.Info().Int("worker_id", workerID).Msg("Processing worker shutting down.")
			return
		case msg, ok := <-s.consumer.Messages():
			if !ok {
				s.logger.Info().Int("worker_id", workerID).Msg("Consumer channel closed, worker exiting.")
				return
			}
			s.processConsumedMessage(msg, workerID)
		}
	}
}

// processConsumedMessage contains the core logic for each worker.
// REFACTORED: It now uses the MessageTransformer on the whole ConsumedMessage.
func (s *ProcessingService[T]) processConsumedMessage(msg types.ConsumedMessage, workerID int) {
	s.logger.Debug().Int("worker_id", workerID).Str("msg_id", msg.ID).Msg("Transforming message")

	// Use the new transformer, which operates on the whole message, not just the payload.
	transformedPayload, skip, err := s.transformer(msg)
	if err != nil {
		s.logger.Error().Err(err).Str("msg_id", msg.ID).Msg("Failed to transform message, Nacking.")
		msg.Nack()
		return
	}

	// The transformer can signal to skip a message (e.g., for filtering or invalid data).
	if skip {
		s.logger.Debug().Str("msg_id", msg.ID).Msg("Transformer signaled to skip message, Acking.")
		msg.Ack()
		return
	}

	// The `types.BatchedMessage` wrapper links the original message (for Ack/Nack)
	// with the successfully transformed payload.
	batchedMsg := &types.BatchedMessage[T]{
		OriginalMessage: msg,
		Payload:         transformedPayload,
	}

	select {
	case s.processor.Input() <- batchedMsg:
		s.logger.Debug().Str("msg_id", msg.ID).Msg("Payload sent to processor.")
	case <-s.shutdownCtx.Done(): // If service shuts down while sending to processor, Nack.
		s.logger.Warn().Str("msg_id", msg.ID).Msg("Shutdown in progress, Nacking message.")
		msg.Nack()
	}
}

// Stop gracefully shuts down the entire service in the correct order.
func (s *ProcessingService[T]) Stop() {
	s.logger.Info().Msg("Stopping generic ProcessingService...")

	// 1. Stop the consumer first. This is the most critical change.
	// Stopping the consumer will cancel its Receive loop and cause its output
	// channel to close after any in-flight messages are sent.
	s.logger.Info().Msg("Stopping message consumer...")
	if s.consumer != nil {
		s.consumer.Stop()
	}

	// 2. Now, wait for the workers to finish.
	// They will naturally exit because the channel they are reading from
	// (`s.consumer.Messages()`) will be closed by the consumer's shutdown process.
	s.logger.Info().Msg("Waiting for processing workers to complete...")
	s.wg.Wait()
	s.logger.Info().Msg("All processing workers completed.")

	// 3. With the pipeline drained, stop the final processor.
	s.logger.Info().Msg("Stopping message processor...")
	if s.processor != nil {
		s.processor.Stop()
	}
	s.logger.Info().Msg("Message processor stopped.")

	// 4. The initial context cancellation can now be a final cleanup step.
	if s.shutdownFunc != nil {
		s.shutdownFunc()
	}

	s.logger.Info().Msg("Generic ProcessingService stopped gracefully.")
}
