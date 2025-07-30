package enrichment

import (
	"context"
	"fmt"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"sync"

	"github.com/rs/zerolog"
)

// EnrichmentServiceConfig holds configuration for an EnrichmentService.
type EnrichmentServiceConfig struct {
	NumWorkers int
}

// EnrichmentService orchestrates a pipeline that consumes messages, enriches them
// in-place, and sends them to a final processor function.
type EnrichmentService struct {
	cfg       EnrichmentServiceConfig
	enricher  MessageEnricher
	consumer  messagepipeline.MessageConsumer
	processor messagepipeline.MessageProcessor
	logger    zerolog.Logger
	wg        sync.WaitGroup
}

// NewEnrichmentService creates a new EnrichmentService.
func NewEnrichmentService(
	cfg EnrichmentServiceConfig,
	enricher MessageEnricher,
	consumer messagepipeline.MessageConsumer,
	processor messagepipeline.MessageProcessor,
	logger zerolog.Logger,
) (*EnrichmentService, error) {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 5
	}
	if consumer == nil || enricher == nil || processor == nil {
		return nil, fmt.Errorf("consumer, enricher, and processor cannot be nil")
	}

	return &EnrichmentService{
		cfg:       cfg,
		consumer:  consumer,
		enricher:  enricher,
		processor: processor,
		logger:    logger.With().Str("service", "EnrichmentService").Logger(),
	}, nil
}

// Start begins the service operation.
func (s *EnrichmentService) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting enrichment service...")

	if err := s.consumer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start message consumer: %w", err)
	}
	s.logger.Info().Msg("Message consumer started.")

	s.logger.Info().Int("worker_count", s.cfg.NumWorkers).Msg("Starting processing workers...")
	s.wg.Add(s.cfg.NumWorkers)
	for i := 0; i < s.cfg.NumWorkers; i++ {
		go s.worker(ctx, i)
	}

	s.logger.Info().Msg("Enrichment service started successfully.")
	return nil
}

// Stop gracefully shuts down the entire service.
func (s *EnrichmentService) Stop(ctx context.Context) error {
	s.logger.Info().Msg("Stopping enrichment service...")
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

	s.logger.Info().Msg("Enrichment service stopped.")
	return nil
}

// worker is the main processing loop for each concurrent worker.
func (s *EnrichmentService) worker(ctx context.Context, workerID int) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-s.consumer.Messages():
			if !ok {
				return
			}
			// We pass a pointer to the message throughout the process.
			s.processConsumedMessage(ctx, &msg, workerID)
		}
	}
}

// processConsumedMessage contains the core logic for a single message.
func (s *EnrichmentService) processConsumedMessage(ctx context.Context, msg *messagepipeline.Message, workerID int) {
	skip, err := s.enricher(ctx, msg)
	if err != nil {
		s.logger.Error().Err(err).Str("msg_id", msg.ID).Msg("Enricher failed, Nacking.")
		msg.Nack()
		return
	}
	if skip {
		s.logger.Debug().Str("msg_id", msg.ID).Msg("Enricher signaled to skip message, Acking.")
		msg.Ack()
		return
	}

	if err := s.processor(ctx, msg); err != nil {
		s.logger.Error().Err(err).Str("msg_id", msg.ID).Msg("Processor failed, Nacking.")
		msg.Nack()
		return
	}

	msg.Ack()
}
