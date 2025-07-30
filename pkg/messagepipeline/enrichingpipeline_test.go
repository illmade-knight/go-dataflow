package messagepipeline_test

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestEnrichmentService(t *testing.T) {
	t.Run("Success Path", func(t *testing.T) {
		consumer := NewMockMessageConsumer(10)
		var enricherCalled, processorCalled int32

		enricher := func(ctx context.Context, msg *messagepipeline.Message) (bool, error) {
			atomic.AddInt32(&enricherCalled, 1)
			msg.EnrichmentData = map[string]interface{}{"enriched": true}
			return false, nil
		}
		processor := func(ctx context.Context, msg *messagepipeline.Message) error {
			atomic.AddInt32(&processorCalled, 1)
			require.True(t, msg.EnrichmentData["enriched"].(bool))
			return nil
		}

		cfg := messagepipeline.EnrichmentServiceConfig{NumWorkers: 1}
		service, err := messagepipeline.NewEnrichmentService(cfg, consumer, enricher, processor, zerolog.Nop())
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_ = service.Start(ctx)

		var acked int32
		msg := messagepipeline.Message{Ack: func() { atomic.AddInt32(&acked, 1) }}
		consumer.Push(msg)

		require.Eventually(t, func() bool { return atomic.LoadInt32(&acked) == 1 }, time.Second, 5*time.Millisecond)
		assert.Equal(t, int32(1), atomic.LoadInt32(&enricherCalled))
		assert.Equal(t, int32(1), atomic.LoadInt32(&processorCalled))
		_ = service.Stop(context.Background())
	})

	t.Run("Enricher Skip", func(t *testing.T) {
		consumer := NewMockMessageConsumer(10)
		processor := func(ctx context.Context, msg *messagepipeline.Message) error {
			t.Error("processor should not be called")
			return nil
		}
		enricher := func(ctx context.Context, msg *messagepipeline.Message) (bool, error) {
			return true, nil // Skip
		}

		service, _ := messagepipeline.NewEnrichmentService(messagepipeline.EnrichmentServiceConfig{NumWorkers: 1}, consumer, enricher, processor, zerolog.Nop())
		_ = service.Start(context.Background())

		var acked int32
		msg := messagepipeline.Message{Ack: func() { atomic.AddInt32(&acked, 1) }}
		consumer.Push(msg)

		require.Eventually(t, func() bool { return atomic.LoadInt32(&acked) == 1 }, time.Second, 5*time.Millisecond, "should ack on skip")
		_ = service.Stop(context.Background())
	})

	t.Run("Processor Error", func(t *testing.T) {
		consumer := NewMockMessageConsumer(10)
		enricher := func(ctx context.Context, msg *messagepipeline.Message) (bool, error) { return false, nil }
		processor := func(ctx context.Context, msg *messagepipeline.Message) error { return errors.New("processor failed") }

		service, _ := messagepipeline.NewEnrichmentService(messagepipeline.EnrichmentServiceConfig{NumWorkers: 1}, consumer, enricher, processor, zerolog.Nop())
		_ = service.Start(context.Background())

		var nacked int32
		msg := messagepipeline.Message{Nack: func() { atomic.AddInt32(&nacked, 1) }}
		consumer.Push(msg)

		require.Eventually(t, func() bool { return atomic.LoadInt32(&nacked) == 1 }, time.Second, 5*time.Millisecond, "should nack on processor error")
		_ = service.Stop(context.Background())
	})
}
