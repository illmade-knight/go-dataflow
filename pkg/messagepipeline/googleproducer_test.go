package messagepipeline_test

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// =============================================================================
//  Test Helpers
// =============================================================================

func sanitizedTestName(t *testing.T) string {
	name := t.Name()
	reg := regexp.MustCompile(`[^a-zA-Z0-9-]+`)
	sanitized := reg.ReplaceAllString(name, "-")
	sanitized = regexp.MustCompile(`^-+|-+$`).ReplaceAllString(sanitized, "")
	if len(sanitized) > 20 {
		sanitized = sanitized[:20]
	}
	return sanitized
}

func setupTestPubsub(t *testing.T, projectID string, topicID string) (*pubsub.Client, *pubsub.Topic) {
	t.Helper()
	ctx := context.Background()
	srv := pstest.NewServer()
	t.Cleanup(func() { _ = srv.Close() })

	opts := []option.ClientOption{
		option.WithEndpoint(srv.Addr),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}

	client, err := pubsub.NewClient(ctx, projectID, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	topic, err := client.CreateTopic(ctx, topicID)
	require.NoError(t, err)
	// REFACTOR: Use t.Cleanup for topic deletion. The producer's Stop now handles stopping the topic.
	t.Cleanup(func() {
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer deleteCancel() // Defer is acceptable for a one-liner inside a cleanup func.
		if err := topic.Delete(deleteCtx); err != nil {
			log.Warn().Err(err).Str("topic_id", topic.ID()).Msg("Error deleting test topic during cleanup.")
		}
	})

	return client, topic
}

type TestPayload struct {
	Value string `json:"value"`
	Index int    `json:"index"`
}

// =============================================================================
//  Test Cases for GooglePubsubProducer
// =============================================================================

// TestGooglePubsubProducer_Lifecycle_And_MessageProduction has been optimized for speed.
// It removes unconditional time.Sleep calls and uses robust, timed waits.
func TestGooglePubsubProducer_Lifecycle_And_MessageProduction(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name           string
		messagesToSend int
		batchSize      int
		batchDelay     time.Duration
		isTimeBound    bool // Flag to identify the time-based batching test.
	}{
		{"Single message flushed by Stop", 1, 10, 1 * time.Second, false},
		{"Batch size reached", 5, 5, 1 * time.Second, false},
		{"Batch delay reached", 4, 10, 150 * time.Millisecond, true}, // REFACTOR: Specific short delay for this test.
		{"Multiple batches by size", 10, 3, 1 * time.Second, false},
		{"No messages sent", 0, 10, 100 * time.Millisecond, false},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			uniqueSuffix := fmt.Sprintf("%s-%d", sanitizedTestName(t), time.Now().UnixNano())
			currentProjectID := fmt.Sprintf("proj-%s", uniqueSuffix)
			dynamicTopicID := fmt.Sprintf("topic-%s", uniqueSuffix)
			pubsubClient, outputTopic := setupTestPubsub(t, currentProjectID, dynamicTopicID)

			producerConfig := messagepipeline.NewGooglePubsubProducerDefaults()
			producerConfig.TopicID = outputTopic.ID()
			producerConfig.BatchSize = tc.batchSize
			producerConfig.BatchDelay = tc.batchDelay
			producerConfig.AutoAckOnPublish = true // Enable auto-Ack/Nack for test verification.

			producerCtx, producerCancel := context.WithCancel(context.Background())
			t.Cleanup(producerCancel)

			producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload](
				ctx, producerConfig, pubsubClient, zerolog.Nop(),
			)
			require.NoError(t, err)
			producer.Start(producerCtx)

			var wgSend sync.WaitGroup
			var ackCount int32

			for i := 0; i < tc.messagesToSend; i++ {
				msgID := fmt.Sprintf("msg-%s-%d", uniqueSuffix, i)
				wgSend.Add(1)
				originalMsg := types.ConsumedMessage{
					PublishMessage: types.PublishMessage{ID: msgID},
					Ack: func() {
						atomic.AddInt32(&ackCount, 1)
						wgSend.Done()
					},
					Nack: func() {
						t.Errorf("PRODUCER TEST (%s): Message %s was unexpectedly Nacked", tc.name, msgID)
						wgSend.Done()
					},
				}
				producer.Input() <- &types.BatchedMessage[TestPayload]{
					OriginalMessage: originalMsg,
					Payload:         &TestPayload{Value: fmt.Sprintf("v-%d", i), Index: i},
				}
			}

			// REFACTOR: The unconditional sleep is removed. We only sleep if we are specifically
			// testing the time-based batch flush. This significantly speeds up other test cases.
			if tc.isTimeBound {
				time.Sleep(tc.batchDelay * 2) // Wait just long enough for the time-based batch to fire.
			}

			// For cases that are NOT flushed by time, producer.Stop() will handle the flush.
			if !tc.isTimeBound {
				stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
				t.Cleanup(stopCancel)
				err := producer.Stop(stopCtx)
				require.NoError(t, err)
			}

			// Wait for all original message ACKs to be called, with a timeout to prevent hangs.
			waitChan := make(chan struct{})
			go func() {
				wgSend.Wait()
				close(waitChan)
			}()

			select {
			case <-waitChan:
				// Success: all messages were acknowledged.
			case <-time.After(5 * time.Second): // A reasonable timeout for all acks to return.
				t.Fatalf("PRODUCER TEST (%s): Timeout waiting for all messages to be acknowledged. Got %d, want %d",
					tc.name, atomic.LoadInt32(&ackCount), tc.messagesToSend)
			}

			// If the test was time-bound, the batch should be flushed and we can now stop the producer.
			if tc.isTimeBound {
				stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
				t.Cleanup(stopCancel)
				err := producer.Stop(stopCtx)
				require.NoError(t, err)
			}

			assert.Equal(t, int32(tc.messagesToSend), atomic.LoadInt32(&ackCount), "Final ack count mismatch")
		})
	}
}
