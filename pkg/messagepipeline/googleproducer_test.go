package messagepipeline_test

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
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
	t.Cleanup(func() { _ = client.Close() }) // Client is cleaned up here.

	topic, err := client.CreateTopic(ctx, topicID)
	require.NoError(t, err)
	t.Cleanup(func() {
		topic.Stop() // Stop the topic first
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer deleteCancel()
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

func TestGooglePubsubProducer_InitializationErrors(t *testing.T) {
	t.Parallel()
	baseProjectID := "test-init-project"
	ctx := context.Background()

	t.Run("nil client", func(t *testing.T) {
		t.Parallel()
		cfg := &messagepipeline.GooglePubsubProducerConfig{ProjectID: baseProjectID, TopicID: "any-topic"}
		producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload](ctx, nil, cfg, zerolog.Nop())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pubsub client cannot be nil")
		assert.Nil(t, producer)
	})

	t.Run("non-existent topic", func(t *testing.T) {
		t.Parallel()
		localUniqueID := fmt.Sprintf("%s-%d", sanitizedTestName(t), time.Now().UnixNano())
		localProjectID := fmt.Sprintf("%s-%s", baseProjectID, localUniqueID)
		localSrv := pstest.NewServer()
		t.Cleanup(func() { _ = localSrv.Close() })

		opts := []option.ClientOption{
			option.WithEndpoint(localSrv.Addr),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		}
		localClient, err := pubsub.NewClient(context.Background(), localProjectID, opts...)
		require.NoError(t, err)
		t.Cleanup(func() { _ = localClient.Close() })

		cfg := &messagepipeline.GooglePubsubProducerConfig{ProjectID: localProjectID, TopicID: "non-existent-topic"}
		producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload](ctx, localClient, cfg, zerolog.Nop())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "topic non-existent-topic does not exist")
		assert.Nil(t, producer)
	})
}

func TestGooglePubsubProducer_Lifecycle_And_MessageProduction(t *testing.T) {
	baseProjectID := "test-producer-project"
	baseTopicID := "test-producer-output-topic"
	testLogger := zerolog.Nop()
	ctx := context.Background()

	testCases := []struct {
		name           string
		messagesToSend int
		batchSize      int
		batchDelay     time.Duration
	}{
		{"Single message, flushed by Stop", 1, 10, 1 * time.Hour},
		{"Batch size reached", 5, 3, 1 * time.Hour},
		{"Batch delay reached", 4, 10, 100 * time.Millisecond},
		{"Multiple batches by size", 10, 3, 1 * time.Hour},
		{"No messages sent", 0, 10, 100 * time.Millisecond},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			uniqueSuffix := fmt.Sprintf("%s-%d", sanitizedTestName(t), time.Now().UnixNano())
			currentProjectID := fmt.Sprintf("%s-%s", baseProjectID, uniqueSuffix)
			dynamicTopicID := fmt.Sprintf("%s-%s", baseTopicID, uniqueSuffix)
			pubsubClient, outputTopic := setupTestPubsub(t, currentProjectID, dynamicTopicID)

			producerConfig := &messagepipeline.GooglePubsubProducerConfig{
				ProjectID:              currentProjectID,
				TopicID:                outputTopic.ID(),
				BatchSize:              tc.batchSize,
				BatchDelay:             tc.batchDelay,
				InputChannelMultiplier: 2,    // Explicitly set to a positive value to avoid warning
				AutoAckOnPublish:       true, // CRITICAL: Enable auto-Ack/Nack for test verification
			}

			producerCtx, producerCancel := context.WithCancel(context.Background())
			defer producerCancel()

			producer, err := messagepipeline.NewGooglePubsubProducer[TestPayload](
				ctx, pubsubClient, producerConfig, testLogger,
			)
			require.NoError(t, err)

			producer.Start(producerCtx) // Pass context

			var wgSend sync.WaitGroup // This WaitGroup tracks *original message Ack/Nack callbacks*
			var ackCount int32        // This tracks actual ACKs from the original message callbacks

			for i := 0; i < tc.messagesToSend; i++ {
				msgID := fmt.Sprintf("msg-%s-%d", uniqueSuffix, i)

				wgSend.Add(1) // Add to WaitGroup for each message sent to producer input

				originalMsg := types.ConsumedMessage{
					PublishMessage: types.PublishMessage{ID: msgID},
					Ack: func() {
						atomic.AddInt32(&ackCount, 1)
						wgSend.Done()
					},
					Nack: func() {
						t.Errorf("PRODUCER TEST (%s): Message %s was unexpectedly Nacked", tc.name, msgID) // Log original test name here
						wgSend.Done()
					},
				}

				producer.Input() <- &types.BatchedMessage[TestPayload]{
					OriginalMessage: originalMsg, // This message carries the Ack/Nack callbacks
					Payload:         &TestPayload{Value: fmt.Sprintf("v-%d", i), Index: i},
				}
			}

			// Give the producer's internal goroutine a moment to pick up all messages
			// and initiate publishes before calling Stop.
			if tc.messagesToSend > 0 {
				time.Sleep(100 * time.Millisecond) // This strategic sleep for pstest quirks
			}

			producer.Stop() // This will cause p.topic.Stop() which blocks until publishes are done.

			// Wait for all original message ACKs/NACKs to be called.
			waitChan := make(chan struct{})
			go func() {
				wgSend.Wait()
				close(waitChan)
			}()

			select {
			case <-waitChan:
				t.Logf("PRODUCER TEST (%s): All messages acknowledged/nacked.", tc.name)
			case <-time.After(15 * time.Second): // Ample time for Pub/Sub confirms
				t.Fatalf("PRODUCER TEST (%s): Timeout waiting for all messages to be acknowledged. Got %d, want %d",
					tc.name, atomic.LoadInt32(&ackCount), tc.messagesToSend)
			}

			// Wait for the producer's internal goroutine to fully stop.
			select {
			case <-producer.Done():
				t.Logf("PRODUCER TEST (%s): Producer confirmed stopped.", tc.name)
			case <-time.After(5 * time.Second):
				t.Fatalf("PRODUCER TEST (%s): Timeout waiting for producer's internal goroutine to stop.", tc.name)
			}

			// Final assertions for total ACK count.
			assert.Equal(t, int32(tc.messagesToSend), atomic.LoadInt32(&ackCount), "Ack count mismatch")
		})
	}
}
