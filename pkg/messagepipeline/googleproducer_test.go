package messagepipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// =============================================================================
//  Test Helpers for Producer
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

// setupTestPubsub creates a mock Pub/Sub server, client, topic, and subscription for testing.
func setupTestPubsub(t *testing.T, projectID, topicID, subID string) (*pubsub.Client, *pubsub.Topic, *pubsub.Subscription) {
	t.Helper()
	ctx := context.Background()
	srv := pstest.NewServer()
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	opts := []option.ClientOption{option.WithGRPCConn(conn)}

	client, err := pubsub.NewClient(ctx, projectID, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	topic, err := client.CreateTopic(ctx, topicID)
	require.NoError(t, err)

	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{Topic: topic})
	require.NoError(t, err)

	return client, topic, sub
}

// =============================================================================
//  Test Cases for GooglePubsubProducer
// =============================================================================

func TestGooglePubsubProducer_PublishAndStop(t *testing.T) {
	// --- Arrange ---
	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(testCancel)

	// Create unique names for this test run to allow parallel execution.
	uniqueSuffix := fmt.Sprintf("%s-%d", sanitizedTestName(t), time.Now().UnixNano())
	projectID := "proj-" + uniqueSuffix
	topicID := "topic-" + uniqueSuffix
	subID := "sub-" + uniqueSuffix

	pubsubClient, _, subscription := setupTestPubsub(t, projectID, topicID, subID)

	producerConfig := messagepipeline.NewGooglePubsubProducerDefaults()
	producerConfig.TopicID = topicID

	producer, err := messagepipeline.NewGooglePubsubProducer(testCtx, producerConfig, pubsubClient, zerolog.Nop())
	require.NoError(t, err)

	// --- Act ---
	msgToPublish := messagepipeline.MessageData{
		ID:          "test-id-123",
		Payload:     []byte("hello producer"),
		PublishTime: time.Now().UTC().Truncate(time.Second),
		EnrichmentData: map[string]interface{}{
			"source": "test",
		},
	}

	publishedID, err := producer.Publish(testCtx, msgToPublish)
	require.NoError(t, err)
	require.NotEmpty(t, publishedID)

	// --- Assert ---
	// Receive the message from the subscription to verify it was published correctly.
	var mu sync.Mutex
	var receivedMsg *pubsub.Message

	receiveCtx, receiveCancel := context.WithCancel(testCtx)
	t.Cleanup(receiveCancel)

	go func() {
		err := subscription.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
			mu.Lock()
			receivedMsg = msg
			mu.Unlock()
			msg.Ack()
			receiveCancel() // Stop receiving after the first message.
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Msg("Receive error")
		}
	}()

	// Wait for the receiver to get the message.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return receivedMsg != nil
	}, 5*time.Second, 50*time.Millisecond, "Did not receive message from subscription")

	// Unmarshal the received data and compare it to the original.
	var receivedData messagepipeline.MessageData
	err = json.Unmarshal(receivedMsg.Data, &receivedData)
	require.NoError(t, err)

	assert.Equal(t, msgToPublish.ID, receivedData.ID)
	assert.Equal(t, msgToPublish.Payload, receivedData.Payload)
	assert.WithinDuration(t, msgToPublish.PublishTime, receivedData.PublishTime, time.Second)
	assert.Equal(t, msgToPublish.EnrichmentData["source"], receivedData.EnrichmentData["source"])

	// --- Act & Assert: Stop ---
	stopCtx, stopCancel := context.WithTimeout(testCtx, 2*time.Second)
	t.Cleanup(stopCancel)
	err = producer.Stop(stopCtx)
	require.NoError(t, err, "producer.Stop() should not return an error on graceful shutdown")
}
