package messagepipeline_test

import (
	"context"
	"encoding/json"
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

func TestGooglePubsubProducer_Publish(t *testing.T) {
	// --- Arrange ---
	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(testCancel)

	// Create unique names for this test run to allow parallel execution.
	uniqueSuffix := fmt.Sprintf("%s-%d", sanitizedTestName(t), time.Now().UnixNano())
	projectID := "proj-" + uniqueSuffix
	topicID := "topic-" + uniqueSuffix
	subID := "sub-" + uniqueSuffix

	pubsubClient, _, subscription := setupTestPubsub(t, projectID, topicID, subID)

	producerConfig := messagepipeline.NewGooglePubsubProducerDefaults(projectID, topicID)

	producer, err := messagepipeline.NewGooglePubsubProducer(testCtx, producerConfig, pubsubClient, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(producer.Stop) // Ensure producer is stopped cleanly.

	// --- Act ---
	// Create the message data to be published.
	msgToPublish := messagepipeline.MessageData{
		ID:          "test-id-123",
		Payload:     []byte("hello producer"),
		PublishTime: time.Now().UTC().Truncate(time.Second),
		EnrichmentData: map[string]interface{}{
			"source": "test",
		},
	}

	// Publish the message. We use a separate context for the publish call itself.
	publishCtx, publishCancel := context.WithTimeout(testCtx, 5*time.Second)
	t.Cleanup(publishCancel)

	publishedID, err := producer.Publish(publishCtx, msgToPublish)
	require.NoError(t, err)
	require.NotEmpty(t, publishedID)

	// --- Assert ---
	// Receive the message from the subscription to verify it was published correctly.
	var wg sync.WaitGroup
	wg.Add(1)
	var receivedMsg *pubsub.Message

	// Use a cancellable context for the receiver.
	receiveCtx, receiveCancel := context.WithCancel(testCtx)
	t.Cleanup(receiveCancel)

	go func() {
		defer wg.Done()
		err := subscription.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
			receivedMsg = msg
			msg.Ack()
			receiveCancel() // Stop receiving after the first message.
		})
		if err != nil && err != context.Canceled {
			log.Error().Err(err).Msg("Receive error")
		}
	}()

	wg.Wait() // Wait for the receiver to finish.

	require.NotNil(t, receivedMsg, "Did not receive message from subscription")

	// Unmarshal the received data and compare it to the original.
	var receivedData messagepipeline.MessageData
	err = json.Unmarshal(receivedMsg.Data, &receivedData)
	require.NoError(t, err)

	assert.Equal(t, msgToPublish.ID, receivedData.ID)
	assert.Equal(t, msgToPublish.Payload, receivedData.Payload)
	assert.WithinDuration(t, msgToPublish.PublishTime, receivedData.PublishTime, time.Second)
	assert.Equal(t, msgToPublish.EnrichmentData["source"], receivedData.EnrichmentData["source"])
}
