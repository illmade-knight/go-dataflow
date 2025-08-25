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

	"cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
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

// setupTestPubsub creates a mock Pub/Sub server and a v2 client.
// REFACTOR: This helper no longer creates topics or subscriptions. It returns the
// server instance so the test can perform administrative setup.
func setupTestPubsub(t *testing.T, projectID string) (*pubsub.Client, *pstest.Server) {
	t.Helper()
	ctx := context.Background()
	srv := pstest.NewServer()
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	opts := []option.ClientOption{option.WithGRPCConn(conn)}

	client, err := pubsub.NewClient(ctx, projectID, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client, srv
}

// =============================================================================
//  Test Cases for GooglePubsubProducer
// =============================================================================

func TestGooglePubsubProducer_PublishAndStop(t *testing.T) {
	// --- Arrange ---
	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(testCancel)

	uniqueSuffix := fmt.Sprintf("%s-%d", sanitizedTestName(t), time.Now().UnixNano())
	projectID := "proj-" + uniqueSuffix
	topicID := "topic-" + uniqueSuffix
	subID := "sub-" + uniqueSuffix

	pubsubClient, srv := setupTestPubsub(t, projectID)

	// REFACTOR: Manually create the topic and subscription on the test server's
	// GServer, as the v2 client does not have these admin functions.
	topicName := fmt.Sprintf("projects/%s/topics/%s", projectID, topicID)
	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID)
	_, err := srv.GServer.CreateTopic(testCtx, &pb.Topic{Name: topicName})
	require.NoError(t, err)
	_, err = srv.GServer.CreateSubscription(testCtx, &pb.Subscription{
		Name:  subName,
		Topic: topicName,
	})
	require.NoError(t, err)

	producerConfig := messagepipeline.NewGooglePubsubProducerDefaults(topicID)

	producer, err := messagepipeline.NewGooglePubsubProducer(producerConfig, pubsubClient, zerolog.Nop())
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
	var mu sync.Mutex
	var receivedMsg *pubsub.Message

	receiveCtx, receiveCancel := context.WithCancel(testCtx)
	t.Cleanup(receiveCancel)

	subscriber := pubsubClient.Subscriber(subID)
	go func() {
		err := subscriber.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
			mu.Lock()
			receivedMsg = msg
			mu.Unlock()
			msg.Ack()
			receiveCancel()
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Msg("Receive error")
		}
	}()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return receivedMsg != nil
	}, 5*time.Second, 50*time.Millisecond, "Did not receive message from subscription")

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
