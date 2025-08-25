package messagepipeline_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGoogleSimplePublisher_PublishAndStop(t *testing.T) {
	// --- Arrange ---
	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(testCancel)

	// Setup mock Pub/Sub server
	srv := pstest.NewServer()
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	opts := []option.ClientOption{option.WithGRPCConn(conn)}

	// Setup Pub/Sub client, topic and subscription for the test
	const projectID = "test-project"
	const topicID = "test-topic-simple"
	const subID = "test-sub-simple"

	client, err := pubsub.NewClient(testCtx, projectID, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	topicName := fmt.Sprintf("projects/%s/topics/%s", projectID, topicID)
	_, err = srv.GServer.CreateTopic(testCtx, &pb.Topic{Name: topicName})
	require.NoError(t, err)

	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID)
	_, err = srv.GServer.CreateSubscription(testCtx, &pb.Subscription{
		Name:  subName,
		Topic: topicName,
	})
	require.NoError(t, err)

	// Create the publisher instance
	publisherCfg := messagepipeline.NewGoogleSimplePublisherDefaults(topicID)
	publisher, err := messagepipeline.NewGoogleSimplePublisher(publisherCfg, client, zerolog.Nop())
	require.NoError(t, err)
	require.NotNil(t, publisher)

	// --- Act ---
	payloadToSend := []byte("hello simple publisher")
	attrsToSend := map[string]string{"source": "test"}

	err = publisher.Publish(testCtx, payloadToSend, attrsToSend)
	require.NoError(t, err)

	// --- Assert ---
	// Since publishing is async, we must receive the message to confirm it was sent.
	var mu sync.Mutex
	var receivedMsg *pubsub.Message

	// Use a cancellable context for the receiver to ensure it stops.
	receiveCtx, receiveCancel := context.WithCancel(testCtx)
	t.Cleanup(receiveCancel)

	subscriber := client.Subscriber(subID)
	go func() {
		err := subscriber.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
			mu.Lock()
			receivedMsg = msg
			mu.Unlock()
			msg.Ack()
			receiveCancel() // Stop receiving after one message
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Subscription receive error: %v", err)
		}
	}()

	// Use require.Eventually to wait for the message to arrive.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return receivedMsg != nil
	}, 5*time.Second, 50*time.Millisecond, "did not receive message in time")

	// Verify the content of the received message
	assert.Equal(t, payloadToSend, receivedMsg.Data)
	assert.Equal(t, attrsToSend["source"], receivedMsg.Attributes["source"])

	// --- Act & Assert: Stop ---
	stopCtx, stopCancel := context.WithTimeout(testCtx, 2*time.Second)
	t.Cleanup(stopCancel)

	err = publisher.Stop(stopCtx)
	require.NoError(t, err, "publisher.Stop() should return no error on success")
}

func TestNewGoogleSimplePublisher_TopicDoesNotExist(t *testing.T) {
	// --- Arrange ---
	testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(testCancel)

	srv := pstest.NewServer()
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	opts := []option.ClientOption{option.WithGRPCConn(conn)}

	client, err := pubsub.NewClient(testCtx, "test-project", opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	// --- Act & Assert ---
	// Attempt to create a publisher for a topic that does not exist.
	// In v2, the constructor will succeed, but the first publish will fail.
	publisherCfg := messagepipeline.NewGoogleSimplePublisherDefaults("non-existent-topic")
	publisher, err := messagepipeline.NewGoogleSimplePublisher(publisherCfg, client, zerolog.Nop())
	require.NoError(t, err) // Constructor should not fail.

	// The asynchronous publish will log an error, which we can't easily capture here.
	// A more advanced test could use a custom logger to capture output.
	// For now, we confirm that the Publish call itself does not block or return an error.
	err = publisher.Publish(testCtx, []byte("test"), nil)
	assert.NoError(t, err, "Fire-and-forget publish should not return an error directly")
}
