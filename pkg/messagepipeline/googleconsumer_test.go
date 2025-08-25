package messagepipeline_test

import (
	"context"
	"fmt"
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

// setupConsumerTest creates a full Pub/Sub environment for consumer testing.
func setupConsumerTest(t *testing.T, projectID, topicID, subID string) (*pubsub.Client, *pstest.Server) {
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

	topicName := fmt.Sprintf("projects/%s/topics/%s", projectID, topicID)
	_, err = srv.GServer.CreateTopic(ctx, &pb.Topic{Name: topicName})
	require.NoError(t, err)

	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID)
	_, err = srv.GServer.CreateSubscription(ctx, &pb.Subscription{
		Name:  subName,
		Topic: topicName,
	})
	require.NoError(t, err)

	return client, srv
}

func TestGooglePubsubConsumer_ReceiveMessage(t *testing.T) {
	// --- Arrange ---
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	projectID := "test-project"
	topicID := "test-topic-consumer"
	subID := "test-sub-consumer"

	client, _ := setupConsumerTest(t, projectID, topicID, subID)

	consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults(subID)
	consumer, err := messagepipeline.NewGooglePubsubConsumer(consumerCfg, client, zerolog.Nop())
	require.NoError(t, err)

	// Start the consumer
	err = consumer.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Stop(context.Background()) })

	// --- Act ---
	// Publish a message for the consumer to receive.
	publisher := client.Publisher(topicID)
	defer publisher.Stop()

	payload := []byte("hello consumer")
	res := publisher.Publish(ctx, &pubsub.Message{
		Data: payload,
		Attributes: map[string]string{
			"source": "test-harness",
		},
	})
	_, err = res.Get(ctx)
	require.NoError(t, err)

	// --- Assert ---
	// Wait for the message to arrive on the consumer's output channel.
	select {
	case msg := <-consumer.Messages():
		assert.Equal(t, payload, msg.Payload)
		assert.Equal(t, "test-harness", msg.Attributes["source"])
		// Ack the message to complete the flow.
		msg.Ack()
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message from consumer")
	}
}

func TestGooglePubsubConsumer_Stop(t *testing.T) {
	// --- Arrange ---
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	projectID := "test-project-stop"
	topicID := "test-topic-stop"
	subID := "test-sub-stop"

	client, _ := setupConsumerTest(t, projectID, topicID, subID)
	consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults(subID)
	consumer, err := messagepipeline.NewGooglePubsubConsumer(consumerCfg, client, zerolog.Nop())
	require.NoError(t, err)

	err = consumer.Start(ctx)
	require.NoError(t, err)

	// --- Act ---
	// Stop the consumer immediately.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	err = consumer.Stop(stopCtx)
	require.NoError(t, err)

	// --- Assert ---
	// The Done() channel should be closed.
	select {
	case <-consumer.Done():
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("consumer.Done() channel was not closed after stop")
	}

	// The Messages() channel should also be closed.
	_, ok := <-consumer.Messages()
	assert.False(t, ok, "consumer.Messages() channel should be closed")
}
