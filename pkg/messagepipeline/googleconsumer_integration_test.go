//go:build integration

package messagepipeline_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGooglePubsubConsumer_Lifecycle_And_MessageReception(t *testing.T) {
	projectID := "test-consumer-lifecycle"
	topicID := "test-consumer-topic"
	subID := "test-consumer-sub"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pubsubEmulatorCfg := emulators.GetDefaultPubsubConfig(projectID, map[string]string{
		topicID: subID,
	})
	emulatorConn := emulators.SetupPubsubEmulator(t, ctx, pubsubEmulatorCfg)
	clientOptions := emulatorConn.ClientOptions

	// Use the defaults which now include the new timeout field.
	cfg := messagepipeline.NewGooglePubsubConsumerDefaults(projectID)
	cfg.ProjectID = projectID
	cfg.SubscriptionID = subID
	cfg.MaxOutstandingMessages = 1
	cfg.NumGoroutines = 1

	client, err := pubsub.NewClient(ctx, projectID, clientOptions...)
	require.NoError(t, err)

	consumer, err := messagepipeline.NewGooglePubsubConsumer(ctx, cfg, client, zerolog.Nop())
	require.NoError(t, err)
	require.NotNil(t, consumer)

	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	defer consumerCancel()

	err = consumer.Start(consumerCtx)
	require.NoError(t, err)

	topic := client.Topic(topicID)
	defer topic.Stop()

	msgPayload := []byte("hello world")
	msgAttributes := map[string]string{
		"uid":      "device-123",
		"location": "garden",
	}

	result := topic.Publish(context.Background(), &pubsub.Message{
		Data:       msgPayload,
		Attributes: msgAttributes,
	})
	_, err = result.Get(context.Background())
	require.NoError(t, err)

	var receivedMsg types.ConsumedMessage
	select {
	case receivedMsg = <-consumer.Messages():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	assert.Equal(t, msgPayload, receivedMsg.Payload)
	require.NotNil(t, receivedMsg.Attributes)
	assert.Equal(t, "device-123", receivedMsg.Attributes["uid"])
	assert.Equal(t, "garden", receivedMsg.Attributes["location"])
	assert.NotContains(t, receivedMsg.Attributes, "EnrichmentData", "Consumer should not populate EnrichmentData")
	receivedMsg.Ack()

	// Pass the parent context to Stop, which has a 30s timeout.
	err = consumer.Stop(ctx)
	require.NoError(t, err)

	// Verify that the Done channel is closed, confirming a graceful shutdown.
	select {
	case <-consumer.Done():
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for consumer to stop")
	}
}
