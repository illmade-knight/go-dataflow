//go:build integration

package messagepipeline_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGooglePubsubConsumer_Lifecycle_And_MessageReception tests the full flow:
// Start -> Receive Message -> Process -> Stop.
func TestGooglePubsubConsumer_Lifecycle_And_MessageReception(t *testing.T) {
	projectID := "test-consumer-lifecycle"
	topicID := "test-consumer-topic"
	subID := "test-consumer-sub"

	// Context for the entire test's lifecycle
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pubsubEmulatorCfg := emulators.GetDefaultPubsubConfig(projectID, map[string]string{
		topicID: subID,
	})
	emulatorConn := emulators.SetupPubsubEmulator(t, ctx, pubsubEmulatorCfg)
	clientOptions := emulatorConn.ClientOptions

	cfg := &messagepipeline.GooglePubsubConsumerConfig{
		ProjectID:              projectID,
		SubscriptionID:         subID,
		MaxOutstandingMessages: 1,
		NumGoroutines:          1,
	}

	client, err := pubsub.NewClient(ctx, projectID, clientOptions...)
	require.NoError(t, err)
	// REMOVED: client.Close() is no longer here. It's now closed by the parent test's cleanup (or orchestrator).

	consumer, err := messagepipeline.NewGooglePubsubConsumer(cfg, client, zerolog.Nop())
	require.NoError(t, err)
	require.NotNil(t, consumer)

	// Context for the consumer's operational lifecycle (passed to consumer.Start)
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	defer consumerCancel()

	err = consumer.Start(consumerCtx) // Pass the consumerCtx to Start
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
	require.NotNil(t, receivedMsg.DeviceInfo)
	assert.Equal(t, "device-123", receivedMsg.DeviceInfo.UID)
	assert.Equal(t, "garden", receivedMsg.DeviceInfo.Location)
	receivedMsg.Ack() // Acknowledge the message here.

	// Explicitly stop the consumer and wait for it to be done.
	err = consumer.Stop()
	require.NoError(t, err)

	select {
	case <-consumer.Done(): // Wait for the consumer's Done channel to confirm full stop
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for consumer to stop")
	}
}
