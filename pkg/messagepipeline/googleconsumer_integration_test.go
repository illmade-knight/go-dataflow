//go:build integration

package messagepipeline_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGooglePubsubConsumer_Lifecycle_And_MessageReception(t *testing.T) {
	// --- Arrange ---
	projectID := "test-consumer-lifecycle"
	topicID := "test-consumer-topic"
	subID := "test-consumer-sub"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	// Setup Pub/Sub emulator
	pubsubEmulatorCfg := emulators.GetDefaultPubsubConfig(projectID, map[string]string{
		topicID: subID,
	})
	emulatorConn := emulators.SetupPubsubEmulator(t, ctx, pubsubEmulatorCfg)
	clientOptions := emulatorConn.ClientOptions

	// Configure the consumer
	cfg := messagepipeline.NewGooglePubsubConsumerDefaults()
	cfg.SubscriptionID = subID
	cfg.MaxOutstandingMessages = 1
	cfg.NumGoroutines = 1

	client, err := pubsub.NewClient(ctx, projectID, clientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	// Create the consumer instance
	consumer, err := messagepipeline.NewGooglePubsubConsumer(ctx, cfg, client, zerolog.Nop())
	require.NoError(t, err)
	require.NotNil(t, consumer)

	// --- Act: Start and Publish ---
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	t.Cleanup(consumerCancel)

	err = consumer.Start(consumerCtx)
	require.NoError(t, err)

	// Publish a test message
	topic := client.Topic(topicID)
	t.Cleanup(func() { topic.Stop() })

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

	// --- Assert: Receive and Verify Message ---
	var receivedMsg messagepipeline.Message
	var mu sync.Mutex

	// Start a goroutine to listen for the message from the consumer's channel.
	go func() {
		select {
		case msg := <-consumer.Messages():
			mu.Lock()
			receivedMsg = msg
			mu.Unlock()
		case <-consumerCtx.Done():
			// Consumer context was cancelled, nothing to do.
			return
		}
	}()

	// Use require.Eventually to poll until the message is received.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		// The condition is met if the message ID is not empty.
		return receivedMsg.ID != ""
	}, 10*time.Second, 100*time.Millisecond, "timed out waiting for message")

	// Now that we have the message, we can safely access it and assert on its contents.
	mu.Lock()
	assert.Equal(t, msgPayload, receivedMsg.Payload)
	require.NotNil(t, receivedMsg.Attributes)
	assert.Equal(t, "device-123", receivedMsg.Attributes["uid"])
	assert.Equal(t, "garden", receivedMsg.Attributes["location"])
	assert.NotContains(t, receivedMsg.Attributes, "EnrichmentData", "Consumer should not populate EnrichmentData")
	receivedMsg.Ack()
	mu.Unlock()

	// --- Act: Stop and Verify Shutdown ---
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(stopCancel)
	err = consumer.Stop(stopCtx)
	require.NoError(t, err)

	// Verify that the Done channel is closed, confirming a graceful shutdown.
	select {
	case <-consumer.Done():
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for consumer to stop")
	}
}
