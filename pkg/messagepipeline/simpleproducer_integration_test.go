//go:build integration

package messagepipeline_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGoogleSimplePublisher_Integration(t *testing.T) {
	// --- Test Setup ---
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// REFACTOR: Use t.Cleanup instead of defer for context cancellation.
	t.Cleanup(cancel)

	const (
		projectID = "test-simple-producer-project"
		topicID   = "test-simple-producer-topic"
		subID     = "test-simple-producer-sub"
	)

	// Set up the Pub/Sub emulator.
	pubsubEmulatorCfg := emulators.GetDefaultPubsubConfig(projectID, map[string]string{
		topicID: subID,
	})
	emulatorConn := emulators.SetupPubsubEmulator(t, ctx, pubsubEmulatorCfg)
	clientOptions := emulatorConn.ClientOptions

	// Instantiate the Pub/Sub client.
	client, err := pubsub.NewClient(ctx, projectID, clientOptions...)
	require.NoError(t, err)
	// REFACTOR: Use t.Cleanup for client teardown. It will run before the context is cancelled.
	t.Cleanup(func() {
		assert.NoError(t, client.Close(), "client.Close() should not return an error")
	})

	// Instantiate the SimplePublisher using the test's context.
	publisher, err := messagepipeline.NewGoogleSimplePublisher(ctx, client, topicID, zerolog.Nop())
	require.NoError(t, err)
	// REFACTOR: Use t.Cleanup for publisher teardown.
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		err := publisher.Stop(stopCtx)
		assert.NoError(t, err, "publisher.Stop() should not return an error on graceful shutdown")
	})

	// Set up a subscription to verify that messages are published.
	verifierSub := client.Subscription(subID)

	// --- Run Test ---
	t.Run("Publish sends message successfully", func(t *testing.T) {
		payload := []byte("hello from simple publisher")
		attributes := map[string]string{"source": "test"}

		err := publisher.Publish(ctx, payload, attributes)
		require.NoError(t, err)

		// --- Verification ---
		// Use a helper to receive one message with a timeout.
		receivedMsg := receiveSingleMessage(t, ctx, verifierSub, 5*time.Second)
		require.NotNil(t, receivedMsg, "Did not receive a message from the simple publisher")

		assert.Equal(t, string(payload), string(receivedMsg.Data))
		assert.Equal(t, "test", receivedMsg.Attributes["source"])

		// Acknowledge the message to remove it from the subscription.
		receivedMsg.Ack()
	})
}
