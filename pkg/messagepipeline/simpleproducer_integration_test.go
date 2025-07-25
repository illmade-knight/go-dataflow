//go:build integration

package messagepipeline_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"testing"
	"time"

	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGoogleSimplePublisher_Integration(t *testing.T) {
	// --- Test Setup ---
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Configure Pub/Sub resources for the test.
	const (
		projectID = "test-simple-producer-project"
		topicID   = "test-simple-producer-topic"
		subID     = "test-simple-producer-sub"
	)

	// 2. Set up the Pub/Sub emulator.
	pubsubEmulatorCfg := emulators.GetDefaultPubsubConfig(projectID, map[string]string{
		topicID: subID,
	})
	emulatorConn := emulators.SetupPubsubEmulator(t, ctx, pubsubEmulatorCfg)
	clientOptions := emulatorConn.ClientOptions

	// 3. Instantiate the SimplePublisher
	client, err := pubsub.NewClient(ctx, projectID, clientOptions...)
	require.NoError(t, err)
	defer client.Close()

	publisher, err := messagepipeline.NewGoogleSimplePublisher(client, topicID, zerolog.Nop())
	require.NoError(t, err)
	defer publisher.Stop()

	// 4. Verification Setup
	verifierSub := client.Subscription(subID)

	// --- Run Test ---
	t.Run("Publish sends message successfully", func(t *testing.T) {
		payload := []byte("hello from simple publisher")
		attributes := map[string]string{"source": "test"}

		err := publisher.Publish(ctx, payload, attributes)
		require.NoError(t, err)

		// --- Verification ---
		receivedMsg := receiveSingleMessage(t, ctx, verifierSub, 5*time.Second)
		require.NotNil(t, receivedMsg, "Did not receive a message from the simple publisher")

		assert.Equal(t, string(payload), string(receivedMsg.Data))
		assert.Equal(t, "test", receivedMsg.Attributes["source"])
	})
}
