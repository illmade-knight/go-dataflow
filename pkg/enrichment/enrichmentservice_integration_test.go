//go:build integration

package enrichment_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/enrichment"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/illmade-knight/go-test/emulators"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Define the concrete data structure for our test.
type DeviceMetadata struct {
	ClientID   string `json:"clientID"`
	LocationID string `json:"locationID"`
	Category   string `json:"category"`
}

// --- Test Implementation of a simple in-memory cache/source ---
// This acts as both the cache and the source for the test, satisfying the generic interfaces.
type InMemoryMetadataStore struct {
	mu   sync.RWMutex
	data map[string]DeviceMetadata
}

func NewInMemoryMetadataStore() *InMemoryMetadataStore {
	return &InMemoryMetadataStore{data: make(map[string]DeviceMetadata)}
}
func (s *InMemoryMetadataStore) AddDevice(eui string, data DeviceMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[eui] = data
}
func (s *InMemoryMetadataStore) Fetch(ctx context.Context, key string) (DeviceMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if val, ok := s.data[key]; ok {
		return val, nil
	}
	return DeviceMetadata{}, fmt.Errorf("device not found: %s", key)
}
func (s *InMemoryMetadataStore) FetchFromCache(ctx context.Context, key string) (DeviceMetadata, error) {
	return s.Fetch(ctx, key) // For the test, cache and source are the same
}
func (s *InMemoryMetadataStore) WriteToCache(ctx context.Context, key string, value DeviceMetadata) error {
	s.AddDevice(key, value) // Write just updates the map
	return nil
}
func (s *InMemoryMetadataStore) Close() error { return nil }

// --- Integration Test ---

func TestEnrichmentService_WithDeviceMetadata(t *testing.T) {
	// --- Test Setup ---
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	logger := zerolog.New(zerolog.NewConsoleWriter()).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	// 1. Configure Pub/Sub resources
	// ... (same as before)
	const (
		projectID         = "test-project"
		inputTopicID      = "raw-messages-topic"
		inputSubID        = "enrichment-service-sub"
		outputTopicID     = "enriched-messages-topic"
		deadLetterTopicID = "enrichment-dead-letter-topic"
	)

	// 2. Set up the Pub/Sub emulator
	// ... (same as before)
	pubsubEmulatorCfg := emulators.GetDefaultPubsubConfig(projectID, map[string]string{
		inputTopicID: inputSubID,
	})
	emulatorConn := emulators.SetupPubsubEmulator(t, ctx, pubsubEmulatorCfg)
	clientOptions := emulatorConn.ClientOptions

	testClient, err := pubsub.NewClient(ctx, projectID, clientOptions...)
	require.NoError(t, err)
	defer testClient.Close()

	outputTopic, err := testClient.CreateTopic(ctx, outputTopicID)
	require.NoError(t, err)
	dltTopic, err := testClient.CreateTopic(ctx, deadLetterTopicID)
	require.NoError(t, err)
	inputTopic := testClient.Topic(inputTopicID)

	// 3. Set up the concrete data source and fetcher
	metadataStore := NewInMemoryMetadataStore()
	metadataStore.AddDevice("DEVICE_EUI_001", DeviceMetadata{
		ClientID:   "client-123",
		LocationID: "location-abc",
		Category:   "temperature-sensor",
	})

	// Create the generic fallback fetcher with our concrete types
	fetcher, cleanup, err := enrichment.NewCacheFallbackFetcher[string, DeviceMetadata](
		ctx,
		metadataStore, // Acting as CachedFetcher
		metadataStore, // Acting as SourceFetcher
		logger,
	)
	require.NoError(t, err)
	defer cleanup()

	// --- Instantiate Pipeline Components ---
	sharedClient, err := pubsub.NewClient(ctx, projectID, clientOptions...)
	require.NoError(t, err)
	defer sharedClient.Close()

	consumer, err := messagepipeline.NewGooglePubsubConsumer(
		&messagepipeline.GooglePubsubConsumerConfig{ProjectID: projectID, SubscriptionID: inputSubID},
		sharedClient, logger,
	)
	require.NoError(t, err)

	mainProducer, err := messagepipeline.NewGooglePubsubProducer[types.PublishMessage](
		sharedClient,
		&messagepipeline.GooglePubsubProducerConfig{ProjectID: projectID, TopicID: outputTopicID, BatchDelay: 20 * time.Millisecond},
		logger,
	)
	require.NoError(t, err)

	dltPublisher, err := messagepipeline.NewGoogleSimplePublisher(sharedClient, deadLetterTopicID, logger)
	require.NoError(t, err)
	defer dltPublisher.Stop()

	// 4. Define our concrete KeyExtractor and Enricher functions
	keyExtractor := func(msg types.ConsumedMessage) (string, bool) {
		uid, ok := msg.Attributes["uid"]
		return uid, ok
	}
	enricherFunc := func(msg *types.PublishMessage, data DeviceMetadata) {
		msg.DeviceInfo.Name = data.ClientID
		msg.DeviceInfo.Location = data.LocationID
		msg.DeviceInfo.ServiceTag = data.Category
	}

	// 5. Instantiate the generic transformer with our concrete implementations
	transformer := enrichment.NewEnrichmentTransformer[string, DeviceMetadata](
		fetcher,
		keyExtractor,
		enricherFunc,
		dltPublisher,
		logger,
	)

	processingService, err := messagepipeline.NewProcessingService(2, consumer, mainProducer, transformer, logger)
	require.NoError(t, err)

	// --- Run Test ---
	err = processingService.Start(context.Background())
	require.NoError(t, err)
	defer processingService.Stop()

	// --- Test Case 1: Successful Enrichment ---
	t.Run("Successful Enrichment", func(t *testing.T) {
		verifierSub, err := testClient.CreateSubscription(ctx, "verifier-sub-success", pubsub.SubscriptionConfig{Topic: outputTopic})
		require.NoError(t, err)
		defer verifierSub.Delete(ctx)

		originalPayload := `{"value": 25.5}`
		publishResult := inputTopic.Publish(ctx, &pubsub.Message{
			Data:       []byte(originalPayload),
			Attributes: map[string]string{"uid": "DEVICE_EUI_001"},
		})
		_, err = publishResult.Get(ctx)
		require.NoError(t, err)

		receivedMsg := receiveSingleMessage(t, ctx, verifierSub, 5*time.Second)
		require.NotNil(t, receivedMsg, "Did not receive an enriched message on the output topic")

		var enrichedResult types.PublishMessage
		err = json.Unmarshal(receivedMsg.Data, &enrichedResult)
		require.NoError(t, err, "Failed to unmarshal enriched message")

		// Assert that the concrete DeviceMetadata was applied correctly
		assert.Equal(t, "client-123", enrichedResult.DeviceInfo.Name)
		assert.Equal(t, "location-abc", enrichedResult.DeviceInfo.Location)
		assert.Equal(t, "temperature-sensor", enrichedResult.DeviceInfo.ServiceTag)
		assert.JSONEq(t, originalPayload, string(enrichedResult.Payload))
	})

	// --- Test Case 2: Device Not Found Sends to Dead-Letter Topic ---
	t.Run("Device Not Found Sends to Dead-Letter Topic", func(t *testing.T) {
		dltVerifierSub, err := testClient.CreateSubscription(ctx, "verifier-sub-dlt", pubsub.SubscriptionConfig{Topic: dltTopic})
		require.NoError(t, err)
		defer dltVerifierSub.Delete(ctx)

		originalPayload := `{"value": 100}`
		publishResult := inputTopic.Publish(ctx, &pubsub.Message{
			Data:       []byte(originalPayload),
			Attributes: map[string]string{"uid": "UNKNOWN_DEVICE"},
		})
		_, err = publishResult.Get(ctx)
		require.NoError(t, err)

		dltMsg := receiveSingleMessage(t, ctx, dltVerifierSub, 5*time.Second)
		require.NotNil(t, dltMsg, "Did not receive a message on the dead-letter topic")
		assert.JSONEq(t, originalPayload, string(dltMsg.Data))
		assert.Equal(t, "data_fetch_failed", dltMsg.Attributes["error"])
		assert.Equal(t, "UNKNOWN_DEVICE", dltMsg.Attributes["enrichment_key"])
	})
}

// receiveSingleMessage helper (same as before)
func receiveSingleMessage(t *testing.T, ctx context.Context, sub *pubsub.Subscription, timeout time.Duration) *pubsub.Message {
	t.Helper()
	var receivedMsg *pubsub.Message
	var mu sync.RWMutex

	receiveCtx, receiveCancel := context.WithTimeout(ctx, timeout)
	defer receiveCancel()

	err := sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()
		if receivedMsg == nil {
			receivedMsg = msg
			msg.Ack()
			receiveCancel()
		} else {
			msg.Nack()
		}
	})

	if err != nil && err != context.Canceled {
		t.Logf("Receive loop ended with error: %v", err)
	}

	mu.RLock()
	defer mu.RUnlock()
	return receivedMsg
}
