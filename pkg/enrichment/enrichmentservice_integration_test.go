//go:build integration

package enrichment_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-dataflow/pkg/enrichment"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DeviceMetadata is the concrete data structure we'll fetch for enrichment.
type DeviceMetadata struct {
	ClientID   string `json:"clientID"`
	LocationID string `json:"locationID"`
	Category   string `json:"category"`
}

// --- Test Implementation of a simple in-memory cache/source ---
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
	return s.Fetch(ctx, key)
}
func (s *InMemoryMetadataStore) WriteToCache(ctx context.Context, key string, value DeviceMetadata) error {
	s.AddDevice(key, value)
	return nil
}
func (s *InMemoryMetadataStore) Close() error { return nil }

// --- Integration Test ---

func TestEnrichmentService_WithDeviceMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	logger := zerolog.Nop()
	const (
		projectID         = "test-project"
		inputTopicID      = "raw-messages-topic"
		inputSubID        = "enrichment-service-sub"
		outputTopicID     = "enriched-messages-topic"
		deadLetterTopicID = "enrichment-dead-letter-topic"
	)

	// --- Setup ---
	pubsubEmulatorCfg := emulators.GetDefaultPubsubConfig(projectID, map[string]string{inputTopicID: inputSubID})
	emulatorConn := emulators.SetupPubsubEmulator(t, ctx, pubsubEmulatorCfg)
	clientOptions := emulatorConn.ClientOptions

	testClient, err := pubsub.NewClient(ctx, projectID, clientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = testClient.Close() })

	outputTopic, err := testClient.CreateTopic(ctx, outputTopicID)
	require.NoError(t, err)
	dltTopic, err := testClient.CreateTopic(ctx, deadLetterTopicID)
	require.NoError(t, err)
	inputTopic := testClient.Topic(inputTopicID)

	metadataStore := NewInMemoryMetadataStore()
	metadataStore.AddDevice("DEVICE_EUI_001", DeviceMetadata{ClientID: "client-123", LocationID: "location-abc", Category: "temperature-sensor"})

	// --- Component Initialization ---
	fetcherCfg := &enrichment.FetcherConfig{CacheWriteTimeout: 2 * time.Second}
	fetcher, cleanup, err := enrichment.NewCacheFallbackFetcher[string, DeviceMetadata](fetcherCfg, metadataStore, metadataStore, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanup() })

	// REFACTOR: Update all constructor calls to use correct, consistent signatures and configs.
	consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults(projectID)
	consumerCfg.ProjectID = projectID
	consumerCfg.SubscriptionID = inputSubID
	consumer, err := messagepipeline.NewGooglePubsubConsumer(ctx, consumerCfg, testClient, logger)
	require.NoError(t, err)

	producerCfg := messagepipeline.NewGooglePubsubProducerDefaults(projectID)
	producerCfg.ProjectID = projectID
	producerCfg.TopicID = outputTopicID
	producerCfg.BatchDelay = 20 * time.Millisecond
	mainProducer, err := messagepipeline.NewGooglePubsubProducer[types.PublishMessage](ctx, producerCfg, testClient, logger)
	require.NoError(t, err)

	dltPublisher, err := messagepipeline.NewGoogleSimplePublisher(ctx, testClient, deadLetterTopicID, logger)
	require.NoError(t, err)

	keyExtractor := func(msg types.ConsumedMessage) (string, bool) {
		uid, ok := msg.Attributes["uid"]
		return uid, ok
	}
	enricherFunc := func(msg *types.PublishMessage, data DeviceMetadata) {
		msg.EnrichmentData["clientID"] = data.ClientID
		msg.EnrichmentData["location"] = data.LocationID
		msg.EnrichmentData["deviceCategory"] = data.Category
	}

	transformer := enrichment.NewEnrichmentTransformer[string, DeviceMetadata](fetcher, keyExtractor, enricherFunc, dltPublisher, logger)

	processingService, err := messagepipeline.NewProcessingService(2, consumer, mainProducer, transformer, logger)
	require.NoError(t, err)

	// --- Service Lifecycle Management ---
	serviceCtx, serviceCancel := context.WithCancel(ctx)
	t.Cleanup(serviceCancel)
	err = processingService.Start(serviceCtx)
	require.NoError(t, err)

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		processingService.Stop(stopCtx)
		_ = dltPublisher.Stop(stopCtx)
	})

	// --- Test Cases ---
	t.Run("Successful Enrichment", func(t *testing.T) {
		verifierSub, err := testClient.CreateSubscription(ctx, "verifier-sub-success", pubsub.SubscriptionConfig{Topic: outputTopic})
		require.NoError(t, err)
		defer verifierSub.Delete(ctx)

		originalPayload := `{"value": 25.5}`
		publishResult := inputTopic.Publish(ctx, &pubsub.Message{Data: []byte(originalPayload), Attributes: map[string]string{"uid": "DEVICE_EUI_001"}})
		_, err = publishResult.Get(ctx)
		require.NoError(t, err)

		receivedMsg := receiveSingleMessage(t, ctx, verifierSub, 5*time.Second)
		require.NotNil(t, receivedMsg, "Did not receive an enriched message on the output topic")

		var enrichedResult types.PublishMessage
		err = json.Unmarshal(receivedMsg.Data, &enrichedResult)
		require.NoError(t, err, "Failed to unmarshal enriched message")

		require.NotNil(t, enrichedResult.EnrichmentData)
		assert.Equal(t, "client-123", enrichedResult.EnrichmentData["clientID"])
		assert.Equal(t, "location-abc", enrichedResult.EnrichmentData["location"])
	})

	t.Run("Device Not Found Sends to Dead-Letter Topic", func(t *testing.T) {
		dltVerifierSub, err := testClient.CreateSubscription(ctx, "verifier-sub-dlt", pubsub.SubscriptionConfig{Topic: dltTopic})
		require.NoError(t, err)
		defer dltVerifierSub.Delete(ctx)

		originalPayload := `{"value": 100}`
		publishResult := inputTopic.Publish(ctx, &pubsub.Message{Data: []byte(originalPayload), Attributes: map[string]string{"uid": "UNKNOWN_DEVICE"}})
		_, err = publishResult.Get(ctx)
		require.NoError(t, err)

		dltMsg := receiveSingleMessage(t, ctx, dltVerifierSub, 5*time.Second)
		require.NotNil(t, dltMsg, "Did not receive a message on the dead-letter topic")
		assert.JSONEq(t, originalPayload, string(dltMsg.Data))
		assert.Equal(t, "data_fetch_failed", dltMsg.Attributes["error"])
	})
}

// receiveSingleMessage helper
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

	if err != nil && !errors.Is(err, context.Canceled) {
		t.Logf("Receive loop ended with error: %v", err)
	}

	mu.RLock()
	defer mu.RUnlock()
	return receivedMsg
}
