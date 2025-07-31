//go:build integration

package icestore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/illmade-knight/go-dataflow/pkg/icestore"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// --- Test-Specific Data Structures ---
type TestPayload struct {
	Sensor   string `json:"sensor"`
	Reading  int    `json:"reading"`
	DeviceID string `json:"device_id"`
}

type PublishedMessage struct {
	Payload     TestPayload
	PublishTime time.Time
	Location    string
}

// --- Table-Driven Test Main ---
func TestIceStorageService_Integration(t *testing.T) {

	const (
		testProjectID      = "icestore-test-project"
		testTopicID        = "icestore-test-topic"
		testSubscriptionID = "icestore-test-sub"
		testBucketName     = "icestore-test-bucket"
	)

	// --- One-time Setup for Emulators ---
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	logger := zerolog.New(os.Stderr).Level(zerolog.InfoLevel)

	pubsubConfig := emulators.GetDefaultPubsubConfig(testProjectID, map[string]string{testTopicID: testSubscriptionID})
	pubsubConnection := emulators.SetupPubsubEmulator(t, ctx, pubsubConfig)

	gcsConfig := emulators.GetDefaultGCSConfig(testProjectID, testBucketName)
	connection := emulators.SetupGCSEmulator(t, ctx, gcsConfig)
	gcsClient := emulators.GetStorageClient(t, ctx, gcsConfig, connection.ClientOptions)

	// --- Test Cases Definition ---
	testCases := []struct {
		name              string
		batchSize         int
		flushInterval     time.Duration
		messagesToPublish []PublishedMessage
		expectedObjects   int
	}{
		{
			name:          "Mixed batch size and interval flush",
			batchSize:     10, // Larger batch size to allow flush interval to trigger grouping
			flushInterval: 2 * time.Second,
			messagesToPublish: []PublishedMessage{
				{Payload: TestPayload{DeviceID: "dev-a1"}, Location: "loc-a", PublishTime: time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)},
				{Payload: TestPayload{DeviceID: "dev-b1"}, Location: "loc-b", PublishTime: time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)},
				{Payload: TestPayload{DeviceID: "dev-a2"}, Location: "loc-a", PublishTime: time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)},
				{Payload: TestPayload{DeviceID: "dev-c1"}, Location: "", PublishTime: time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)},
			},
			expectedObjects: 3, // loc-a gets 1 file, loc-b gets 1, and no-location gets 1
		},
		{
			name:          "Multiple full batches",
			batchSize:     10,
			flushInterval: 5 * time.Second,
			messagesToPublish: []PublishedMessage{
				{Payload: TestPayload{DeviceID: "dev-a1"}, Location: "loc-a", PublishTime: time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)},
				{Payload: TestPayload{DeviceID: "dev-b1"}, Location: "loc-b", PublishTime: time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)},
				{Payload: TestPayload{DeviceID: "dev-a2"}, Location: "loc-a", PublishTime: time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)},
				{Payload: TestPayload{DeviceID: "dev-b2"}, Location: "loc-b", PublishTime: time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)},
			},
			expectedObjects: 2, // loc-a gets 1 file, loc-b gets 1
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// --- Per-Test Setup ---
			testCtx, testCancel := context.WithTimeout(ctx, 1*time.Minute)
			t.Cleanup(testCancel)

			require.NoError(t, clearBucket(testCtx, gcsClient.Bucket(testBucketName)), "Failed to clear GCS bucket")

			psClient, err := pubsub.NewClient(testCtx, testProjectID, pubsubConnection.ClientOptions...)
			require.NoError(t, err)
			t.Cleanup(func() { _ = psClient.Close() })

			// --- Initialize Service Components ---
			consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults()
			consumerCfg.SubscriptionID = testSubscriptionID
			consumer, err := messagepipeline.NewGooglePubsubConsumer(testCtx, consumerCfg, psClient, logger)
			require.NoError(t, err)

			serviceCfg := icestore.IceStorageServiceConfig{
				NumWorkers:    2,
				BatchSize:     tc.batchSize,
				FlushInterval: tc.flushInterval,
			}
			uploaderCfg := icestore.GCSBatchUploaderConfig{
				BucketName:   testBucketName,
				ObjectPrefix: "archived-data",
			}

			service, err := icestore.NewIceStorageService(
				serviceCfg,
				consumer,
				icestore.NewGCSClientAdapter(gcsClient),
				uploaderCfg,
				icestore.ArchivalTransformer,
				logger,
			)
			require.NoError(t, err)

			// --- Run the Service ---
			serviceCtx, serviceCancel := context.WithCancel(testCtx)
			t.Cleanup(serviceCancel)
			err = service.Start(serviceCtx)
			require.NoError(t, err)
			t.Cleanup(func() {
				stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer stopCancel()
				_ = service.Stop(stopCtx)
			})

			// --- Publish Test Messages ---
			publishMessages(t, testCtx, psClient, testTopicID, tc.messagesToPublish)

			// --- Verification ---
			require.Eventually(t, func() bool {
				objects, err := listGCSObjectAttrs(testCtx, gcsClient.Bucket(testBucketName))
				if err != nil {
					t.Logf("Verification failed to list objects, will retry: %v", err)
					return false
				}
				return len(objects) == tc.expectedObjects
			}, 20*time.Second, 500*time.Millisecond, "Expected %d objects in GCS, but found a different number.", tc.expectedObjects)
		})
	}
}

// publishMessages is a helper to publish a slice of messages for a test case.
func publishMessages(t *testing.T, ctx context.Context, client *pubsub.Client, testTopicID string, messages []PublishedMessage) {
	t.Helper()
	if len(messages) == 0 {
		return
	}
	topic := client.Topic(testTopicID)
	defer topic.Stop()

	for _, msg := range messages {
		payloadBytes, _ := json.Marshal(msg.Payload)
		attributes := map[string]string{
			"uid":               msg.Payload.DeviceID,
			"location":          msg.Location,
			"test_publish_time": msg.PublishTime.Format(time.RFC3339),
		}

		pubResult := topic.Publish(ctx, &pubsub.Message{
			Data:       payloadBytes,
			Attributes: attributes,
		})
		_, err := pubResult.Get(ctx)
		require.NoError(t, err)
	}
}

// clearBucket deletes all objects in a GCS bucket.
func clearBucket(ctx context.Context, bucket *storage.BucketHandle) error {
	it := bucket.Objects(ctx, nil)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("list objects for deletion: %w", err)
		}
		if err := bucket.Object(attrs.Name).Delete(ctx); err != nil {
			return fmt.Errorf("delete object %s: %w", attrs.Name, err)
		}
	}
	return nil
}

// listGCSObjectAttrs lists all object attributes in a bucket.
func listGCSObjectAttrs(ctx context.Context, bucket *storage.BucketHandle) ([]*storage.ObjectAttrs, error) {
	var attrs []*storage.ObjectAttrs
	it := bucket.Objects(ctx, &storage.Query{Prefix: "archived-data/"})
	for {
		objAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}
		attrs = append(attrs, objAttrs)
	}
	return attrs, nil
}
