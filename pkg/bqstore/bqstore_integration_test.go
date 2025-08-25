//go:build integration

package bqstore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow/pkg/bqstore"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// MonitorReadings represents the structure of the data written to BigQuery.
type MonitorReadings struct {
	DE       string
	Sequence int
	Battery  int
}

// TestUpstreamMessage represents the structure of the message published to Pub/Sub.
type TestUpstreamMessage struct {
	Payload *MonitorReadings `json:"payload"`
}

// ConsumedMessageTransformer implements the MessageTransformer logic for this test.
func ConsumedMessageTransformer(_ context.Context, msg *messagepipeline.Message) (*MonitorReadings, bool, error) {
	var upstreamMsg TestUpstreamMessage
	if err := json.Unmarshal(msg.Payload, &upstreamMsg); err != nil {
		return nil, false, fmt.Errorf("failed to unmarshal upstream message: %w", err)
	}
	if upstreamMsg.Payload == nil {
		return nil, true, nil // Skip message but Ack.
	}
	return upstreamMsg.Payload, false, nil // Success.
}

// createPubsubResources is a test helper that encapsulates the administrative
// task of creating and tearing down the Pub/Sub topic and subscription.
func createPubsubResources(t *testing.T, ctx context.Context, client *pubsub.Client, projectID, topicID, subID string) {
	t.Helper()
	topicAdmin := client.TopicAdminClient
	subAdmin := client.SubscriptionAdminClient

	topicName := fmt.Sprintf("projects/%s/topics/%s", projectID, topicID)
	_, err := topicAdmin.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = topicAdmin.DeleteTopic(context.Background(), &pubsubpb.DeleteTopicRequest{Topic: topicName})
	})

	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID)
	_, err = subAdmin.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  subName,
		Topic: topicName,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = subAdmin.DeleteSubscription(context.Background(), &pubsubpb.DeleteSubscriptionRequest{Subscription: subName})
	})
}

func TestBigQueryService_Integration_FullFlow(t *testing.T) {
	const testProjectID = "test-garden-project"
	// REFACTOR: Use unique names for test resources.
	runID := uuid.NewString()
	testInputTopicID := "garden-monitor-topic-" + runID
	testInputSubscriptionID := "garden-monitor-sub-" + runID
	testBigQueryDatasetID := "garden_data_dataset"
	testBigQueryTableID := "monitor_payloads"
	const testDeviceUID = "GARDEN_MONITOR_001"

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// --- Emulator Setup ---
	// REFACTOR: Use the updated GetDefaultPubsubConfig.
	pubsubConfig := emulators.GetDefaultPubsubConfig(testProjectID)
	connection := emulators.SetupPubsubEmulator(t, ctx, pubsubConfig)

	bigquerySchema := map[string]interface{}{testBigQueryTableID: MonitorReadings{}}
	bigqueryCfg := emulators.GetDefaultBigQueryConfig(testProjectID, map[string]string{testBigQueryDatasetID: testBigQueryTableID}, bigquerySchema)
	bigqueryConnection := emulators.SetupBigQueryEmulator(t, ctx, bigqueryCfg)

	// --- Configuration and Client Setup ---
	logger := zerolog.New(io.Discard)

	// REFACTOR: Use v2 pubsub client.
	psClient, err := pubsub.NewClient(ctx, testProjectID, connection.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })

	// REFACTOR: Create test-specific pubsub resources.
	createPubsubResources(t, ctx, psClient, testProjectID, testInputTopicID, testInputSubscriptionID)

	bqClient, err := bigquery.NewClient(ctx, testProjectID, bigqueryConnection.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bqClient.Close() })

	// --- Initialize Pipeline Components ---
	consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults(testInputSubscriptionID)
	consumer, err := messagepipeline.NewGooglePubsubConsumer(consumerCfg, psClient, logger)
	require.NoError(t, err)

	bqInserterCfg := &bqstore.BigQueryDatasetConfig{DatasetID: testBigQueryDatasetID, TableID: testBigQueryTableID}
	bqInserter, err := bqstore.NewBigQueryInserter[MonitorReadings](ctx, bqClient, bqInserterCfg, logger)
	require.NoError(t, err)

	serviceCfg := messagepipeline.BatchingServiceConfig{
		NumWorkers:    2,
		BatchSize:     5,
		FlushInterval: time.Second,
	}

	processingService, err := bqstore.NewBigQueryService[MonitorReadings](serviceCfg, consumer, bqInserter, ConsumedMessageTransformer, logger)
	require.NoError(t, err)

	// --- Start the Service ---
	serviceCtx, serviceCancel := context.WithCancel(ctx)
	t.Cleanup(serviceCancel)
	err = processingService.Start(serviceCtx)
	require.NoError(t, err)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		_ = processingService.Stop(stopCtx)
	})

	// --- Publish Test Messages ---
	const messageCount = 7
	// REFACTOR: Use the v2 publisher.
	publisher := psClient.Publisher(testInputTopicID)
	t.Cleanup(func() { publisher.Stop() })

	for i := 0; i < messageCount; i++ {
		msgDataBytes, err := json.Marshal(TestUpstreamMessage{Payload: &MonitorReadings{DE: testDeviceUID, Sequence: 100 + i}})
		require.NoError(t, err)
		pubResult := publisher.Publish(ctx, &pubsub.Message{Data: msgDataBytes})
		_, err = pubResult.Get(ctx)
		require.NoError(t, err)
	}
	t.Logf("%d test messages published.", messageCount)

	// --- Verification ---
	getRowCount := func() (int, error) {
		q := bqClient.Query(fmt.Sprintf("SELECT count(*) FROM `%s.%s`", testBigQueryDatasetID, testBigQueryTableID))
		it, err := q.Read(ctx)
		if err != nil {
			return -1, err
		}
		var row []bigquery.Value
		err = it.Next(&row)
		if err != nil {
			return -1, err
		}
		return int(row[0].(int64)), nil
	}

	require.Eventually(t, func() bool {
		count, err := getRowCount()
		if err != nil {
			return false
		}
		return count == messageCount
	}, 15*time.Second, 200*time.Millisecond, "Expected to find %d rows in BigQuery, but didn't.", messageCount)

	t.Log("Successfully verified all rows were written to BigQuery.")
}
