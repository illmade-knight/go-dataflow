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
	"cloud.google.com/go/pubsub"

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

func TestBigQueryService_Integration_FullFlow(t *testing.T) {

	const (
		testProjectID           = "test-garden-project"
		testInputTopicID        = "garden-monitor-topic"
		testInputSubscriptionID = "garden-monitor-sub"
		testBigQueryDatasetID   = "garden_data_dataset"
		testBigQueryTableID     = "monitor_payloads"
		testDeviceUID           = "GARDEN_MONITOR_001"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// --- Emulator Setup ---
	pubsubConfig := emulators.GetDefaultPubsubConfig(testProjectID, map[string]string{testInputTopicID: testInputSubscriptionID})
	connection := emulators.SetupPubsubEmulator(t, ctx, pubsubConfig)

	bigquerySchema := map[string]interface{}{testBigQueryTableID: MonitorReadings{}}
	bigqueryCfg := emulators.GetDefaultBigQueryConfig(testProjectID, map[string]string{testBigQueryDatasetID: testBigQueryTableID}, bigquerySchema)
	bigqueryConnection := emulators.SetupBigQueryEmulator(t, ctx, bigqueryCfg)

	// --- Configuration and Client Setup ---
	logger := zerolog.New(io.Discard)

	psClient, err := pubsub.NewClient(ctx, testProjectID, connection.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })

	bqClient, err := bigquery.NewClient(ctx, testProjectID, bigqueryConnection.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bqClient.Close() })

	// --- Initialize Pipeline Components ---
	consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults(testProjectID)
	consumerCfg.SubscriptionID = testInputSubscriptionID
	consumer, err := messagepipeline.NewGooglePubsubConsumer(ctx, consumerCfg, psClient, logger)
	require.NoError(t, err)

	bqInserterCfg := &bqstore.BigQueryDatasetConfig{ProjectID: testProjectID, DatasetID: testBigQueryDatasetID, TableID: testBigQueryTableID}
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
	inputTopic := psClient.Topic(testInputTopicID)
	t.Cleanup(func() { inputTopic.Stop() })

	for i := 0; i < messageCount; i++ {
		msgDataBytes, err := json.Marshal(TestUpstreamMessage{Payload: &MonitorReadings{DE: testDeviceUID, Sequence: 100 + i}})
		require.NoError(t, err)
		pubResult := inputTopic.Publish(ctx, &pubsub.Message{Data: msgDataBytes})
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
