//go:build integration

package bqstore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	emulators2 "github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/types"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-iot/pkg/bqstore"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline" // Using shared consumers package
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

// --- Constants for the integration test environment (Unchanged) ---
const (
	testProjectID = "test-garden-project"

	testInputTopicID        = "garden-monitor-topic"
	testInputSubscriptionID = "garden-monitor-sub"

	testBigQueryDatasetID = "garden_data_dataset"
	testBigQueryTableID   = "monitor_payloads"
	testDeviceUID         = "GARDEN_MONITOR_001"
)

type TestUpstreamMessage struct {
	Topic     string
	MessageID string
	Timestamp time.Time
	Payload   *types.GardenMonitorReadings
}

// TestBigQueryService_Integration_FullFlow tests the entire generic bqstore flow.
func TestBigQueryService_Integration_FullFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pubsubCtx, pubsubCancel := context.WithTimeout(ctx, time.Second*20)
	defer pubsubCancel()

	pubsubConfig := emulators2.GetDefaultPubsubConfig(testProjectID, map[string]string{testInputTopicID: testInputSubscriptionID})

	connection := emulators2.SetupPubsubEmulator(t, pubsubCtx, pubsubConfig)

	bigqueryCtx, bigqueryCancel := context.WithTimeout(ctx, time.Second*20)
	defer bigqueryCancel()

	bigqueryCfg := emulators2.GetDefaultBigQueryConfig(testProjectID, map[string]string{testBigQueryDatasetID: testBigQueryTableID},
		map[string]interface{}{testBigQueryTableID: types.GardenMonitorReadings{}})
	bigqueryConnection := emulators2.SetupBigQueryEmulator(t, bigqueryCtx, bigqueryCfg)

	// --- Configuration setup (Unchanged) ---
	var logBuf bytes.Buffer
	writer := io.MultiWriter(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}, &logBuf)
	logger := zerolog.New(writer).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	consumerCfg := &messagepipeline.GooglePubsubConsumerConfig{
		ProjectID:      testProjectID,
		SubscriptionID: testInputSubscriptionID,
	}
	batcherCfg := &bqstore.BatchInserterConfig{
		BatchSize:    5,
		FlushTimeout: 10 * time.Second,
	}
	bqInserterCfg := &bqstore.BigQueryDatasetConfig{
		ProjectID: testProjectID,
		DatasetID: testBigQueryDatasetID,
		TableID:   testBigQueryTableID,
	}

	// --- Initialize Components with new, refactored structure ---
	//opts := []option.ClientOption{option.WithEndpoint("localhost:58752"), option.WithoutAuthentication()}
	psClient, err := pubsub.NewClient(ctx, testProjectID, connection.ClientOptions...)
	require.NoError(t, err)
	consumer, err := messagepipeline.NewGooglePubsubConsumer(consumerCfg, psClient, logger)
	require.NoError(t, err)

	bqClient, err := bigquery.NewClient(ctx, testProjectID, bigqueryConnection.ClientOptions...)
	require.NotNil(t, bqClient)
	defer bqClient.Close()

	// *** REFACTORED PART: Use the new, single convenience constructor ***
	batchInserter, err := bqstore.NewBigQueryBatchProcessor[types.GardenMonitorReadings](ctx, bqClient, batcherCfg, bqInserterCfg, logger)
	require.NoError(t, err)

	// *** REFACTORED PART: Use the new service constructor ***
	numWorkers := 2
	processingService, err := bqstore.NewBigQueryService[types.GardenMonitorReadings](numWorkers, consumer, batchInserter, types.ConsumedMessageTransformer, logger)
	require.NoError(t, err)

	// --- Test Execution (Unchanged) ---
	go func() {
		err := processingService.Start(ctx)
		assert.NoError(t, err, "ProcessingService.Start() should not return an error on graceful shutdown")
	}()

	const messageCount = 7
	pubsubTestPublisherClient, err := pubsub.NewClient(ctx, testProjectID)
	require.NoError(t, err)
	defer pubsubTestPublisherClient.Close()
	inputTopic := pubsubTestPublisherClient.Topic(testInputTopicID)
	defer inputTopic.Stop()

	var lastTestPayload types.GardenMonitorReadings
	for i := 0; i < messageCount; i++ {
		iPayload := types.GardenMonitorReadings{
			DE:       testDeviceUID,
			Sequence: 1337 + i,
			Battery:  95 - i,
		}
		lastTestPayload = iPayload

		testUpstreamMsg := TestUpstreamMessage{
			Topic:     "devices/garden-monitor/telemetry",
			MessageID: "test-message-id-" + strconv.Itoa(i),
			Timestamp: time.Now().UTC().Truncate(time.Second),
			Payload:   &lastTestPayload,
		}
		msgDataBytes, err := json.Marshal(testUpstreamMsg)
		require.NoError(t, err)

		pubResult := inputTopic.Publish(ctx, &pubsub.Message{Data: msgDataBytes})
		_, err = pubResult.Get(ctx)
		require.NoError(t, err)
	}
	t.Logf("%d test messages published to Pub/Sub topic: %s", messageCount, testInputTopicID)

	time.Sleep(2 * time.Second)
	processingService.Stop()
	time.Sleep(2 * time.Second)

	queryString := fmt.Sprintf("SELECT * FROM `%s.%s` WHERE uid = @uid ORDER BY sequence", testBigQueryDatasetID, testBigQueryTableID)
	query := bqClient.Query(queryString)
	query.Parameters = []bigquery.QueryParameter{{Name: "uid", Value: testDeviceUID}}

	it, err := query.Read(ctx)
	require.NoError(t, err, "query.Read failed")

	var receivedRows []types.GardenMonitorReadings
	for {
		var row types.GardenMonitorReadings
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		require.NoError(t, err, "it.Next failed")
		receivedRows = append(receivedRows, row)
	}

	require.Len(t, receivedRows, messageCount, "The number of rows in BigQuery should match the number of messages sent.")

	finalRow := receivedRows[len(receivedRows)-1]
	assert.Equal(t, lastTestPayload.DE, finalRow.DE, "DE mismatch")
	assert.Equal(t, lastTestPayload.Sequence, finalRow.Sequence, "Sequence mismatch")
	assert.Equal(t, lastTestPayload.Battery, finalRow.Battery, "Battery mismatch")

	t.Logf("Successfully verified %d rows in BigQuery for DE: %s", len(receivedRows), testDeviceUID)
	t.Logf("Full flow integration test completed.")
}
