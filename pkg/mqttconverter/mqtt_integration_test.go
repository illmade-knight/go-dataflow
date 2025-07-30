//go:build integration

package mqttconverter_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/mqttconverter"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMqttPipeline_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	logger := zerolog.New(os.Stderr).Level(zerolog.InfoLevel)
	const (
		projectID     = "test-project"
		outputTopicID = "processed-topic"
		outputSubID   = "processed-sub"
		mqttTopic     = "devices/+/data"
	)

	// --- 1. Setup Emulators ---
	mqttConnection := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	pubsubConnection := emulators.SetupPubsubEmulator(t, ctx, emulators.GetDefaultPubsubConfig(projectID, map[string]string{outputTopicID: outputSubID}))

	// --- 2. Create Pipeline Components ---
	psClient, err := pubsub.NewClient(ctx, projectID, pubsubConnection.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })

	mqttCfg := mqttconverter.LoadMQTTClientConfigFromEnv()
	mqttCfg.BrokerURL = mqttConnection.EmulatorAddress
	mqttCfg.Topic = mqttTopic
	mqttCfg.ClientIDPrefix = "ingestion-test-"
	consumer, err := mqttconverter.NewMqttConsumer(mqttCfg, logger)
	require.NoError(t, err)

	producerCfg := messagepipeline.NewGooglePubsubProducerDefaults()
	producer, err := messagepipeline.NewGooglePubsubProducer(ctx, producerCfg, psClient, logger)
	require.NoError(t, err)
	t.Cleanup(producer.Stop)

	processor := func(ctx context.Context, original messagepipeline.Message, payload *mqttconverter.RawMessage) error {
		outgoingData := messagepipeline.MessageData{
			ID:             original.ID,
			Payload:        payload.Payload,
			PublishTime:    payload.Timestamp,
			EnrichmentData: map[string]interface{}{"mqttTopic": payload.Topic},
		}
		_, err := producer.Publish(ctx, outgoingData)
		return err
	}

	// --- 3. Assemble the StreamingService ---
	service, err := messagepipeline.NewStreamingService[mqttconverter.RawMessage](
		messagepipeline.StreamingServiceConfig{NumWorkers: 5},
		consumer,
		mqttconverter.ToRawMessageTransformer,
		processor,
		logger,
	)
	require.NoError(t, err)

	// --- 4. Start the Service and Test Clients ---
	serviceCtx, serviceCancel := context.WithCancel(ctx)
	t.Cleanup(serviceCancel)
	err = service.Start(serviceCtx)
	require.NoError(t, err)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		_ = service.Stop(stopCtx)
	})

	mqttTestPubClient, err := emulators.CreateTestMqttPublisher(mqttConnection.EmulatorAddress, "test-publisher-main")
	require.NoError(t, err)
	t.Cleanup(func() { mqttTestPubClient.Disconnect(250) })

	processedSub := psClient.Subscription(outputSubID)

	require.Eventually(t, consumer.IsConnected, 10*time.Second, 250*time.Millisecond, "MQTT consumer did not connect in time")

	// --- 5. Publish Test Message and Verify ---
	devicePayload := map[string]interface{}{"value": 42, "status": "ok"}
	msgBytes, err := json.Marshal(devicePayload)
	require.NoError(t, err)

	publishTopic := "devices/test-eui-001/data"
	token := mqttTestPubClient.Publish(publishTopic, 1, false, msgBytes)
	require.True(t, token.WaitTimeout(10*time.Second), "MQTT Publish token timed out")
	require.NoError(t, token.Error(), "MQTT Publish failed")

	// --- Verification ---
	pullCtx, pullCancel := context.WithTimeout(ctx, 30*time.Second)
	t.Cleanup(pullCancel)

	var receivedMsg *pubsub.Message
	err = processedSub.Receive(pullCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
		msg.Ack()
		receivedMsg = msg
		pullCancel()
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		require.NoError(t, err, "Receiving from Pub/Sub failed")
	}
	require.NotNil(t, receivedMsg, "Did not receive a message from Pub/Sub")

	// ** FIX **: Unmarshal into the correct struct to handle Base64 decoding.
	var result messagepipeline.MessageData
	err = json.Unmarshal(receivedMsg.Data, &result)
	require.NoError(t, err)

	// Assertions on the final message content
	assert.JSONEq(t, string(msgBytes), string(result.Payload))
	require.NotNil(t, result.EnrichmentData)
	assert.Equal(t, publishTopic, result.EnrichmentData["mqttTopic"])
}
