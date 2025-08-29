//go:build integration

package mqttconverter_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/mqttconverter"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createPubsubResources encapsulates creating and tearing down Pub/Sub resources using the v2 API.
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

func TestMqttPipeline_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	logger := zerolog.New(os.Stderr).Level(zerolog.InfoLevel)
	const projectID = "test-project"
	runID := uuid.NewString()
	outputTopicID := "processed-topic-" + runID
	outputSubID := "processed-sub-" + runID

	// --- 1. Setup Emulators ---
	mqttConnection := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	pubsubConnection := emulators.SetupPubsubEmulator(t, ctx, emulators.GetDefaultPubsubConfig(projectID))

	psClient, err := pubsub.NewClient(ctx, projectID, pubsubConnection.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })

	createPubsubResources(t, ctx, psClient, projectID, outputTopicID, outputSubID)

	producerCfg := messagepipeline.NewGooglePubsubProducerDefaults(outputTopicID)
	producer, err := messagepipeline.NewGooglePubsubProducer(producerCfg, psClient, logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = producer.Stop(ctx)
	})

	canarySub := psClient.Subscriber(outputSubID)
	canaryMsgID := "canary-" + uuid.NewString()

	_, err = producer.Publish(ctx, messagepipeline.MessageData{Payload: []byte(canaryMsgID)})
	require.NoError(t, err, "Failed to publish canary message")

	canaryReceived := false
	require.Eventually(t, func() bool {
		cctx, ccancel := context.WithTimeout(ctx, 5*time.Second)
		defer ccancel()
		_ = canarySub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
			// BUG FIX: Unmarshal the JSON envelope before checking the payload.
			var receivedData messagepipeline.MessageData
			if json.Unmarshal(msg.Data, &receivedData) != nil {
				msg.Nack()
				return
			}

			if string(receivedData.Payload) == canaryMsgID {
				canaryReceived = true
				msg.Ack()
				ccancel()
			} else {
				// This is a real message that arrived early, nack it so it can be redelivered.
				msg.Nack()
			}
		})
		return canaryReceived
	}, 15*time.Second, 250*time.Millisecond, "Canary message was not received, subscription is not ready.")

	mqttCfg := mqttconverter.LoadMQTTClientConfigFromEnv()
	mqttCfg.BrokerURL = mqttConnection.EmulatorAddress
	mqttCfg.ClientIDPrefix = "ingestion-test-"
	mqttCfg.TopicMappings = []mqttconverter.TopicMapping{
		{
			Name:  "uplink-data",
			Topic: "devices/+/data",
			QoS:   1,
		},
		{
			Name:  "device-status",
			Topic: "devices/+/status",
			QoS:   1,
		},
	}
	consumer, err := mqttconverter.NewMqttConsumer(mqttCfg, logger, 10)
	require.NoError(t, err)

	processor := func(ctx context.Context, original messagepipeline.Message, payload *mqttconverter.RawMessage) error {
		enrichment := map[string]interface{}{
			"mqttTopic": payload.Topic,
		}
		if routeName, ok := original.Attributes["route_name"]; ok {
			enrichment["routeName"] = routeName
		}
		outgoingData := messagepipeline.MessageData{
			ID:             original.ID,
			Payload:        payload.Payload,
			PublishTime:    payload.Timestamp,
			EnrichmentData: enrichment,
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

	require.Eventually(t, consumer.IsConnected, 10*time.Second, 250*time.Millisecond, "MQTT consumer did not connect in time")

	// --- 5. Publish Test Messages and Verify ---
	dataPayload := map[string]interface{}{"value": 42, "status": "ok"}
	dataBytes, err := json.Marshal(dataPayload)
	require.NoError(t, err)
	dataTopic := "devices/test-eui-001/data"
	token := mqttTestPubClient.Publish(dataTopic, 1, false, dataBytes)
	require.True(t, token.WaitTimeout(10*time.Second), "MQTT Publish (data) token timed out")
	require.NoError(t, token.Error(), "MQTT Publish (data) failed")

	statusPayload := map[string]interface{}{"battery": 99, "online": true}
	statusBytes, err := json.Marshal(statusPayload)
	require.NoError(t, err)
	statusTopic := "devices/test-eui-002/status"
	token = mqttTestPubClient.Publish(statusTopic, 1, false, statusBytes)
	require.True(t, token.WaitTimeout(10*time.Second), "MQTT Publish (status) token timed out")
	require.NoError(t, token.Error(), "MQTT Publish (status) failed")

	// --- Verification ---
	pullCtx, pullCancel := context.WithTimeout(ctx, 30*time.Second)
	t.Cleanup(pullCancel)

	var receivedMsgs []*pubsub.Message
	var mu sync.Mutex
	err = canarySub.Receive(pullCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
		msg.Ack()
		mu.Lock()
		receivedMsgs = append(receivedMsgs, msg)
		if len(receivedMsgs) == 2 {
			pullCancel()
		}
		mu.Unlock()
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		require.NoError(t, err, "Receiving from Pub/Sub failed")
	}
	require.Len(t, receivedMsgs, 2, "Did not receive the expected number of messages from Pub/Sub")

	var dataMsgFound, statusMsgFound bool
	for _, msg := range receivedMsgs {
		var result messagepipeline.MessageData
		err = json.Unmarshal(msg.Data, &result)
		require.NoError(t, err)
		require.NotNil(t, result.EnrichmentData)

		// The canary message has no enrichment data, so we skip it.
		if result.EnrichmentData["routeName"] == nil {
			continue
		}

		routeName, _ := result.EnrichmentData["routeName"].(string)
		if routeName == "uplink-data" {
			dataMsgFound = true
			assert.JSONEq(t, string(dataBytes), string(result.Payload))
			assert.Equal(t, dataTopic, result.EnrichmentData["mqttTopic"])
		} else if routeName == "device-status" {
			statusMsgFound = true
			assert.JSONEq(t, string(statusBytes), string(result.Payload))
			assert.Equal(t, statusTopic, result.EnrichmentData["mqttTopic"])
		}
	}

	assert.True(t, dataMsgFound, "The uplink data message was not found")
	assert.True(t, statusMsgFound, "The device status message was not found")
}
