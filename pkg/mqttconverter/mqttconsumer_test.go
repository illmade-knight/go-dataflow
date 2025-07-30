package mqttconverter_test

import (
	"context"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/mqttconverter"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mocks for Paho MQTT Client (Message only) ---
type mockMqttMessage struct {
	topic     string
	payload   []byte
	messageID uint16
}

func (m *mockMqttMessage) Topic() string     { return m.topic }
func (m *mockMqttMessage) Payload() []byte   { return m.payload }
func (m *mockMqttMessage) MessageID() uint16 { return m.messageID }
func (m *mockMqttMessage) Duplicate() bool   { return false }
func (m *mockMqttMessage) Qos() byte         { return 1 }
func (m *mockMqttMessage) Retained() bool    { return false }
func (m *mockMqttMessage) Ack()              {}

// TestMqttConsumer_MessageHandler verifies that the consumer correctly transforms
// a Paho message into a standard messagepipeline.Message.
func TestMqttConsumer_MessageHandler(t *testing.T) {
	// Arrange
	cfg := &mqttconverter.MQTTClientConfig{BrokerURL: "tcp://localhost:1883", Topic: "test/topic"}
	consumer, err := mqttconverter.NewMqttConsumer(cfg, zerolog.Nop())
	require.NoError(t, err)

	handler := consumer.GetMessageHandlerForTest(context.Background())

	expectedPayload := []byte("hello world")
	pahoMsg := &mockMqttMessage{
		topic:     "devices/test-123/data",
		payload:   expectedPayload,
		messageID: 42,
	}

	// Act: Call the handler directly with the mock message.
	handler(nil, pahoMsg) // The client argument is not used in our handler.

	// Assert: Check that a correctly formatted message appears on the output channel.
	select {
	case receivedMsg := <-consumer.Messages():
		assert.Equal(t, expectedPayload, receivedMsg.Payload)
		assert.Equal(t, "42", receivedMsg.ID)
		assert.Equal(t, "devices/test-123/data", receivedMsg.Attributes["mqtt_topic"])
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for message from consumer")
	}
}
