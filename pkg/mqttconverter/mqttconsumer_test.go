package mqttconverter_test

import (
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/mqttconverter"
	"github.com/stretchr/testify/assert"
)

// --- Mocks for Paho MQTT Client (Message only) ---
// This mock implements the mqtt.Message interface required by ToPipelineMessage.
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

// TestToPipelineMessage verifies that the transformation from a Paho message
// to a messagepipeline.Message is correct.
func TestToPipelineMessage(t *testing.T) {
	// Arrange
	expectedPayload := []byte("hello world")
	pahoMsg := &mockMqttMessage{
		topic:     "devices/test-123/data",
		payload:   expectedPayload,
		messageID: 42,
	}
	expectedRouteName := "test-route"

	// Act
	// REFACTOR: Call the updated function with the route name.
	receivedMsg := mqttconverter.ToPipelineMessage(pahoMsg, expectedRouteName)

	// Assert
	// No channels, no goroutines, no timeouts needed for this synchronous test.
	assert.Equal(t, expectedPayload, receivedMsg.Payload)
	assert.Equal(t, "42", receivedMsg.ID)
	assert.Equal(t, "devices/test-123/data", receivedMsg.Attributes["mqtt_topic"])
	// REFACTOR: Add assertion for the new route_name attribute.
	assert.Equal(t, expectedRouteName, receivedMsg.Attributes["route_name"])
	// Check that the timestamp is recent.
	assert.WithinDuration(t, time.Now().UTC(), receivedMsg.PublishTime, 5*time.Second)
	assert.NotNil(t, receivedMsg.Ack)
	assert.NotNil(t, receivedMsg.Nack)
}
