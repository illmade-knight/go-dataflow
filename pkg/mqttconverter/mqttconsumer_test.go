// mqttconverter/mqttconsumer_test.go
package mqttconverter_test

import (
	"context"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/illmade-knight/go-dataflow/pkg/mqttconverter"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mocks for Paho MQTT Client ---
type mockToken struct{ err error }

func (m *mockToken) Wait() bool                       { return true }
func (m *mockToken) WaitTimeout(_ time.Duration) bool { return true }
func (m *mockToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (m *mockToken) Error() error { return m.err }

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

type mockMqttClient struct {
	isConnected      bool
	disconnectCalled bool
	subscribedTopic  string
	messageHandler   mqtt.MessageHandler
}

func (m *mockMqttClient) IsConnected() bool      { return m.isConnected }
func (m *mockMqttClient) IsConnectionOpen() bool { return m.isConnected }
func (m *mockMqttClient) Connect() mqtt.Token {
	m.isConnected = true
	return &mockToken{}
}
func (m *mockMqttClient) Disconnect(quiesce uint) {
	m.isConnected = false
	m.disconnectCalled = true
}
func (m *mockMqttClient) Subscribe(topic string, qos byte, callback mqtt.MessageHandler) mqtt.Token {
	m.subscribedTopic = topic
	m.messageHandler = callback
	return &mockToken{}
}
func (m *mockMqttClient) Unsubscribe(topics ...string) mqtt.Token { return &mockToken{} }

// Add stubs for unused methods to satisfy the interface
func (m *mockMqttClient) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	return &mockToken{}
}
func (m *mockMqttClient) SubscribeMultiple(filters map[string]byte, callback mqtt.MessageHandler) mqtt.Token {
	return &mockToken{}
}
func (m *mockMqttClient) AddRoute(topic string, callback mqtt.MessageHandler) {}
func (m *mockMqttClient) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.ClientOptionsReader{}
}

// --- Test Cases ---

func TestMqttConsumer_StartAndReceive(t *testing.T) {
	// Arrange
	cfg := &mqttconverter.MQTTClientConfig{
		BrokerURL:      "tcp://localhost:1883",
		Topic:          "test/topic",
		ConnectTimeout: 2 * time.Second,
	}
	mockClient := &mockMqttClient{}

	// The consumer now receives the client via its constructor.
	consumer, err := mqttconverter.NewMqttConsumer(mockClient, cfg, zerolog.Nop())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Act
	err = consumer.Start(ctx)
	require.NoError(t, err)

	// Assert that Start() subscribed to the correct topic.
	assert.Equal(t, cfg.Topic, mockClient.subscribedTopic)
	require.NotNil(t, mockClient.messageHandler)

	// Simulate the client receiving a message by calling the handler.
	expectedPayload := []byte("hello world")
	mockClient.messageHandler(mockClient, &mockMqttMessage{
		topic:     cfg.Topic,
		payload:   expectedPayload,
		messageID: 123,
	})

	// Assert the message was placed on the output channel.
	select {
	case receivedMsg := <-consumer.Messages():
		assert.Equal(t, expectedPayload, receivedMsg.Payload)
		assert.Equal(t, "123", receivedMsg.ID)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for message from consumer")
	}
}

func TestMqttConsumer_Stop(t *testing.T) {
	// Arrange
	cfg := &mqttconverter.MQTTClientConfig{
		BrokerURL: "tcp://localhost:1883",
		Topic:     "test/topic",
	}
	mockClient := &mockMqttClient{}
	consumer, err := mqttconverter.NewMqttConsumer(mockClient, cfg, zerolog.Nop())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	err = consumer.Start(ctx)
	require.NoError(t, err)

	// Act
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(stopCancel)
	err = consumer.Stop(stopCtx)
	require.NoError(t, err)

	// Assert
	assert.True(t, mockClient.disconnectCalled, "Disconnect should have been called on the client")
	select {
	case <-consumer.Done():
		// Success, channel is closed.
	default:
		t.Fatal("Done() channel should be closed after Stop()")
	}
}
