package mqttconverter

import "time"

// TopicMapping defines a route from an MQTT topic to a logical stream name.
type TopicMapping struct {
	// Name is a logical identifier for this route (e.g., "device-uplinks", "status-messages").
	// This name will be added to the message attributes for downstream routing.
	Name string `json:"name"`

	// Topic is the MQTT topic filter to subscribe to (e.g., "devices/+/data").
	Topic string `json:"topic"`

	// QoS is the quality of service level for this specific subscription.
	QoS byte `json:"qos"`
}

// InMessage represents a raw message received directly from the MQTT broker.
type InMessage struct {
	Payload   []byte    `json:"payload"`
	Topic     string    `json:"topic"`
	MessageID string    `json:"message_id"`
	Timestamp time.Time `json:"timestamp"`
	Duplicate bool      `json:"duplicate"`
}

// RawMessage is the canonical, transformed data structure that will be published to Pub/Sub.
type RawMessage struct {
	Topic     string    `json:"topic"`
	Payload   []byte    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}
