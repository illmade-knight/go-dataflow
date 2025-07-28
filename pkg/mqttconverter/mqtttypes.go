// mqttconverter/types.go
package mqttconverter

import "time"

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
