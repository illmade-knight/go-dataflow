package mqttconverter

import (
	"context"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
)

// ToRawMessageTransformer is a MessageTransformer that converts a generic Message
// (originating from MQTT) into the canonical RawMessage format.
func ToRawMessageTransformer(_ context.Context, msg messagepipeline.Message) (*RawMessage, bool, error) {
	// In a real-world scenario, you might have logic to handle duplicates or other properties.
	// For this example, we perform a direct transformation.
	transformed := &RawMessage{
		Topic:     msg.Attributes["mqtt_topic"], // The consumer stores the topic here.
		Payload:   msg.Payload,
		Timestamp: msg.PublishTime,
	}
	return transformed, false, nil
}
