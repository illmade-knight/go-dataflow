// mqttconverter/transformer.go
package mqttconverter

import (
	"context"

	"github.com/illmade-knight/go-dataflow/pkg/types"
)

// ToRawMessageTransformer is a MessageTransformer that converts a generic ConsumedMessage
// (originating from MQTT) into the canonical RawMessage format.
func ToRawMessageTransformer(_ context.Context, msg types.ConsumedMessage) (*RawMessage, bool, error) {
	// In a real-world scenario, you might have logic to handle duplicates or other properties.
	// For this example, we perform a direct transformation.
	transformed := &RawMessage{
		Topic:     msg.Attributes["mqtt_topic"], // The consumer stores the topic here.
		Payload:   msg.Payload,
		Timestamp: msg.PublishTime,
	}
	return transformed, false, nil
}
