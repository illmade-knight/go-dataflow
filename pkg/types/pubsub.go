package types

import (
	"time"
)

type ConsumedMessage struct {
	PublishMessage

	Attributes map[string]string

	Ack func()
	// Nack is a function to call to signal that processing has failed and the
	// message should be re-queued or sent to a dead-letter queue.
	Nack func()
}

type PublishMessage struct {
	// ID is the unique identifier for the message from the source broker.
	ID string
	// Payload is the raw byte content of the message.
	Payload []byte
	// PublishTime is the timestamp when the message was originally published.
	PublishTime time.Time

	// REFACTORED: DeviceInfo has been replaced with a generic map to hold any
	// kind of enrichment data, not just device-specific info.
	EnrichmentData map[string]interface{} `json:"enrichmentData,omitempty"`
}
