package messagepipeline

import (
	"time"
)

// Message is the canonical, internal representation of an event flowing through the
// pipeline. It contains the core data, metadata, and acknowledgment handles.
type Message struct {
	// MessageData contains the core payload and user-defined enrichment data.
	MessageData

	// Attributes holds metadata from the message broker (e.g., Pub/Sub attributes, MQTT topic).
	Attributes map[string]string

	// Ack is a function to call to signal that processing was successful and the
	// message can be permanently removed from the source.
	Ack func()

	// Nack is a function to call to signal that processing has failed and the
	// message should be re-queued or sent to a dead-letter queue.
	Nack func()
}

// MessageData holds the essential payload of a message. This struct is often
// serialized and used as the data for messages being published to a downstream system.
type MessageData struct {
	// ID is the unique identifier for the message from the source broker.
	ID string `json:"id"`

	// Payload is the raw byte content of the message.
	Payload []byte `json:"payload"`

	// PublishTime is the timestamp when the message was originally published.
	PublishTime time.Time `json:"publishTime"`

	// EnrichmentData is a generic map to hold any kind of data added by
	// pipeline transformers. This data is serialized along with the rest of
	// the MessageData when published.
	EnrichmentData map[string]interface{} `json:"enrichmentData,omitempty"`
}
