package messagepipeline

import (
	"context"
	"github.com/illmade-knight/go-dataflow/pkg/types"
)

// ====================================================================================
// This file defines the core interfaces for a generic, reusable consumer and
// processing pipeline.
// ====================================================================================

// --- Core Pipeline Interfaces ---

// MessageProcessor defines the contract for any component that receives and
// handles transformed messages. Both `bqstore.BatchInserter` and `icestore.Batcher`
// are perfect implementations of this interface.
type MessageProcessor[T any] interface {
	// Input returns a write-only channel for sending transformed messages to the processor.
	Input() chan<- *types.BatchedMessage[T]
	// Start begins the processor's operations (e.g., its batching worker).
	// It accepts a context to manage its lifecycle.
	Start(ctx context.Context) // CHANGED: Added context.Context
	// Stop gracefully shuts down the processor, ensuring any buffered items are handled.
	Stop()
}

// MessageConsumer defines the interface for a message source (e.g., Pub/Sub, Kafka).
// It is responsible for fetching raw messages from the broker.
type MessageConsumer interface {
	// Messages returns a read-only channel from which raw messages can be consumed.
	Messages() <-chan types.ConsumedMessage
	// Start initiates the consumption of messages.
	Start(ctx context.Context) error
	// Stop gracefully ceases message consumption.
	Stop() error
	// Done returns a channel that is closed when the consumer has fully stopped.
	Done() <-chan struct{}
}

// --- Transformation Function (The Core of the Refactor) ---

// MessageTransformer defines a function that transforms a whole ConsumedMessage
// into a structured payload of type T. This new, more powerful interface REPLACES
// the old `PayloadDecoder`. It provides access to all message metadata, like
// PublishTime, enabling more robust processing logic.
//
// It returns the transformed payload, a boolean to indicate if the message
// should be skipped, and an.
type MessageTransformer[T any] func(msg types.ConsumedMessage) (payload *T, skip bool, err error)
