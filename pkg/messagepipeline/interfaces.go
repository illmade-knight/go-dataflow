package messagepipeline

import (
	"context"
)

// ====================================================================================
// This file defines the core interfaces and function types for building a dataflow
// pipeline. It outlines the contracts for consuming, transforming, and processing
// messages.
// ====================================================================================

// --- Stage 1: Consumer ---

// MessageConsumer defines the interface for a message source (e.g., Pub/Sub, Kafka).
// It is responsible for fetching messages and handing them off to the pipeline.
type MessageConsumer interface {
	// Messages returns a read-only channel from which pipeline workers will receive messages.
	Messages() <-chan Message
	// Start begins the consumption process (e.g., by calling subscription.Receive).
	Start(ctx context.Context) error
	// Stop gracefully ceases message consumption and waits for background tasks to finish.
	Stop(ctx context.Context) error
	// Done returns a channel that is closed when the consumer has completely shut down.
	Done() <-chan struct{}
}

// --- Stage 2: Transformer / Enricher ---

// MessageTransformer defines a function that transforms a generic `Message` into a
// new, specific, structured payload of type T.
//
// The 'skip' return value can be set to true to signal that this message should
// be acknowledged and not processed further, effectively filtering it from the pipeline.
//
// IMPORTANT: The implementation should populate the returned payload struct (T) with
// raw data. It should NOT serialize the final struct into JSON or any other format;
// the final publishing stage (e.g., GooglePubsubProducer) is responsible for serialization.
// Pre-serializing the payload can lead to inefficient "double-wrapping" of messages.
type MessageTransformer[T any] func(ctx context.Context, msg *Message) (payload *T, skip bool, err error)

// --- Stage 3: Processor ---

// ProcessableItem links a transformed payload with its original message. This allows
// processors to access the original message for acknowledgment (Ack/Nack) purposes.
type ProcessableItem[T any] struct {
	Original Message
	Payload  *T
}

// StreamProcessor defines the contract for an endpoint that handles transformed
// messages of type T one by one. The implementation should return an error if
// processing fails, which will cause the pipeline to Nack the message.
type StreamProcessor[T any] func(ctx context.Context, original Message, payload *T) error

// BatchProcessor defines the contract for an endpoint that handles batches of transformed messages of type T.
//
// The implementation is responsible for the entire batch's lifecycle, including
// deciding whether to Ack or Nack individual messages based on the processing outcome.
// An error returned from this function will be logged by the service, but it is up
// to the implementation to handle the message acknowledgements within the batch.
type BatchProcessor[T any] func(ctx context.Context, batch []ProcessableItem[T]) error

// MessageProcessor defines the contract for an endpoint that handles an enriched
// message. This is the non-generic counterpart to the StreamProcessor.
type MessageProcessor func(ctx context.Context, msg *Message) error
