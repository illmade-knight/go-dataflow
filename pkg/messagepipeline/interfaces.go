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
// It is responsible for creating the initial `Message` objects.
type MessageConsumer interface {
	// Messages returns the read-only channel where consumed messages are sent.
	Messages() <-chan Message

	// Start begins the message consumption process. The provided context manages
	// the lifecycle of the consumer's background operations.
	Start(ctx context.Context) error

	// Stop gracefully shuts down the consumer, respecting the provided context's deadline.
	Stop(ctx context.Context) error

	// Done returns a channel that is closed when the consumer has fully stopped.
	// This is useful for orchestrating graceful shutdowns.
	Done() <-chan struct{}
}

// --- Stage 2: Transformer ---

// MessageTransformer defines a function that transforms a generic `Message` into a
// specific, structured payload of type T. This is the bridge between the generic
// pipeline and domain-specific data structures.
//
// It can also signal to skip the message, preventing it from reaching the processor.
type MessageTransformer[T any] func(ctx context.Context, msg Message) (payload *T, skip bool, err error)

// --- Stage 3: Processor ---

// ProcessableItem links a successfully transformed payload of type T with its
// original raw `Message`. This is essential for batch processors that need to Ack/Nack
// the original message after a batch operation completes.
type ProcessableItem[T any] struct {
	Original Message
	Payload  *T
}

// StreamProcessor defines the contract for an endpoint that handles messages
// one by one. It receives the original message for context (e.g., for Ack/Nack)
// and the transformed, type-safe payload.
type StreamProcessor[T any] func(ctx context.Context, original Message, payload *T) error

// BatchProcessor defines the contract for an endpoint that handles messages
// in batches. It receives a slice of ProcessableItem, allowing it to operate
// on the typed payloads while retaining access to the original messages.
type BatchProcessor[T any] func(ctx context.Context, batch []ProcessableItem[T]) error
