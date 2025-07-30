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
type MessageConsumer interface {
	Messages() <-chan Message
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Done() <-chan struct{}
}

// --- Stage 2: Transformer / Enricher ---

// MessageTransformer defines a function that **transforms** a generic `Message` into a
// new, specific, structured payload of type T. This is used by the generic
// StreamingService and BatchingService.
type MessageTransformer[T any] func(ctx context.Context, msg *Message) (payload *T, skip bool, err error)

// --- Stage 3: Processor ---

// ProcessableItem links a transformed payload with its original message for Ack/Nack.
type ProcessableItem[T any] struct {
	Original Message
	Payload  *T
}

// StreamProcessor defines the contract for an endpoint that handles transformed
// messages of type T one by one.
type StreamProcessor[T any] func(ctx context.Context, original Message, payload *T) error

// BatchProcessor defines the contract for an endpoint that handles batches of transformed messages of type T.
type BatchProcessor[T any] func(ctx context.Context, batch []ProcessableItem[T]) error

// MessageProcessor defines the contract for an endpoint that handles an enriched
// message. This is the non-generic counterpart to the StreamProcessor.
type MessageProcessor func(ctx context.Context, msg *Message) error
