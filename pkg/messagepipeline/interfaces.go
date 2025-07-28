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
// handles transformed messages.
type MessageProcessor[T any] interface {
	Input() chan<- *types.BatchedMessage[T]
	Start(ctx context.Context)
	Stop(ctx context.Context) error
}

// MessageConsumer defines the interface for a message source (e.g., Pub/Sub, Kafka).
type MessageConsumer interface {
	Messages() <-chan types.ConsumedMessage
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Done() <-chan struct{}
}

// --- Transformation Function ---

// MessageTransformer defines a function that transforms a ConsumedMessage into a
// structured payload of type T.
//
// REFACTOR: The function signature now includes a context.Context. This allows the
// transformer's logic (e.g., fetching enrichment data) to be tied to the
// lifecycle of the processing service.
type MessageTransformer[T any] func(ctx context.Context, msg types.ConsumedMessage) (payload *T, skip bool, err error)
