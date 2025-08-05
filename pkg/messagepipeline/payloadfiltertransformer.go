package messagepipeline

import (
	"context"
	"github.com/rs/zerolog"
)

// WithPayloadValidation is a decorator function. It takes an existing MessageTransformer
// and returns a new one that first performs payload size validation.
func WithPayloadValidation[T any](
	innerTransformer MessageTransformer[T],
	minSize int,
	maxSize int,
	logger zerolog.Logger,
) MessageTransformer[T] {

	// Return a new closure that is also a valid MessageTransformer[T]
	return func(ctx context.Context, msg *Message) (*T, bool, error) {

		// 1. Perform the size validation check first.
		payloadLen := len(msg.Payload)
		if payloadLen < minSize || payloadLen > maxSize {
			logger.Warn().Str("msg_id", msg.ID).Int("payload_size", payloadLen).Msg("Rejecting message due to invalid payload size.")
			// Skip the message by returning true. The innerTransformer is never called.
			return nil, true, nil
		}

		// 2. If validation passes, call the original, "inner" transformer.
		// The decorator is completely transparent to it.
		return innerTransformer(ctx, msg)
	}
}
