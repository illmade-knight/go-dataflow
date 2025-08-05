package messagepipeline_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validationTestPayload is a simple struct used for transformation in tests.
type validationTestPayload struct {
	Content string `json:"content"`
}

// TestWithPayloadValidation tests the payload size validation decorator.
func TestWithPayloadValidation(t *testing.T) {
	// This transformer will be the "inner" function that the decorator wraps.
	// We use a boolean flag to track whether it was called or not.
	var innerTransformerCalled bool
	innerTransformer := func(ctx context.Context, msg *messagepipeline.Message) (*validationTestPayload, bool, error) {
		innerTransformerCalled = true // Mark that the inner function was reached.
		var p validationTestPayload
		// In a real scenario, this would be the core transformation logic.
		err := json.Unmarshal(msg.Payload, &p)
		return &p, false, err
	}

	// Define the test cases in a table-driven format.
	testCases := []struct {
		name            string
		payload         []byte
		minSize         int
		maxSize         int
		expectSkip      bool // Should the decorator signal to skip the message?
		expectInnerCall bool // Should the inner transformer be called?
		expectErr       bool // Should the inner transformer return an error?
	}{
		{
			name:            "Success case: payload within valid range",
			payload:         []byte(`{"content":"this is valid"}`), // 26 bytes
			minSize:         13,
			maxSize:         30,
			expectSkip:      false,
			expectInnerCall: true,
			expectErr:       false,
		},
		{
			name:            "Failure case: payload too short",
			payload:         []byte(`{"c":"v"}`), // 8 bytes
			minSize:         13,
			maxSize:         30,
			expectSkip:      true,
			expectInnerCall: false,
			expectErr:       false,
		},
		{
			name:            "Failure case: payload too long",
			payload:         []byte(`{"content":"this payload is definitely too long"}`), // 51 bytes
			minSize:         13,
			maxSize:         30,
			expectSkip:      true,
			expectInnerCall: false,
			expectErr:       false,
		},
		{
			name:            "Edge case: payload is exactly min size",
			payload:         []byte(`{"content":""}`), // 13 bytes
			minSize:         13,
			maxSize:         30,
			expectSkip:      false,
			expectInnerCall: true,
			expectErr:       false,
		},
		{
			name:            "Edge case: payload is exactly max size",
			payload:         []byte(`{"content":"012345678901234"}`), // 30 bytes
			minSize:         13,
			maxSize:         30,
			expectSkip:      false,
			expectInnerCall: true,
			expectErr:       false,
		},
	}

	// Loop through each test case.
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset the tracker for each sub-test to ensure they are independent.
			innerTransformerCalled = false

			// --- Arrange ---
			// Create the decorated transformer by wrapping the inner one.
			decoratedTransformer := messagepipeline.WithPayloadValidation(
				innerTransformer,
				tc.minSize,
				tc.maxSize,
				zerolog.Nop(), // Use a no-op logger to keep test output clean.
			)

			// Create a message to be processed.
			msg := &messagepipeline.Message{
				MessageData: messagepipeline.MessageData{
					ID:      "test-id-" + tc.name,
					Payload: tc.payload,
				},
			}

			// --- Act ---
			// Call the decorated transformer, which contains the validation logic.
			_, skip, err := decoratedTransformer(context.Background(), msg)

			// --- Assert ---
			if tc.expectErr {
				require.Error(t, err, "Expected an error from the inner transformer but got nil")
			} else {
				require.NoError(t, err, "Received an unexpected error")
			}
			assert.Equal(t, tc.expectSkip, skip, "The skip status was not as expected.")
			assert.Equal(t, tc.expectInnerCall, innerTransformerCalled, "The inner transformer's call status was not as expected.")
		})
	}
}
