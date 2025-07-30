package enrichment_test

import (
	"context"
	"errors"
	"testing"

	"github.com/illmade-knight/go-dataflow/pkg/enrichment"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test Data Structures ---
type DeviceInfo struct {
	Name     string `json:"name"`
	Location string `json:"location"`
}

// --- Test Setup ---
func TestNewEnricherFunc(t *testing.T) {
	ctx := context.Background()

	keyExtractor := func(msg *messagepipeline.Message) (string, bool) {
		if id, ok := msg.Attributes["deviceID"]; ok {
			return id, true
		}
		return "", false
	}

	applier := func(msg *messagepipeline.Message, data DeviceInfo) {
		msg.EnrichmentData["deviceInfo"] = data
	}

	t.Run("Success case", func(t *testing.T) {
		// Arrange
		mockFetcher := func(ctx context.Context, key string) (DeviceInfo, error) {
			if key == "dev-123" {
				return DeviceInfo{Name: "Sensor A", Location: "Garden"}, nil
			}
			return DeviceInfo{}, errors.New("device not found")
		}

		enricherFunc, err := enrichment.NewEnricherFunc(mockFetcher, keyExtractor, applier, zerolog.Nop())
		require.NoError(t, err)

		msg := &messagepipeline.Message{
			Attributes: map[string]string{"deviceID": "dev-123"},
		}

		// Act
		skip, err := enricherFunc(ctx, msg)

		// Assert
		require.NoError(t, err)
		assert.False(t, skip)
		require.NotNil(t, msg.EnrichmentData)
		enrichedInfo, ok := msg.EnrichmentData["deviceInfo"].(DeviceInfo)
		require.True(t, ok)
		assert.Equal(t, "Sensor A", enrichedInfo.Name)
	})

	t.Run("Key not found in message", func(t *testing.T) {
		// Arrange
		mockFetcher := func(ctx context.Context, key string) (DeviceInfo, error) {
			t.Error("Fetcher should not be called if key is not found")
			return DeviceInfo{}, nil
		}
		enricherFunc, err := enrichment.NewEnricherFunc(mockFetcher, keyExtractor, applier, zerolog.Nop())
		require.NoError(t, err)

		msg := &messagepipeline.Message{
			Attributes: map[string]string{"other_id": "xyz"}, // No deviceID
		}

		// Act
		skip, err := enricherFunc(ctx, msg)

		// Assert
		require.NoError(t, err)
		assert.False(t, skip, "Should continue pipeline, just without enrichment")
		assert.Nil(t, msg.EnrichmentData)
	})

	t.Run("Failure on data fetch", func(t *testing.T) {
		// Arrange
		expectedErr := errors.New("database is down")
		mockFetcher := func(ctx context.Context, key string) (DeviceInfo, error) {
			return DeviceInfo{}, expectedErr
		}

		enricherFunc, err := enrichment.NewEnricherFunc(mockFetcher, keyExtractor, applier, zerolog.Nop())
		require.NoError(t, err)

		msg := &messagepipeline.Message{
			Attributes: map[string]string{"deviceID": "dev-456"},
		}

		// Act
		skip, err := enricherFunc(ctx, msg)

		// Assert
		require.NoError(t, err, "The enricher itself should not return an error")
		assert.True(t, skip, "Should signal to skip the message on fetch failure")
		assert.Nil(t, msg.EnrichmentData)
	})
}
