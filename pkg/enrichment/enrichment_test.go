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
func TestEnricher_Enrich(t *testing.T) {
	ctx := context.Background()

	// 1. Define the dependencies as simple functions for the test.
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

		enricher, err := enrichment.NewEnricher(mockFetcher, keyExtractor, applier, zerolog.Nop())
		require.NoError(t, err)

		msg := &messagepipeline.Message{
			Attributes: map[string]string{"deviceID": "dev-123"},
		}

		// Act
		err = enricher.Enrich(ctx, msg)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, msg.EnrichmentData)
		enrichedInfo, ok := msg.EnrichmentData["deviceInfo"].(DeviceInfo)
		require.True(t, ok)
		assert.Equal(t, "Sensor A", enrichedInfo.Name)
		assert.Equal(t, "Garden", enrichedInfo.Location)
	})

	t.Run("Failure on key extraction", func(t *testing.T) {
		// Arrange
		mockFetcher := func(ctx context.Context, key string) (DeviceInfo, error) {
			t.Error("Fetcher should not be called if key extraction fails")
			return DeviceInfo{}, nil
		}
		// Use a key extractor that always fails for this test case.
		failingKeyExtractor := func(msg *messagepipeline.Message) (string, bool) {
			return "", false
		}

		enricher, err := enrichment.NewEnricher(mockFetcher, failingKeyExtractor, applier, zerolog.Nop())
		require.NoError(t, err)

		msg := &messagepipeline.Message{
			Attributes: map[string]string{"some_other_id": "xyz"}, // No deviceID
		}

		// Act
		err = enricher.Enrich(ctx, msg)

		// Assert
		require.NoError(t, err, "Should not return an error, just skip enrichment")
		assert.Nil(t, msg.EnrichmentData, "EnrichmentData should not be populated")
	})

	t.Run("Failure on data fetch", func(t *testing.T) {
		// Arrange
		expectedErr := errors.New("database is down")
		mockFetcher := func(ctx context.Context, key string) (DeviceInfo, error) {
			return DeviceInfo{}, expectedErr
		}

		enricher, err := enrichment.NewEnricher(mockFetcher, keyExtractor, applier, zerolog.Nop())
		require.NoError(t, err)

		msg := &messagepipeline.Message{
			Attributes: map[string]string{"deviceID": "dev-456"},
		}

		// Act
		err = enricher.Enrich(ctx, msg)

		// Assert
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr, "The original error should be wrapped and returned")
		assert.Nil(t, msg.EnrichmentData, "EnrichmentData should not be populated on fetch failure")
	})
}
