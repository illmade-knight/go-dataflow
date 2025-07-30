package icestore

import (
	"context"
	"fmt"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
)

// ArchivalData represents the final structured data written to a GCS object.
type ArchivalData struct {
	ID                    string    `json:"id"`
	BatchKey              string    `json:"batchKey"`
	OriginalPubSubPayload []byte    `json:"originalPubSubPayload"`
	ArchivedAt            time.Time `json:"archivedAt"`
}

// GetBatchKey returns the key used for grouping data in GCS.
func (d *ArchivalData) GetBatchKey() string {
	return d.BatchKey
}

// ArchivalTransformer is a MessageTransformer function that converts a generic
// Message into the icestore-specific ArchivalData format.
func ArchivalTransformer(ctx context.Context, msg messagepipeline.Message) (*ArchivalData, bool, error) {
	var ts time.Time

	// Allow overriding timestamp for testing purposes.
	if testTime, ok := msg.Attributes["test_publish_time"]; ok {
		parsedTime, err := time.Parse(time.RFC3339, testTime)
		if err == nil {
			ts = parsedTime
		}
	}

	// Fallback to message publish time or current time.
	if ts.IsZero() {
		ts = msg.PublishTime
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
	}

	// Create a batch key based on the date, e.g., "2025/06/15".
	batchKey := fmt.Sprintf("%d/%02d/%02d", ts.Year(), ts.Month(), ts.Day())

	// Append a location to the batch key if it exists in enrichment data or attributes.
	if msg.EnrichmentData != nil {
		if location, ok := msg.EnrichmentData["location"].(string); ok && location != "" {
			batchKey = fmt.Sprintf("%s/%s", batchKey, location)
		}
	} else if location, ok := msg.Attributes["location"]; ok && location != "" {
		batchKey = fmt.Sprintf("%s/%s", batchKey, location)
	}

	return &ArchivalData{
		ID:                    msg.ID,
		BatchKey:              batchKey,
		OriginalPubSubPayload: msg.Payload,
		ArchivedAt:            time.Now().UTC(),
	}, false, nil
}
