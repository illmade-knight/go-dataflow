package icestore

import (
	"context"
	"fmt"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
)

// ArchivalData represents the final structured data that is serialized and
// written as a line in a GCS object.
type ArchivalData struct {
	// ID is the unique identifier from the original message broker.
	ID string `json:"id"`
	// BatchKey is the application-specific key used to group messages into the
	// same GCS object. For example, "2025/06/15/location-a".
	BatchKey string `json:"batchKey"`
	// OriginalPubSubPayload contains the raw, unmodified payload from the
	// original message, preserved for archival purposes.
	OriginalPubSubPayload []byte `json:"originalPubSubPayload"`
	// ArchivedAt is the timestamp of when the message was processed by this service.
	ArchivedAt time.Time `json:"archivedAt"`
}

// GetBatchKey returns the key used for grouping data in GCS.
func (d *ArchivalData) GetBatchKey() string {
	return d.BatchKey
}

// ArchivalTransformer is a MessageTransformer function that converts a generic
// messagepipeline.Message into the icestore-specific ArchivalData format.
//
// It constructs a BatchKey based on the message's timestamp and an optional
// "location" field found in the message's attributes or enrichment data. This
// allows the pipeline to automatically partition archived data into date- and
// location-based folders in GCS.
func ArchivalTransformer(_ context.Context, msg *messagepipeline.Message) (*ArchivalData, bool, error) {
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
