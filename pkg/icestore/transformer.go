package icestore

import (
	"context"
	"fmt"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/types"
)

// ArchivalTransformer is a MessageTransformer function that converts a generic
// ConsumedMessage into the icestore-specific ArchivalData format.
// FIX: The signature now accepts a context to match the updated MessageTransformer interface.
func ArchivalTransformer(ctx context.Context, msg types.ConsumedMessage) (*ArchivalData, bool, error) {
	var ts time.Time

	if testTime, ok := msg.Attributes["test_publish_time"]; ok {
		parsedTime, err := time.Parse(time.RFC3339, testTime)
		if err == nil {
			ts = parsedTime
		}
	}

	if ts.IsZero() {
		ts = msg.PublishTime
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
	}

	batchKey := fmt.Sprintf("%d/%02d/%02d", ts.Year(), ts.Month(), ts.Day())

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
