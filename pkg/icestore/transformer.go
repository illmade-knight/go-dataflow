package icestore

import (
	"fmt"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/types"
)

func ArchivalTransformer(msg types.ConsumedMessage) (*ArchivalData, bool, error) {
	ts := msg.PublishTime
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	batchKey := fmt.Sprintf("%d/%02d/%02d", ts.Year(), ts.Month(), ts.Day())

	// REFACTORED: The transformer now looks for enrichment data in the generic
	// EnrichmentData map instead of the old DeviceInfo struct.
	if msg.EnrichmentData != nil {
		// Safely access the location value using a type assertion.
		if location, ok := msg.EnrichmentData["location"].(string); ok && location != "" {
			batchKey = fmt.Sprintf("%s/%s", batchKey, location)
		}
	}

	return &ArchivalData{
		ID:                    msg.ID,
		BatchKey:              batchKey,
		OriginalPubSubPayload: msg.Payload,
		ArchivedAt:            time.Now().UTC(),
	}, false, nil
}
