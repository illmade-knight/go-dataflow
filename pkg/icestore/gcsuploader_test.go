package icestore_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/illmade-knight/go-dataflow/pkg/icestore"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGCSBatchUploader_UploadGroup(t *testing.T) {
	// Arrange
	mockClient := newMockGCSClient(false)
	config := icestore.GCSBatchUploaderConfig{
		BucketName:   "test-bucket",
		ObjectPrefix: "uploads",
	}
	uploader, err := icestore.NewGCSBatchUploader(mockClient, config, zerolog.Nop())
	require.NoError(t, err)

	batchKey := "2025/06/13/loc-a"
	batch := []*icestore.ArchivalData{
		{ID: "msg-1", BatchKey: batchKey, OriginalPubSubPayload: []byte(`{"data":"one"}`)},
		{ID: "msg-2", BatchKey: batchKey, OriginalPubSubPayload: []byte(`{"data":"two"}`)},
	}

	// Act
	err = uploader.UploadGroup(context.Background(), batchKey, batch)
	require.NoError(t, err)

	// Assert
	bucket := mockClient.Bucket("test-bucket").(*mockGCSBucketHandle)
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	require.Len(t, bucket.objects, 1, "Expected one object to be created for the group")

	for objectName, handle := range bucket.objects {
		assert.Contains(t, objectName, "uploads/2025/06/13/loc-a/", "Object path is incorrect")
		objHandle := handle.(*mockGCSObjectHandle)
		writer := objHandle.writer.(*mockGCSWriter)

		// Decompress and verify the written content
		gzReader, err := gzip.NewReader(bytes.NewReader(writer.Bytes()))
		require.NoError(t, err)
		content, err := io.ReadAll(gzReader)
		require.NoError(t, err)

		lines := bytes.Split(bytes.TrimSpace(content), []byte("\n"))
		require.Len(t, lines, 2, "Expected two JSON records in the file")

		var record1, record2 icestore.ArchivalData
		require.NoError(t, json.Unmarshal(lines[0], &record1))
		require.NoError(t, json.Unmarshal(lines[1], &record2))

		assert.Equal(t, "msg-1", record1.ID)
		assert.Equal(t, "msg-2", record2.ID)
	}
}
