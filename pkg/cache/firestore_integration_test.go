// cache/firestore_test.go
//go:build integration

package cache_test

import (
	"cloud.google.com/go/firestore"
	"context"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/cache" // Assumed import path
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type firestoreTestValue struct {
	Name  string
	Count int
}

func TestFirestoreSource_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	const projectID = "test-project"
	const collectionName = "test-collection"
	const docID = "test-doc-1"

	// Assumes a helper that sets up a Firestore emulator.
	firestoreDefaults := emulators.GetDefaultFirestoreConfig(projectID)
	firestoreConn := emulators.SetupFirestoreEmulator(t, ctx, firestoreDefaults)

	// Pre-populate data for the test
	client, err := firestore.NewClient(ctx, projectID, firestoreConn.ClientOptions...)
	require.NoError(t, err)
	docData := firestoreTestValue{Name: "test-item", Count: 42}
	_, err = client.Collection(collectionName).Doc(docID).Set(ctx, docData)
	require.NoError(t, err)

	// Create the FirestoreSource instance
	cfg := &cache.FirestoreConfig{
		ProjectID:      projectID,
		CollectionName: collectionName,
	}
	source, err := cache.NewFirestoreSource[string, firestoreTestValue](cfg, client, zerolog.Nop())
	require.NoError(t, err)

	t.Run("Fetch Hit", func(t *testing.T) {
		retrieved, err := source.Fetch(ctx, docID)
		require.NoError(t, err)
		assert.Equal(t, docData, retrieved)
	})

	t.Run("Fetch Miss", func(t *testing.T) {
		_, err := source.Fetch(ctx, "non-existent-doc")
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok, "Error should be a gRPC status error")
		assert.Equal(t, codes.NotFound, st.Code())
	})
}
