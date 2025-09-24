//go:build integration

package cache_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFirestorePresenceCache_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	const projectID = "test-project"
	const collectionName = "presence-collection"

	// Setup Firestore emulator
	firestoreDefaults := emulators.GetDefaultFirestoreConfig(projectID)
	firestoreConn := emulators.SetupFirestoreEmulator(t, ctx, firestoreDefaults)

	client, err := firestore.NewClient(ctx, projectID, firestoreConn.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	// Create the cache instance to be tested
	presenceCache, err := cache.NewFirestorePresenceCache[string, presenceTestValue](client, collectionName)
	require.NoError(t, err)

	const testKey = "user:pres:456"
	testValue := presenceTestValue{ServerID: "instance-2", ConnectedAt: time.Now().Unix()}

	t.Run("Set, Fetch, and Delete cycle", func(t *testing.T) {
		// Act 1: Set a value
		err := presenceCache.Set(ctx, testKey, testValue)
		require.NoError(t, err)

		// Assert 1: Verify directly in Firestore that the document exists
		doc, err := client.Collection(collectionName).Doc(testKey).Get(ctx)
		require.NoError(t, err)
		require.True(t, doc.Exists(), "Document should exist in Firestore after Set")

		// Act 2: Fetch the value back
		retrieved, err := presenceCache.Fetch(ctx, testKey)
		require.NoError(t, err)
		assert.Equal(t, testValue, retrieved)

		// Act 3: Delete the value
		err = presenceCache.Delete(ctx, testKey)
		require.NoError(t, err)

		// Assert 3: Verify directly in Firestore that the document is gone
		doc, err = client.Collection(collectionName).Doc(testKey).Get(ctx)
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code(), "Document should not exist after Delete")
	})
}
