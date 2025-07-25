// enrichment/enrichment.go
package enrichment

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog"
)

// Fetcher is a generic function type for fetching data.
type Fetcher[K any, V any] func(ctx context.Context, key K) (V, error)

// SourceFetcher is a generic interface for a source of truth.
type SourceFetcher[K any, V any] interface {
	Fetch(ctx context.Context, key K) (V, error)
	io.Closer
}

// CachedFetcher is a generic interface for a caching layer.
// This interface is compatible with the one in your new 'cache' package.
type CachedFetcher[K any, V any] interface {
	FetchFromCache(ctx context.Context, key K) (V, error)
	WriteToCache(ctx context.Context, key K, value V) error
	io.Closer
}

// NewCacheFallbackFetcher creates a generic Fetcher that uses a cache-then-source strategy.
func NewCacheFallbackFetcher[K comparable, V any](
	parentCtx context.Context,
	cacheFetcher CachedFetcher[K, V],
	sourceFetcher SourceFetcher[K, V],
	logger zerolog.Logger,
) (Fetcher[K, V], func() error, error) {
	if cacheFetcher == nil {
		return nil, nil, fmt.Errorf("cacheFetcher cannot be nil")
	}
	if sourceFetcher == nil {
		return nil, nil, fmt.Errorf("sourceFetcher cannot be nil")
	}

	fetchLogic := func(ctx context.Context, key K) (V, error) {
		var zero V
		// 1. Try to fetch from cache
		value, err := cacheFetcher.FetchFromCache(ctx, key)
		if err == nil {
			logger.Debug().Msg("Cache hit.")
			return value, nil
		}
		// We assume any error from cache means miss for simplicity, but you could add specific error checking.
		logger.Debug().Err(err).Msg("Cache miss. Falling back to source.")

		// 2. Cache miss, fallback to source
		value, err = sourceFetcher.Fetch(ctx, key)
		if err != nil {
			logger.Error().Err(err).Msg("Error fetching from source.")
			return zero, fmt.Errorf("error fetching from source: %w", err)
		}
		logger.Debug().Msg("Source hit. Writing back to cache.")

		// 3. Source hit, write back to cache in the background
		go func(bgCtx context.Context, k K, v V) {
			writeCtx, cancel := context.WithTimeout(bgCtx, 5*time.Second)
			defer cancel()
			if writeErr := cacheFetcher.WriteToCache(writeCtx, k, v); writeErr != nil {
				logger.Error().Err(writeErr).Msg("Failed to write to cache in background.")
			}
		}(parentCtx, key, value) // Use the parentCtx for the background routine's lifetime

		return value, nil
	}

	cleanupFunc := func() error {
		log := logger.With().Str("component", "FallbackFetcherCleanup").Logger()
		log.Info().Msg("Closing fallback fetcher resources...")
		var firstErr error
		if err := cacheFetcher.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing cache fetcher")
			firstErr = err
		}
		if err := sourceFetcher.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing source fetcher")
			if firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}

	return fetchLogic, cleanupFunc, nil
}
