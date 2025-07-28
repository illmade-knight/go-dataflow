// enrichment/fetcher.go
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
type CachedFetcher[K any, V any] interface {
	FetchFromCache(ctx context.Context, key K) (V, error)
	WriteToCache(ctx context.Context, key K, value V) error
	io.Closer
}

// REFACTOR: Added a config struct for the fetcher.
// FetcherConfig holds configuration for the cache-fallback fetcher.
type FetcherConfig struct {
	CacheWriteTimeout time.Duration
}

// NewCacheFallbackFetcher creates a generic Fetcher that uses a cache-then-source strategy.
// REFACTOR: The constructor no longer accepts a parentCtx.
func NewCacheFallbackFetcher[K comparable, V any](
	cfg *FetcherConfig,
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
		logger.Debug().Err(err).Msg("Cache miss. Falling back to source.")

		// 2. Cache miss, fallback to source
		value, err = sourceFetcher.Fetch(ctx, key)
		if err != nil {
			logger.Error().Err(err).Msg("Error fetching from source.")
			return zero, fmt.Errorf("error fetching from source: %w", err)
		}
		logger.Debug().Msg("Source hit. Writing back to cache.")

		// 3. Source hit, write back to cache in the background.
		// REFACTOR: The goroutine is now parented by context.Background to make it a
		// true fire-and-forget operation, independent of any single request.
		go func(k K, v V) {
			writeCtx, cancel := context.WithTimeout(context.Background(), cfg.CacheWriteTimeout)
			defer cancel()
			if writeErr := cacheFetcher.WriteToCache(writeCtx, k, v); writeErr != nil {
				logger.Error().Err(writeErr).Msg("Failed to write to cache in background.")
			}
		}(key, value)

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
