package enrichment

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog"
)

// Fetcher is a generic function type for fetching data by a key
type Fetcher[K any, V any] func(ctx context.Context, key K) (V, error)

type CacheFetcher[K any, V any] interface {
	Fetch(ctx context.Context, key K) (V, error)
	Close() error
}

// SourceFetcher is a generic interface for a source of truth.
type SourceFetcher[K any, V any] interface {
	Fetch(ctx context.Context, key K) (V, error)
	io.Closer
}

// CachingFetcher is a generic interface for a caching layer that updates from a fallback
type CachingFetcher[K any, V any] interface {
	Fetch(ctx context.Context, key K) (V, error)
	WriteToCache(ctx context.Context, key K, value V) error
	io.Closer
}

// FetcherConfig holds configuration for the cache-fallback fetcher.
type FetcherConfig struct {
	CacheWriteTimeout time.Duration
}

// NewCacheFallbackFetcher creates a generic Fetcher that uses a cache-then-source strategy.
// REFACTOR: The constructor no longer accepts a parentCtx.
func NewCacheFallbackFetcher[K comparable, V any](
	cfg *FetcherConfig,
	cacheFetcher CachingFetcher[K, V],
	sourceFetcher SourceFetcher[K, V],
	logger zerolog.Logger,
) CacheFetcher[K, V] {
	return CacheFallbackFetcher[K, V]{
		cacheTimeout: cfg.CacheWriteTimeout,
		logger:       logger,
		fallback:     sourceFetcher,
		cache:        cacheFetcher,
	}
}

type CacheFallbackFetcher[K comparable, V any] struct {
	cacheTimeout time.Duration
	logger       zerolog.Logger
	fallback     SourceFetcher[K, V]
	cache        CachingFetcher[K, V]
}

func (c CacheFallbackFetcher[K, V]) Fetch(ctx context.Context, key K) (V, error) {
	var zero V
	// 1. Try to fetch from cache
	value, err := c.cache.Fetch(ctx, key)
	if err == nil {
		c.logger.Debug().Msg("Cache hit.")
		return value, nil
	}
	c.logger.Debug().Err(err).Msg("Cache miss. Falling back to source.")

	// 2. Cache miss, fallback to source
	value, err = c.fallback.Fetch(ctx, key)
	if err != nil {
		c.logger.Error().Err(err).Msg("Error fetching from source.")
		return zero, fmt.Errorf("error fetching from source: %w", err)
	}
	c.logger.Debug().Msg("Source hit. Writing back to cache.")

	// 3. Source hit, write back to cache in the background.
	go func(k K, v V) {
		writeCtx, cancel := context.WithTimeout(ctx, c.cacheTimeout)
		defer cancel()
		if writeErr := c.cache.WriteToCache(writeCtx, k, v); writeErr != nil {
			c.logger.Error().Err(writeErr).Msg("Failed to write to cache in background.")
		}
	}(key, value)

	return value, nil
}

func (c CacheFallbackFetcher[K, V]) Close() error {
	if err := c.cache.Close(); err != nil {
		c.logger.Error().Err(err).Msg("Error closing cache.")
		return fmt.Errorf("error closing cache: %w", err)
	}
	if err := c.fallback.Close(); err != nil {
		c.logger.Error().Err(err).Msg("Error closing source.")
		return fmt.Errorf("error closing source: %w", err)
	}
	return nil
}
