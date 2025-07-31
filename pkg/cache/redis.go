package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

// RedisConfig holds the configuration for the Redis client.
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	CacheTTL time.Duration
}

// RedisCache is a generic cache implementation using Redis.
// It implements the Fetcher interface and can be configured with a fallback Fetcher
// to use on a cache miss.
type RedisCache[K comparable, V any] struct {
	redisClient *redis.Client
	logger      zerolog.Logger
	ttl         time.Duration
	fallback    Fetcher[K, V]
}

// NewRedisCache creates and connects a new generic RedisCache.
// It pings the Redis server to ensure connectivity before returning.
// It can optionally be provided with a fallback Fetcher to use on a cache miss.
func NewRedisCache[K comparable, V any](
	ctx context.Context,
	cfg *RedisConfig,
	logger zerolog.Logger,
	fallback Fetcher[K, V],
) (*RedisCache[K, V], error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	logger.Info().Str("redis_address", cfg.Addr).Msg("Successfully connected to Redis.")

	return &RedisCache[K, V]{
		redisClient: rdb,
		logger:      logger.With().Str("component", "RedisCache").Logger(),
		ttl:         cfg.CacheTTL,
		fallback:    fallback,
	}, nil
}

// Fetch retrieves an item by key. It first checks Redis. On a cache miss, if a
// fallback is configured, it fetches from the fallback, writes the result back
// to Redis in the background, and returns the value.
func (c *RedisCache[K, V]) Fetch(ctx context.Context, key K) (V, error) {
	// 1. Try to fetch from Redis
	value, err := c.fetchFromRedis(ctx, key)
	if err == nil {
		return value, nil // Cache hit
	}

	// A redis.Nil error is a normal cache miss. Any other error is a genuine problem.
	if !errors.Is(err, redis.Nil) {
		c.logger.Error().Err(err).Msg("Unexpected Redis error during fetch.")
		return *new(V), err
	}

	// 2. Cache miss, check for a fallback
	var zero V
	if c.fallback == nil {
		return zero, fmt.Errorf("key '%v' not found in cache and no fallback is configured", key)
	}

	// 3. Fallback to the source Fetcher
	sourceValue, sourceErr := c.fallback.Fetch(ctx, key)
	if sourceErr != nil {
		return zero, sourceErr
	}

	// 4. Write the result from the source back to Redis in the background.
	// This avoids blocking the main request path on the cache write.
	go func() {
		// Use a new background context with a timeout for the write operation.
		writeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if writeErr := c.write(writeCtx, key, sourceValue); writeErr != nil {
			c.logger.Error().Err(writeErr).Str("key", fmt.Sprintf("%v", key)).Msg("Failed to write to cache in background.")
		}
	}()

	return sourceValue, nil
}

// fetchFromRedis is an unexported method to get a value directly from Redis.
func (c *RedisCache[K, V]) fetchFromRedis(ctx context.Context, key K) (V, error) {
	var zero V
	stringKey := fmt.Sprintf("%v", key)
	cachedData, err := c.redisClient.Get(ctx, stringKey).Result()
	if err != nil {
		// Let the caller handle distinguishing redis.Nil from other errors.
		return zero, err
	}

	var value V
	if err := json.Unmarshal([]byte(cachedData), &value); err != nil {
		c.logger.Error().Err(err).Str("key", stringKey).Msg("Failed to unmarshal cached data.")
		return zero, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	c.logger.Debug().Str("key", stringKey).Msg("Redis cache hit.")
	return value, nil
}

// write is an unexported method to set a value in Redis with the configured TTL.
func (c *RedisCache[K, V]) write(ctx context.Context, key K, value V) error {
	stringKey := fmt.Sprintf("%v", key)
	jsonData, err := json.Marshal(value)
	if err != nil {
		c.logger.Error().Err(err).Str("key", stringKey).Msg("Failed to marshal data for caching.")
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	if err := c.redisClient.Set(ctx, stringKey, jsonData, c.ttl).Err(); err != nil {
		c.logger.Error().Err(err).Str("key", stringKey).Msg("Failed to set data in Redis cache.")
		return fmt.Errorf("failed to set in redis: %w", err)
	}

	c.logger.Debug().Str("key", stringKey).Msg("Successfully stored data in Redis cache.")
	return nil
}

// Close closes the Redis client connection.
func (c *RedisCache[K, V]) Close() error {
	if c.redisClient != nil {
		c.logger.Info().Msg("Closing Redis client connection...")
		return c.redisClient.Close()
	}
	return nil
}
