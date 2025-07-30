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

// RedisCache is a generic cache implementation using Redis. It satisfies the Cache interface.
type RedisCache[K comparable, V any] struct {
	redisClient *redis.Client
	logger      zerolog.Logger
	ttl         time.Duration
}

// NewRedisCache creates and connects a new generic RedisCache.
// It pings the Redis server to ensure connectivity before returning.
func NewRedisCache[K comparable, V any](
	ctx context.Context,
	cfg *RedisConfig,
	logger zerolog.Logger,
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
	}, nil
}

// FetchFromCache retrieves an item from the Redis cache.
func (c *RedisCache[K, V]) FetchFromCache(ctx context.Context, key K) (V, error) {
	var zero V
	stringKey := fmt.Sprintf("%v", key)
	cachedData, err := c.redisClient.Get(ctx, stringKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			c.logger.Debug().Str("key", stringKey).Msg("Redis cache miss.")
			return zero, fmt.Errorf("key not found in cache: %w", err)
		}
		c.logger.Error().Err(err).Str("key", stringKey).Msg("Error fetching from Redis cache.")
		return zero, fmt.Errorf("redis fetch error: %w", err)
	}

	var value V
	if err := json.Unmarshal([]byte(cachedData), &value); err != nil {
		c.logger.Error().Err(err).Str("key", stringKey).Msg("Failed to unmarshal cached data.")
		return zero, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	c.logger.Debug().Str("key", stringKey).Msg("Redis cache hit.")
	return value, nil
}

// WriteToCache adds an item to the Redis cache with a configured TTL.
func (c *RedisCache[K, V]) WriteToCache(ctx context.Context, key K, value V) error {
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
