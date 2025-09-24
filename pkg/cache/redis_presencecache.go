// Package cache provides generic caching components for data pipelines.
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

// RedisPresenceCache is a production-ready, distributed implementation of PresenceCache using Redis.
type RedisPresenceCache[K comparable, V any] struct {
	redisClient *redis.Client
	logger      zerolog.Logger
	ttl         time.Duration
}

// NewRedisPresenceCache creates and connects a new RedisPresenceCache.
func NewRedisPresenceCache[K comparable, V any](
	ctx context.Context,
	cfg *RedisConfig,
	logger zerolog.Logger,
) (*RedisPresenceCache[K, V], error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("failed to connect to redis for presence cache: %w", err)
	}
	logger.Info().Str("redis_address", cfg.Addr).Msg("Successfully connected to Redis for PresenceCache.")

	return &RedisPresenceCache[K, V]{
		redisClient: rdb,
		logger:      logger.With().Str("component", "RedisPresenceCache").Logger(),
		ttl:         cfg.CacheTTL,
	}, nil
}

// Set marshals the value to JSON and stores it in Redis with a TTL.
func (c *RedisPresenceCache[K, V]) Set(ctx context.Context, key K, value V) error {
	stringKey := fmt.Sprintf("%v", key)
	jsonData, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal presence data for key %s: %w", stringKey, err)
	}
	if err := c.redisClient.Set(ctx, stringKey, jsonData, c.ttl).Err(); err != nil {
		return fmt.Errorf("failed to set presence in redis for key %s: %w", stringKey, err)
	}
	return nil
}

// Fetch retrieves and unmarshals a value from Redis.
func (c *RedisPresenceCache[K, V]) Fetch(ctx context.Context, key K) (V, error) {
	var zero V
	stringKey := fmt.Sprintf("%v", key)
	cachedData, err := c.redisClient.Get(ctx, stringKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return zero, fmt.Errorf("key '%v' not found in presence cache", key)
		}
		return zero, fmt.Errorf("redis get failed for key %s: %w", stringKey, err)
	}
	var value V
	if err := json.Unmarshal([]byte(cachedData), &value); err != nil {
		return zero, fmt.Errorf("failed to unmarshal presence data for key %s: %w", stringKey, err)
	}
	return value, nil
}

// Delete removes a key from Redis.
func (c *RedisPresenceCache[K, V]) Delete(ctx context.Context, key K) error {
	stringKey := fmt.Sprintf("%v", key)
	if err := c.redisClient.Del(ctx, stringKey).Err(); err != nil {
		return fmt.Errorf("redis del failed for key %s: %w", stringKey, err)
	}
	return nil
}

// Close closes the Redis client connection.
func (c *RedisPresenceCache[K, V]) Close() error {
	if c.redisClient != nil {
		return c.redisClient.Close()
	}
	return nil
}
