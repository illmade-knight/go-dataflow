# **Go Pipeline Caching Package**

This Go package provides a generic, multi-layered caching framework designed for high-performance data pipeline operations. It allows services to fetch data efficiently by chaining multiple caching layers together, falling back from faster, ephemeral caches (like in-memory or Redis) to slower, persistent sources of truth (like Firestore).

The entire framework is built on a single, simple interface and uses Go generics, making it type-safe and adaptable to any data structure.

## **Core Concept: The Fetcher Interface**

The foundation of this package is the Fetcher interface. It defines a simple contract for any component that can retrieve data.

````
type Fetcher[K comparable, V any] interface {
	Fetch(ctx context.Context, key K) (V, error)
	io.Closer  
}
````

Any service that needs to retrieve data should depend on this interface, not on a concrete implementation. This allows the caching strategy to be configured and changed without altering the business logic of the consuming service.

The key design pattern is the **fallback mechanism**. Each cache implementation can be configured with another Fetcher to use as a fallback. When a Fetch call results in a cache miss, the cache will automatically call Fetch on its fallback, retrieve the data, store it for future requests (a "write-back"), and then return it to the original caller.

## **Key Components**

This package is composed of several key components that work together:

* **cache.go**: Defines the central Fetcher interface that underpins the entire package.
* **inmemory.go**: A simple, thread-safe, in-memory cache with no size limit. Ideal for testing or simple use cases.
* **inmemory\_lru.go**: A production-ready, size-limited, in-memory cache that uses a Least Recently Used (LRU) eviction policy to prevent memory leaks.
* **redis.go**: A distributed cache implementation backed by Redis, suitable for sharing cached data across multiple service instances.
* **firestore.go**: A data fetcher for retrieving documents from Google Cloud Firestore, typically used as the final "source of truth" in a cache fallback chain.

## **Implementations**

This package provides four primary implementations of the Fetcher interface:

* **InMemoryCache**: A basic, thread-safe, in-memory cache. This is the fastest caching layer, but it has no eviction policy and will grow indefinitely, making it best suited for testing or short-lived applications.
* InMemoryLRUCache: A more advanced, production-ready in-memory cache. It is configured with a maximum size and uses a **Least Recently Used (LRU)** policy to evict older items when the cache is full. This is the recommended implementation for an L1 cache as it prevents memory leaks and ensures the most relevant data is kept hot.
* **RedisCache**: A distributed cache implementation using Redis. This layer is perfect for sharing cached data across multiple instances of a service. It supports a Time-To-Live (TTL) on cached items. On a cache miss, it will call its fallback Fetcher.
* **Firestore**: A data fetcher that retrieves documents directly from a Google Cloud Firestore collection. This component is not typically used as a cache itself, but rather as the final "source of truth" at the end of a fallback chain.

## **Usage: Chaining Caches**

The real power of this package comes from chaining the Fetcher implementations together to create a multi-layered cache. A typical setup for a high-performance data enrichment pipeline would be:

**Service Logic \-\> InMemoryLRUCache \-\> RedisCache \-\> Firestore**

1. The service calls Fetch on the InMemoryLRUCache.
2. If the data is in memory (L1 cache hit), it's returned instantly.
3. If not (L1 miss), the InMemoryLRUCache calls Fetch on its fallback, the RedisCache.
4. If the data is in Redis (L2 cache hit), it's returned, and the InMemoryLRUCache stores it for next time.
5. If not (L2 miss), the RedisCache calls Fetch on its fallback, the Firestore fetcher.
6. The Firestore fetcher retrieves the data from the database (the source of truth).
7. The data is then written back to the RedisCache (with a TTL) and the InMemoryLRUCache on its way back to the original caller.

This ensures that subsequent requests for the same data will be served from the fastest available cache layer.

### **Example Initialization**

// Example of creating a chained cache for a UserProfile struct

````
type UserProfile struct {  
    Name  string `firestore:"name"`  
    Email string `firestore:"email"`  
}

// 1\. Configure the components  
ctx := context.Background()  
logger := zerolog.New(os.Stdout)

firestoreCfg := &cache.FirestoreConfig{  
    ProjectID:      "my-gcp-project",  
    CollectionName: "users",  
}  

redisCfg := &cache.RedisConfig{  
    Addr:     "localhost:6379",  
    CacheTTL: 1 * time.Hour,  
}

// 2\. Create the clients for the external services  
firestoreClient, err := firestore.NewClient(ctx, firestoreCfg.ProjectID)  
// ... handle error

// 3\. Create the fetcher instances, starting from the source of truth  
//    and working backwards.

// Layer 3: The source of truth (Firestore)  
firestoreFetcher, err := cache.NewFirestore[string, UserProfile](ctx, firestoreCfg, firestoreClient, logger)  
// ... handle error

// Layer 2: The Redis cache, which falls back to Firestore  
redisCache, err := cache.NewRedisCache[string, UserProfile](ctx, redisCfg, logger, firestoreFetcher)  
// ... handle error

// Layer 1: The in-memory cache, which falls back to Redis  
inMemoryLRUCache, err := cache.NewInMemoryLRUCache[string, UserProfile](10000, redisCache)

// 4\. The service now uses the top-level cache.  
//    The entire fallback chain is transparent to the caller.  
user, err := inMemoryLRUCache.Fetch(ctx, "user-123")  
// ... handle error and use the user profile
````

## **Testing**

This package is designed to be highly testable.

* **Unit Tests**: Each cache implementation has its own unit tests that use a mock Fetcher to verify the fallback logic.
* **Integration Tests**: The package includes full integration tests for the RedisCache and Firestore fetchers. These tests use the official Google Cloud emulators and Testcontainers to create ephemeral Redis and Firestore instances, allowing for validation against real service behavior without external dependencies.

Go Pipeline Caching Package
This Go package provides a generic, multi-layered caching framework designed for high-performance data pipeline operations. It supports two primary caching patterns to handle different data access needs.

Pattern 1: Read-Through Caching (Fetcher Interface)
The first pattern is built on the Fetcher interface, designed for read-through caching. Its purpose is to cache data that has a permanent "source of truth" (like a user profile in a database).

The key design pattern is the fallback mechanism. When a Fetch call results in a cache miss, the cache automatically calls Fetch on its fallback, retrieves the data, stores it for future requests (a "write-back"), and then returns it to the original caller.

Use this pattern for: Data that is read frequently and has a persistent source (e.g., user profiles, device configurations, product details).

Fetcher Implementations
InMemoryCache: Simple in-memory cache.

InMemoryLRUCache: Production-ready, size-limited in-memory cache with LRU eviction.

RedisCache: Distributed cache backed by Redis.

Firestore: A data fetcher (not a cache) for retrieving documents, typically used as the final "source of truth" in a fallback chain.

Pattern 2: Ephemeral State Management (PresenceCache Interface)
The second pattern is built on the PresenceCache interface, designed for managing ephemeral, real-time state. This type of data has no persistent source of truth and must be explicitly written and deleted. The most common use case is tracking the online status of users connected via WebSockets.

The interface requires explicit Set, Fetch, and Delete methods.

Use this pattern for: Short-lived, frequently changing state that needs to be shared across service instances (e.g., user online status, session data, distributed locks).

PresenceCache Implementations
InMemoryPresenceCache: A simple, thread-safe, in-memory implementation suitable for single-instance services or local testing.

RedisPresenceCache: A production-ready, distributed implementation using Redis. This is the recommended choice for multi-instance deployments as it provides a fast, shared view of presence state.

FirestorePresenceCache: A production-ready implementation using Firestore. This is a viable alternative for smaller deployments where running a separate Redis instance might be undesirable.

Usage and Examples
(Existing documentation for chaining Fetchers remains here...)