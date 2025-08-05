# **Go Dataflow & Pipeline Library**

This repository contains a comprehensive, modular, and scalable Go library for building robust, high-performance data processing pipelines. It is designed for a microservice architecture where services need to consume, transform, enrich, and sink data from various sources to various destinations.

The library is built on a set of core interfaces and generic components, allowing developers to quickly assemble complex pipelines for different use cases, from real-time streaming to large-scale batch archival.

## **Core Architecture**

The entire library is built on the **messagepipeline** package, which provides the fundamental building blocks for all dataflows.

**Key Concepts:**

* **Message**: A canonical struct representing a single event flowing through any pipeline. It contains the payload, metadata, and acknowledgment (Ack/Nack) handles.
* **MessageConsumer**: An interface for data sources (e.g., Pub/Sub, MQTT).
* **MessageTransformer**: A function for converting a raw Message into a typed Go struct.
* **Processor**: An interface for the final business logic that operates on a message (either one-by-one or in batches).
* **Services**: High-level orchestrators (StreamingService, BatchingService) that manage the concurrent processing of messages, wiring together the consumer, transformer, and processor.

## **Packages Overview**

The library is organized into several distinct packages, each serving a specific role in the data processing ecosystem.

### **1\. messagepipeline (Core)**

The heart of the library. It provides all the core interfaces, data types, and generic service orchestrators (StreamingService, BatchingService) needed to construct a pipeline. It also includes ready-to-use components for **Google Cloud Pub/Sub**, such as a consumer and multiple producer types.

### **2\. mqttconverter (Ingestion)**

A service designed for data ingestion from an **MQTT broker**. It provides an MqttConsumer that connects to a broker, subscribes to topics, and transforms MQTT messages into the canonical messagepipeline.Message format, allowing them to be processed by any other service in the ecosystem.

### **3\. enrichment (Transformation)**

A specialized service for enriching messages with external data. A common use case is to take an ID from an incoming message (e.g., a deviceID) and fetch detailed metadata from a data store. This service is designed to work seamlessly with the cache package to perform these lookups at high speed.

### **4\. cache (Data Fetching & Caching)**

A powerful, generic, multi-layered caching framework. It is built around a single Fetcher interface, allowing for different caching strategies to be chained together. This is a critical component for high-performance enrichment.

* **Core Pattern**: The Fetcher interface supports a fallback mechanism. A cache miss in one layer automatically triggers a fetch from the next, slower layer, with the result being written back to the faster caches on its way to the caller.
* **Implementations**:
    * InMemoryLRUCache: A size-limited, in-memory L1 cache with a Least Recently Used (LRU) policy.
    * RedisCache: A distributed L2 cache using Redis.
    * Firestore: A data fetcher for the source of truth, typically Google Cloud Firestore.
* **Typical Chain**: InMemoryLRUCache \-\> RedisCache \-\> Firestore

### **5\. bqstore (Data Sinking)**

A specialized service for streaming data into **Google BigQuery**. It provides a high-level factory that assembles a BatchingService with a pre-configured BigQueryInserter. This inserter uses the BigQuery Storage Write API for high-throughput streaming and can even infer and create a table schema automatically from your Go structs if the table doesn't exist.

### **6\. icestore (Data Archival)**

A specialized service for long-term archival of raw message data to **Google Cloud Storage (GCS)**. This service uses a unique **KeyAwareBatcher** that groups messages by a logical key (e.g., date and location) before uploading. This ensures that related data is co-located in the same compressed object in GCS, which is highly efficient for later analysis or re-processing.

## **How the Packages Work Together: A Common Architecture**

These packages are designed to be composed into a complete, end-to-end data platform. A typical architecture might look like this:

1. **Ingestion**: An **mqttconverter** service subscribes to a live MQTT stream of sensor data. It transforms each raw MQTT message into the canonical Message format and publishes it to a central "raw data" Pub/Sub topic.
2. **Enrichment**: An **enrichment** service subscribes to the "raw data" topic. For each message, it uses the deviceID from the message attributes to fetch detailed metadata. It does this by calling its Fetcher, which is a chained **cache** (InMemoryLRU \-\> Redis \-\> Firestore). The fetched metadata is added to the message's EnrichmentData map, and the enriched message is published to a new "enriched data" Pub/Sub topic.
3. **Data Warehousing**: A **bqstore** service subscribes to the "enriched data" topic. It transforms the message into a structured format for BigQuery and uses its efficient batching service to stream the data into a "live" analytics table.
4. **Archival**: An **icestore** service *also* subscribes to the "enriched data" topic. It uses its KeyAwareBatcher to group messages by date and device location, archiving the raw payloads into compressed JSONL files in GCS for long-term storage and compliance.

This modular, decoupled architecture allows each service to be scaled, deployed, and maintained independently, providing a robust and flexible foundation for any data-intensive application.