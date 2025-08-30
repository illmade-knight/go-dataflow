# **Go Dataflow: A Library for Production-Grade Data Pipelines**

This repository contains a comprehensive, modular, and scalable Go library (/pkg) for building robust, high-performance data processing pipelines. It is designed to be the foundation for a microservice architecture where services need to consume, transform, enrich, and sink data from various sources to various destinations, particularly within the Google Cloud ecosystem.

The core philosophy is to provide a set of powerful, reusable building blocks that can be easily composed into complete, testable, and deployable microservices.

## **Core Packages**

The /pkg directory contains the core library components. These are the reusable, generic building blocks for constructing any dataflow. The packages provide high-level abstractions and ready-to-use implementations for common pipeline tasks, including:

* **Ingestion**: Connecting to data sources like MQTT brokers (mqttconverter).
* **Processing**: Implementing streaming (messagepipeline) and batching (bqstore, icestore) services.
* **Enrichment**: Augmenting data in-flight using a powerful, multi-layered caching framework (enrichment, cache).
* **Sinking**: Writing data to destinations like Google Cloud Storage (icestore) and BigQuery (bqstore).

For a detailed breakdown of each package and a complete architectural overview of the library, please see the [**Package Readme**](https://www.google.com/search?q=./pkg/README.md).

## **Usage in a Microservice Architecture**

While this repository provides the core libraries, it is intended to be used as a dependency in a separate microservice deployment repository. This separation of concerns keeps the reusable library code distinct from the application-specific main packages and deployment configurations.

A typical deployment repository would be structured like this:

/my-data-platform  
|-- go.mod  
|-- /cmd  
|   |-- /ingestion-service  
|   |   |-- main.go          // Wraps the 'ingestion' package from go-dataflow  
|   |   |-- routes.yaml      // Service-specific configuration  
|   |-- /icestore-service  
|   |   |-- main.go          // Wraps the 'icestore' package from go-dataflow  
|-- /e2e                     // End-to-end tests for the data platform  
|   |-- devflow\_test.go  
|   |-- icestore\_test.go  

We have a full services based usage of this available at [go-dataflow-services](https://github.com/illmade-knight/go-dataflow-services)