# **Icestore \- Data Archival Service**

icestore is a Go package that provides a robust and efficient data pipeline service for consuming messages, transforming them, and archiving them to Google Cloud Storage (GCS).

The service is designed to be highly performant and resilient. It intelligently groups incoming data into batches based on a customizable key (e.g., date and location), and then uploads each batch as a single, compressed object to GCS. This approach is ideal for creating partitioned datasets in a data lake for later analysis.

## **Core Concepts & Architecture**

The icestore service operates as a multi-stage pipeline:

1. **Consume**: An external message consumer (like a Google Pub/Sub client) provides raw messages to the service.
2. **Transform**: Each message is passed through a Transformer function. The ArchivalTransformer included in this package converts the message into a structured ArchivalData format. It also generates a BatchKey (e.g., 2025/06/15/location-a) based on the message's timestamp and attributes. This key is crucial for partitioning.
3. **Batch**: The transformed data is sent to a KeyAwareBatcher. This component groups items in memory based on their BatchKey. A batch for a specific key is flushed when either of these conditions is met:
    * The number of items for that key reaches the configured BatchSize.
    * The FlushInterval timer expires, flushing all pending batches.
4. **Upload**: Once a batch is flushed, the GCSBatchUploader takes over. It streams all items from the batch into a single GCS object, compressing the data with Gzip and formatting it as JSON Lines (.jsonl.gz). The object path in the GCS bucket is determined by the BatchKey, resulting in a well-organized, partitioned data store.

This architecture ensures that data is uploaded to GCS efficiently, minimizing the number of write operations and creating larger, more cost-effective files.

## **Key Components**

This package is composed of several key components that work together:

* **icestoreservice.go**: The main service orchestrator. It initializes and connects the consumer, transformer, batcher, and uploader, and manages the worker pool that processes messages.
* **keyawarebatcher.go**: A stateful, in-memory batching engine. Its core responsibility is to group data by key to create logical, partitioned batches for upload.
* **gcsuploader.go**: The final "sink" in the pipeline. It handles the specifics of streaming and compressing a batch of data into a single object in a GCS bucket.
* **transformer.go**: Defines the canonical ArchivalData struct and provides the ArchivalTransformer function for converting raw messages into this structure. The logic for generating the BatchKey resides here.
* **gcsclient.go**: Provides a set of Go interfaces that abstract the official Google Cloud Storage client. This is a crucial design feature that allows the uploader and the service to be unit-tested with mocks, without requiring a connection to a real GCS environment.

## **Usage**

To use the service, you need to create an instance of IceStorageService, providing it with a message consumer, a GCS client, and the necessary configuration.

// Example Initialization

// 1\. Create clients and logger  
ctx := context.Background()  
logger := zerolog.New(os.Stdout)  
pubsubClient, err := pubsub.NewClient(ctx, "my-gcp-project")  
// ... handle error  
storageClient, err := storage.NewClient(ctx)  
// ... handle error

// 2\. Configure the service components  
consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults()  
consumerCfg.SubscriptionID \= "my-subscription"  
consumer, err := messagepipeline.NewGooglePubsubConsumer(ctx, consumerCfg, pubsubClient, logger)  
// ... handle error

serviceCfg := icestore.IceStorageServiceConfig{  
NumWorkers:    10,  
BatchSize:     100,  
FlushInterval: 5 \* time.Minute,  
}

uploaderCfg := icestore.GCSBatchUploaderConfig{  
BucketName:   "my-archive-bucket",  
ObjectPrefix: "raw-data/",  
}

// 3\. Create the service instance  
// Note: We wrap the concrete storage client with the adapter for testability.  
gcsClientAdapter := icestore.NewGCSClientAdapter(storageClient)  
service, err := icestore.NewIceStorageService(  
serviceCfg,  
consumer,  
gcsClientAdapter,  
uploaderCfg,  
icestore.ArchivalTransformer, // Use the default transformer  
logger,  
)  
// ... handle error

// 4\. Start the service  
err \= service.Start(ctx)  
// ... handle error

// ... wait for shutdown signal, then call service.Stop(ctx)

## **Testing**

This package was designed with testability as a core principle.

* **Unit Tests**: The KeyAwareBatcher and GCSBatchUploader have comprehensive unit tests that use mock objects to verify their logic in isolation.
* **Integration Tests**: The icestore\_integration\_test.go file provides a full, end-to-end integration test for the service. It uses the official Google Cloud emulators for Pub/Sub and GCS to validate the entire pipeline flow without connecting to live GCP resources.
* **Mocking**: The gcsclient.go interfaces are key to enabling testing. By programming against these interfaces, components like the GCSBatchUploader can be tested with a simple mock GCS client, as demonstrated in gcsuploader\_test.go.