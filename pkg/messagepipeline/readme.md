# **Go Message Pipeline**

This Go package provides a powerful and flexible framework for building robust, concurrent, and testable data processing pipelines. It is designed for microservice architectures where services consume messages from a source (like Google Pub/Sub), transform or enrich them, and then process them individually or in batches.

The core of the package is a set of interfaces and generic service implementations that allow you to quickly assemble a pipeline while abstracting away the complexities of concurrency, graceful shutdown, and message acknowledgment (Ack/Nack) lifecycles.

## **Core Concepts**

The pipeline is built around a few key interfaces and types that define the stages of data flow.

### **Message**

The canonical, internal representation of an event flowing through the pipeline. It contains:

* MessageData: The core payload, ID, publish time, and a map for enrichment data.
* Attributes: Metadata from the message broker (e.g., Pub/Sub attributes).
* Ack(): A function to call to signal successful processing.
* Nack(): A function to call to signal a processing failure, allowing for redelivery.

### **Pipeline Stages**

1. **MessageConsumer**: The entry point of the pipeline. This interface defines a message source (e.g., a Google Pub/Sub subscription). It's responsible for fetching messages and feeding them into the pipeline.
    * GooglePubsubConsumer is the provided implementation for Google Cloud Pub/Sub.
2. **MessageTransformer\[T\]**: A function type that transforms a raw Message into a specific, structured payload of a generic type T. It can also signal to filter (skip) a message from the pipeline.
3. **Processors**: The final stage of the pipeline where business logic is executed. There are two types:
    * **StreamProcessor\[T\]**: Handles one transformed message at a time. Ideal for low-latency, individual processing.
    * **BatchProcessor\[T\]**: Handles a batch of transformed messages. Ideal for sinking data into systems that benefit from bulk operations, like data warehouses.

## **Pipeline Orchestration Services**

The package provides two main service implementations to orchestrate the pipeline flow.

### **StreamingService**

The StreamingService orchestrates a pipeline that processes messages one by one. It's best for use cases that require low latency and don't benefit from batching.

Flow:  
Consumer \-\> Worker Pool \-\> Transformer \-\> StreamProcessor \-\> Ack/Nack  

It manages a pool of concurrent workers that pull messages from the consumer, apply the transformation, and immediately execute the processor.

### **BatchingService**

The BatchingService orchestrates a pipeline that groups transformed messages into batches before processing. It's ideal for high-throughput scenarios and writing to data stores like BigQuery or databases.

Flow:  
Consumer \-\> Worker Pool (Transform) \-\> Batching Worker \-\> BatchProcessor \-\> Ack/Nack (handled by processor)  

It uses a pool of workers to transform messages and then funnels them to a single worker that assembles batches based on size or a flush interval. The BatchProcessor implementation is then responsible for handling the entire batch, including Acknowledging or Negatively Acknowledging the original messages.

## **Google Cloud Pub/Sub Components**

The package includes ready-to-use components for Google Cloud Pub/Sub.

* **GooglePubsubConsumer**: An implementation of MessageConsumer that subscribes to a Pub/Sub topic.
* **GooglePubsubProducer**: A high-throughput producer for publishing enriched MessageData to a downstream Pub/Sub topic. It uses the client's built-in batching for efficiency.
* **GoogleSimplePublisher**: A "fire-and-forget" style publisher that sends a single message without batching and handles the result asynchronously. It's perfect for simple tasks like sending a message to a dead-letter queue.

## **Usage Example**

### **1\. A Streaming Pipeline**

This example sets up a service that consumes messages, transforms them, and processes them one by one.

````
package main

import (  
    "context"  
    "encoding/json"
	"os"
	
	"github.com/rs/zerolog"
    // ... other imports  
    "github.com/illmade-knight/go-dataflow/pkg/messagepipeline"  
)

// Define the target data structure  
type MyPayload struct {  
    Name  string `json:"name"`  
    Value int    `json:"value"`  
}

func main() {  
    // --- 1. Setup (Context, Logger, Pub/Sub Client) ---  
    ctx := context.Background()  
    logger := zerolog.New(os.Stdout)  
    psClient, err := pubsub.NewClient(ctx, "my-gcp-project")  
    // ... handle error

    // --- 2. Create Pipeline Components ---

    // Consumer  
    consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults()  
    consumerCfg.SubscriptionID = "my-input-subscription"  
    consumer, err := messagepipeline.NewGooglePubsubConsumer(ctx, consumerCfg, psClient, logger)  
    // ... handle error

    // Transformer  
    transformer := func(ctx context.Context, msg *messagepipeline.Message) (*MyPayload, bool, error) {  
        var p MyPayload  
        if err := json.Unmarshal(msg.Payload, &p); err != nil {  
            return nil, false, err // Nacks the message  
        }  
        return &p, false, nil  
    }

    // Processor  
    processor := func(ctx context.Context, original messagepipeline.Message, payload *MyPayload) error {  
        logger.Info().Str("name", payload.Name).Int("value", payload.Value).Msg("Processing item")  
        // ... do work ...  
        return nil // Acks the message  
    }

    // --- 3. Assemble and Start the Service ---  
    serviceCfg := messagepipeline.StreamingServiceConfig{NumWorkers: 10}  
    streamService, err := messagepipeline.NewStreamingService[MyPayload](  
        serviceCfg, consumer, transformer, processor, logger,  
    )  
    // ... handle error

    // Start and manage graceful shutdown  
    err = streamService.Start(ctx)
	// ... handle error
	err = streamService.Stop(context.Background())
	// ... handle error
}

````


### **2. A Batching Pipeline**

This example is similar but collects messages into batches before processing.

// ... (setup and component creation is similar to the streaming example)

````
// Batch Processor  
batchProcessor := func(ctx context.Context, batch []messagepipeline.ProcessableItem[MyPayload]) error {  
    logger.Info().Int("batch_size", len(batch)).Msg("Processing batch")
    
    // In a real scenario, you would insert this batch into a database or data warehouse.  
    // The implementation is responsible for Ack/Nack logic.  
    for _, item := range batch {  
        // Here, we just Ack every message for simplicity.  
        item.Original.Ack()  
    }  
    return nil  
}
    
// --- Assemble and Start the Service ---  
serviceCfg := messagepipeline.BatchingServiceConfig{  
    NumWorkers:    10,  
    BatchSize:     50,  
    FlushInterval: 5 * time.Second,  
}
	
batchService, err := messagepipeline.NewBatchingService[MyPayload](  
    serviceCfg, consumer, transformer, batchProcessor, logger,  
)  
// ... handle error

// Start and manage graceful shutdown  
err = batchService.Start(ctx)  
// ...  
batchService.Stop(context.Background())  

````

