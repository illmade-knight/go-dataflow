

# **Go BigQuery Store (bqstore)**

The bqstore package provides robust and high-level components for building data processing pipelines in a microservice architecture that sink data into Google BigQuery. It is designed to integrate seamlessly with a generic message pipeline framework, abstracting away the complexities of batch processing, data insertion, and error handling.

This package is ideal for scenarios where you are consuming messages from a streaming source like Google Pub/Sub, transforming them, and then efficiently batch-inserting them into BigQuery.

## **Features**

* **High-Level Service Factory**: NewBigQueryService assembles a complete data processing pipeline, from message consumption to BigQuery insertion, with minimal boilerplate.
* **Batch Processing**: Automatically handles the batching of incoming messages to optimize writes to BigQuery.
* **At-Least-Once Delivery**: In case of a failure to insert a batch into BigQuery, it ensures that all messages in that batch are negatively-acknowledged (Nacked), allowing them to be reprocessed.
* **Abstracted Data Inserter**: The DataBatchInserter interface decouples the pipeline from the specific data store, making the system more modular and easier to test.
* **Automatic Schema Inference**: The NewBigQueryInserter can automatically infer a BigQuery table schema from your Go data structures and create the table if it doesn't exist.
* **Graceful Shutdown**: The service supports context cancellation for graceful shutdown of the pipeline.

## **Architecture Overview**

The bqstore package acts as a specialized layer on top of a more generic messagepipeline package. It provides a pre-configured BatchProcessor that is tailored for BigQuery operations.

The typical data flow is as follows:

1. A MessageConsumer (e.g., from a Pub/Sub topic) receives messages.
2. A MessageTransformer parses and transforms the raw message into a typed Go struct.
3. The BatchingService collects these transformed items into batches based on size or time.
4. The batchProcessor function provided by bqstore takes a complete batch.
5. It uses the DataBatchInserter (specifically the BigQueryInserter) to write the entire batch to a BigQuery table.
6. If the BigQuery insert is successful, all original messages in the batch are Acknowledged (Acked).
7. If the insert fails, all messages are Negatively Acknowledged (Nacked) to signal for redelivery and reprocessing.

## **Usage**

The primary entry point for creating a data pipeline is the NewBigQueryService function. You will need to provide it with a message consumer, a BigQuery inserter, and a message transformer.

### **Example: Setting up a Full Pipeline**

Here is an example demonstrating how to set up a pipeline that consumes messages from Google Pub/Sub, transforms them, and inserts them into BigQuery.

*(This example is based on the integration test provided in bqstore\_integration\_test.go)*

**1\. Define your data structures:**

You need a struct for your BigQuery table schema and another for the incoming message format if it's structured (e.g., JSON).

Go
````
// MonitorReadings represents the structure of the data written to BigQuery.  
type MonitorReadings struct {  
    DE       string  
    Sequence int  
    Battery  int  
}

// TestUpstreamMessage represents the structure of the message published to Pub/Sub.  
type TestUpstreamMessage struct {  
    Payload \*MonitorReadings \`json:"payload"\`  
}

````


**2\. Implement a MessageTransformer:**

This function is responsible for converting a raw message into your typed Go struct.

````
// ConsumedMessageTransformer implements the MessageTransformer logic.  
func ConsumedMessageTransformer(_ context.Context, msg *messagepipeline.Message) (*MonitorReadings, bool, error) {  
	var upstreamMsg TestUpstreamMessage  
        if err := json.Unmarshal(msg.Payload, &upstreamMsg); err != nil {  
        // Signal for Nack  
        return nil, false, fmt.Errorf("failed to unmarshal upstream message: %w", err)  
    }  
    if upstreamMsg.Payload == nil {  
        // Skip message but Ack it  
        return nil, true, nil  
    }  
    return upstreamMsg.Payload, false, nil // Success  
}
````

**3\. Assemble the Pipeline:**

In your main application setup, initialize the components and create the service.

Go
````
package main

import (  
    "context"  
    "log"  
    "time"
    
    "cloud.google.com/go/bigquery"
    "cloud.google.com/go/pubsub"
    "github.com/illmade-knight/go-dataflow/pkg/bqstore"
    "github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
    "github.com/rs/zerolog"  
)

func main() {  
    ctx := context.Background()  
    logger := zerolog.New(os.Stdout)

	// --- Configuration ---  
	const (  
		projectID      = "your-gcp-project-id"  
		subscriptionID = "your-pubsub-subscription-id"  
		datasetID      = "your_bigquery_dataset_id"  
		tableID        = "your_bigquery_table_id"  
	)

	// --- Client Setup ---  
	psClient, err := pubsub.NewClient(ctx, projectID)  
	if err != nil {  
		log.Fatalf("Failed to create Pub/Sub client: %v", err)  
	}  
	defer psClient.Close()

	bqClient, err := bigquery.NewClient(ctx, projectID)  
	if err != nil {  
		log.Fatalf("Failed to create BigQuery client: %v", err)  
	}  
	defer bqClient.Close()

	// --- Initialize Pipeline Components ---  
	consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults()  
	consumerCfg.SubscriptionID = subscriptionID  
	consumer, err := messagepipeline.NewGooglePubsubConsumer(ctx, consumerCfg, psClient, logger)  
	if err != nil {  
		log.Fatalf("Failed to create consumer: %v", err)  
	}

	bqInserterCfg := &bqstore.BigQueryDatasetConfig{DatasetID: datasetID, TableID: tableID}  
	bqInserter, err := bqstore.NewBigQueryInserter[MonitorReadings](ctx, bqClient, bqInserterCfg, logger)  
	if err != nil {  
		log.Fatalf("Failed to create BigQuery inserter: %v", err)  
	}

	serviceCfg := messagepipeline.BatchingServiceConfig{  
		NumWorkers:    4,                // Number of concurrent processing workers  
		BatchSize:     100,              // Max items per batch  
		FlushInterval: 5 * time.Second,  // Max time before a batch is flushed  
	}

	// --- Create and Start the Service ---  
	processingService, err := bqstore.NewBigQueryService[MonitorReadings](  
		serviceCfg,  
		consumer,  
		bqInserter,  
		ConsumedMessageTransformer,  
		logger,  
	)  
	if err != nil {  
		log.Fatalf("Failed to create bqstore service: %v", err)  
	}

	// Start the service and handle graceful shutdown  
	serviceCtx, cancel := context.WithCancel(ctx)  
	// ... handle os.Interrupt to call cancel() ...

	if err := processingService.Start(serviceCtx); err != nil {  
		log.Fatalf("Failed to start service: %v", err)  
	}

    // Wait for the service to be done (e.g. via a signal)  
    <-serviceCtx.Done()

    // Stop the service gracefully  
    stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)  
    defer stopCancel()  
    if err := processingService.Stop(stopCtx); err != nil {  
        log.Printf("Error during service shutdown: %v", err)  
    }  
}

````

## **Testing**

The package is designed to be highly testable. 
The DataBatchInserter interface allows you to easily mock the BigQuery dependency for your unit tests. 
The provided shared_helpers_test.go file contains a MockDataBatchInserter and MockMessageConsumer that can be used for this purpose.

For an example of how to write unit tests, 
see bigqueryservice_test.go. 
For an example of a full integration test using Google Cloud emulators, 
see bqstore\_integration\_test.go.