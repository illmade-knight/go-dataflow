# **Go Enrichment Service**

This Go package provides a specialized service for data enrichment within a microservice pipeline. 

An example enrichment would be adding metadata from a cache but the pattern is versatile and can mutate or skip data.

This is a common pattern in data pipelines where raw events need to be augmented with contextual information (e.g., adding user details to a clickstream event, or device metadata to an IoT sensor reading).

The package is designed to be generic, testable, and to integrate seamlessly with the messagepipeline framework.

## **Architecture and Core Concepts**

The EnrichmentService orchestrates a simple but powerful data flow:

Flow:  
Consumer \-\> Worker Pool \-\> MessageEnricher \-\> MessageProcessor \-\> Ack/Nack  
A pool of concurrent workers consumes messages. Each message is passed to a MessageEnricher function, which performs the core lookup and data-injection logic. If enrichment is successful, the modified message is then passed to a final MessageProcessor.

### **Key Components**

The enrichment logic is built by composing a few key function types. This makes the system highly modular and easy to test. The NewEnricherFunc factory is used to assemble these components into a single MessageEnricher function.

1. **KeyExtractor\[K\]**: A function that inspects an incoming messagepipeline.Message and extracts a lookup key of a generic type K. It returns the key and a boolean indicating if the key was found.
2. **Fetcher\[K, V\]**: This function takes the key provided by the KeyExtractor and is responsible for fetching the relevant data of type V. This is where you would implement your logic to call a Redis cache, a SQL database, or any other data store.
3. **Applier\[V\]**: Once the Fetcher successfully returns data, this function's job is to apply that data to the original message, typically by adding it to the EnrichmentData map on the Message struct.

### **Error Handling**

The enrichment process has specific error handling behavior:

* If a KeyExtractor fails to find a key, the message proceeds through the pipeline unenriched.
* If the Fetcher returns an error (e.g., the record is not found in the database, or the cache is down), the MessageEnricher will log the error and signal to **skip** the message. The pipeline will then Ack the message to prevent it from being reprocessed in a loop, which is crucial for handling permanently invalid keys or transient data source issues gracefully.

## **Usage Example**

This example demonstrates how to set up an EnrichmentService that fetches device metadata and adds it to an incoming message.

````go

package main

import (  
    "context"  
    "errors"  
    // ... other imports  
    "github.com/illmade-knight/go-dataflow/pkg/enrichment"  
    "github.com/illmade-knight/go-dataflow/pkg/messagepipeline"  
    "github.com/rs/zerolog"  
)

// Define the data structure you're fetching  
type DeviceInfo struct {  
    Name     string `json:"name"`  
    Location string `json:"location"`  
}

func main() {  
    // --- 1. Setup (Context, Logger, Pub/Sub Client, etc.) ---  
    ctx := context.Background()  
    logger := zerolog.New(os.Stdout)  
    // ... (consumer setup is omitted for brevity)  
    var consumer messagepipeline.MessageConsumer // Assume this is initialized

    // --- 2. Define Enrichment Logic Components ---

    // KeyExtractor: Get the deviceID from the message's attributes  
    keyExtractor := func(msg *messagepipeline.Message) (string, bool) {  
        if id, ok := msg.Attributes["deviceID"]; ok {  
            return id, true  
        }  
        return "", false  
    }

    // Fetcher: Get device info from a (mock) cache/database  
    fetcher := func(ctx context.Context, key string) (DeviceInfo, error) {  
        // In a real application, this would query a database or cache.  
        if key == "dev-123" {  
            return DeviceInfo{Name: "Garden Sensor", Location: "Backyard"}, nil  
        }  
        return DeviceInfo{}, errors.New("device not found in data store")  
    }

    // Applier: Add the fetched data to the message's enrichment map  
    applier := func(msg *messagepipeline.Message, data DeviceInfo) {  
        if msg.EnrichmentData == nil {  
            msg.EnrichmentData = make(map[string]interface{})  
        }  
        msg.EnrichmentData["deviceInfo"] = data  
    }

    // --- 3. Create the Enricher and Final Processor ---

    // Use the factory to assemble the enricher function  
    enricher, err := enrichment.NewEnricherFunc(fetcher, keyExtractor, applier, logger)  
    if err != nil {  
        logger.Fatal().Err(err).Msg("Failed to create enricher")  
    }

    // Final Processor: This runs *after* enrichment is complete  
    processor := func(ctx context.Context, msg *messagepipeline.Message) error {  
        if info, ok := msg.EnrichmentData["deviceInfo"]; ok {  
            logger.Info().Interface("device_info", info).Msg("Processing enriched message")  
        } else {  
            logger.Info().Str("msg_id", msg.ID).Msg("Processing message without enrichment.")  
        }  
        // This could be a publisher to another topic, a database write, etc.  
        return nil // Returning nil Acks the message  
    }

    // --- 4. Assemble and Start the EnrichmentService ---  
    serviceCfg := enrichment.EnrichmentServiceConfig{NumWorkers: 10}  
    enrichmentService, err := enrichment.NewEnrichmentService(  
        serviceCfg,  
        enricher,  
        consumer,  
        processor,  
        logger,  
    )  
    if err != nil {  
        logger.Fatal().Err(err).Msg("Failed to create enrichment service")  
    }

    // Start the service and handle graceful shutdown  
    if err := enrichmentService.Start(ctx); err != nil {  
        logger.Fatal().Err(err).Msg("Enrichment service failed to start")  
    }  
      
    // ... wait for shutdown signal ...  
      
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)  
    defer cancel()  
    if err := enrichmentService.Stop(shutdownCtx); err != nil {  
        logger.Error().Err(err).Msg("Enrichment service failed to stop gracefully")  
    }  
}  

````