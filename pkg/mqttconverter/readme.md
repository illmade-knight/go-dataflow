# **MQTT Converter Package (mqttconverter)**

The mqttconverter package provides the necessary components to consume messages from an MQTT broker and integrate them into a messagepipeline. It acts as a bridge, converting raw MQTT messages into a standardized format that can be processed by downstream services.

This package is designed to be a robust, configurable, and easy-to-use component for building data ingestion pipelines that source data from MQTT-enabled devices or systems. The latest version supports subscribing to **multiple topics** simultaneously and tagging messages with logical route names for advanced downstream processing.

## **Core Components**

### **MqttConsumer**

The central component of the package. MqttConsumer implements the messagepipeline.MessageConsumer interface.

**Responsibilities:**

* Establishes and maintains a connection to an MQTT broker.
* Automatically handles reconnects with exponential backoff in case of connection loss.
* Subscribes to **multiple MQTT topics** as defined in the configuration (wildcards are supported).
* Receives messages from the broker and converts them into the standard messagepipeline.Message format.
* Tags each message with a route\_name based on its originating topic mapping.
* Pushes the converted messages into a single output channel for consumption by a pipeline.

**Lifecycle:**

* NewMqttConsumer(): Creates and configures a new consumer.
* Start(): Initiates the connection process in the background. It's a non-blocking call.
* Stop(): Gracefully unsubscribes from all topics and disconnects from the broker.
* Messages(): Returns a read-only channel for accessing the consumed messages.

### **MQTTClientConfig & TopicMapping**

The configuration is split into two structs for clarity and flexibility.

TopicMapping defines a single route from an MQTT topic to a logical stream name.

* Name: A logical identifier for this route (e.g., "device-uplinks"). This name is added to the message attributes.
* Topic: The MQTT topic filter to subscribe to (e.g., "devices/+/data").
* QoS: The Quality of Service level for this subscription.

MQTTClientConfig holds all the client-level configuration.

* BrokerURL: The address of the MQTT broker (e.g., tls://mqtt.example.com:8883).
* TopicMappings: A slice of TopicMapping structs that defines all subscriptions for this consumer.
* ClientIDPrefix: A prefix for the MQTT client ID. A unique suffix is added automatically.
* Username & Password: Credentials for connecting to the broker.
* TLS options for secure connections.

**Configuration:**

The LoadMQTTClientConfigFromEnv() function provides a convenient way to initialize client settings from environment variables, with sensible defaults for timeouts and keep-alive intervals. The TopicMappings must be configured programmatically.

## **How It Works**

1. **Initialization**: An MqttConsumer is created with an MQTTClientConfig. The configuration specifies the broker details, credentials, and a list of TopicMappings.
2. **Start**: The Start() method is called. The underlying Paho MQTT client begins the connection process in the background.
3. **Connection & Subscription**: Once connected, the OnConnectHandler is triggered. It iterates through each TopicMapping and subscribes to the specified topic with its own dedicated message handler.
4. **Message Handling**: When a message arrives on a subscribed topic, the appropriate handler is invoked. This handler knows the route\_name for the topic it's responsible for.
5. **Transformation**: The message is transformed into a messagepipeline.Message. The payload is copied, and two key attributes are added: mqtt\_topic (the full topic the message arrived on) and route\_name (the logical name from the TopicMapping).
6. **Publishing to Channel**: The transformed message is sent to the internal output channel.
7. **Pipeline Consumption**: A messagepipeline service reads from the MqttConsumer.Messages() channel to process the data, and can now use the route\_name for intelligent routing.

## **Usage Example**

Here is a conceptual example of how to use the MqttConsumer to subscribe to two different logical routes.

package main

import (  
"context"  
"fmt"  
"os"  
"time"

	"\[github.com/illmade-knight/go-dataflow/pkg/messagepipeline\](https://github.com/illmade-knight/go-dataflow/pkg/messagepipeline)"  
	"\[github.com/illmade-knight/go-dataflow/pkg/mqttconverter\](https://github.com/illmade-knight/go-dataflow/pkg/mqttconverter)"  
	"\[github.com/rs/zerolog\](https://github.com/rs/zerolog)"  
)

func main() {  
logger := zerolog.New(os.Stdout)  
ctx, cancel := context.WithTimeout(context.Background(), 2\*time.Minute)  
defer cancel()

	// 1\. Load MQTT client configuration from environment variables  
	mqttCfg := mqttconverter.LoadMQTTClientConfigFromEnv()  
	mqttCfg.BrokerURL \= "tcp://localhost:1883" // Example broker  
	mqttCfg.Username \= "user"  
	mqttCfg.Password \= "password"

	// 2\. Define the topic-to-route mappings  
	mqttCfg.TopicMappings \= \[\]mqttconverter.TopicMapping{  
		{  
			Name:  "uplink-data",  
			Topic: "devices/+/data",  
			QoS:   1,  
		},  
		{  
			Name:  "device-status",  
			Topic: "devices/+/status",  
			QoS:   1,  
		},  
	}

	// 3\. Create a new MqttConsumer  
	consumer, err := mqttconverter.NewMqttConsumer(mqttCfg, logger, 100\)  
	if err \!= nil {  
		logger.Fatal().Err(err).Msg("Failed to create consumer")  
	}

	// 4\. Start the consumer (non-blocking)  
	if err := consumer.Start(ctx); err \!= nil {  
		logger.Fatal().Err(err).Msg("Failed to start consumer")  
	}  
	defer consumer.Stop(ctx)

	logger.Info().Msg("Consumer started. Waiting for messages...")

	// 5\. Create a simple processor that uses the route\_name  
	// In a real application, this would be part of a larger service.  
	messageChannel := consumer.Messages()  
	for msg := range messageChannel {  
		routeName := msg.Attributes\["route\_name"\]  
		topic := msg.Attributes\["mqtt\_topic"\]

		fmt.Printf("Received message for route '%s' from topic '%s': %s\\n",  
			routeName, topic, string(msg.Payload))

		// Here you could add logic to route the message based on \`routeName\`  
		// switch routeName {  
		// case "uplink-data":  
		// 	// send to data producer  
		// case "device-status":  
		// 	// send to status producer  
		// }

		msg.Ack() // In this consumer, Ack/Nack are no-ops but good practice.  
	}

	logger.Info().Msg("Consumer channel closed. Shutting down.")  
}

## **Configuration via Environment Variables**

The LoadMQTTClientConfigFromEnv function supports the following environment variables for client settings:

* MQTT\_KEEP\_ALIVE\_SECONDS: (integer) Interval for sending keep-alive pings. Default: 60\.
* MQTT\_CONNECT\_TIMEOUT\_SECONDS: (integer) Timeout for the initial connection attempt. Default: 10\.
* MQTT\_INSECURE\_SKIP\_VERIFY: (true/false) **Not for production.** Skips TLS certificate verification. Default: false.

Other critical settings like BrokerURL, Username, Password, and the TopicMappings slice must be set directly in the MQTTClientConfig struct.