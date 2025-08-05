MQTT Converter Package (mqttconverter)
The mqttconverter package provides the necessary components to consume messages from an MQTT broker and integrate them into a messagepipeline. It acts as a bridge, converting raw MQTT messages into a standardized format that can be processed by downstream services.

This package is designed to be a robust, configurable, and easy-to-use component for building data ingestion pipelines that source data from MQTT-enabled devices or systems.

Core Components
MqttConsumer
The central component of the package. MqttConsumer implements the messagepipeline.MessageConsumer interface.

Responsibilities:

Establishes and maintains a connection to an MQTT broker.

Automatically handles reconnects with exponential backoff in case of connection loss.

Subscribes to a specified MQTT topic (wildcards are supported).

Receives messages from the broker and converts them into the standard messagepipeline.Message format.

Pushes the converted messages into an output channel for consumption by a pipeline.

Lifecycle:

NewMqttConsumer(): Creates and configures a new consumer.

Start(): Initiates the connection process in the background. It's a non-blocking call.

Stop(): Gracefully unsubscribes from the topic and disconnects from the broker.

Messages(): Returns a read-only channel for accessing the consumed messages.

MQTTClientConfig
A struct that holds all the necessary configuration for the MQTT client.

Key Fields:

BrokerURL: The address of the MQTT broker (e.g., tls://mqtt.example.com:8883).

Topic: The MQTT topic to subscribe to (e.g., devices/+/data).

ClientIDPrefix: A prefix for the MQTT client ID. A unique suffix is added automatically to ensure client uniqueness.

Username & Password: Credentials for connecting to the broker.

TLS options (CACertFile, ClientCertFile, ClientKeyFile, InsecureSkipVerify) for secure connections.

Configuration:

The LoadMQTTClientConfigFromEnv() function provides a convenient way to initialize the configuration from environment variables, with sensible defaults for timeouts and keep-alive intervals.

Transformers & Types
ToPipelineMessage: A pure function that transforms a raw paho.mqtt.golang.Message into a messagepipeline.Message. It copies the payload and adds the MQTT topic to the message attributes.

ToRawMessageTransformer: A messagepipeline.MessageTransformer that converts a messagepipeline.Message into a RawMessage, which is a canonical structure intended for further processing or publishing.

RawMessage: A simple, canonical struct (Topic, Payload, Timestamp) representing the essential data from an MQTT message.

How It Works
Initialization: An MqttConsumer is created with an MQTTClientConfig. The configuration specifies the broker details, topic, and credentials.

Start: The Start() method is called. The underlying Paho MQTT client begins the connection process in the background. It will automatically attempt to reconnect if the connection fails or is lost.

Connection & Subscription: Once connected, the OnConnectHandler is triggered, which automatically subscribes to the configured topic.

Message Handling: When a message arrives on the subscribed topic, the handleIncomingMessage callback is invoked.

Transformation: The message is transformed from its raw Paho format into a messagepipeline.Message using ToPipelineMessage.

Publishing to Channel: The transformed message is sent to the internal outputChan.

Pipeline Consumption: A messagepipeline service (like StreamingService or EnrichmentService) reads from the MqttConsumer.Messages() channel to process the data.

Usage Example
Here is a conceptual example of how to use the MqttConsumer within a data pipeline.

package main

import (
"context"
"fmt"
"os"
"time"

    "[github.com/illmade-knight/go-dataflow/pkg/messagepipeline](https://github.com/illmade-knight/go-dataflow/pkg/messagepipeline)"
    "[github.com/illmade-knight/go-dataflow/pkg/mqttconverter](https://github.com/illmade-knight/go-dataflow/pkg/mqttconverter)"
    "[github.com/rs/zerolog](https://github.com/rs/zerolog)"
)

func main() {
logger := zerolog.New(os.Stdout)
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
defer cancel()

    // 1. Load MQTT configuration from environment variables
    mqttCfg := mqttconverter.LoadMQTTClientConfigFromEnv()
    mqttCfg.BrokerURL = "tcp://localhost:1883" // Example broker
    mqttCfg.Topic = "telemetry/+/events"
    mqttCfg.Username = "user"
    mqttCfg.Password = "password"


    // 2. Create a new MqttConsumer
    consumer, err := mqttconverter.NewMqttConsumer(mqttCfg, logger, 100)
    if err != nil {
        logger.Fatal().Err(err).Msg("Failed to create consumer")
    }

    // 3. Start the consumer (non-blocking)
    if err := consumer.Start(ctx); err != nil {
        logger.Fatal().Err(err).Msg("Failed to start consumer")
    }
    defer consumer.Stop(ctx)

    logger.Info().Msg("Consumer started. Waiting for messages...")

    // 4. Create a simple processor to handle the messages
    // In a real application, this would be part of a larger service.
    messageChannel := consumer.Messages()
    for msg := range messageChannel {
        fmt.Printf("Received message from topic '%s': %s\n", msg.Attributes["mqtt_topic"], string(msg.Payload))
        msg.Ack() // In this consumer, Ack/Nack are no-ops but good practice.
    }

    logger.Info().Msg("Consumer channel closed. Shutting down.")
}

Configuration via Environment Variables
The LoadMQTTClientConfigFromEnv function supports the following environment variables:

MQTT_KEEP_ALIVE_SECONDS: (integer) Interval for sending keep-alive pings. Default: 60.

MQTT_CONNECT_TIMEOUT_SECONDS: (integer) Timeout for the initial connection attempt. Default: 10.

MQTT_INSECURE_SKIP_VERIFY: (true/false) Not for production. Skips TLS certificate verification. Default: false.

Other critical settings like BrokerURL, Topic, Username, and Password should be set directly in the MQTTClientConfig struct,