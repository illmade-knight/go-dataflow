// mqttconverter/mqttconsumer.go
package mqttconverter

import (
	"context"
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
)

// MqttConsumer implements the messagepipeline.MessageConsumer interface for an MQTT source.
type MqttConsumer struct {
	pahoClient mqtt.Client
	logger     zerolog.Logger
	outputChan chan types.ConsumedMessage
	doneChan   chan struct{}
	mqttCfg    *MQTTClientConfig
	stopOnce   sync.Once
}

// NewMqttConsumer creates a new MqttConsumer with a provided MQTT client.
func NewMqttConsumer(client mqtt.Client, cfg *MQTTClientConfig, logger zerolog.Logger) (*MqttConsumer, error) {
	if client == nil {
		return nil, fmt.Errorf("MQTT client cannot be nil")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("MQTT topic is required")
	}

	return &MqttConsumer{
		pahoClient: client,
		logger:     logger.With().Str("component", "MqttConsumer").Logger(),
		outputChan: make(chan types.ConsumedMessage, 1000),
		doneChan:   make(chan struct{}),
		mqttCfg:    cfg,
	}, nil
}

// Messages returns the read-only channel from which raw messages can be consumed.
func (c *MqttConsumer) Messages() <-chan types.ConsumedMessage {
	return c.outputChan
}

// Start connects the provided MQTT client and subscribes to the configured topic.
// It blocks until a connection is established and the topic is subscribed.
func (c *MqttConsumer) Start(ctx context.Context) error {
	if c.pahoClient.IsConnected() {
		c.logger.Warn().Msg("Start called on an already connected MqttConsumer.")
		return nil
	}

	if token := c.pahoClient.Connect(); token.WaitTimeout(c.mqttCfg.ConnectTimeout) && token.Error() != nil {
		return fmt.Errorf("paho MQTT client connect error: %w", token.Error())
	}

	token := c.pahoClient.Subscribe(c.mqttCfg.Topic, 1, c.handleIncomingMessage(ctx))
	if token.WaitTimeout(5*time.Second) && token.Error() != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", c.mqttCfg.Topic, token.Error())
	}

	c.logger.Info().Str("topic", c.mqttCfg.Topic).Msg("MQTT consumer started and subscribed successfully.")

	// This goroutine handles graceful shutdown if the parent context is cancelled.
	go func() {
		<-ctx.Done()
		c.logger.Info().Msg("Shutdown signal received, disconnecting MQTT client.")
		_ = c.Stop(context.Background())
	}()

	return nil
}

// Stop gracefully ceases message consumption.
func (c *MqttConsumer) Stop(ctx context.Context) error {
	c.stopOnce.Do(func() {
		c.logger.Info().Msg("Stopping MqttConsumer...")
		if c.pahoClient != nil && c.pahoClient.IsConnected() {
			if token := c.pahoClient.Unsubscribe(c.mqttCfg.Topic); token.WaitTimeout(2*time.Second) && token.Error() != nil {
				c.logger.Warn().Err(token.Error()).Str("topic", c.mqttCfg.Topic).Msg("Failed to unsubscribe from MQTT topic.")
			}
			c.pahoClient.Disconnect(500)
			c.logger.Info().Msg("Paho MQTT client disconnected.")
		}
		close(c.outputChan)
		close(c.doneChan)
		c.logger.Info().Msg("MqttConsumer stopped.")
	})
	return nil
}

// Done returns a channel that is closed when the consumer has fully stopped.
func (c *MqttConsumer) Done() <-chan struct{} {
	return c.doneChan
}

// handleIncomingMessage is the callback that converts MQTT messages to our standard format.
func (c *MqttConsumer) handleIncomingMessage(ctx context.Context) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		c.logger.Debug().Str("topic", msg.Topic()).Msg("Received MQTT message")
		payloadCopy := make([]byte, len(msg.Payload()))
		copy(payloadCopy, msg.Payload())
		consumedMsg := types.ConsumedMessage{
			PublishMessage: types.PublishMessage{
				ID:          fmt.Sprintf("%d", msg.MessageID()),
				Payload:     payloadCopy,
				PublishTime: time.Now().UTC(),
			},
			Attributes: map[string]string{"mqtt_topic": msg.Topic()},
			Ack:        func() {},
			Nack:       func() {},
		}
		select {
		case c.outputChan <- consumedMsg:
		case <-ctx.Done():
			c.logger.Warn().Str("topic", msg.Topic()).Msg("Consumer is shutting down, dropping MQTT message.")
		}
	}
}
