// mqttconverter/mqttconsumer.go
package mqttconverter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
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

// NewMqttConsumer creates a new MqttConsumer. It does not connect until Start is called.
func NewMqttConsumer(cfg *MQTTClientConfig, logger zerolog.Logger) (*MqttConsumer, error) {
	if cfg.BrokerURL == "" {
		return nil, fmt.Errorf("MQTT broker URL is required")
	}
	return &MqttConsumer{
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

// Start initializes and connects the MQTT client and begins consuming messages.
func (c *MqttConsumer) Start(ctx context.Context) error {
	subscribed := make(chan error, 1)

	opts := c.createMqttOptions(subscribed)
	opts.SetDefaultPublishHandler(c.handleIncomingMessage(ctx))
	c.pahoClient = mqtt.NewClient(opts)

	c.logger.Info().Str("client_id", opts.ClientID).Msg("Paho MQTT client created. Attempting to connect...")

	if token := c.pahoClient.Connect(); token.WaitTimeout(c.mqttCfg.ConnectTimeout) && token.Error() != nil {
		return fmt.Errorf("paho MQTT client connect error: %w", token.Error())
	}

	select {
	case err := <-subscribed:
		if err != nil {
			return fmt.Errorf("MQTT subscription failed: %w", err)
		}
		c.logger.Info().Msg("MQTT consumer started and subscribed successfully.")
	case <-time.After(c.mqttCfg.ConnectTimeout):
		return fmt.Errorf("timed out waiting for MQTT subscription")
	}

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

// GetMessageHandlerForTest returns the internal message handler for unit testing.
func (c *MqttConsumer) GetMessageHandlerForTest(ctx context.Context) mqtt.MessageHandler {
	return c.handleIncomingMessage(ctx)
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

// createMqttOptions assembles the Paho client options from the config.
func (c *MqttConsumer) createMqttOptions(subChan chan error) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(c.mqttCfg.BrokerURL)
	uniqueSuffix := time.Now().UnixNano() % 1000000
	opts.SetClientID(fmt.Sprintf("%s%d", c.mqttCfg.ClientIDPrefix, uniqueSuffix))
	opts.SetUsername(c.mqttCfg.Username)
	opts.SetPassword(c.mqttCfg.Password)
	opts.SetKeepAlive(c.mqttCfg.KeepAlive)
	opts.SetConnectTimeout(c.mqttCfg.ConnectTimeout)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(c.mqttCfg.ReconnectWaitMax)
	opts.SetOrderMatters(false)

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		c.logger.Info().Str("broker", c.mqttCfg.BrokerURL).Msg("Paho client connected to MQTT broker")
		token := client.Subscribe(c.mqttCfg.Topic, 1, nil) // Nil handler uses the default handler
		if token.WaitTimeout(5*time.Second) && token.Error() != nil {
			c.logger.Error().Err(token.Error()).Str("topic", c.mqttCfg.Topic).Msg("Failed to subscribe to MQTT topic")
			subChan <- token.Error()
		} else {
			c.logger.Info().Str("topic", c.mqttCfg.Topic).Msg("Successfully subscribed to MQTT topic")
			subChan <- nil
		}
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		c.logger.Error().Err(err).Msg("Paho client lost MQTT connection.")
	})

	if strings.HasPrefix(strings.ToLower(c.mqttCfg.BrokerURL), "tls://") {
		tlsConfig, err := newTLSConfig(c.mqttCfg)
		if err != nil {
			c.logger.Error().Err(err).Msg("Failed to create TLS config, proceeding without it.")
		} else {
			opts.SetTLSConfig(tlsConfig)
			c.logger.Info().Msg("TLS configured for MQTT client.")
		}
	}
	return opts
}

// newTLSConfig is a helper to create a tls.Config.
func newTLSConfig(cfg *MQTTClientConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify}
	if cfg.CACertFile != "" {
		caCert, err := os.ReadFile(cfg.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert file %s: %w", cfg.CACertFile, err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA cert from %s", cfg.CACertFile)
		}
		tlsConfig.RootCAs = caCertPool
	}
	if cfg.ClientCertFile != "" && cfg.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate/key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, nil
}
