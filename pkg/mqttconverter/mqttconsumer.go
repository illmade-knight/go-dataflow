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
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
)

// MqttConsumer implements the messagepipeline.MessageConsumer interface for an MQTT source.
// It connects to an MQTT broker, subscribes to a topic, and converts incoming
// MQTT messages into the standard messagepipeline.Message format.
type MqttConsumer struct {
	pahoClient mqtt.Client
	logger     zerolog.Logger
	outputChan chan messagepipeline.Message
	doneChan   chan struct{}
	mqttCfg    *MQTTClientConfig
	stopOnce   sync.Once
	isStopped  bool
	mu         sync.Mutex
}

// ToPipelineMessage transforms a Paho MQTT message into the canonical messagepipeline.Message.
// This function is exported to allow for direct unit testing of the transformation logic.
func ToPipelineMessage(msg mqtt.Message) messagepipeline.Message {
	payloadCopy := make([]byte, len(msg.Payload()))
	copy(payloadCopy, msg.Payload())

	return messagepipeline.Message{
		MessageData: messagepipeline.MessageData{
			ID:          fmt.Sprintf("%d", msg.MessageID()),
			Payload:     payloadCopy,
			PublishTime: time.Now().UTC(),
		},
		Attributes: map[string]string{"mqtt_topic": msg.Topic()},
		// For MQTT with QoS > 0, the ack is handled at the protocol level by the Paho client.
		// We provide empty functions to satisfy the interface, as no further action is needed
		// from the pipeline to acknowledge the message with the broker.
		Ack:  func() {},
		Nack: func() {},
	}
}

// NewMqttConsumer creates a new MqttConsumer. It performs initial configuration but
// does not connect to the broker until Start is called.
func NewMqttConsumer(cfg *MQTTClientConfig, logger zerolog.Logger, bufferSize int) (*MqttConsumer, error) {
	if cfg.BrokerURL == "" {
		return nil, fmt.Errorf("MQTT broker URL is required")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("MQTT topic is required")
	}

	return &MqttConsumer{
		logger:     logger.With().Str("component", "MqttConsumer").Logger(),
		outputChan: make(chan messagepipeline.Message, bufferSize),
		doneChan:   make(chan struct{}),
		mqttCfg:    cfg,
	}, nil
}

// Messages returns the read-only channel from which pipeline workers can receive messages.
func (c *MqttConsumer) Messages() <-chan messagepipeline.Message {
	return c.outputChan
}

// Start configures and connects the underlying Paho MQTT client and begins consuming
// messages. If the initial connection fails, it will continue to retry in the background.
// The provided context's cancellation will trigger a graceful shutdown of the consumer.
func (c *MqttConsumer) Start(ctx context.Context) error {
	opts := c.createMqttOptions(ctx)
	c.pahoClient = mqtt.NewClient(opts)

	c.logger.Info().Msg("Attempting to connect to MQTT broker...")
	if token := c.pahoClient.Connect(); token.WaitTimeout(c.mqttCfg.ConnectTimeout) && token.Error() != nil {
		c.logger.Warn().Err(token.Error()).Msg("Failed to connect to MQTT broker on startup. The Paho client will continue to retry in the background.")
	} else if token.Error() == nil {
		c.logger.Info().Msg("Initial connection to MQTT broker successful.")
	}

	// This goroutine acts as a failsafe to ensure that if the parent context
	// is cancelled, the consumer's Stop method is called.
	go func() {
		<-ctx.Done()
		c.logger.Info().Msg("Shutdown signal received, ensuring consumer is stopped.")
		// Use a new context with a timeout for the stop procedure.
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.Stop(stopCtx)
	}()

	return nil
}

// Stop gracefully unsubscribes from the MQTT topic and disconnects the client.
// It respects the provided context for timeout/cancellation during the unsubscribe operation.
func (c *MqttConsumer) Stop(ctx context.Context) error {
	c.stopOnce.Do(func() {
		c.logger.Info().Msg("Stopping MqttConsumer...")
		if c.pahoClient != nil && c.pahoClient.IsConnected() {
			c.logger.Info().Str("topic", c.mqttCfg.Topic).Msg("Unsubscribing from MQTT topic.")
			token := c.pahoClient.Unsubscribe(c.mqttCfg.Topic)

			// Wait for unsubscribe to complete, respecting the context's deadline.
			if !token.WaitTimeout(getTimeoutFromContext(ctx, 5*time.Second)) {
				c.logger.Warn().Msg("Timed out waiting for MQTT unsubscribe confirmation.")
			} else if err := token.Error(); err != nil {
				c.logger.Warn().Err(err).Msg("Failed to unsubscribe from MQTT topic.")
			}

			// Disconnect with a fixed grace period for in-flight messages.
			c.pahoClient.Disconnect(500) // 500ms grace period
			c.logger.Info().Msg("Paho MQTT client disconnected.")
		}
		c.mu.Lock()
		c.isStopped = true
		c.mu.Unlock()

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

// IsConnected returns the connection status of the underlying Paho client.
func (c *MqttConsumer) IsConnected() bool {
	return c.pahoClient != nil && c.pahoClient.IsConnected()
}

// handleIncomingMessage is the Paho callback that converts MQTT messages to our standard format.
func (c *MqttConsumer) handleIncomingMessage(ctx context.Context) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		c.logger.Debug().Str("topic", msg.Topic()).Msg("Received MQTT message")

		c.mu.Lock()
		if c.isStopped {
			c.mu.Unlock()
			c.logger.Warn().Str("topic", msg.Topic()).Msg("Consumer is stopped, dropping MQTT message.")
			return
		}
		c.mu.Unlock()

		// Use the new, testable transformation function
		consumedMsg := ToPipelineMessage(msg)

		select {
		case c.outputChan <- consumedMsg:
		case <-ctx.Done():
			c.logger.Warn().Str("topic", msg.Topic()).Msg("Consumer is shutting down, dropping MQTT message.")
		}
	}
}

// createMqttOptions assembles the Paho client options from the config.
func (c *MqttConsumer) createMqttOptions(ctx context.Context) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(c.mqttCfg.BrokerURL)
	uniqueSuffix := time.Now().UnixNano() % 1000000
	opts.SetClientID(fmt.Sprintf("%s-%d", c.mqttCfg.ClientIDPrefix, uniqueSuffix))
	opts.SetUsername(c.mqttCfg.Username)
	opts.SetPassword(c.mqttCfg.Password)
	opts.SetKeepAlive(c.mqttCfg.KeepAlive)
	opts.SetConnectTimeout(c.mqttCfg.ConnectTimeout)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(c.mqttCfg.ReconnectWaitMax)
	opts.SetOrderMatters(false) // Allow concurrent processing of messages
	opts.SetDefaultPublishHandler(c.handleIncomingMessage(ctx))

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		c.logger.Info().Str("broker", c.mqttCfg.BrokerURL).Msg("Paho client connected to MQTT broker.")
		token := client.Subscribe(c.mqttCfg.Topic, 1, nil) // Subscribe with QoS 1
		go func() {
			if !token.WaitTimeout(10 * time.Second) {
				c.logger.Error().Str("topic", c.mqttCfg.Topic).Msg("Timed out waiting for MQTT subscribe confirmation.")
			} else if err := token.Error(); err != nil {
				c.logger.Error().Err(err).Str("topic", c.mqttCfg.Topic).Msg("Failed to subscribe to MQTT topic.")
			} else {
				c.logger.Info().Str("topic", c.mqttCfg.Topic).Msg("Successfully subscribed to MQTT topic.")
			}
		}()
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		c.logger.Error().Err(err).Msg("Paho client lost MQTT connection.")
	})

	if strings.HasPrefix(strings.ToLower(c.mqttCfg.BrokerURL), "tls://") || strings.HasPrefix(strings.ToLower(c.mqttCfg.BrokerURL), "ssl://") {
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

// getTimeoutFromContext calculates the remaining time until the context's deadline.
// If the context has no deadline, it returns the default timeout.
func getTimeoutFromContext(ctx context.Context, defaultTimeout time.Duration) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		if timeout > 0 {
			return timeout
		}
		return 0 // Context already expired
	}
	return defaultTimeout
}
