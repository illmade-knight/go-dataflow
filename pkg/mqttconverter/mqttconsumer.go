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
type MqttConsumer struct {
	pahoClient mqtt.Client
	logger     zerolog.Logger
	outputChan chan messagepipeline.Message
	doneChan   chan struct{}
	mqttCfg    *MQTTClientConfig
	stopOnce   sync.Once
	mu         sync.RWMutex
	isStopped  bool
}

// ToPipelineMessage transforms a Paho MQTT message into the canonical messagepipeline.Message.
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
		Ack:        func() {},
		Nack:       func() {},
	}
}

// NewMqttConsumer creates a new MqttConsumer.
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

// EDITED: The Start method is now simpler and more robust.
// It relies on the Paho client's auto-reconnect and OnConnect handler for all logic.
func (c *MqttConsumer) Start(ctx context.Context) error {
	opts := c.createMqttOptions(ctx)
	c.pahoClient = mqtt.NewClient(opts)

	c.logger.Info().Msg("Starting MQTT client. The Paho client will connect in the background.")
	c.pahoClient.Connect() // This is non-blocking.

	// This goroutine ensures that if the parent context is canceled,
	// the consumer's Stop method is called for a clean shutdown.
	go func() {
		<-ctx.Done()
		c.logger.Info().Msg("Shutdown signal received, ensuring consumer is stopped.")
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.Stop(stopCtx)
	}()

	return nil
}

// Stop gracefully unsubscribes from the MQTT topic and disconnects the client.
func (c *MqttConsumer) Stop(ctx context.Context) error {
	c.stopOnce.Do(func() {
		c.mu.Lock()
		c.isStopped = true
		c.mu.Unlock()

		c.logger.Info().Msg("Stopping MqttConsumer...")
		if c.pahoClient != nil && c.pahoClient.IsConnected() {
			c.logger.Info().Str("topic", c.mqttCfg.Topic).Msg("Unsubscribing from MQTT topic.")
			token := c.pahoClient.Unsubscribe(c.mqttCfg.Topic)

			if !token.WaitTimeout(getTimeoutFromContext(ctx, 5*time.Second)) {
				c.logger.Warn().Msg("Timed out waiting for MQTT unsubscribe confirmation.")
			} else if err := token.Error(); err != nil {
				c.logger.Warn().Err(err).Msg("Failed to unsubscribe from MQTT topic.")
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

// IsConnected returns the connection status of the underlying Paho client.
func (c *MqttConsumer) IsConnected() bool {
	return c.pahoClient != nil && c.pahoClient.IsConnected()
}

// handleIncomingMessage is the Paho callback that converts and forwards messages.
func (c *MqttConsumer) handleIncomingMessage(client mqtt.Client, msg mqtt.Message) {
	c.logger.Debug().Str("topic", msg.Topic()).Int("payload_size", len(msg.Payload())).Msg("Received MQTT message")

	c.mu.RLock()
	if c.isStopped {
		c.mu.RUnlock()
		c.logger.Warn().Str("topic", msg.Topic()).Msg("Consumer is stopped, dropping MQTT message.")
		return
	}
	c.mu.RUnlock()

	consumedMsg := ToPipelineMessage(msg)

	select {
	case c.outputChan <- consumedMsg:
		// Message successfully sent to the pipeline
	default:
		c.logger.Warn().Int("channel_capacity", cap(c.outputChan)).Msg("Output channel is full. Dropping MQTT message.")
	}
}

// EDITED: The OnConnect handler now contains all subscription logic.
// The message handler is passed directly to the Subscribe call for clarity.
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
	opts.SetOrderMatters(false)

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		c.logger.Info().Str("broker", c.mqttCfg.BrokerURL).Msg("Paho client connected to MQTT broker. Attempting to subscribe.")
		// The message handler is now passed directly to the Subscribe function.
		token := client.Subscribe(c.mqttCfg.Topic, 1, c.handleIncomingMessage)

		// This goroutine waits for the subscription to be acknowledged.
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
		c.logger.Error().Err(err).Msg("Paho client lost MQTT connection. Will attempt to reconnect.")
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

func getTimeoutFromContext(ctx context.Context, defaultTimeout time.Duration) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		if timeout > 0 {
			return timeout
		}
		return 0
	}
	return defaultTimeout
}
