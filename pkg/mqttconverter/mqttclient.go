package mqttconverter

import (
	"os"
	"time"

	"github.com/rs/zerolog/log"
)

// MQTTClientConfig holds all necessary configuration for the Paho MQTT client.
// It defines connection parameters, security settings, and the topic subscriptions for the consumer.
type MQTTClientConfig struct {
	// BrokerURL is the full URL of the MQTT broker to connect to.
	// Example: "tls://mqtt.example.com:8883"
	BrokerURL string
	// TopicMappings defines all the topic-to-route mappings for this consumer.
	// This allows a single consumer to subscribe to multiple topics and tag messages
	// with a logical route name for downstream processing.
	TopicMappings []TopicMapping
	// ClientIDPrefix is a prefix for the MQTT client ID. A unique suffix is
	// automatically added to ensure client uniqueness, which is required by most brokers.
	ClientIDPrefix string
	// AllowPublicBroker is a security flag that, when set to true, permits the client
	// to connect to a broker without providing a username and password. This should
	// only be enabled for trusted, public data sources. Defaults to false.
	AllowPublicBroker bool
	// Username for authenticating with the MQTT broker.
	Username string
	// Password for authenticating with the MQTT broker.
	Password string
	// KeepAlive is the interval at which the client sends keep-alive pings to the broker.
	KeepAlive time.Duration
	// ConnectTimeout is the timeout for the initial connection attempt.
	ConnectTimeout time.Duration
	// ReconnectWaitMin is the minimum time to wait before attempting to reconnect.
	ReconnectWaitMin time.Duration
	// ReconnectWaitMax is the maximum time to wait before attempting to reconnect.
	ReconnectWaitMax time.Duration
	// CACertFile is an optional path to a CA certificate file for verifying the broker's certificate.
	CACertFile string
	// ClientCertFile is an optional path to a client certificate file for mTLS authentication.
	ClientCertFile string
	// ClientKeyFile is an optional path to a client key file for mTLS authentication.
	ClientKeyFile string
	// InsecureSkipVerify skips TLS certificate verification.
	// This is NOT recommended for production environments.
	InsecureSkipVerify bool
}

// Env constants for setting Mqtt settings
const (
	MqttSkipVerify            = "MQTT_INSECURE_SKIP_VERIFY"
	MqttKeepAliveSeconds      = "MQTT_KEEP_ALIVE_SECONDS"
	MqttConnectTimeoutSeconds = "MQTT_CONNECT_TIMEOUT_SECONDS"
)

// LoadMQTTClientConfigWithEnv loads MQTT operational configuration from environment variables.
// It populates settings like timeouts and keep-alive intervals with sensible defaults if
// the environment variables are not set.
// Note: TopicMappings are not loaded from the environment and must be configured programmatically.
func LoadMQTTClientConfigWithEnv() *MQTTClientConfig {

	cfg := &MQTTClientConfig{
		KeepAlive:        60 * time.Second,  // Default
		ConnectTimeout:   10 * time.Second,  // Default
		ReconnectWaitMin: 1 * time.Second,   // Default
		ReconnectWaitMax: 120 * time.Second, // Default
		ClientIDPrefix:   "ingestion-service-",
	}
	if skipVerify := os.Getenv(MqttSkipVerify); skipVerify == "true" {
		cfg.InsecureSkipVerify = true
	}

	// Parse durations if set in env, otherwise use defaults
	if ka := os.Getenv(MqttKeepAliveSeconds); ka != "" {
		s, err := time.ParseDuration(ka + "s")
		if err == nil {
			cfg.KeepAlive = s
		} else {
			log.Printf("mqttconverter: error parsing keepAlive seconds: %s, using default", err)
		}
	}
	if ct := os.Getenv(MqttConnectTimeoutSeconds); ct != "" {
		s, err := time.ParseDuration(ct + "s")
		if err == nil {
			cfg.ConnectTimeout = s
		} else {
			log.Printf("mqttconverter: error parsing connect timeout seconds: %s, using default", err)
		}
	}

	return cfg
}
