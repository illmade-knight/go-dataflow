// mqttconverter/mqttclient_test.go
package mqttconverter_test

import (
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/mqttconverter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadMQTTClientConfigFromEnv(t *testing.T) {
	t.Run("Default values are set correctly", func(t *testing.T) {
		cfg := mqttconverter.LoadMQTTClientConfigFromEnv()
		require.NotNil(t, cfg)
		assert.Equal(t, 60*time.Second, cfg.KeepAlive)
		assert.Equal(t, 10*time.Second, cfg.ConnectTimeout)
		assert.Equal(t, "ingestion-service-", cfg.ClientIDPrefix)
	})

	t.Run("Values are loaded from environment", func(t *testing.T) {
		t.Setenv("MQTT_KEEP_ALIVE_SECONDS", "30")
		t.Setenv("MQTT_CONNECT_TIMEOUT_SECONDS", "5")
		t.Setenv("MQTT_INSECURE_SKIP_VERIFY", "true")

		cfg := mqttconverter.LoadMQTTClientConfigFromEnv()
		require.NotNil(t, cfg)

		assert.Equal(t, 30*time.Second, cfg.KeepAlive)
		assert.Equal(t, 5*time.Second, cfg.ConnectTimeout)
		assert.True(t, cfg.InsecureSkipVerify)
	})

	t.Run("Invalid duration values fall back to defaults", func(t *testing.T) {
		t.Setenv("MQTT_KEEP_ALIVE_SECONDS", "not-a-number")
		t.Setenv("MQTT_CONNECT_TIMEOUT_SECONDS", "invalid")

		cfg := mqttconverter.LoadMQTTClientConfigFromEnv()
		require.NotNil(t, cfg)
		assert.Equal(t, 60*time.Second, cfg.KeepAlive, "KeepAlive should default if env var is invalid")
		assert.Equal(t, 10*time.Second, cfg.ConnectTimeout, "ConnectTimeout should default if env var is invalid")
	})
}
