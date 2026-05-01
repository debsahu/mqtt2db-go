package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/debsahu/mqtt2db-go/internal/config"
)

// minimalYAML is the smallest doc that satisfies Validate when overlaid on
// NewDefault. Everything else relies on built-in defaults.
const minimalYAML = `
mqtt:
  brokers:
    - tcp://comqtt:1883
  shared_subscription: $share/ingest/t/+/d/+/evt/#
  username: u
  password: p
postgres:
  dsn: postgres://ingest:secret@pgbouncer:6432/telemetry
dead_letter:
  s3:
    bucket: mqtt2db-go-dlq
`

func writeYAML(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

func TestNewDefault_ProducesSensibleDefaults(t *testing.T) {
	d := config.NewDefault()
	assert.Equal(t, "mqtt2db-go", d.MQTT.ClientIDPrefix)
	assert.Equal(t, 5, d.MQTT.ProtocolVersion)
	assert.False(t, d.MQTT.CleanSession, "clean_session must default to false to honor at-least-once")
	assert.Equal(t, 100_000, d.Buffer.MaxMessages)
	assert.InDelta(t, 0.8, d.Buffer.SpillThreshold, 0.001)
	assert.InDelta(t, 0.95, d.Buffer.PauseThreshold, 0.001)
	assert.Equal(t, "/var/lib/mqtt2db-go/wal", d.Badger.Path)
	assert.Equal(t, ":9090", d.Metrics.Listen)
	assert.Equal(t, "info", d.Logging.Level)
}

func TestLoad_MinimalYAMLPasses(t *testing.T) {
	p := writeYAML(t, minimalYAML)
	cfg, err := config.Load(p)
	require.NoError(t, err)
	assert.Equal(t, []string{"tcp://comqtt:1883"}, cfg.MQTT.Brokers)
	assert.Equal(t, "u", cfg.MQTT.Username)
	assert.Equal(t, time.Hour, cfg.Postgres.ConnMaxLifetime.AsDuration(),
		"defaults should survive YAML overlay when not overridden")
}

func TestLoad_RejectsUnknownYAMLKey(t *testing.T) {
	p := writeYAML(t, minimalYAML+"\nfooBar: 1\n")
	_, err := config.Load(p)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fooBar")
}

func TestLoad_DurationParsedFromString(t *testing.T) {
	p := writeYAML(t, `
mqtt:
  brokers:
    - tcp://comqtt:1883
  shared_subscription: $share/ingest/t/+/d/+/evt/#
  username: u
  password: p
  keepalive: 7s
postgres:
  dsn: postgres://ingest:secret@pgbouncer:6432/telemetry
dead_letter:
  s3:
    bucket: mqtt2db-go-dlq
`)
	cfg, err := config.Load(p)
	require.NoError(t, err)
	assert.Equal(t, 7*time.Second, cfg.MQTT.Keepalive.AsDuration())
}

func TestLoad_EnvOverridesYAML(t *testing.T) {
	t.Setenv("MQTT2DB_MQTT_USERNAME", "from-env")
	t.Setenv("MQTT2DB_MQTT_BROKERS", "tcp://override:1883,tcp://override-2:1883")
	t.Setenv("MQTT2DB_LOGGING_LEVEL", "debug")
	t.Setenv("MQTT2DB_DEAD_LETTER_S3_ACCESS_KEY", "akid")
	t.Setenv("MQTT2DB_DEAD_LETTER_S3_SECRET_KEY", "secret")

	p := writeYAML(t, minimalYAML)
	cfg, err := config.Load(p)
	require.NoError(t, err)

	assert.Equal(t, "from-env", cfg.MQTT.Username)
	assert.Equal(t, []string{"tcp://override:1883", "tcp://override-2:1883"}, cfg.MQTT.Brokers)
	assert.Equal(t, "debug", cfg.Logging.Level)
	assert.Equal(t, "akid", cfg.DeadLetter.S3.AccessKey)
	assert.Equal(t, "secret", cfg.DeadLetter.S3.SecretKey)
}

func TestLoad_MissingFileFails(t *testing.T) {
	_, err := config.Load("/tmp/does-not-exist-mqtt2db-go.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read config")
}

func TestLoad_EmptyPathUsesDefaultsAndEnv(t *testing.T) {
	// Defaults alone are not a valid config (no brokers, no DSN, no bucket).
	_, err := config.Load("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid config")

	t.Setenv("MQTT2DB_MQTT_BROKERS", "tcp://b:1883")
	t.Setenv("MQTT2DB_MQTT_SHARED_SUBSCRIPTION", "$share/ingest/#")
	t.Setenv("MQTT2DB_POSTGRES_DSN", "postgres://x")
	t.Setenv("MQTT2DB_DEAD_LETTER_S3_BUCKET", "b")
	cfg, err := config.Load("")
	require.NoError(t, err)
	assert.Equal(t, []string{"tcp://b:1883"}, cfg.MQTT.Brokers)
}

func TestValidate_RequiredFields(t *testing.T) {
	cfg := config.NewDefault()
	err := cfg.Validate()
	require.Error(t, err)
	msg := err.Error()
	for _, want := range []string{
		"mqtt.brokers",
		"mqtt.shared_subscription",
		"postgres.dsn",
		"dead_letter.s3.bucket",
	} {
		assert.True(t, strings.Contains(msg, want), "expected error to mention %q, got: %s", want, msg)
	}
}

func TestValidate_ThresholdRanges(t *testing.T) {
	cfg := mustValidConfig(t)
	cfg.Buffer.SpillThreshold = 0.95
	cfg.Buffer.PauseThreshold = 0.8
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "spill_threshold")
	assert.Contains(t, err.Error(), "pause_threshold")
}

func TestValidate_FlusherLatencyOrdering(t *testing.T) {
	cfg := mustValidConfig(t)
	cfg.Flusher.ElevatedLatencyThreshold = config.Duration(3 * time.Second)
	cfg.Flusher.CriticalLatencyThreshold = config.Duration(2 * time.Second)
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "elevated_latency_threshold")
}

func TestValidate_LogLevelAndFormat(t *testing.T) {
	cfg := mustValidConfig(t)
	cfg.Logging.Level = "loud"
	cfg.Logging.Format = "yaml"
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "logging.level")
	assert.Contains(t, err.Error(), "logging.format")
}

func TestValidate_SharedSubscriptionPrefix(t *testing.T) {
	cfg := mustValidConfig(t)
	cfg.MQTT.SharedSubscription = "ingest/t/+/d/+/evt/#"
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "$share/")
}

func mustValidConfig(t *testing.T) config.Config {
	t.Helper()
	p := writeYAML(t, minimalYAML)
	cfg, err := config.Load(p)
	require.NoError(t, err)
	return cfg
}
