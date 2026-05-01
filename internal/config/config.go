// Package config parses, validates, and exposes the runtime configuration
// for mqtt2db-go.
//
// Resolution order (later wins): built-in defaults -> YAML file -> env vars.
// Defaults are seeded by NewDefault before yaml.Unmarshal overlays the file;
// env.Parse from caarlos0/env/v10 then overlays env values declared with
// `env:"…"` tags. Validate is the gate before returning a Config — callers
// receive a Config that is structurally legal or an error.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v10"
	"gopkg.in/yaml.v3"
)

// EnvPrefix is the env-variable prefix for all overrides.
const EnvPrefix = "MQTT2DB_"

// Config is the root configuration document.
type Config struct {
	MQTT       MQTTConfig       `yaml:"mqtt"        envPrefix:"MQTT_"`
	Buffer     BufferConfig     `yaml:"buffer"      envPrefix:"BUFFER_"`
	Badger     BadgerConfig     `yaml:"badger"      envPrefix:"BADGER_"`
	Postgres   PostgresConfig   `yaml:"postgres"    envPrefix:"POSTGRES_"`
	Flusher    FlusherConfig    `yaml:"flusher"     envPrefix:"FLUSHER_"`
	DeadLetter DeadLetterConfig `yaml:"dead_letter" envPrefix:"DEAD_LETTER_"`
	Metrics    MetricsConfig    `yaml:"metrics"     envPrefix:"METRICS_"`
	Health     HealthConfig     `yaml:"health"      envPrefix:"HEALTH_"`
	Logging    LoggingConfig    `yaml:"logging"     envPrefix:"LOGGING_"`
}

// MQTTConfig configures the broker connection.
type MQTTConfig struct {
	Brokers             []string `yaml:"brokers"               env:"BROKERS"               envSeparator:","`
	ClientIDPrefix      string   `yaml:"client_id_prefix"      env:"CLIENT_ID_PREFIX"`
	SharedSubscription  string   `yaml:"shared_subscription"   env:"SHARED_SUBSCRIPTION"`
	Username            string   `yaml:"username"              env:"USERNAME"`
	Password            string   `yaml:"password"              env:"PASSWORD,unset"`
	Keepalive           Duration `yaml:"keepalive"             env:"KEEPALIVE"`
	ConnectTimeout      Duration `yaml:"connect_timeout"       env:"CONNECT_TIMEOUT"`
	MaxReconnectBackoff Duration `yaml:"max_reconnect_backoff" env:"MAX_RECONNECT_BACKOFF"`
	CleanSession        bool     `yaml:"clean_session"         env:"CLEAN_SESSION"`
	ProtocolVersion     int      `yaml:"protocol_version"      env:"PROTOCOL_VERSION"`
}

// BufferConfig configures the in-memory ring buffer.
type BufferConfig struct {
	MaxMessages    int     `yaml:"max_messages"    env:"MAX_MESSAGES"`
	MaxBytes       int64   `yaml:"max_bytes"       env:"MAX_BYTES"`
	SpillThreshold float64 `yaml:"spill_threshold" env:"SPILL_THRESHOLD"`
	PauseThreshold float64 `yaml:"pause_threshold" env:"PAUSE_THRESHOLD"`
}

// BadgerConfig configures the on-disk overflow log.
type BadgerConfig struct {
	Path             string   `yaml:"path"                env:"PATH"`
	TTL              Duration `yaml:"ttl"                 env:"TTL"`
	SyncWrites       bool     `yaml:"sync_writes"         env:"SYNC_WRITES"`
	ValueLogFileSize int64    `yaml:"value_log_file_size" env:"VALUE_LOG_FILE_SIZE"`
}

// PostgresConfig configures the destination database.
type PostgresConfig struct {
	DSN             string   `yaml:"dsn"               env:"DSN,unset"`
	MaxConns        int32    `yaml:"max_conns"         env:"MAX_CONNS"`
	MinConns        int32    `yaml:"min_conns"         env:"MIN_CONNS"`
	ConnMaxLifetime Duration `yaml:"conn_max_lifetime" env:"CONN_MAX_LIFETIME"`
	Schema          string   `yaml:"schema"            env:"SCHEMA"`
	Table           string   `yaml:"table"             env:"TABLE"`
	Columns         []string `yaml:"columns"           env:"COLUMNS"           envSeparator:","`
}

// FlusherConfig configures batching and adaptive backpressure.
type FlusherConfig struct {
	BatchSize                int      `yaml:"batch_size"                 env:"BATCH_SIZE"`
	FlushInterval            Duration `yaml:"flush_interval"             env:"FLUSH_INTERVAL"`
	ElevatedLatencyThreshold Duration `yaml:"elevated_latency_threshold" env:"ELEVATED_LATENCY_THRESHOLD"`
	ElevatedBatchSize        int      `yaml:"elevated_batch_size"        env:"ELEVATED_BATCH_SIZE"`
	ElevatedFlushInterval    Duration `yaml:"elevated_flush_interval"    env:"ELEVATED_FLUSH_INTERVAL"`
	CriticalLatencyThreshold Duration `yaml:"critical_latency_threshold" env:"CRITICAL_LATENCY_THRESHOLD"`
	RecoveryWindow           Duration `yaml:"recovery_window"            env:"RECOVERY_WINDOW"`
	MaxRetries               int      `yaml:"max_retries"                env:"MAX_RETRIES"`
}

// DeadLetterConfig configures the S3-compatible sink.
type DeadLetterConfig struct {
	S3 S3Config `yaml:"s3" envPrefix:"S3_"`
}

// S3Config holds S3-compatible client settings.
type S3Config struct {
	Bucket       string `yaml:"bucket"         env:"BUCKET"`
	Region       string `yaml:"region"         env:"REGION"`
	Endpoint     string `yaml:"endpoint"       env:"ENDPOINT"`
	UsePathStyle bool   `yaml:"use_path_style" env:"USE_PATH_STYLE"`
	UseSSL       bool   `yaml:"use_ssl"        env:"USE_SSL"`
	AccessKey    string `yaml:"access_key"     env:"ACCESS_KEY,unset"`
	SecretKey    string `yaml:"secret_key"     env:"SECRET_KEY,unset"`
}

// MetricsConfig configures the Prometheus endpoint.
type MetricsConfig struct {
	Listen string `yaml:"listen" env:"LISTEN"`
	Path   string `yaml:"path"   env:"PATH"`
}

// HealthConfig configures liveness/readiness endpoints.
type HealthConfig struct {
	Listen        string `yaml:"listen"          env:"LISTEN"`
	LivenessPath  string `yaml:"liveness_path"   env:"LIVENESS_PATH"`
	ReadinessPath string `yaml:"readiness_path"  env:"READINESS_PATH"`
}

// LoggingConfig configures slog initialisation.
type LoggingConfig struct {
	Level       string `yaml:"level"        env:"LEVEL"`
	Format      string `yaml:"format"       env:"FORMAT"`
	LogPayloads bool   `yaml:"log_payloads" env:"LOG_PAYLOADS"`
}

// NewDefault returns a Config populated with built-in defaults from
// docs/CONFIGURATION.md. Required fields without sensible defaults are
// left zero so Validate can reject them.
func NewDefault() Config {
	return Config{
		MQTT: MQTTConfig{
			ClientIDPrefix:      "mqtt2db-go",
			Keepalive:           Duration(30 * time.Second),
			ConnectTimeout:      Duration(10 * time.Second),
			MaxReconnectBackoff: Duration(30 * time.Second),
			CleanSession:        false,
			ProtocolVersion:     5,
		},
		Buffer: BufferConfig{
			MaxMessages:    100_000,
			MaxBytes:       100 * 1024 * 1024,
			SpillThreshold: 0.8,
			PauseThreshold: 0.95,
		},
		Badger: BadgerConfig{
			Path:             "/var/lib/mqtt2db-go/wal",
			TTL:              Duration(7 * 24 * time.Hour),
			SyncWrites:       true,
			ValueLogFileSize: 1 << 30,
		},
		Postgres: PostgresConfig{
			MaxConns:        10,
			MinConns:        2,
			ConnMaxLifetime: Duration(time.Hour),
			Schema:          "public",
			Table:           "telemetry",
			Columns: []string{
				"tenant_id", "device_uuid", "topic", "payload", "received_at", "dedup_key",
			},
		},
		Flusher: FlusherConfig{
			BatchSize:                1000,
			FlushInterval:            Duration(100 * time.Millisecond),
			ElevatedLatencyThreshold: Duration(500 * time.Millisecond),
			ElevatedBatchSize:        5000,
			ElevatedFlushInterval:    Duration(500 * time.Millisecond),
			CriticalLatencyThreshold: Duration(2 * time.Second),
			RecoveryWindow:           Duration(60 * time.Second),
			MaxRetries:               5,
		},
		DeadLetter: DeadLetterConfig{
			S3: S3Config{
				Region: "us-east-1",
				UseSSL: true,
			},
		},
		Metrics: MetricsConfig{Listen: ":9090", Path: "/metrics"},
		Health: HealthConfig{
			Listen:        ":8080",
			LivenessPath:  "/healthz",
			ReadinessPath: "/readyz",
		},
		Logging: LoggingConfig{Level: "info", Format: "json"},
	}
}

// Load reads YAML from path, overlays environment variables, and validates
// the result. An empty path returns defaults overlaid with env overrides.
func Load(path string) (Config, error) {
	cfg := NewDefault()

	if path != "" {
		// path comes from the operator-set --config flag, not from
		// untrusted input — gosec G304 is a false positive here.
		raw, err := os.ReadFile(path) //nolint:gosec
		if err != nil {
			return Config{}, fmt.Errorf("read config %q: %w", path, err)
		}
		dec := yaml.NewDecoder(strings.NewReader(string(raw)))
		dec.KnownFields(true) // reject typos in YAML keys
		if err := dec.Decode(&cfg); err != nil {
			return Config{}, fmt.Errorf("parse config %q: %w", path, err)
		}
	}

	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: EnvPrefix}); err != nil {
		return Config{}, fmt.Errorf("apply env overrides: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

// Validate enforces the constraints documented in docs/CONFIGURATION.md.
// It returns errors.Join of every problem found so operators see the full
// list rather than fixing them one boot at a time.
func (c *Config) Validate() error {
	var errs []error

	// MQTT
	if len(c.MQTT.Brokers) == 0 {
		errs = append(errs, errors.New("mqtt.brokers: at least one broker URL required"))
	}
	for i, b := range c.MQTT.Brokers {
		if _, err := url.Parse(b); err != nil || b == "" {
			errs = append(errs, fmt.Errorf("mqtt.brokers[%d]: invalid URL %q", i, b))
		}
	}
	if c.MQTT.SharedSubscription == "" {
		errs = append(errs, errors.New("mqtt.shared_subscription: required"))
	} else if !strings.HasPrefix(c.MQTT.SharedSubscription, "$share/") {
		errs = append(errs, fmt.Errorf("mqtt.shared_subscription: must start with $share/, got %q", c.MQTT.SharedSubscription))
	}
	if c.MQTT.ClientIDPrefix == "" {
		errs = append(errs, errors.New("mqtt.client_id_prefix: required"))
	}
	if c.MQTT.ProtocolVersion != 3 && c.MQTT.ProtocolVersion != 5 {
		errs = append(errs, fmt.Errorf("mqtt.protocol_version: must be 3 or 5, got %d", c.MQTT.ProtocolVersion))
	}
	for name, d := range map[string]Duration{
		"mqtt.keepalive":             c.MQTT.Keepalive,
		"mqtt.connect_timeout":       c.MQTT.ConnectTimeout,
		"mqtt.max_reconnect_backoff": c.MQTT.MaxReconnectBackoff,
	} {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s: must be > 0", name))
		}
	}

	// Buffer
	if c.Buffer.MaxMessages <= 0 {
		errs = append(errs, errors.New("buffer.max_messages: must be > 0"))
	}
	if c.Buffer.MaxBytes <= 0 {
		errs = append(errs, errors.New("buffer.max_bytes: must be > 0"))
	}
	if c.Buffer.SpillThreshold <= 0 || c.Buffer.SpillThreshold >= 1 {
		errs = append(errs, fmt.Errorf("buffer.spill_threshold: must be in (0, 1), got %v", c.Buffer.SpillThreshold))
	}
	if c.Buffer.PauseThreshold <= 0 || c.Buffer.PauseThreshold >= 1 {
		errs = append(errs, fmt.Errorf("buffer.pause_threshold: must be in (0, 1), got %v", c.Buffer.PauseThreshold))
	}
	if c.Buffer.SpillThreshold >= c.Buffer.PauseThreshold {
		errs = append(errs, fmt.Errorf("buffer.spill_threshold (%v) must be < buffer.pause_threshold (%v)",
			c.Buffer.SpillThreshold, c.Buffer.PauseThreshold))
	}

	// Badger
	if c.Badger.Path == "" {
		errs = append(errs, errors.New("badger.path: required"))
	}
	if c.Badger.TTL <= 0 {
		errs = append(errs, errors.New("badger.ttl: must be > 0"))
	}
	if c.Badger.ValueLogFileSize <= 0 {
		errs = append(errs, errors.New("badger.value_log_file_size: must be > 0"))
	}

	// Postgres
	if c.Postgres.DSN == "" {
		errs = append(errs, errors.New("postgres.dsn: required"))
	}
	if c.Postgres.MaxConns <= 0 {
		errs = append(errs, errors.New("postgres.max_conns: must be > 0"))
	}
	if c.Postgres.MinConns < 0 || c.Postgres.MinConns > c.Postgres.MaxConns {
		errs = append(errs, fmt.Errorf("postgres.min_conns: must be in [0, max_conns], got %d", c.Postgres.MinConns))
	}
	if c.Postgres.Schema == "" {
		errs = append(errs, errors.New("postgres.schema: required"))
	}
	if c.Postgres.Table == "" {
		errs = append(errs, errors.New("postgres.table: required"))
	}
	if len(c.Postgres.Columns) == 0 {
		errs = append(errs, errors.New("postgres.columns: at least one column required"))
	}

	// Flusher
	if c.Flusher.BatchSize <= 0 {
		errs = append(errs, errors.New("flusher.batch_size: must be > 0"))
	}
	if c.Flusher.ElevatedBatchSize < c.Flusher.BatchSize {
		errs = append(errs, fmt.Errorf("flusher.elevated_batch_size (%d) must be >= batch_size (%d)",
			c.Flusher.ElevatedBatchSize, c.Flusher.BatchSize))
	}
	if c.Flusher.MaxRetries < 0 {
		errs = append(errs, errors.New("flusher.max_retries: must be >= 0"))
	}
	for name, d := range map[string]Duration{
		"flusher.flush_interval":             c.Flusher.FlushInterval,
		"flusher.elevated_flush_interval":    c.Flusher.ElevatedFlushInterval,
		"flusher.elevated_latency_threshold": c.Flusher.ElevatedLatencyThreshold,
		"flusher.critical_latency_threshold": c.Flusher.CriticalLatencyThreshold,
		"flusher.recovery_window":            c.Flusher.RecoveryWindow,
	} {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s: must be > 0", name))
		}
	}
	if c.Flusher.ElevatedLatencyThreshold >= c.Flusher.CriticalLatencyThreshold {
		errs = append(errs, fmt.Errorf("flusher.elevated_latency_threshold (%v) must be < critical_latency_threshold (%v)",
			c.Flusher.ElevatedLatencyThreshold.AsDuration(),
			c.Flusher.CriticalLatencyThreshold.AsDuration()))
	}

	// Dead-letter
	if c.DeadLetter.S3.Bucket == "" {
		errs = append(errs, errors.New("dead_letter.s3.bucket: required"))
	}
	if c.DeadLetter.S3.Region == "" {
		errs = append(errs, errors.New("dead_letter.s3.region: required"))
	}
	if c.DeadLetter.S3.Endpoint != "" {
		if _, err := url.Parse(c.DeadLetter.S3.Endpoint); err != nil {
			errs = append(errs, fmt.Errorf("dead_letter.s3.endpoint: %w", err))
		}
	}

	// Logging
	switch strings.ToLower(c.Logging.Level) {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("logging.level: must be debug|info|warn|error, got %q", c.Logging.Level))
	}
	switch strings.ToLower(c.Logging.Format) {
	case "json", "text":
	default:
		errs = append(errs, fmt.Errorf("logging.format: must be json|text, got %q", c.Logging.Format))
	}

	return errors.Join(errs...)
}
