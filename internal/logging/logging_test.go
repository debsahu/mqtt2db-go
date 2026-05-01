package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/debsahu/mqtt2db-go/internal/config"
)

func TestNew_RejectsUnknownLevel(t *testing.T) {
	_, err := New(config.LoggingConfig{Level: "loud", Format: "json"})
	require.Error(t, err)
}

func TestNew_RejectsUnknownFormat(t *testing.T) {
	_, err := New(config.LoggingConfig{Level: "info", Format: "yaml"})
	require.Error(t, err)
}

func TestNew_JSONEmitsServiceAndVersion(t *testing.T) {
	var buf bytes.Buffer
	logger, err := newWithWriter(
		config.LoggingConfig{Level: "info", Format: "json"},
		&buf,
	)
	require.NoError(t, err)

	logger.Info("hello", "component", "test", "event", "boot")

	var got map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got))
	assert.Equal(t, "mqtt2db-go", got["service"])
	assert.Contains(t, got, "version")
	assert.Equal(t, "test", got["component"])
	assert.Equal(t, "boot", got["event"])
	assert.Equal(t, "hello", got["msg"])
}

func TestNew_TextFormatHonoursLevel(t *testing.T) {
	var buf bytes.Buffer
	logger, err := newWithWriter(
		config.LoggingConfig{Level: "warn", Format: "text"},
		&buf,
	)
	require.NoError(t, err)

	logger.Info("info-line")
	logger.Warn("warn-line")

	out := buf.String()
	assert.NotContains(t, out, "info-line", "info should be suppressed at level=warn")
	assert.True(t, strings.Contains(out, "warn-line"), "warn should be emitted")
}
