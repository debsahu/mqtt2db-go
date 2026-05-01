// Package logging builds the process-wide *slog.Logger from
// config.LoggingConfig.
//
// The logger always writes to stdout so container runtimes pick it up.
// All loggers carry "service=mqtt2db-go" and the build version so logs are
// identifiable when several services share a sink. Per-component loggers
// should add `component=...` and `event=...` per the conventions in
// CLAUDE.md.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/debsahu/mqtt2db-go/internal/config"
	"github.com/debsahu/mqtt2db-go/internal/version"
)

// New returns a *slog.Logger configured per cfg, writing to stdout.
func New(cfg config.LoggingConfig) (*slog.Logger, error) {
	return newWithWriter(cfg, os.Stdout)
}

func newWithWriter(cfg config.LoggingConfig, w io.Writer) (*slog.Logger, error) {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	switch strings.ToLower(cfg.Format) {
	case "json":
		handler = slog.NewJSONHandler(w, opts)
	case "text":
		handler = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("unknown log format %q (want json|text)", cfg.Format)
	}

	logger := slog.New(handler).With(
		"service", "mqtt2db-go",
		"version", version.Version,
	)
	return logger, nil
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q", s)
	}
}
