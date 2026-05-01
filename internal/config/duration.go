package config

import (
	"errors"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that knows how to unmarshal itself from a
// YAML scalar containing Go duration syntax (e.g. "5s", "100ms", "1h").
// It also satisfies caarlos0/env's parser via its time.Duration kind.
type Duration time.Duration

// AsDuration returns the underlying time.Duration.
func (d Duration) AsDuration() time.Duration { return time.Duration(d) }

// UnmarshalYAML decodes a YAML scalar into a Duration. Empty strings stay
// at zero so callers can distinguish "unset" from "explicit zero" in tests
// — Validate is responsible for rejecting illegal zero values.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return errors.New("duration must be a scalar")
	}
	if node.Value == "" {
		*d = 0
		return nil
	}
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

// MarshalYAML emits the duration as a Go-style string so round-tripping
// retains the original units (`5s`, not `5000000000`).
func (d Duration) MarshalYAML() (any, error) {
	return time.Duration(d).String(), nil
}
