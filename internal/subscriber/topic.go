package subscriber

import (
	"errors"
	"strings"

	"github.com/google/uuid"
)

// ErrBadTopic is returned by ParseTopic when a topic does not match the
// configured pattern. We do not silently drop these — they are surfaced as
// HandlerErrors so operators can spot device firmware bugs.
var ErrBadTopic = errors.New("subscriber: topic does not match t/{tenant}/d/{device_uuid}/evt/...")

// ParsedTopic carries the tenant ID and device UUID extracted from an
// incoming MQTT topic. The topic pattern is hard-coded to match the
// shared subscription default `t/+/d/+/evt/#` because changing it would
// invalidate the whole device firmware contract.
type ParsedTopic struct {
	Tenant string
	Device uuid.UUID
}

// ParseTopic extracts tenant + device from a topic of the form
// "t/{tenant}/d/{device_uuid}/evt/...". Anything shorter or shaped
// differently returns ErrBadTopic.
func ParseTopic(topic string) (ParsedTopic, error) {
	parts := strings.Split(topic, "/")
	if len(parts) < 5 || parts[0] != "t" || parts[2] != "d" || parts[4] != "evt" {
		return ParsedTopic{}, ErrBadTopic
	}
	tenant := parts[1]
	if tenant == "" || tenant == "+" || tenant == "#" {
		return ParsedTopic{}, ErrBadTopic
	}
	id, err := uuid.Parse(parts[3])
	if err != nil {
		return ParsedTopic{}, ErrBadTopic
	}
	return ParsedTopic{Tenant: tenant, Device: id}, nil
}
