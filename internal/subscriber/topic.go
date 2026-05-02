package subscriber

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

// Topic parse error classes. These string values are written verbatim
// into telemetry_unparseable.error_class so operators can group by them
// (`SELECT error_class, count(*) ...`). Adding new classes does not
// require a migration but should be reflected in
// docs/adr/0006-unparseable-side-table.md.
const (
	TopicErrTooLong       = "topic_too_long"
	TopicErrStructure     = "topic_structure"
	TopicErrTenantInvalid = "tenant_invalid"
	TopicErrDeviceUUID    = "device_uuid_invalid"
)

// MaxTopicLen caps how long a topic the parser will accept. MQTT 5
// permits up to 65,535 bytes, but production telemetry topics are
// typically ~200 bytes; 1 KiB is generous without exposing the ingest
// path to abusive topics.
const MaxTopicLen = 1024

// MaxTenantLen bounds the tenant segment. Tenant slugs in production
// configs are short identifiers (e.g. "acme", "customer-42"); 64 chars
// is generous.
const MaxTenantLen = 64

// tenantRE enforces a URL-safe ASCII slug. Rejects whitespace, MQTT
// wildcards (`+`, `#`), control characters, and any character that
// would surprise downstream tools (SQL clients, log scrapers, REST
// path encoders).
var tenantRE = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// ErrBadTopic is the sentinel returned when a topic does not match the
// configured pattern. Every TopicParseError satisfies errors.Is against
// it, so callers that just want a yes/no can keep checking
// `errors.Is(err, ErrBadTopic)`.
var ErrBadTopic = errors.New("subscriber: bad topic")

// TopicParseError carries the machine-readable Class plus an optional
// human-readable Detail (e.g. the offending segment). The subscriber
// uses Class to populate telemetry_unparseable.error_class.
type TopicParseError struct {
	Class  string
	Detail string
}

func (e *TopicParseError) Error() string {
	if e.Detail == "" {
		return "subscriber: bad topic: " + e.Class
	}
	return "subscriber: bad topic: " + e.Class + ": " + e.Detail
}

// Is lets callers use errors.Is(err, ErrBadTopic) without caring about
// the specific class.
func (e *TopicParseError) Is(target error) bool { return target == ErrBadTopic }

// ParsedTopic carries the tenant ID and device UUID extracted from an
// incoming MQTT topic. The topic pattern is part of the device firmware
// contract — see docs/adr/0006-unparseable-side-table.md for the
// rationale on keeping it hard-coded rather than configurable.
type ParsedTopic struct {
	Tenant string
	Device uuid.UUID
}

// ParseTopic extracts tenant + device from a topic of the form
// "t/{tenant}/d/{device_uuid}/evt/...". On failure it returns a
// *TopicParseError with a Class identifying which rule failed; callers
// that don't care can still check errors.Is(err, ErrBadTopic).
func ParseTopic(topic string) (ParsedTopic, error) {
	if len(topic) > MaxTopicLen {
		return ParsedTopic{}, &TopicParseError{
			Class:  TopicErrTooLong,
			Detail: fmt.Sprintf("len=%d", len(topic)),
		}
	}
	parts := strings.Split(topic, "/")
	if len(parts) < 5 || parts[0] != "t" || parts[2] != "d" || parts[4] != "evt" {
		return ParsedTopic{}, &TopicParseError{Class: TopicErrStructure}
	}
	tenant := parts[1]
	if len(tenant) == 0 || len(tenant) > MaxTenantLen || !tenantRE.MatchString(tenant) {
		return ParsedTopic{}, &TopicParseError{
			Class:  TopicErrTenantInvalid,
			Detail: tenant,
		}
	}
	id, err := uuid.Parse(parts[3])
	if err != nil {
		return ParsedTopic{}, &TopicParseError{
			Class:  TopicErrDeviceUUID,
			Detail: parts[3],
		}
	}
	return ParsedTopic{Tenant: tenant, Device: id}, nil
}
