package subscriber_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/debsahu/mqtt2db-go/internal/subscriber"
)

func TestParseTopic_Valid(t *testing.T) {
	id := uuid.New()
	p, err := subscriber.ParseTopic("t/acme/d/" + id.String() + "/evt/state")
	require.NoError(t, err)
	assert.Equal(t, "acme", p.Tenant)
	assert.Equal(t, id, p.Device)
}

func TestParseTopic_DeeperPath(t *testing.T) {
	id := uuid.New()
	p, err := subscriber.ParseTopic("t/acme/d/" + id.String() + "/evt/sensors/temp/c")
	require.NoError(t, err)
	assert.Equal(t, "acme", p.Tenant)
	assert.Equal(t, id, p.Device)
}

func TestParseTopic_StrictTenantCharset(t *testing.T) {
	id := uuid.New()
	// All allowed: alphanumeric, underscore, hyphen.
	for _, tenant := range []string{
		"acme", "customer-42", "tenant_a", "T", "0", "AbCdEf-123_xyz",
	} {
		p, err := subscriber.ParseTopic("t/" + tenant + "/d/" + id.String() + "/evt/state")
		require.NoError(t, err, "tenant=%q should be accepted", tenant)
		assert.Equal(t, tenant, p.Tenant)
	}
}

// Each rejected case asserts both the broad sentinel (ErrBadTopic) and
// the specific Class — the Class is what gets written to
// telemetry_unparseable.error_class downstream.
func TestParseTopic_RejectsByClass(t *testing.T) {
	id := uuid.New().String()

	cases := []struct {
		name      string
		topic     string
		wantClass string
	}{
		{"empty", "", subscriber.TopicErrStructure},
		{"too_short", "too/short", subscriber.TopicErrStructure},
		{"wrong_prefix", "x/acme/d/" + id + "/evt/state", subscriber.TopicErrStructure},
		{"wrong_d_marker", "t/acme/x/" + id + "/evt/state", subscriber.TopicErrStructure},
		{"not_evt", "t/acme/d/" + id + "/cmd/state", subscriber.TopicErrStructure},

		{"empty_tenant", "t//d/" + id + "/evt/state", subscriber.TopicErrTenantInvalid},
		{"plus_tenant", "t/+/d/+/evt/#", subscriber.TopicErrTenantInvalid},
		{"hash_tenant", "t/#/d/" + id + "/evt/state", subscriber.TopicErrTenantInvalid},
		{"space_tenant", "t/ /d/" + id + "/evt/state", subscriber.TopicErrTenantInvalid},
		{"space_in_multichar_tenant", "t/acme prod/d/" + id + "/evt/state", subscriber.TopicErrTenantInvalid},
		{"unicode_tenant", "t/café/d/" + id + "/evt/state", subscriber.TopicErrTenantInvalid},
		{"oversized_tenant", "t/" + strings.Repeat("a", subscriber.MaxTenantLen+1) + "/d/" + id + "/evt/state", subscriber.TopicErrTenantInvalid},

		{"bad_uuid", "t/acme/d/not-a-uuid/evt/state", subscriber.TopicErrDeviceUUID},
		{"empty_uuid", "t/acme/d//evt/state", subscriber.TopicErrDeviceUUID},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := subscriber.ParseTopic(tc.topic)
			require.Error(t, err)
			assert.True(t, errors.Is(err, subscriber.ErrBadTopic),
				"errors.Is(err, ErrBadTopic) should hold")
			var pe *subscriber.TopicParseError
			require.True(t, errors.As(err, &pe),
				"err should unwrap to *TopicParseError")
			assert.Equal(t, tc.wantClass, pe.Class)
		})
	}
}

func TestParseTopic_TooLong(t *testing.T) {
	id := uuid.New().String()
	// Pad the trailing event segment until total topic exceeds MaxTopicLen.
	prefix := "t/acme/d/" + id + "/evt/"
	pad := strings.Repeat("x", subscriber.MaxTopicLen-len(prefix)+1)
	topic := prefix + pad

	_, err := subscriber.ParseTopic(topic)
	require.Error(t, err)
	assert.True(t, errors.Is(err, subscriber.ErrBadTopic))
	var pe *subscriber.TopicParseError
	require.True(t, errors.As(err, &pe))
	assert.Equal(t, subscriber.TopicErrTooLong, pe.Class)
	assert.Contains(t, pe.Detail, "len=")
}

func TestTopicParseError_StringFormat(t *testing.T) {
	e := &subscriber.TopicParseError{Class: subscriber.TopicErrTenantInvalid, Detail: "+"}
	assert.Equal(t, "subscriber: bad topic: tenant_invalid: +", e.Error())

	e2 := &subscriber.TopicParseError{Class: subscriber.TopicErrStructure}
	assert.Equal(t, "subscriber: bad topic: topic_structure", e2.Error())
}
