package subscriber_test

import (
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

func TestParseTopic_Rejects(t *testing.T) {
	id := uuid.New().String()
	cases := []string{
		"",
		"too/short",
		"t//d/" + id + "/evt/state",
		"t/acme/d/not-a-uuid/evt/state",
		"x/acme/d/" + id + "/evt/state",
		"t/acme/x/" + id + "/evt/state",
		"t/acme/d/" + id + "/cmd/state", // not "evt"
		"t/+/d/+/evt/#",
	}
	for _, topic := range cases {
		_, err := subscriber.ParseTopic(topic)
		assert.Error(t, err, "topic=%q should be rejected", topic)
	}
}
