package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/debsahu/mqtt2db-go/internal/config"
)

func TestNewUnparseableWriter_RejectsNilPool(t *testing.T) {
	_, err := NewUnparseableWriter(nil, config.PostgresConfig{Schema: "public"})
	require.Error(t, err)
}

func TestNewUnparseableWriter_RequiresSchema(t *testing.T) {
	_, err := NewUnparseableWriter(fakePool(t), config.PostgresConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema required")
}

func TestNewUnparseableWriter_UsesFixedTable(t *testing.T) {
	w, err := NewUnparseableWriter(fakePool(t), config.PostgresConfig{
		Schema: "public",
		Table:  "ignored", // operator-configurable Table is for telemetry only
	})
	require.NoError(t, err)
	assert.Equal(t, UnparseableTableName, w.table)
}

func TestInsertUnparseable_RejectsMissingFields(t *testing.T) {
	w, err := NewUnparseableWriter(fakePool(t), config.PostgresConfig{Schema: "public"})
	require.NoError(t, err)

	now := time.Now().UTC()

	cases := []struct {
		name string
		row  Unparseable
	}{
		{"missing_topic", Unparseable{ErrorClass: "x", ReceivedAt: now}},
		{"missing_class", Unparseable{Topic: "t/...", ReceivedAt: now}},
		{"missing_received_at", Unparseable{Topic: "t/...", ErrorClass: "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := w.InsertUnparseable(context.Background(), tc.row)
			require.Error(t, err)
		})
	}
}

func TestMaxErrorDetailLen_BoundsTruncation(t *testing.T) {
	// MaxErrorDetailLen is the persistence-side cap; this test exists
	// so a future change has to touch the test if the cap moves.
	assert.Equal(t, 256, MaxErrorDetailLen)

	// Sanity-check that strings longer than the cap exist in the
	// parser's reachable space (tenants up to MaxTenantLen=64 are well
	// under 256, but a too-long topic detail like "len=99999" is
	// short, while a future Detail field carrying a full segment up
	// to 1024 chars would be truncated). Just exercise the slicing
	// arithmetic so we know it compiles and behaves.
	long := strings.Repeat("x", MaxErrorDetailLen+50)
	got := long[:MaxErrorDetailLen]
	assert.Len(t, got, MaxErrorDetailLen)
}
