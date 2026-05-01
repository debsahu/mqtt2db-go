package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/debsahu/mqtt2db-go/internal/config"
)

func TestNewPool_RequiresDSN(t *testing.T) {
	_, err := NewPool(context.Background(), config.PostgresConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dsn required")
}

func TestNewPool_RejectsMalformedDSN(t *testing.T) {
	_, err := NewPool(context.Background(), config.PostgresConfig{
		DSN: "not a valid dsn ://",
	})
	require.Error(t, err)
}

func TestNewCopier_RejectsNilPool(t *testing.T) {
	_, err := NewCopier(nil, validPGConfig())
	require.Error(t, err)
}

func TestNewCopier_RequiresSchemaAndTable(t *testing.T) {
	cfg := validPGConfig()
	cfg.Schema = ""
	_, err := NewCopier(fakePool(t), cfg)
	require.Error(t, err)

	cfg = validPGConfig()
	cfg.Table = ""
	_, err = NewCopier(fakePool(t), cfg)
	require.Error(t, err)
}

func TestNewCopier_RejectsMisalignedColumns(t *testing.T) {
	cfg := validPGConfig()
	// Reorder one pair so the slice no longer matches Message.copyRow.
	cfg.Columns = []string{"device_uuid", "tenant_id", "topic", "payload", "received_at", "dedup_key"}
	_, err := NewCopier(fakePool(t), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "do not match Message field order")
}

func TestNewCopier_RejectsExtraColumns(t *testing.T) {
	cfg := validPGConfig()
	cfg.Columns = append(cfg.Columns, "extra")
	_, err := NewCopier(fakePool(t), cfg)
	require.Error(t, err)
}

func TestMessage_CopyRowOrderMatchesCanonicalColumns(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	id := uuid.New()
	m := Message{
		TenantID:   "acme",
		DeviceUUID: id,
		Topic:      "t/acme/d/abc/evt/state",
		Payload:    []byte(`{"x":1}`),
		ReceivedAt: now,
		DedupKey:   id.String() + ":1234567890",
	}
	row := m.copyRow()
	require.Len(t, row, len(canonicalColumns))
	assert.Equal(t, "acme", row[0])
	assert.Equal(t, id, row[1])
	assert.Equal(t, "t/acme/d/abc/evt/state", row[2])
	assert.Equal(t, []byte(`{"x":1}`), row[3])
	assert.Equal(t, now, row[4])
	assert.Equal(t, m.DedupKey, row[5])
}

func TestQuoteIdent_EscapesEmbeddedQuotes(t *testing.T) {
	assert.Equal(t, `"plain"`, quoteIdent("plain"))
	assert.Equal(t, `"weird""name"`, quoteIdent(`weird"name`))
	assert.Equal(t, `""""`, quoteIdent(`"`))
}

func TestJoinIdents_QuotesEvery(t *testing.T) {
	assert.Equal(t, "", joinIdents(nil))
	assert.Equal(t, `"a"`, joinIdents([]string{"a"}))
	assert.Equal(t, `"a", "b", "c"`, joinIdents([]string{"a", "b", "c"}))
}

func TestColumnsMatchMessage(t *testing.T) {
	assert.True(t, columnsMatchMessage(canonicalColumns))
	assert.False(t, columnsMatchMessage(nil))
	assert.False(t, columnsMatchMessage([]string{"a", "b"}))
	mut := append([]string(nil), canonicalColumns...)
	mut[0], mut[1] = mut[1], mut[0]
	assert.False(t, columnsMatchMessage(mut))
}

// --- helpers ---

func validPGConfig() config.PostgresConfig {
	return config.PostgresConfig{
		DSN:     "postgres://example",
		Schema:  "public",
		Table:   "telemetry",
		Columns: append([]string(nil), canonicalColumns...),
	}
}

// fakePool returns a non-nil pool against a parseable but never-dialed DSN.
// pgxpool.New is lazy: it does not connect until first use, so these tests
// exercise the validation paths that come after the nil-pool guard.
func fakePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://nobody@127.0.0.1:1/none?connect_timeout=1")
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}
