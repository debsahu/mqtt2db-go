package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/debsahu/mqtt2db-go/internal/config"
)

// Unparseable is the row shape persisted to telemetry_unparseable when
// the subscriber cannot extract (tenant_id, device_uuid) from an MQTT
// topic. See docs/adr/0006-unparseable-side-table.md for context.
type Unparseable struct {
	Topic       string
	Payload     []byte
	ErrorClass  string
	ErrorDetail string // empty -> persisted as NULL
	ReceivedAt  time.Time
}

// MaxErrorDetailLen caps the size of ErrorDetail at the persistence
// layer. The parser may put the offending tenant or UUID segment into
// Detail; topics are already bounded to MaxTopicLen but this is an
// extra guard so a misbehaving payload cannot bloat the side-table.
const MaxErrorDetailLen = 256

// UnparseableInserter is the narrow surface the subscriber uses to
// persist parse failures. Defined as an interface so tests can mock it
// without spinning up a real pool.
type UnparseableInserter interface {
	InsertUnparseable(ctx context.Context, row Unparseable) error
}

// UnparseableWriter writes single rows into telemetry_unparseable.
// Inserts are not batched: parse failures are by design rare, so the
// extra round trip per failure is fine and keeps this path off the
// hot flusher loop.
type UnparseableWriter struct {
	pool   *pgxpool.Pool
	schema string
	table  string
}

// UnparseableTableName is the side-table created by migration 0002.
// Hard-coded (not config-driven) because the subscriber writes a fixed
// row shape; an operator who renames it must also update the constant.
const UnparseableTableName = "telemetry_unparseable"

// NewUnparseableWriter wires a writer against an existing pool. It uses
// the same schema as the main telemetry table.
func NewUnparseableWriter(pool *pgxpool.Pool, cfg config.PostgresConfig) (*UnparseableWriter, error) {
	if pool == nil {
		return nil, errors.New("postgres: nil pool")
	}
	if cfg.Schema == "" {
		return nil, errors.New("postgres: schema required")
	}
	return &UnparseableWriter{
		pool:   pool,
		schema: cfg.Schema,
		table:  UnparseableTableName,
	}, nil
}

// InsertUnparseable persists a single parse-failure row. ErrorDetail is
// truncated to MaxErrorDetailLen and stored as NULL when empty.
// Returns an error on any DB failure; callers should log + increment a
// failure metric and ack the MQTT message anyway to avoid poisoning the
// queue (see ADR 0006).
func (w *UnparseableWriter) InsertUnparseable(ctx context.Context, row Unparseable) error {
	if row.Topic == "" {
		return errors.New("unparseable: topic required")
	}
	if row.ErrorClass == "" {
		return errors.New("unparseable: error_class required")
	}
	if row.ReceivedAt.IsZero() {
		return errors.New("unparseable: received_at required")
	}

	detail := row.ErrorDetail
	if len(detail) > MaxErrorDetailLen {
		detail = detail[:MaxErrorDetailLen]
	}
	var detailArg any
	if detail == "" {
		detailArg = nil
	} else {
		detailArg = detail
	}

	sql := fmt.Sprintf(`
		INSERT INTO %s.%s (topic, payload, error_class, error_detail, received_at)
		VALUES ($1, $2, $3, $4, $5)`,
		quoteIdent(w.schema), quoteIdent(w.table),
	)
	_, err := w.pool.Exec(ctx, sql,
		row.Topic, row.Payload, row.ErrorClass, detailArg, row.ReceivedAt)
	if err != nil {
		return fmt.Errorf("insert unparseable: %w", err)
	}
	return nil
}
