//go:build slowdown

// Realistic-schema stress (Milestone 14b / ADR 0007). Switched on
// via SLOWDOWN_SCHEMA=wide. Adds the `telemetry_wide` table with
// extra columns + two extra secondary indexes on top of the M13
// minimal schema.
//
// The DDL lives here, not under migrations/. Production operators
// run `cmd/migrate up` against migrations/; we do NOT want them
// creating stress-test artifacts in their database. See ADR 0007 §2.
//
// telemetry_wide's first six columns match telemetry exactly so the
// existing postgres.Copier writes to either table without changes —
// the harness only swaps the table NAME, not the row shape.
package slowdown

import (
	"context"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaMode is the active schema for the run, controlled by
// SLOWDOWN_SCHEMA=minimal (default) or wide.
type schemaMode string

const (
	schemaMinimal schemaMode = "minimal"
	schemaWide    schemaMode = "wide"
)

// activeSchema returns the schema mode for this run.
func activeSchema() schemaMode {
	switch strings.ToLower(os.Getenv("SLOWDOWN_SCHEMA")) {
	case "wide":
		return schemaWide
	default:
		return schemaMinimal
	}
}

// targetTable is the name the Copier and verification queries use.
func targetTable() string {
	if activeSchema() == schemaWide {
		return "telemetry_wide"
	}
	return "telemetry"
}

// payloadSize is the byte count for synthetic publish bodies. Set via
// SLOWDOWN_PAYLOAD_BYTES; defaults to 128 to match v0.1.2 lab numbers.
// The realistic make target sets this to 4096.
func payloadSize() int {
	return envInt("SLOWDOWN_PAYLOAD_BYTES", 128)
}

// wideSchemaDDL is applied after the standard migrations whenever
// SLOWDOWN_SCHEMA=wide is set. IF NOT EXISTS makes it idempotent
// across re-runs of the same scenario.
//
// The first six columns match `telemetry` exactly. The "realistic
// extras" (schema_version, content_type, region) carry sensible
// defaults so the 6-column Copier path writes valid rows. Two extra
// btree indexes (`(tenant_id, content_type, received_at DESC)` and
// `(region, received_at DESC)`) are the index-maintenance cost the
// realistic sweep is designed to surface.
const wideSchemaDDL = `
CREATE TABLE IF NOT EXISTS telemetry_wide (
    id           BIGSERIAL    PRIMARY KEY,
    tenant_id    TEXT         NOT NULL,
    device_uuid  UUID         NOT NULL,
    topic        TEXT         NOT NULL,
    payload      BYTEA        NOT NULL,
    received_at  TIMESTAMPTZ  NOT NULL,
    dedup_key    TEXT         NOT NULL,
    inserted_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),

    schema_version       TEXT NOT NULL DEFAULT 'v1',
    content_type         TEXT NOT NULL DEFAULT 'application/octet-stream',
    region               TEXT NOT NULL DEFAULT 'us-east-1',
    payload_size_bytes   INTEGER GENERATED ALWAYS AS (octet_length(payload)) STORED
);

CREATE UNIQUE INDEX IF NOT EXISTS telemetry_wide_dedup_key_uidx
    ON telemetry_wide (dedup_key);
CREATE INDEX IF NOT EXISTS telemetry_wide_tenant_received_idx
    ON telemetry_wide (tenant_id, received_at DESC);
CREATE INDEX IF NOT EXISTS telemetry_wide_device_received_idx
    ON telemetry_wide (device_uuid, received_at DESC);

-- realistic extras: two more secondary indexes that real workloads
-- typically carry for tenant-scoped and region-scoped queries.
CREATE INDEX IF NOT EXISTS telemetry_wide_tenant_content_idx
    ON telemetry_wide (tenant_id, content_type, received_at DESC);
CREATE INDEX IF NOT EXISTS telemetry_wide_region_received_idx
    ON telemetry_wide (region, received_at DESC);
`

// applyWideSchemaIfNeeded creates telemetry_wide when the harness is
// in wide mode. No-op for minimal mode.
func applyWideSchemaIfNeeded(ctx context.Context, pool *pgxpool.Pool) error {
	if activeSchema() != schemaWide {
		return nil
	}
	_, err := pool.Exec(ctx, wideSchemaDDL)
	return err
}
