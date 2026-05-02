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
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaMode is the active schema for the run, controlled by
// SLOWDOWN_SCHEMA=minimal (default) or wide.
type schemaMode string

const (
	schemaMinimal schemaMode = "minimal"
	schemaWide    schemaMode = "wide"
)

// activeSchemaTB resolves SLOWDOWN_SCHEMA strictly. Unknown values
// fail the test loud (Copilot caught the silent-fallback risk: a typo
// in a make recipe would run the wrong benchmark and still produce a
// PASS). Empty defaults to minimal.
func activeSchemaTB(tb testing.TB) schemaMode {
	tb.Helper()
	v := strings.ToLower(strings.TrimSpace(os.Getenv("SLOWDOWN_SCHEMA")))
	switch v {
	case "", "minimal":
		return schemaMinimal
	case "wide":
		return schemaWide
	default:
		tb.Fatalf("SLOWDOWN_SCHEMA=%q invalid: must be \"minimal\" or \"wide\"", v)
		return schemaMinimal
	}
}

// activeSchema is the no-tb variant for callers that don't have a
// testing.TB at hand (the load gen, the report writer). Strict
// validation already ran via activeSchemaTB inside setupHarness, so
// we trust the value here. Reads "wide" as wide; everything else as
// minimal.
func activeSchema() schemaMode {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("SLOWDOWN_SCHEMA")), "wide") {
		return schemaWide
	}
	return schemaMinimal
}

// targetTable is the name the Copier and verification queries use.
func targetTable() string {
	if activeSchema() == schemaWide {
		return "telemetry_wide"
	}
	return "telemetry"
}

// MaxPayloadBytes bounds SLOWDOWN_PAYLOAD_BYTES so a typo can't push
// the load generator into a megabyte-per-message regime. 64 KiB
// matches a reasonable upper bound for IoT telemetry payloads.
const MaxPayloadBytes = 64 * 1024

// payloadSizeTB resolves SLOWDOWN_PAYLOAD_BYTES strictly. <=0 and
// >MaxPayloadBytes both fail the test instead of silently producing
// empty-payload or memory-eating traffic.
func payloadSizeTB(tb testing.TB) int {
	tb.Helper()
	n := envInt("SLOWDOWN_PAYLOAD_BYTES", 128)
	if n <= 0 {
		tb.Fatalf("SLOWDOWN_PAYLOAD_BYTES=%d invalid: must be > 0", n)
	}
	if n > MaxPayloadBytes {
		tb.Fatalf("SLOWDOWN_PAYLOAD_BYTES=%d invalid: max %d", n, MaxPayloadBytes)
	}
	return n
}

// payloadSize is the no-tb variant for callers without a testing.TB.
// Strict validation already ran via payloadSizeTB inside setupHarness;
// here we clamp to the default rather than panic the load goroutine.
func payloadSize() int {
	n := envInt("SLOWDOWN_PAYLOAD_BYTES", 128)
	if n <= 0 || n > MaxPayloadBytes {
		return 128
	}
	return n
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
//
// IMPORTANT: the base columns below MUST stay in sync with
// migrations/0001_init.up.sql. assertWideSchemaBaseColumnsMatch (run
// from the harness during applyWideSchemaIfNeeded) catches drift at
// the moment of greatest cost — when someone tries to use the wide
// sweep — instead of letting it pass silently.
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

// baseTelemetryColumns is the contract the wide-schema sweep depends
// on: telemetry_wide MUST share these columns with telemetry so the
// existing Copier writes to either table without changes. If a
// future migration changes telemetry's base columns and this list
// drifts, assertWideSchemaBaseColumnsMatch fails the test loud.
var baseTelemetryColumns = []string{
	"dedup_key",
	"device_uuid",
	"id",
	"payload",
	"received_at",
	"tenant_id",
	"topic",
}

// applyWideSchemaIfNeeded creates telemetry_wide when the harness is
// in wide mode, then verifies the base-column contract still holds
// against the live `telemetry` table. No-op for minimal mode.
func applyWideSchemaIfNeeded(ctx context.Context, pool *pgxpool.Pool) error {
	if activeSchema() != schemaWide {
		return nil
	}
	if _, err := pool.Exec(ctx, wideSchemaDDL); err != nil {
		return fmt.Errorf("apply wide-schema DDL: %w", err)
	}
	return assertWideSchemaBaseColumnsMatch(ctx, pool)
}

// assertWideSchemaBaseColumnsMatch reads the live column lists for
// telemetry and telemetry_wide from information_schema and verifies
// the base contract: every column in baseTelemetryColumns exists in
// BOTH tables. Catches the failure mode where a future migration
// changes telemetry's base columns and the embedded wide DDL is
// silently out of sync (Copilot caught the absence of this check).
func assertWideSchemaBaseColumnsMatch(ctx context.Context, pool *pgxpool.Pool) error {
	got := func(table string) ([]string, error) {
		rows, err := pool.Query(ctx, `
			SELECT column_name
			FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = $1
			ORDER BY column_name`, table)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				return nil, err
			}
			out = append(out, c)
		}
		return out, rows.Err()
	}

	tCols, err := got("telemetry")
	if err != nil {
		return fmt.Errorf("read telemetry columns: %w", err)
	}
	wCols, err := got("telemetry_wide")
	if err != nil {
		return fmt.Errorf("read telemetry_wide columns: %w", err)
	}
	tSet, wSet := toSet(tCols), toSet(wCols)

	var missingFromTelemetry, missingFromWide []string
	for _, c := range baseTelemetryColumns {
		if !tSet[c] {
			missingFromTelemetry = append(missingFromTelemetry, c)
		}
		if !wSet[c] {
			missingFromWide = append(missingFromWide, c)
		}
	}
	if len(missingFromTelemetry) > 0 || len(missingFromWide) > 0 {
		return fmt.Errorf(
			"wide-schema base-column contract broken: missing from telemetry=%v, missing from telemetry_wide=%v "+
				"(see baseTelemetryColumns in test/stress/slowdown/wide_schema.go)",
			missingFromTelemetry, missingFromWide)
	}
	return nil
}

func toSet(s []string) map[string]bool {
	m := make(map[string]bool, len(s))
	for _, x := range s {
		m[x] = true
	}
	return m
}

