# 0007. Realistic-Schema Stress Test (Milestone 14b)

Date: 2026-05-02

## Status

Accepted

## Context

The v0.1.1 / v0.1.2 stress numbers — *25 K rows/sec sustained drain
post-recovery on M1-class hardware* — are measured against the
minimal `telemetry` schema:

- 6 columns
- 1 unique index (`dedup_key`)
- 2 btree indexes (`(tenant_id, received_at DESC)`,
  `(device_uuid, received_at DESC)`)
- ~10-byte synthetic payloads

External feedback after v0.1.2 raised the right concern:

> Secondary indexes and wider rows are where 'works in the lab'
> numbers usually fall apart. Honest gaps beat fake certainty every
> time.

Real production schemas usually carry more columns (firmware version,
content type, region, payload size, etc.) and one or two more
secondary indexes for tenant-scoped or region-scoped queries. Real
payloads run 1 KB to 16 KB, not ten bytes. Index maintenance and
heap-tuple width dominate insert cost at high throughput, so the
"~25 K rows/sec" figure is an **upper bound** that the realistic
case won't meet.

Two ways to fix the credibility gap:

1. **Stop quoting numbers** until we run the realistic sweep. Bad —
   operators planning capacity need *something*.
2. **Quote both numbers**, with a clear caveat for what each
   represents. Better. The lab number is still useful for code-level
   regression tracking; the realistic number is what an operator
   sizes their PG cluster against.

This ADR picks (2) and defines the realistic sweep.

## Decision

### 1. Test-only `telemetry_wide` schema

A second table for the stress harness only. **Same six base columns**
as `telemetry` (so the existing `postgres.Copier` can write to it
without code changes) plus four extra columns and two extra
secondary indexes:

```sql
CREATE TABLE telemetry_wide (
    -- base columns (same shape as `telemetry`, so the Copier path
    -- needs no changes to write to this table)
    id           BIGSERIAL    PRIMARY KEY,
    tenant_id    TEXT         NOT NULL,
    device_uuid  UUID         NOT NULL,
    topic        TEXT         NOT NULL,
    payload      BYTEA        NOT NULL,
    received_at  TIMESTAMPTZ  NOT NULL,
    dedup_key    TEXT         NOT NULL,
    inserted_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- realistic extras
    schema_version       TEXT NOT NULL DEFAULT 'v1',
    content_type         TEXT NOT NULL DEFAULT 'application/octet-stream',
    region               TEXT NOT NULL DEFAULT 'us-east-1',
    payload_size_bytes   INTEGER GENERATED ALWAYS AS (octet_length(payload)) STORED
);

CREATE UNIQUE INDEX telemetry_wide_dedup_key_uidx
    ON telemetry_wide (dedup_key);
CREATE INDEX telemetry_wide_tenant_received_idx
    ON telemetry_wide (tenant_id, received_at DESC);
CREATE INDEX telemetry_wide_device_received_idx
    ON telemetry_wide (device_uuid, received_at DESC);

-- realistic extras
CREATE INDEX telemetry_wide_tenant_content_idx
    ON telemetry_wide (tenant_id, content_type, received_at DESC);
CREATE INDEX telemetry_wide_region_received_idx
    ON telemetry_wide (region, received_at DESC);
```

### 2. Embedded DDL, **not** a `migrations/` file

The DDL lives **embedded as a Go string in the stress harness**, not
as a file under `migrations/`. Reason: production operators run
`cmd/migrate up` against `migrations/`, and we do not want them to
unintentionally create a stress-test artifact in their database. Two
secondary failure modes the embedded approach avoids:

- An ops automation that scrapes `migrations/` for "latest version"
  picking up `0003_telemetry_wide_test.up.sql` and treating it as a
  schema-version bump.
- An integration test (`TestMigrate_TelemetryUnparseableExists` etc.)
  having to special-case a "test-only" file in the migration dir.

The harness applies the DDL via `pool.Exec` after the standard
migrations have run. `IF NOT EXISTS` makes it idempotent across
re-runs of the same scenario.

### 3. Configurable payload size

Today's harness produces a fixed 128-byte random body. Add an env
knob `SLOWDOWN_PAYLOAD_BYTES` (default 128 for back-compat with the
minimal-schema lab numbers) that the realistic make target sets to
**4096** — a defensible mid-range for IoT telemetry that includes
JSON envelopes plus a moderately rich sensor payload.

### 4. New env: `SLOWDOWN_SCHEMA`

`SLOWDOWN_SCHEMA=wide` switches the harness to:

- Apply the wide DDL after migrations.
- Configure `postgres.Copier` to write to `telemetry_wide`.
- Point the verification queries (`rowCount`, `distinctDedupKeys`,
  `awaitFullDrain`) at the same table.

Default `SLOWDOWN_SCHEMA=minimal` preserves the M13/M14a behavior
exactly. Existing `make stress-slowdown` and `make stress-oscillation`
targets continue to use the minimal schema.

### 5. Make targets

```
make stress-realistic        # full sweep: M13 + M14a scenarios @ wide schema
make stress-realistic-quick  # abbreviated, for development feedback
```

### 6. Reporting: bytes/sec alongside rows/sec

Realistic-schema runs are bottlenecked by I/O bytes, not row count.
The Markdown report adds a `Throughput` section with both:

- `rows/sec` — `flusher.inserted_total / runtime`
- `bytes/sec` — `(rows × avg_payload) / runtime`, useful for
  PG-side tuning conversations.

Existing reports for minimal-schema scenarios get the same section
for consistency. No reports need backfilling.

### 7. README honesty fix

The README quotes the M13 number without a schema caveat:

> The post-recovery drain rate sits at ~25 K rows/sec on
> M1-class hardware...

Replace with both numbers and the caveat:

> The post-recovery drain rate is ~25 K rows/sec on minimal lab
> schema and **~X K rows/sec / ~Y MB/s on the realistic
> `telemetry_wide` test schema** (two extra secondary indexes,
> 4 KB payloads). The realistic number is the one to size production
> PG against.

`X` and `Y` come from the actual sweep, not extrapolation.

## Alternatives Considered

### A. `migrations/0003_telemetry_wide_test.up.sql`

Rejected as primary path — see §2 above. Operators running
`cmd/migrate up` would create a stress-test artifact in production.

### B. Add the wide columns to `telemetry` itself

Rejected. The current `telemetry` schema is a public contract used
by the projection-service (planned). Adding columns "just for
testing" pollutes that contract and would confuse the next reader.

### C. Different `Message` / `Copier` for the wide path

Rejected as over-engineering. The wide schema's extra columns all
have `DEFAULT` values or are `GENERATED`, so the existing 6-column
Copier can write to either table without changes. The harness only
swaps the table name, not the row shape.

### D. Use TimescaleDB hypertables

Out of scope. The client may opt into Timescale later; that's a
separate sweep.

### E. GIN index on payload (jsonb)

Rejected for v0.1.3. Likely too expensive at 10 K msg/s and
introduces a payload-shape requirement (must be valid JSON) that
mqtt2db-go intentionally does not enforce. If a future deployment
wants payload-search, partial expression indexes on hot tenant slugs
are cheaper and don't constrain the payload format.

## Consequences

### Easier

- Operators get a *realistic* number to size their PG against,
  alongside the lab number for regression tracking.
- The "honest gaps beat fake certainty" point is addressed by the
  README change — both numbers are quoted with their schema caveat.
- Future schema additions are easy to validate against the existing
  scenarios (M13 + M14a), since `SLOWDOWN_SCHEMA=wide` is the only
  switch.

### Harder

- One more code path in the harness (~50 LoC). The wide-schema
  switch must be off by default so M13 / M14a runs reproduce v0.1.1
  / v0.1.2 numbers exactly.
- Maintenance: any future migration that touches `telemetry`'s base
  columns needs a parallel touch in the embedded `telemetry_wide`
  DDL. Cheap if remembered, painful if not. Mitigation: code review
  guidance + a CI check that diffs the base-column SQL between the
  two locations.
- Reports for runs against the wide schema are bigger (more cycles
  of slower drain). Disk usage in `test/stress/slowdown/results/`
  grows ~30 % per realistic sweep.

### Out of scope (intentionally)

- TimescaleDB hypertables.
- GIN / payload-search indexes.
- Partitioning by `received_at`.
- Multi-replica realistic sweeps (single-replica is enough to
  characterize per-replica throughput).

## References

- `test/stress/slowdown/harness.go` — embedded DDL + plumbing.
- `test/stress/slowdown/results/` — sweep outputs.
- ADR 0005 — sustained-slowdown stress test (parent).
- `docs/DEVELOPMENT_PLAN.md` Milestone 14b.
