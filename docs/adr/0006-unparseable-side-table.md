# 0006. Preserve Unparseable Messages in a Postgres Side-Table

Date: 2026-05-02

## Status

Accepted

## Context

`subscriber.HandleMessage` parses the incoming MQTT topic with
`ParseTopic` to extract `tenant_id` and `device_uuid`. When the parse
fails — malformed topic, non-UUID device segment, missing prefix —
the v0.1.1 implementation logs a warning, increments
`mqtt2db_subscriber_handler_errors_total`, calls `ack()` so the broker
drops the message, and then **discards the payload entirely**.

This violates the spirit of the "do not silently drop" principle in
`CLAUDE.md`. It is not strictly silent (there is a counter and a log
line) but the original message — topic and payload — is irrecoverable.
For operators investigating a firmware bug, a tenant onboarding mistake,
or a device on the wrong topic, that record is exactly what would
identify the cause.

The v0.1.0 design considered routing parse failures to the S3
dead-letter sink, but never wired it up because the DLQ envelope schema
assumed a `device_uuid` was available, and the parse failure means it
is not.

A second, related question came up while drafting this ADR: the topic
parser today accepts any non-empty tenant string. A device firmware
bug that sends `tenant=" "` (single space) or
`tenant="acme/prod"` (slash-injected — though this would already fail
the structural split) would land in `telemetry` with weird tenant
values that downstream queries have to defend against. The strict
parser here closes that gap so all "almost valid but not quite"
messages flow through the unparseable path instead of polluting the
main table.

## Decision

### 1. Side-table in the same Postgres instance

Add a new table `telemetry_unparseable`:

```sql
CREATE TABLE telemetry_unparseable (
    id           BIGSERIAL    PRIMARY KEY,
    topic        TEXT         NOT NULL,
    payload      BYTEA        NOT NULL,
    error_class  TEXT         NOT NULL,
    error_detail TEXT,
    received_at  TIMESTAMPTZ  NOT NULL,
    inserted_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX telemetry_unparseable_received_idx
    ON telemetry_unparseable (received_at DESC);
CREATE INDEX telemetry_unparseable_error_class_idx
    ON telemetry_unparseable (error_class, received_at DESC);
```

Subscriber writes parse failures here via a new
`postgres.InsertUnparseable(ctx, row)` (single-row insert, not
batched — these are rare).

### 2. Strict topic criteria

`ParseTopic` returns `ErrBadTopic` for any of:

| Rule | Rationale |
|------|-----------|
| Topic ≤ 1024 bytes | Defense against abusive topics; MQTT spec allows 64 KiB but real telemetry never needs more than ~200 bytes. |
| ≥ 5 segments separated by `/` | Structural minimum: `t / {tenant} / d / {device} / evt / ...`. |
| `parts[0] == "t"`, `parts[2] == "d"`, `parts[4] == "evt"` | Device firmware contract. |
| Tenant matches `^[a-zA-Z0-9_-]{1,64}$` | URL-safe ASCII slug. Rejects whitespace, MQTT wildcards (`+`, `#`), control characters, slashes (would already be split), and unbounded length. |
| Device segment parses as a UUID (any version) | `uuid.Parse` covers v1..v8 and Microsoft GUID forms. |

Each failure mode contributes a distinct `error_class` value when the
unparseable row is inserted, so `SELECT error_class, count(*)` is the
operator's first triage query:

- `topic_too_long`
- `topic_structure` (segment count or fixed segments wrong)
- `tenant_invalid` (charset or length)
- `device_uuid_invalid`

### 3. Bounded best-effort writer

`InsertUnparseable` runs on the subscriber's goroutine — it is not
batched and is not retried in a long loop. Behavior:

1. Try the insert with a 5 s context timeout.
2. On success: ack the MQTT message so the broker drops it.
3. On failure: log + increment `unparseable_insert_errors_total` +
   ack anyway. We trade preservation guarantee for queue-poisoning
   prevention. (If we did not ack, the same message would loop forever
   and we'd lose new messages too.)

Rationale: parse failures are by definition a small fraction of
traffic. If even 1% of messages were unparseable we would have a
firmware incident, not a routine condition. Treating this path as
best-effort keeps it from competing with the main flusher for pool
slots and removes a class of cascading-failure modes.

### 4. New metric

`mqtt2db_subscriber_unparseable_inserted_total{error_class="..."}` is
incremented on successful insert. The existing
`handler_errors_total` is **kept** as the "we observed a parse failure"
counter (incremented unconditionally), so existing dashboards keep
working; the new counter narrows down to "we successfully preserved
it."

Failed-insert path: `mqtt2db_subscriber_unparseable_insert_errors_total`.

## Alternatives Considered

### A. S3 dead-letter for unparseable

The original idea. Rejected because:
- Adds a hard dependency on S3 reachability for a path that has nothing
  to do with PG durability.
- Operators have to look at two systems to triage one incident.
- PG already exists, has backups, and has the operator's existing
  query tooling (`psql`, dashboards). No reason to reach further.

### B. Sentinel rows in `telemetry` (NULL or `__unparseable__` tenant)

Considered briefly. Rejected because:
- Either dropping NOT NULL constraints (which polluted every existing
  query with `WHERE tenant_id IS NOT NULL` filters) or using sentinel
  string values (which still pollute `GROUP BY tenant_id` rollups).
- Dedup behavior is wrong: parse failures have no `device_uuid` to
  build a dedup key from, so we'd need a per-row UUID just for this
  case.
- Operationally the side-table is cleaner: separate retention, separate
  vacuum behavior, separate access control if needed.

### C. Don't tighten the parser; only add the side-table

Rejected because the gap between "parses successfully" and "should
parse successfully" is currently large. A device sending
`tenant=" "` or `tenant="acme prod"` (space) lands in the main
telemetry table today. Tightening the parser is a behavior change but
small (the new restriction is a strict subset of what production
devices already use), and the same side-table catches anything that
falls off the new edge of the criteria — so the change is recoverable
in the worst case.

### D. Configurable parse rule (regex in YAML)

Tempting but a slippery slope. The topic shape is part of the device
firmware contract, not an operator concern; making it config gives
operators a foot-gun (forget to update the firmware, deploy a regex
that breaks all ingest). If a future deployment needs a different
shape, it is an ADR + code change, not a runtime knob.

## Consequences

### Easier

- Operators can run `SELECT topic, payload, error_class FROM
  telemetry_unparseable ORDER BY received_at DESC LIMIT 50` to
  diagnose firmware bugs.
- The "do not silently drop" contract becomes literally true: every
  message either lands in `telemetry`, lands in
  `telemetry_unparseable`, or lands in S3 dead-letter (deterministic
  poison only).
- The main `telemetry` table stays clean of weird-tenant rows because
  the strict parser catches them upstream.

### Harder

- One more table for ops to vacuum, monitor, and back up.
- `handler_errors_total` and `unparseable_inserted_total` should
  match in steady state; if they diverge, the
  `unparseable_insert_errors_total` counter explains the gap. Three
  metrics where there used to be one — small dashboard update.
- Devices with tenants outside `[a-zA-Z0-9_-]` will start landing in
  the side-table when they previously landed in `telemetry`. This is
  intentional but counts as a minor behavior change for v0.1.2.
- The strict parser is a Go-internal rule, not a config field. Any
  future deployment that needs a different tenant charset has to
  patch the code (or write ADR 0007).

### Operational notes

- The side-table is unbounded by design. Operators should set a
  retention policy via cron: `DELETE FROM telemetry_unparseable
  WHERE received_at < now() - interval '30 days'` or similar. The
  service does not own this — same as the main `telemetry` table.
- A pure parse-failure storm (e.g. a misconfigured device fleet
  sending malformed topics at full rate) will hit
  `InsertUnparseable` on the subscriber goroutine. If PG is also
  slow that path serializes one insert per parse failure. A future
  optimization is to batch unparseable inserts via a small channel,
  but at the rates we expect (≪ 1% of traffic) the simpler
  implementation wins.

## References

- ADR 0001 — three-tier buffer architecture
- ADR 0002 — durability boundary at PostgreSQL
- ADR 0005 — sustained-slowdown stress test (defines the
  "do not silently drop" property tested by the
  conservation invariant)
- `internal/subscriber/topic.go` — current ParseTopic
- `migrations/0002_telemetry_unparseable.up.sql` — side-table schema
