-- Side-table for messages whose MQTT topic could not be parsed into
-- (tenant_id, device_uuid). The main `telemetry` table requires both
-- columns NOT NULL; messages missing them are preserved here instead of
-- being dropped, so operators can diagnose firmware bugs after the fact.
--
-- See docs/adr/0006-unparseable-side-table.md for the full rationale.
--
-- Schema notes:
--   error_class: enum-like text. Current values are "topic_too_long",
--                "topic_structure", "tenant_invalid", "device_uuid_invalid".
--                New values may be added without a migration; downstream
--                consumers should treat unknown values as "other".
--   error_detail: free-form context (e.g. the offending segment). May be
--                 NULL when the error_class is self-explanatory.
--   received_at: when the subscriber first observed the message. Used by
--                operators to correlate with broker logs.
--   inserted_at: when the row landed in PG. Differs from received_at if
--                the unparseable insert was retried.
--
-- Retention is operator-owned (no TTL here). Recommended:
--   DELETE FROM telemetry_unparseable WHERE received_at < now() - interval '30 days';
BEGIN;

CREATE TABLE IF NOT EXISTS telemetry_unparseable (
    id           BIGSERIAL    PRIMARY KEY,
    topic        TEXT         NOT NULL,
    payload      BYTEA        NOT NULL,
    error_class  TEXT         NOT NULL,
    error_detail TEXT,
    received_at  TIMESTAMPTZ  NOT NULL,
    inserted_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS telemetry_unparseable_received_idx
    ON telemetry_unparseable (received_at DESC);

CREATE INDEX IF NOT EXISTS telemetry_unparseable_error_class_idx
    ON telemetry_unparseable (error_class, received_at DESC);

COMMIT;
