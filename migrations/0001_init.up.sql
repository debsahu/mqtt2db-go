-- Telemetry table: append-only sink for messages that survive the ring
-- buffer + WAL + flusher pipeline.
--
-- Idempotency: dedup_key is unique. The flusher relies on
-- INSERT ... ON CONFLICT DO NOTHING (or COPY into a staging table followed
-- by INSERT ... SELECT ... ON CONFLICT DO NOTHING) so duplicate MQTT
-- redeliveries collapse to a single row. Callers compute dedup_key as
-- "{device_uuid}:{timestamp_nanos}" but the column is treated as opaque text.
--
-- Time semantics:
--   received_at: when the ingest service first observed the message.
--                Used for ordering, partitioning candidates, and replay
--                windows. Always populated by the service.
--
-- Partitioning is intentionally not enabled here. Tenants who want monthly
-- range partitioning by received_at can opt in via a follow-up migration;
-- it is out of scope for the v0.1 schema.
BEGIN;

CREATE TABLE IF NOT EXISTS telemetry (
    id           BIGSERIAL    PRIMARY KEY,
    tenant_id    TEXT         NOT NULL,
    device_uuid  UUID         NOT NULL,
    topic        TEXT         NOT NULL,
    payload      BYTEA        NOT NULL,
    received_at  TIMESTAMPTZ  NOT NULL,
    dedup_key    TEXT         NOT NULL,
    inserted_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS telemetry_dedup_key_uidx
    ON telemetry (dedup_key);

CREATE INDEX IF NOT EXISTS telemetry_tenant_received_idx
    ON telemetry (tenant_id, received_at DESC);

CREATE INDEX IF NOT EXISTS telemetry_device_received_idx
    ON telemetry (device_uuid, received_at DESC);

COMMIT;
