BEGIN;

DROP INDEX IF EXISTS telemetry_device_received_idx;
DROP INDEX IF EXISTS telemetry_tenant_received_idx;
DROP INDEX IF EXISTS telemetry_dedup_key_uidx;
DROP TABLE IF EXISTS telemetry;

COMMIT;
