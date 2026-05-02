BEGIN;

DROP INDEX IF EXISTS telemetry_unparseable_error_class_idx;
DROP INDEX IF EXISTS telemetry_unparseable_received_idx;
DROP TABLE IF EXISTS telemetry_unparseable;

COMMIT;
