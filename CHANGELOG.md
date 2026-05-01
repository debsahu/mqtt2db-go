# Changelog

All notable changes to mqtt2db-go are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-05-01

Initial public release. Subscribes to a Comqtt MQTT 5 cluster via shared
subscriptions and persists messages to PostgreSQL with at-least-once
semantics.

### Added

- **MQTT subscriber** (paho.golang/autopaho) with persistent session
  (`clean_session=false`), QoS 1, manual ack, and a stable per-replica
  client ID derived from the pod hostname.
- **Three-tier buffering**: in-memory ring (default 100K msgs / 100MB) →
  Badger v4 WAL (configurable TTL, default 7d) → PostgreSQL flush. Spill
  threshold 80%, pause threshold 95%; the subscriber stops acking when
  paused so Comqtt redelivers via the shared subscription.
- **Adaptive flusher** with three modes (normal / elevated / critical)
  driven by p95 latency over a rolling window. Default batch 1000 rows
  / 100ms; elevated batch 5000 / 500ms; critical mode pauses pulling.
  Retries with exponential backoff (1s, 2s, 4s, 8s, 16s) before
  dead-lettering.
- **PostgreSQL layer** uses `pgx.CopyFrom` into a transaction-scoped
  staging table and then `INSERT ... SELECT ... ON CONFLICT (dedup_key)
  DO NOTHING`. Benchmark: ~78K rows/sec on a single connection (M1 Pro,
  Postgres 17 in Docker).
- **Dead-letter sink** to any S3-compatible bucket via the AWS SDK v2
  client (`aws-sdk-go-v2/service/s3`). Per-message JSON envelopes with
  full error context; key shape
  `dead-letter/{date}/{tenant}/{device}/{nanos}-{seq}.json`.
- **Operational endpoints**: `/metrics` (Prometheus, scrape-ready) and
  `/healthz` + `/readyz` (Kubernetes probes). Readiness aggregates a
  Postgres `Ping` and a dead-letter `HeadBucket`.
- **Migration runner** `cmd/migrate` (golang-migrate v4) with
  `up`/`down [N]`/`version` subcommands; reads `--dsn` or
  `MQTT2DB_POSTGRES_DSN`.
- **Helm chart** in `deploy/helm/`: StatefulSet with PVC for Badger,
  ConfigMap for config, Secret for credentials, PDB, optional
  ServiceMonitor and PrometheusRule.
- **Local dev stack** (`deploy/docker-compose.yml`): Comqtt v2.6.2
  (built from upstream via `deploy/comqtt/Dockerfile`), Postgres 17,
  PgBouncer (transaction pooling, SCRAM), RustFS with auto-created
  `mqtt2db-go-dlq` bucket.
- **Load test harness** (`test/loadgen`): configurable rate, duration,
  device count, payload size, parallel publishers; reports achieved
  vs. target throughput.
- Architecture Decision Records:
  - ADR-0001 three-tier buffer architecture
  - ADR-0002 durability boundary at PostgreSQL
  - ADR-0003 RustFS for dev object storage; AWS SDK v2 for the S3 client
  - ADR-0004 build Comqtt from source (the operator owns prod broker
    deployment)

### Out of scope (intentional)

- **Authentication and authorization**: Comqtt enforces these via its
  own configuration. This service authenticates with a single MQTT
  username/password pair from a Kubernetes secret and trusts Comqtt's
  ACL decisions.
- **Comqtt deployment**: operator-owned, separate repo or Helm chart.
- **PostgreSQL deployment / DBA**: separate concern; this service
  consumes a DSN.
- **Live config reload**: configuration changes are deployment events.
