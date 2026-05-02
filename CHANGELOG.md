# Changelog

All notable changes to mqtt2db-go are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-05-02

### Added

- **Sustained-slowdown stress test** (Milestone 13). New harness under
  `test/stress/slowdown/` drives the full pipeline through three
  scenarios using toxiproxy as a fault injector between the flusher and
  Postgres:
  * **moderate** — +300 ms latency for 5 minutes
  * **severe** — +2.5 s latency for 5 minutes
  * **outage** — TCP rejected for 3 minutes
  Each run produces a Markdown report under
  `test/stress/slowdown/results/`. `make stress-slowdown` runs all
  three at full duration; `make stress-slowdown-quick` shrinks the
  windows for development feedback.

- `mqtt2db_flusher_requeued_total` counter — records messages
  re-enqueued to the WAL after a transient terminal flush failure (so
  operators can distinguish real flusher errors from connection blips).

- `mqtt2db_subscriber_paused` gauge and `mqtt2db_subscriber_pauses_total`
  counter — track when the subscriber is dropping unacked messages
  because the ring is full and the WAL refused them.

### Changed

- **Flusher transient-failure handling** (ADR 0005). When
  `MaxRetries` is exhausted against a connection-level error, the batch
  is now re-enqueued to the WAL via the new `WALSource.Append` method
  rather than dead-lettered. Dead-letter is reserved for deterministic
  poison (SQLSTATE 22xxx data exceptions, 23xxx integrity violations).
  This fixes a bug where a sustained PG outage of more than ~31 seconds
  would dead-letter every batch, in violation of the "do not silently
  drop" contract from CLAUDE.md.

- `flusher.WALSource` interface gained an `Append(msg) ([]byte, error)`
  method. `wal.Store` already exposed it; the change is non-breaking
  for production code but is a small breaking change for anyone who
  implemented their own `WALSource` (none in tree).

- **Per-connection cached staging table** (ADR 0005). The pgx pool now
  installs `mqtt2db_staging` once per real connection via
  `pgxpool.Config.AfterConnect`, with `ON COMMIT DELETE ROWS` so it is
  empty at the start of every transaction. This removes the per-batch
  `CREATE TEMP TABLE` round trip that dominated flusher overhead at
  high throughput. The CopyFrom + ON CONFLICT staging hop is preserved.

- **Parallel flusher workers** (ADR 0005). New `flusher.workers`
  config field (default 1) spawns N worker goroutines, each running an
  independent `pull → flush` loop. Mode transitions and the WAL drain
  are mutex-protected; everything else is lock-free. With
  `max_conns: 8` and `workers: 7` the pipeline now sustains ~25K
  rows/sec post-recovery on M1-class hardware (was ~5K rows/sec).

### Stress-test results (10K msg/s, full duration)

All three sustained-slowdown scenarios pass at the milestone-target
10K msg/s with the A′ + B improvements above:

| scenario | toxic    | duration | WAL peak  | drain  | criteria  |
|----------|----------|----------|-----------|--------|-----------|
| moderate | +300 ms  | 10m26s   | 0         | 18s    | 5/5 PASS  |
| severe   | +2.5 s   | 30m05s   | 2,419,545 | 19m56s | 6/6 PASS  |
| outage   | TCP rej. | 16m07s   | 1,721,213 | 8m02s  | 6/6 PASS  |

Conservation (`distinct == flusher.inserted`) is exact in every
scenario; zero messages dead-lettered for transient PG issues.

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
