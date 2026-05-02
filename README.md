# mqtt2db-go

High-throughput Go service that subscribes to MQTT topics on a Comqtt cluster and persists messages to PostgreSQL via PgBouncer. Designed to mimic Google Pub/Sub semantics (at-least-once delivery, durable buffering, backpressure-aware drain) for IoT telemetry workloads.

## Status

v0.1.0 shipped. v0.1.1 in review — sustained-slowdown stress test,
flusher transient-failure handling, per-connection cached staging,
and parallel flusher workers. See [CHANGELOG.md](CHANGELOG.md) for
the full list.

66 unit tests + 6 integration tests pass against real Comqtt v2.6.2 +
Postgres 17 + RustFS via testcontainers.

**Steady-state stress** (`test/stress/`): 600,000 messages over 30s @
20K msg/s with zero internal loss; the buffer correctly spilled to WAL
when the ring hit 80% and drained the WAL fully on recovery.

**Sustained-slowdown stress** (`test/stress/slowdown/`, three scenarios
via toxiproxy at the full **10 K msg/s** the milestone calls for, all
6/6 success criteria pass per scenario):
- moderate (+300 ms): mode reaches elevated, zero DLQ, WAL peak 0,
  drain 18 s
- severe (+2.5 s): mode reaches critical, subscriber paused, WAL peak
  ~2.4 M, zero DLQ, drain 19 m 56 s
- outage (TCP rejected): mode critical, subscriber paused, WAL peak
  ~1.7 M, zero DLQ, drain 8 m 02 s

Conservation (`distinct == flusher.inserted`) is exact in every
scenario. The post-recovery drain rate sits at ~25 K rows/sec on
M1-class hardware after the per-connection cached staging
(`AfterConnect` hook) and parallel-worker (`flusher.workers`) changes
in ADR 0005. `make stress-slowdown-quick` runs the same scenarios at
2 K msg/s for fast development feedback.

Each scenario writes a Markdown report to `test/stress/slowdown/results/`.

## Architecture at a Glance

```
Devices --MQTT--> Comqtt cluster --shared sub--> mqtt2db-go --> PgBouncer --> PostgreSQL
                                                      |
                                                      +--> Badger (local WAL/overflow)
                                                      |
                                                      +--> S3-compatible store (RustFS in dev, S3 in prod)
```

## Why This Exists

Off-the-shelf options for moving MQTT data into PostgreSQL fall into two camps. Either you adopt a heavyweight broker like EMQX with bundled rule engine and database sinks (which couples ingest to the broker hot path and creates licensing risk), or you wire up Telegraf or NiFi (which work but offer little control over backpressure and dead-letter semantics). This service occupies the middle ground: a focused Go binary that does one thing well, with operational properties tuned for IoT telemetry at scale.

## Container Image

Multi-arch (linux/amd64 + linux/arm64) images are published to GitHub
Container Registry on every push to `main` and on every `vX.Y.Z` tag:

```
ghcr.io/debsahu/mqtt2db-go:latest      # tracks main
ghcr.io/debsahu/mqtt2db-go:sha-<7>     # commit-pinned
ghcr.io/debsahu/mqtt2db-go:0.1.0       # release-pinned
```

The Helm chart's default `image.repository` already points there.

## Quickstart

```bash
# Spin up the local dev stack (Comqtt, Postgres, PgBouncer, RustFS)
docker compose -f deploy/docker-compose.yml up -d

# Run migrations
go run ./cmd/migrate up

# Start the ingest service
go run ./cmd/mqtt2db-go --config=config.dev.yaml

# In another terminal, generate load
go run ./test/loadgen --rate=1000 --duration=30s --devices=100
```

## Configuration

See `docs/CONFIGURATION.md` for the full reference. The minimum viable config:

```yaml
mqtt:
  brokers:
    - tcp://comqtt:1883
  client_id_prefix: mqtt2db-go
  shared_subscription: $share/ingest/t/+/d/+/evt/#
  username: ${MQTT_USERNAME}
  password: ${MQTT_PASSWORD}

postgres:
  dsn: postgres://ingest:${PG_PASSWORD}@pgbouncer:6432/telemetry
  max_conns: 10

badger:
  path: /var/lib/mqtt2db-go/wal
  ttl_hours: 168

dead_letter:
  s3:
    endpoint: s3.amazonaws.com
    bucket: mqtt2db-go-dlq
    region: us-east-1

flusher:
  batch_size: 1000
  flush_interval_ms: 100

metrics:
  listen: :9090
```

## Operational Properties

- **Delivery**: at-least-once. Downstream consumers must handle duplicates.
- **Durability**: messages are acknowledged to Comqtt only after they land in PostgreSQL.
- **Backpressure**: slow PostgreSQL slows MQTT consumption; Comqtt buffers or redelivers via shared subscription.
- **Horizontal scaling**: deploy N replicas of this service; the shared subscription distributes load.
- **Failure recovery**: replica loss is recoverable (Comqtt redelivers); local Badger volume loss may lose unflushed messages within the configured WAL window.

## Authentication and Authorization

This service does not implement auth or ACL. Comqtt enforces both via its own configuration (Postgres-backed, HTTP-backed, or static, depending on the operator's choice). The ingest service connects to Comqtt as a single authenticated MQTT client using credentials from a Kubernetes secret. Topic-level permissions, tenant isolation, and device authorization are configured at the Comqtt layer before this service ever sees a message.

The deployment assumes Comqtt is already running with auth and ACL appropriate for the client's environment. See the Comqtt documentation at https://github.com/wind-c/comqtt for configuration options.

## Documentation

- [`CHANGELOG.md`](CHANGELOG.md) — release notes
- [`config.example.yaml`](config.example.yaml) — minimum runnable config
- Detailed architecture, runbook, configuration reference, and ADRs are
  maintained privately by the project owner.

## License

MIT — see [LICENSE](LICENSE).

Copyright (c) 2026 Debashish Sahu. Hosted at
https://github.com/debsahu/mqtt2db-go.
