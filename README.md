# mqtt2db-go

High-throughput Go service that subscribes to MQTT topics on a Comqtt cluster and persists messages to PostgreSQL via PgBouncer. Designed to mimic Google Pub/Sub semantics (at-least-once delivery, durable buffering, backpressure-aware drain) for IoT telemetry workloads.

## Status

v0.1.1 — see [CHANGELOG.md](CHANGELOG.md) for release notes.

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

# (One-time) enable the pre-push lint+vet+test hook
make hooks
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
- **Malformed messages**: if a message arrives with a topic that doesn't match the expected shape (wrong tenant format, bad device UUID, etc.), it's preserved in a `telemetry_unparseable` side-table — original topic, payload, and reason intact — instead of being discarded. Lets you debug device firmware after the fact.

## Scaling: When You Need More Throughput

More pods alone is rarely the answer. The bottleneck is almost always
how fast PostgreSQL can absorb writes, not how fast the service can
read from MQTT. Work through these in order:

1. **Make PostgreSQL bigger.** More CPU, faster disks, and raise its
   connection limit. If the database can't keep up, nothing downstream
   helps.
2. **Open the PgBouncer valve.** Raise PgBouncer's pool size so the
   new database capacity is actually reachable.
3. **Use more flusher workers per pod.** Each pod can write in parallel
   to PostgreSQL — bump that number before adding pods.
4. **Give each pod more database connections.** So the workers in step 3
   have slots to grab. (Keep workers ≤ max connections.)
5. **Then add more pods.** Only after the database side can absorb them.
   Adding pods first just makes more pods wait on the same database.

Rule of thumb: each step has to keep up with the one before it.
Skipping ahead just moves the queue into the local buffer, where you'll
see it as growing buffer depth and slower drains.

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
