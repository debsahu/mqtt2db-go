# 0001. Three-Tier Buffer Architecture: Memory, Badger, S3

Date: 2026-04-30

## Status

Accepted

## Context

The service must absorb bursty MQTT traffic and drain into PostgreSQL at the rate Postgres can sustain. Three failure modes need to be handled distinctly: short bursts (seconds), sustained slowness (minutes to hours), and poison messages (indefinite).

A single buffer tier cannot serve all three. In-memory only loses data on crash. Disk-only sacrifices throughput for normal operation. Object storage only is too slow for the hot path.

## Decision

Three-tier buffer architecture:

1. **In-memory ring buffer** (default 100K messages, 100MB) absorbs short bursts and serves the steady-state hot path.
2. **Badger embedded WAL** on a Kubernetes PVC handles sustained backpressure when the in-memory buffer overflows. Configurable TTL caps disk usage.
3. **S3 dead-letter sink** receives poison messages after retry exhaustion, taking them out of the queue permanently.

Each tier handles a specific failure mode. The transitions between tiers are explicit and metered.

## Alternatives Considered

**Single-tier in-memory only.** Rejected: replica restart loses unflushed data, violating the at-least-once contract for in-flight messages.

**Single-tier disk only (Badger as primary buffer).** Rejected: every message pays disk write cost on the hot path, reducing peak throughput by roughly 3x in benchmarks. The in-memory tier serves the steady state; Badger is the safety net.

**Use Kafka or NATS JetStream as the buffer.** Rejected: introduces a second messaging system to operate, conflicts with the design goal of a focused service. The whole point of mqtt-ingest is to be the buffer between Comqtt and Postgres; deferring to another buffer relocates the problem rather than solving it.

**Postgres unlogged tables as buffer.** Rejected: couples buffer durability to Postgres availability, defeating the point of buffering during Postgres slowness.

## Consequences

**Easier**: graceful degradation under sustained Postgres slowness; clear operational metrics per tier; replica restarts are recoverable from WAL.

**Harder**: three states of message location to reason about; WAL TTL must be tuned to client's recovery time objectives; Badger volume must be sized for worst-case backpressure duration.

**New responsibilities**: PVC sizing per replica; Badger compaction monitoring; dead-letter S3 lifecycle management.

## References

- `docs/ARCHITECTURE.md` for the broader design
- Discussion: https://github.com/dgraph-io/badger for Badger characteristics
