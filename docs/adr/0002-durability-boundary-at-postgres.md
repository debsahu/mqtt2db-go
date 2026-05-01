# 0002. Durability Boundary at PostgreSQL, Not WAL

Date: 2026-04-30

## Status

Accepted

## Context

At-least-once delivery requires acknowledging MQTT messages only after they have been persisted somewhere durable. The choice of "where" determines the operational guarantees and throughput characteristics.

Two reasonable choices exist:

1. Acknowledge after WAL write (Badger on local PVC).
2. Acknowledge after PostgreSQL insert.

Choice 1 maximizes throughput because Badger handles tens of thousands of writes per second per disk. Choice 2 maximizes durability semantics because a single replica's PVC failure does not lose data.

## Decision

Acknowledge MQTT messages only after successful PostgreSQL insert. This is the default behavior. A configuration flag (`ack_at_wal: true`) is reserved for workloads that explicitly opt into the throughput tradeoff, but it is not the default.

## Alternatives Considered

**Acknowledge at WAL.** Rejected as default: a replica's PVC failure (volume corruption, accidental deletion, node failure with non-replicated storage) would lose any messages in WAL but not yet in PG. For IoT telemetry that may inform business decisions or compliance reporting, this risk is unacceptable as a default.

**Acknowledge at memory.** Rejected: replica crash loses in-flight data with no recovery path.

**Replicate Badger volumes via Longhorn or similar.** Considered: would make ack-at-WAL safe. Rejected as default because it introduces dependency on a CSI driver with replication, which not all client environments have. Available as a deployment-time option for clients who want the throughput.

## Consequences

**Easier**: clear end-to-end durability semantics; no special handling for replica disk failures; aligns with Postgres's existing backup and replication story.

**Harder**: throughput is bounded by Postgres write rate. For workloads exceeding ~50K msg/sec sustained, Postgres tuning becomes critical, or the client should consider tiered architecture.

**New responsibilities**: monitoring Postgres insert latency closely; capacity planning for Postgres becomes part of ingest planning.

## References

- `docs/ARCHITECTURE.md` for failure mode analysis
- ADR-0001 for the buffer tier architecture
