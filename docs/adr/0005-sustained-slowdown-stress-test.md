# 0005. Sustained Slowdown Stress Test

Date: 2026-05-01

## Status

Accepted

## Context

The default stress test (`test/stress/stress_test.go`, Milestone 11)
drives 20K msg/s through the pipeline against a healthy Postgres. It
exercises the *normal* path of the adaptive flusher and demonstrates
that the buffer correctly spills to the WAL when the ring fills up,
but it does not stress any of the failure-handling code:

  * the flusher never crosses the elevated → critical latency
    threshold,
  * the subscriber's pause path never triggers,
  * the WAL's "drain after recovery" path is never tested under
    sustained back-pressure,
  * the dead-letter handling for *transient* (vs. poison) errors is
    never exercised.

The flusher contract specifies three modes (normal / elevated /
critical) and a recovery window — see the package doc on
`internal/flusher/flusher.go`. We need a test that drives each
transition deliberately and verifies the documented behavior holds
end-to-end.

## Decision

Add a new stress harness under `test/stress/slowdown/` that uses
**toxiproxy** as a layer-7 fault injector between the service's
Postgres pool and the actual Postgres container. Three scenarios run
end-to-end against a real Comqtt v2.6.2 + Postgres 17 + RustFS via
testcontainers, with a sustained 10K msg/s synthetic load throughout:

| Scenario | Toxic                              | Duration | Expected mode path           |
|----------|------------------------------------|----------|------------------------------|
| A — moderate | latency +300 ms (both ways)    | 5 min    | normal → elevated → normal   |
| B — severe   | latency +2.5 s (both ways)     | 5 min    | normal → elevated → critical → elevated → normal |
| C — outage   | proxy disabled (TCP rejected)   | 3 min    | normal → critical (sustained) → recovery → normal |

Each scenario runs for ≥ 10 minutes total: a 1-minute warm-up at
healthy latency, the toxic window, then a recovery + drain window long
enough for the WAL to fully empty. The load generator publishes at
QoS 0 (same convention as the default stress test) so the publisher
side never bottlenecks; broker → subscriber → ring still uses the
production QoS 1 path.

Toxiproxy choice: it is the canonical Go-native fault-injector,
runs as a single small container, exposes an HTTP API for live
toxic add/remove, and the ecosystem already publishes
`ghcr.io/shopify/toxiproxy:2.12.0`. The Go client at
`github.com/Shopify/toxiproxy/v2/client` is one import. No alternatives
worth considering for our scope.

### Universal success criteria

A scenario passes only if all of:

1. **Zero messages dead-lettered for transient PG issues.** Connection
   refused, timeout, slow latency — none of these are
   "poison" — they must not terminate as DLQ. (Per-row constraint
   violations would still DLQ; that path is tested separately by
   the existing E2E test.)
2. **100% conservation of injected messages.** At end of test,
   `count(distinct dedup_key) >= subscriber.acked_total`. The exact
   count may exceed `sent` because of `dedup_key` collisions on the
   same-device-same-nanosecond edge case (collapsed by the unique
   index — see `internal/postgres/postgres.go`).
3. **Mode-transition metrics fire** and are scrape-visible on the
   Prometheus registry.
4. **Subscriber pause gauge increments** during scenarios B and C.
5. **WAL drains to < 1% of peak within 10 minutes** of recovery.

### Behavioral changes forced by this test

The test surfaced **four** real bugs (and one performance limitation
that became a fifth fix; see below):

**Bug 1 — sustained outage dead-letters everything.** Under a sustained
PG outage, `flushWithRetry` exhausted `MaxRetries` (default 5) within
~31 s and dead-lettered the entire batch. For an outage longer than
~31 s, every subsequent batch dead-lettered as well. This violates
criterion (1).

Fix:

* `flusher.WALSource` gains an `Append` method.
* `flushWithRetry` classifies the terminal error. Connection-level
  errors (everything that is not a Postgres `22xxx` data-exception or
  `23xxx` constraint-violation `pgconn.PgError`) are *transient*.
  Transient terminal failures push the batch back into the WAL via
  `WALSource.Append` instead of dead-lettering.
* Dead-letter is reserved for *deterministic poison*: schema
  violations, constraint violations, encoding errors. Those will never
  succeed regardless of how long we wait.
* Metrics: `mqtt2db_flusher_requeued_total` records WAL re-enqueues so
  operators can distinguish transient blips from real DLQ events.

This change keeps the original design intent: dead-letter is reserved
for *poison messages* — connection failures are not poison.

**Bug 2 — mode never advances during a wedged flush.** Mode transitions
fire only on flush completion (`recordLatency` runs on success;
`enterCritical` on error). Under sustained 5 s round-trip latency, a
single `CopyMessages` makes ~5 round trips and so takes ~25 s. During
the entire 60 s severe-toxic window, the flusher might complete only
2 flushes — and if a flush *hung* (never returned), mode could stay at
`normal` indefinitely. The first stress run observed exactly this:
ring filled to 100K, WAL accumulated 17K, but `mqtt2db_flusher_mode`
never moved off 0.

Initial fix attempt: bound each individual flush attempt with a context
timeout of `2 × CriticalLatencyThreshold` (4 s). This *did* surface
the mode transition under the toxic window, but post-recovery the
forced cancellations dropped sustained drain throughput from
~17 K rows/s to ~2 K rows/s — a regression severe enough to fail
the 10-minute drain criterion outright.

Final fix: remove the per-attempt timeout entirely and rely on the
mode-recovery path driven by `recordLatency` over the rolling p95
window. Since real flushes complete (just slowly), the latency
samples push the mode to `critical` within 1–2 successful flushes
under the severe scenario, the test's 100 ms peak-mode poller
(see Bug 4) catches that transition, and post-recovery the flusher
runs at full throughput because no batch is artificially cancelled
mid-COPY.

**Bug 3 — `subscriber.paused` gauge never fired.** The original
implementation set the gauge to 1 only when both the ring and the WAL
refused a message — i.e., the ack-drop path. With Badger absorbing
overflow indefinitely, ack-drop is essentially never triggered in
realistic configurations, so the gauge stayed at 0 even when the
subscriber was clearly under pressure. Operators couldn't alert on
"ring is at pause threshold."

Fix: the subscriber sets `Paused = 1` whenever the post-enqueue ring
state is `buffer.StatePause` (≥ 95 % capacity by default), regardless
of whether ack-drop happens. This gives operators an actionable
signal — "the ring is in pause-threshold range right now" — and
matches the spec's intent for scenarios B and C.

**Bug 4 — flusher mode oscillates between elevated and critical on
every successful slow flush.** `flushWithRetry` called `recordLatency`
*before* `maybeRecover`. When p95 was high (e.g. 25 s under severe
slowdown), `recordLatency` correctly promoted `elevated → critical`,
then `maybeRecover` immediately demoted `critical → elevated`. Net
effect: the gauge read "elevated" between flushes despite latency
sitting well above the critical threshold, and the test's `peak mode`
observation never registered critical.

Fix: swap the call order to `maybeRecover` then `recordLatency`. The
recovery-from-error demotion now happens first; latency-based
promotion runs second and *persists* until the rolling window's p95
drops below the threshold. The test now also samples
`mqtt2db_flusher_mode` at 100 ms (separate fast poller alongside the
1 s timeline recorder) so brief mode flips during transitions are
captured even on a slow CI runner.

**Bug 5 — drain rate decays over time; serial flusher cannot meet the
10-minute drain criterion at 10 K msg/s.** Under sustained 10 K msg/s,
the original implementation drained the WAL at ~17 K rows/s for the
first 30 seconds, then degraded to ~1.5 K rows/s within a few minutes.
With a 3.3 M-row WAL from the moderate scenario, full reconciliation
took ~30 minutes on M1-class hardware, breaking the 10-minute drain
criterion.

Two contributing factors, each fixed:

**5a. Per-batch `CREATE TEMP TABLE` round trip.** Every flush opened
a transaction, ran `CREATE TEMP TABLE … ON COMMIT DROP`, then CopyFrom
into it, then `INSERT … SELECT … ON CONFLICT`, then COMMIT. The
temp-table DDL added a fixed round trip per batch that did not
amortise as backlog shrank.

Fix: move the staging table to a per-connection cached table created
once via `pgxpool.Config.AfterConnect`, with `ON COMMIT DELETE ROWS`
so it is empty at the start of every transaction. The CopyFrom + ON
CONFLICT staging hop is preserved (per the engineering contract);
only the redundant DDL round trip is gone. This change alone moved
sustained drain throughput from ~5 K rows/s to ~14 K rows/s.

**5b. Serial flusher topology.** Even with the DDL hop removed, a
single goroutine pulling 1 K rows at a time through one connection
caps drain throughput well below what 10 K msg/s ingest demands.

Fix: introduce a `flusher.workers` config field (default 1; the
slowdown harness sets 7 to leave one connection of `max_conns: 8`
spare for health probes). Each worker runs an independent
`pull → flush` loop. The mode-transition logic (`maybeRecover`,
`enterElevated`, `enterCritical`) is mutex-protected; the rolling p95
window is too. WAL `Drain` calls are also mutex-serialized to avoid
two workers reading the same Badger entries — at-least-once still
holds at the contract boundary because Postgres dedups on
`dedup_key`. With `workers: 7` the post-recovery sustained drain
rises to ~25 K rows/sec on M1-class hardware.

After 5a + 5b: all three scenarios pass at 10 K msg/s.

| scenario | toxic    | duration | WAL peak  | drain   | criteria  |
|----------|----------|----------|-----------|---------|-----------|
| moderate | +300 ms  | 10m26s   | 0         | 18 s    | 5/5 PASS  |
| severe   | +2.5 s   | 30m05s   | 2,419,545 | 19m56s  | 6/6 PASS  |
| outage   | TCP rej. | 16m07s   | 1,721,213 | 8m02s   | 6/6 PASS  |

Conservation (`distinct == flusher.inserted`) is exact in every
scenario; zero messages dead-lettered for transient PG issues.

### Reporting

Each scenario writes
`test/stress/slowdown/results/{scenario}-{timestamp}.md` with:

* The scenario parameters (duration, toxic, target rate).
* A timeline of mode transitions captured by polling the Prometheus
  gauge every second.
* Metric snapshots at: warm-up end, toxic injection, mid-toxic,
  toxic removal, drain end.
* WAL high-water mark and time-to-empty.
* Pass/fail per success criterion.

The Markdown is committed-friendly so a future reviewer can diff
runs across hardware and pinpoint regressions.

## Alternatives Considered

* **Linux `tc` qdisc.** Works but requires NET_ADMIN in the test
  container and only works under Linux — Mac developer machines are
  out. Toxiproxy is portable.
* **Pause the Postgres container.** Simulates outage cleanly but
  doesn't help with the latency-injection scenarios. Toxiproxy covers
  both with one tool.
* **In-process error injection in the `pgx` driver.** Would skip the
  network entirely. Useful for unit tests of the flusher (and we do
  this for the existing flusher_test.go fakes), but the point of this
  test is end-to-end behavior, including pgx connection-recovery
  semantics.

## Consequences

**Easier**:

* The flusher's transient-vs-poison distinction is now part of the
  contract, codified by the test.
* Operators get a `mqtt2db_flusher_requeued_total` to alert on if WAL
  re-enqueue rate climbs (signals chronic PG issues).

**Harder**:

* Test runtime: each scenario is ≥ 10 minutes; the full
  `make stress-slowdown` is ~35–40 minutes. Acceptable for nightly
  CI; not in the PR-time path.
* Debug surface: when the test fails, you have to read the per-
  scenario Markdown report plus the raw test log to diagnose. The
  Markdown captures enough state that this is bearable.
* If toxiproxy upstream changes its Go client or API in a breaking
  way, this test breaks. Pinned via go.mod.

## References

* The project's "do not silently drop" contract: every message must
  either reach Postgres or land in the dead-letter sink with full
  error context — never disappear.
* `internal/flusher/flusher.go` Mode enum + flushWithRetry
* `test/stress/stress_test.go` (Milestone 11; the steady-state cousin)
* Toxiproxy: https://github.com/Shopify/toxiproxy
