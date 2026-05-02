# slowdown stress: oscillation-fast

Started: 2026-05-02T21:07:13Z
Ended:   2026-05-02T21:10:06Z
Duration: 2m53s

## Headline

- Schema:         wide (4096-byte payloads)
- Target rate:    2000 msg/s
- Sent:           340008
- Publish errors: 0
- Persisted (rows in PG): 340007
- Distinct dedup_keys: 340007
- Flusher inserted (after ON CONFLICT): 340007
- Dedup collisions: 1 (collapsed by unique index — by design)
- WAL high-water: 111700
- WAL drained fully: true
- Drain time:     2s

## Throughput

- rows/sec:  1967
- bytes/sec: 8056999 (~7.68 MB/s payload-only)

## Pass / Fail

- ✅ zero messages dead-lettered (transient PG issues)
- ✅ conservation OK (distinct=340007 >= flusher.inserted=340007, dedup_collisions=1)
- ✅ WAL drained to <1% of peak in 2s
- ✅ no WAL ratcheting across cycles (peak stable / decreasing)
- ✅ mode transitions 4 ≤ 10 (no flapping)

## Per-cycle Stats (oscillation)

| cycle | slow_start_mode | slow_end_mode | clean_end_mode | wal_start | wal_peak | wal_clean_end | inserted | batch@slow_end |
|------:|:----------------|:--------------|:---------------|---------:|--------:|-------------:|--------:|--------------:|
|     1 | normal | critical | critical | 0 | 29700 | 29892 | 5409 | 5000 |
|     2 | critical | critical | critical | 29892 | 89700 | 90036 | 0 | 5000 |
|     3 | critical | critical | critical | 90036 | 111700 | 0 | 173640 | 5000 |

## Mode Transitions

Sampled every second. Rows emitted on mode change OR on a `note` (toxic add/remove, end of phase).

| t (s) | mode | batch | ring | wal | paused | note |
|------:|:-----|------:|-----:|----:|-------:|:-----|
|     0 |  | 0 | 0 | 0 | 0 | warmup start |
|     1 | normal | 1000 | 112 | 0 | 0 |  |
|    19 | normal | 1000 | 0 | 0 | 0 | warmup end |
|    20 | normal | 1000 | 5 | 0 | 0 | cycle 1 toxic ON |
|    26 | critical | 5000 | 11600 | 0 | 0 |  |
|    28 | elevated | 5000 | 15600 | 0 | 0 |  |
|    32 | critical | 5000 | 18600 | 0 | 0 |  |
|    35 | critical | 5000 | 24600 | 0 | 1 | cycle 1 toxic OFF |
|    50 | critical | 5000 | 24900 | 29700 | 0 | cycle 2 toxic ON |
|    65 | critical | 5000 | 24900 | 59700 | 0 | cycle 2 toxic OFF |
|    80 | critical | 5000 | 24900 | 89700 | 0 | cycle 3 toxic ON |
|    95 | critical | 5000 | 6672 | 102928 | 0 | cycle 3 toxic OFF |
|   112 | elevated | 5000 | 48 | 0 | 0 |  |
|   170 | elevated | 5000 | 320 | 0 | 0 | load stopped |
|   172 | elevated | 5000 | 0 | 0 | 0 | drain complete |

## Metric Snapshots

### warmup_begin — t+0s

```
  buffer.depth                   0
  buffer.dequeued                0
  buffer.enqueued                0
  buffer.rejected                0
  buffer.spilled                 0
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 0
  flusher.flushed                0
  flusher.inserted               0
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                0
  sub.acked                      0
  sub.paused                     0
  sub.pauses_total               0
  sub.received                   0
  wal.appended                   0
  wal.drained                    0
```

### warmup_end — t+20s

```
  buffer.depth                   5
  buffer.dequeued                40000
  buffer.enqueued                40005
  buffer.rejected                0
  buffer.spilled                 0
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 0
  flusher.flushed                813
  flusher.inserted               39991
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                0
  sub.acked                      40005
  sub.paused                     0
  sub.pauses_total               0
  sub.received                   40005
  wal.appended                   0
  wal.drained                    0
```

### cycle_1_end — t+50s

```
  buffer.depth                   24900
  buffer.dequeued                45400
  buffer.enqueued                70300
  buffer.rejected                29892
  buffer.spilled                 29892
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             5000
  flusher.dead_lettered          0
  flusher.errors                 4
  flusher.flushed                821
  flusher.inserted               45400
  flusher.mode                   2
  flusher.requeued               0
  flusher.retries                416
  sub.acked                      100192
  sub.paused                     0
  sub.pauses_total               1
  sub.received                   100192
  wal.appended                   29892
  wal.drained                    0
```

### cycle_2_end — t+1m20s

```
  buffer.depth                   24900
  buffer.dequeued                45400
  buffer.enqueued                70300
  buffer.rejected                90036
  buffer.spilled                 90036
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             5000
  flusher.dead_lettered          0
  flusher.errors                 4
  flusher.flushed                821
  flusher.inserted               45400
  flusher.mode                   2
  flusher.requeued               0
  flusher.retries                416
  sub.acked                      160336
  sub.paused                     0
  sub.pauses_total               1
  sub.received                   160336
  wal.appended                   90036
  wal.drained                    0
```

### cycle_3_end — t+1m50s

```
  buffer.depth                   128
  buffer.dequeued                107323
  buffer.enqueued                107451
  buffer.rejected                112917
  buffer.spilled                 112917
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             5000
  flusher.dead_lettered          0
  flusher.errors                 4
  flusher.flushed                880
  flusher.inserted               219040
  flusher.mode                   2
  flusher.requeued               0
  flusher.retries                416
  sub.acked                      220368
  sub.paused                     0
  sub.pauses_total               1
  sub.received                   220368
  wal.appended                   112917
  wal.drained                    112917
```

### drain_complete — t+2m53s

```
  buffer.depth                   0
  buffer.dequeued                227091
  buffer.enqueued                227091
  buffer.rejected                112917
  buffer.spilled                 112917
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             5000
  flusher.dead_lettered          0
  flusher.errors                 4
  flusher.flushed                1221
  flusher.inserted               340007
  flusher.mode                   1
  flusher.requeued               0
  flusher.retries                416
  sub.acked                      340008
  sub.paused                     0
  sub.pauses_total               1
  sub.received                   340008
  wal.appended                   112917
  wal.drained                    112917
```

