# slowdown stress: oscillation-fast

Started: 2026-05-02T21:53:33Z
Ended:   2026-05-02T21:56:25Z
Duration: 2m52s

## Headline

- Schema:         minimal (128-byte payloads)
- Target rate:    2000 msg/s
- Sent:           340000
- Publish errors: 0
- Persisted (rows in PG): 339993
- Distinct dedup_keys: 339993
- Flusher inserted (after ON CONFLICT): 339993
- Dedup collisions: 7 (collapsed by unique index — by design)
- WAL high-water: 42512
- WAL drained fully: true
- Drain time:     2s

## Throughput

- rows/sec:  1972
- bytes/sec: 252380 (~0.24 MB/s payload-only)

## Pass / Fail

- ✅ zero messages dead-lettered (transient PG issues)
- ✅ conservation OK (distinct=339993 >= flusher.inserted=339993, dedup_collisions=7)
- ✅ WAL drained to <1% of peak in 2s
- ✅ no WAL ratcheting across cycles (peak stable / decreasing)
- ✅ mode transitions 2 ≤ 16 (no flapping; 100 ms sampled)
- ✅ slow_end mode reached critical (≥ elevated expected)

## Per-cycle Stats (oscillation)

| cycle | slow_start_mode | slow_end_mode | clean_end_mode | wal_start | wal_peak | wal_clean_end | inserted | batch@slow_end |
|------:|:----------------|:--------------|:---------------|---------:|--------:|-------------:|--------:|--------------:|
|     1 | normal | critical | critical | 0 | 0 | 0 | 795 | 5000 |
|     2 | critical | critical | critical | 0 | 19416 | 19416 | 0 | 5000 |
|     3 | critical | critical | normal | 19419 | 42512 | 0 | 179214 | 5000 |

## Mode Transitions

Sampled every second. Rows emitted on mode change OR on a `note` (toxic add/remove, end of phase).

| t (s) | mode | batch | ring | wal | paused | note |
|------:|:-----|------:|-----:|----:|-------:|:-----|
|     0 |  | 0 | 0 | 0 | 0 | warmup start |
|     1 | normal | 1000 | 144 | 0 | 0 |  |
|    20 | normal | 1000 | 16 | 0 | 0 | cycle 1 toxic ON |
|    26 | critical | 5000 | 11376 | 0 | 0 |  |
|    35 | critical | 5000 | 29376 | 0 | 0 | cycle 1 toxic OFF |
|    50 | critical | 5000 | 59376 | 0 | 0 | cycle 2 toxic ON |
|    65 | critical | 5000 | 89376 | 0 | 0 | cycle 2 toxic OFF |
|    80 | critical | 5000 | 100000 | 19368 | 0 | cycle 3 toxic ON |
|    95 | critical | 5000 | 71864 | 42512 | 0 | cycle 3 toxic OFF |
|   102 | normal | 1000 | 16 | 0 | 0 |  |
|   170 | normal | 1000 | 0 | 0 | 0 | load stopped |
|   172 | normal | 1000 | 0 | 0 | 0 | drain complete |

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
  buffer.depth                   16
  buffer.dequeued                39984
  buffer.enqueued                40000
  buffer.rejected                0
  buffer.spilled                 0
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 0
  flusher.flushed                716
  flusher.inserted               39829
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                0
  sub.acked                      40000
  sub.paused                     0
  sub.pauses_total               0
  sub.received                   40000
  wal.appended                   0
  wal.drained                    0
```

### cycle_1_end — t+50s

```
  buffer.depth                   59393
  buffer.dequeued                40624
  buffer.enqueued                100017
  buffer.rejected                0
  buffer.spilled                 0
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             5000
  flusher.dead_lettered          0
  flusher.errors                 2
  flusher.flushed                724
  flusher.inserted               40624
  flusher.mode                   2
  flusher.requeued               0
  flusher.retries                384
  sub.acked                      100017
  sub.paused                     0
  sub.pauses_total               0
  sub.received                   100017
  wal.appended                   0
  wal.drained                    0
```

### cycle_2_end — t+1m20s

```
  buffer.depth                   100000
  buffer.dequeued                40624
  buffer.enqueued                140624
  buffer.rejected                19418
  buffer.spilled                 19418
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             5000
  flusher.dead_lettered          0
  flusher.errors                 2
  flusher.flushed                724
  flusher.inserted               40624
  flusher.mode                   2
  flusher.requeued               0
  flusher.retries                384
  sub.acked                      160042
  sub.paused                     0
  sub.pauses_total               1
  sub.received                   160042
  wal.appended                   19418
  wal.drained                    0
```

### cycle_3_end — t+1m50s

```
  buffer.depth                   0
  buffer.dequeued                177552
  buffer.enqueued                177552
  buffer.rejected                42512
  buffer.spilled                 42512
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 2
  flusher.flushed                1057
  flusher.inserted               219838
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                384
  sub.acked                      220064
  sub.paused                     0
  sub.pauses_total               2
  sub.received                   220064
  wal.appended                   42512
  wal.drained                    42512
```

### drain_complete — t+2m52s

```
  buffer.depth                   0
  buffer.dequeued                297488
  buffer.enqueued                297488
  buffer.rejected                42512
  buffer.spilled                 42512
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 2
  flusher.flushed                2964
  flusher.inserted               339993
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                384
  sub.acked                      340000
  sub.paused                     0
  sub.pauses_total               2
  sub.received                   340000
  wal.appended                   42512
  wal.drained                    42512
```

