# slowdown stress: oscillation-fast

Started: 2026-05-02T20:22:09Z
Ended:   2026-05-02T20:25:01Z
Duration: 2m52s

## Headline

- Target rate:    2000 msg/s
- Sent:           340000
- Publish errors: 0
- Persisted (rows in PG): 339993
- Distinct dedup_keys: 339993
- Flusher inserted (after ON CONFLICT): 339993
- Dedup collisions: 7 (collapsed by unique index — by design)
- WAL high-water: 41784
- WAL drained fully: true
- Drain time:     2s

## Pass / Fail

- ✅ zero messages dead-lettered (transient PG issues)
- ✅ conservation OK (distinct=339993 >= flusher.inserted=339993, dedup_collisions=7)
- ✅ WAL drained to <1% of peak in 2s
- ✅ no WAL ratcheting across cycles (peak stable / decreasing)
- ✅ mode transitions 2 ≤ 10 (no flapping)

## Per-cycle Stats (oscillation)

| cycle | slow_start_mode | slow_end_mode | clean_end_mode | wal_start | wal_peak | wal_clean_end | inserted | batch@slow_end |
|------:|:----------------|:--------------|:---------------|---------:|--------:|-------------:|--------:|--------------:|
|     1 | normal | critical | critical | 0 | 0 | 0 | 1512 | 5000 |
|     2 | critical | critical | critical | 0 | 18584 | 18648 | 0 | 5000 |
|     3 | critical | critical | normal | 18648 | 41784 | 0 | 178616 | 5000 |

## Mode Transitions

Sampled every second. Rows emitted on mode change OR on a `note` (toxic add/remove, end of phase).

| t (s) | mode | batch | ring | wal | paused | note |
|------:|:-----|------:|-----:|----:|-------:|:-----|
|     0 |  | 0 | 0 | 0 | 0 | warmup start |
|     1 | normal | 1000 | 14 | 0 | 0 |  |
|    20 | normal | 1000 | 0 | 0 | 0 | cycle 1 toxic ON |
|    26 | critical | 5000 | 10584 | 0 | 0 |  |
|    35 | critical | 5000 | 28584 | 0 | 0 | cycle 1 toxic OFF |
|    50 | critical | 5000 | 58584 | 0 | 0 | cycle 2 toxic ON |
|    65 | critical | 5000 | 88584 | 0 | 0 | cycle 2 toxic OFF |
|    80 | critical | 5000 | 100000 | 18584 | 0 | cycle 3 toxic ON |
|    95 | critical | 5000 | 71800 | 41784 | 0 | cycle 3 toxic OFF |
|   102 | normal | 1000 | 0 | 0 | 0 |  |
|   170 | normal | 1000 | 80 | 0 | 0 | load stopped |
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
  buffer.depth                   0
  buffer.dequeued                40000
  buffer.enqueued                40000
  buffer.rejected                0
  buffer.spilled                 0
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 0
  flusher.flushed                709
  flusher.inserted               39903
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
  buffer.depth                   58616
  buffer.dequeued                41416
  buffer.enqueued                100032
  buffer.rejected                0
  buffer.spilled                 0
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             5000
  flusher.dead_lettered          0
  flusher.errors                 6
  flusher.flushed                719
  flusher.inserted               41415
  flusher.mode                   2
  flusher.requeued               0
  flusher.retries                416
  sub.acked                      100032
  sub.paused                     0
  sub.pauses_total               0
  sub.received                   100032
  wal.appended                   0
  wal.drained                    0
```

### cycle_2_end — t+1m20s

```
  buffer.depth                   100000
  buffer.dequeued                41416
  buffer.enqueued                141416
  buffer.rejected                18648
  buffer.spilled                 18648
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             5000
  flusher.dead_lettered          0
  flusher.errors                 6
  flusher.flushed                719
  flusher.inserted               41415
  flusher.mode                   2
  flusher.requeued               0
  flusher.retries                416
  sub.acked                      160064
  sub.paused                     0
  sub.pauses_total               1
  sub.received                   160064
  wal.appended                   18648
  wal.drained                    0
```

### cycle_3_end — t+1m50s

```
  buffer.depth                   80
  buffer.dequeued                178248
  buffer.enqueued                178328
  buffer.rejected                41784
  buffer.spilled                 41784
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 6
  flusher.flushed                1067
  flusher.inserted               220031
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                416
  sub.acked                      220112
  sub.paused                     0
  sub.pauses_total               1
  sub.received                   220112
  wal.appended                   41784
  wal.drained                    41784
```

### drain_complete — t+2m52s

```
  buffer.depth                   0
  buffer.dequeued                298216
  buffer.enqueued                298216
  buffer.rejected                41784
  buffer.spilled                 41784
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 6
  flusher.flushed                3000
  flusher.inserted               339993
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                416
  sub.acked                      340000
  sub.paused                     0
  sub.pauses_total               1
  sub.received                   340000
  wal.appended                   41784
  wal.drained                    41784
```

