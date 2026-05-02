# slowdown stress: oscillation-slow

Started: 2026-05-02T21:56:36Z
Ended:   2026-05-02T22:09:38Z
Duration: 8m54s

## Headline

- Schema:         minimal (128-byte payloads)
- Target rate:    2000 msg/s
- Sent:           1060003
- Publish errors: 0
- Persisted (rows in PG): 1059957
- Distinct dedup_keys: 1059957
- Flusher inserted (after ON CONFLICT): 1059957
- Dedup collisions: 46 (collapsed by unique index — by design)
- WAL high-water: 85032
- WAL drained fully: true
- Drain time:     3s

## Throughput

- rows/sec:  1987
- bytes/sec: 254276 (~0.24 MB/s payload-only)

## Pass / Fail

- ✅ zero messages dead-lettered (transient PG issues)
- ✅ conservation OK (distinct=1059957 >= flusher.inserted=1059957, dedup_collisions=46)
- ✅ WAL drained to <1% of peak in 3s
- ✅ no WAL ratcheting across cycles (peak stable / decreasing)
- ✅ mode transitions 12 ≤ 16 (no flapping; 100 ms sampled)
- ✅ slow_end mode reached critical (≥ elevated expected)
- ✅ clean_end mode recovered to normal by cycle 2

## Per-cycle Stats (oscillation)

| cycle | slow_start_mode | slow_end_mode | clean_end_mode | wal_start | wal_peak | wal_clean_end | inserted | batch@slow_end |
|------:|:----------------|:--------------|:---------------|---------:|--------:|-------------:|--------:|--------------:|
|     1 | normal | critical | normal | 0 | 85032 | 0 | 299929 | 5000 |
|     2 | normal | critical | normal | 0 | 0 | 0 | 300042 | 5000 |
|     3 | normal | critical | normal | 0 | 0 | 0 | 300074 | 5000 |

## Mode Transitions

Sampled every second. Rows emitted on mode change OR on a `note` (toxic add/remove, end of phase).

| t (s) | mode | batch | ring | wal | paused | note |
|------:|:-----|------:|-----:|----:|-------:|:-----|
|     0 |  | 0 | 0 | 0 | 0 | warmup start |
|     1 | normal | 1000 | 0 | 0 | 0 |  |
|    19 | normal | 1000 | 65 | 0 | 0 | warmup end |
|    20 | normal | 1000 | 1 | 0 | 0 | cycle 1 toxic ON |
|    26 | critical | 5000 | 11408 | 0 | 0 |  |
|    28 | elevated | 5000 | 15408 | 0 | 0 |  |
|    32 | critical | 5000 | 18408 | 0 | 0 |  |
|    41 | elevated | 5000 | 26408 | 0 | 0 |  |
|    46 | critical | 5000 | 36408 | 0 | 0 |  |
|    95 | critical | 5000 | 100000 | 34408 | 0 | cycle 1 toxic OFF |
|   127 | normal | 1000 | 96 | 0 | 0 |  |
|   170 | normal | 1000 | 64 | 0 | 0 | cycle 2 toxic ON |
|   196 | critical | 5000 | 25680 | 0 | 0 |  |
|   245 | critical | 5000 | 48680 | 0 | 0 | cycle 2 toxic OFF |
|   252 | elevated | 5000 | 0 | 0 | 0 |  |
|   313 | normal | 1000 | 79 | 0 | 0 |  |
|   320 | normal | 1000 | 10 | 0 | 0 | cycle 3 toxic ON |
|   346 | critical | 5000 | 20632 | 0 | 0 |  |
|   395 | critical | 5000 | 48632 | 0 | 0 | cycle 3 toxic OFF |
|   402 | elevated | 5000 | 480 | 0 | 0 |  |
|   463 | normal | 1000 | 0 | 0 | 0 |  |
|   530 | normal | 1000 | 48 | 0 | 0 | load stopped |
|   533 | normal | 1000 | 0 | 0 | 0 | drain complete |

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
  buffer.depth                   1
  buffer.dequeued                40000
  buffer.enqueued                40001
  buffer.rejected                0
  buffer.spilled                 0
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 0
  flusher.flushed                720
  flusher.inserted               40000
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                0
  sub.acked                      40001
  sub.paused                     0
  sub.pauses_total               0
  sub.received                   40001
  wal.appended                   0
  wal.drained                    0
```

### cycle_1_end — t+2m50s

```
  buffer.depth                   96
  buffer.dequeued                254904
  buffer.enqueued                255000
  buffer.rejected                85032
  buffer.spilled                 85032
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 13
  flusher.flushed                2954
  flusher.inserted               339929
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                1024
  sub.acked                      340032
  sub.paused                     0
  sub.pauses_total               2
  sub.received                   340032
  wal.appended                   85032
  wal.drained                    85032
```

### cycle_2_end — t+5m20s

```
  buffer.depth                   90
  buffer.dequeued                554958
  buffer.enqueued                555048
  buffer.rejected                85032
  buffer.spilled                 85032
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 13
  flusher.flushed                4059
  flusher.inserted               639971
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                1024
  sub.acked                      640080
  sub.paused                     0
  sub.pauses_total               2
  sub.received                   640080
  wal.appended                   85032
  wal.drained                    85032
```

### cycle_3_end — t+7m50s

```
  buffer.depth                   16
  buffer.dequeued                855064
  buffer.enqueued                855080
  buffer.rejected                85032
  buffer.spilled                 85032
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 13
  flusher.flushed                5150
  flusher.inserted               940045
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                1024
  sub.acked                      940112
  sub.paused                     0
  sub.pauses_total               2
  sub.received                   940112
  wal.appended                   85032
  wal.drained                    85032
```

### drain_complete — t+8m53s

```
  buffer.depth                   0
  buffer.dequeued                974971
  buffer.enqueued                974971
  buffer.rejected                85032
  buffer.spilled                 85032
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 13
  flusher.flushed                8162
  flusher.inserted               1059957
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                1024
  sub.acked                      1060003
  sub.paused                     0
  sub.pauses_total               2
  sub.received                   1060003
  wal.appended                   85032
  wal.drained                    85032
```

