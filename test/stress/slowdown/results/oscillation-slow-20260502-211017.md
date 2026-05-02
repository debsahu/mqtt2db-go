# slowdown stress: oscillation-slow

Started: 2026-05-02T21:10:17Z
Ended:   2026-05-02T21:16:49Z
Duration: 6m31s

## Headline

- Schema:         wide (4096-byte payloads)
- Target rate:    2000 msg/s
- Sent:           700005
- Publish errors: 0
- Persisted (rows in PG): 697998
- Distinct dedup_keys: 697998
- Flusher inserted (after ON CONFLICT): 697998
- Dedup collisions: 0 (collapsed by unique index — by design)
- WAL high-water: 142164
- WAL drained fully: true
- Drain time:     39s

## Throughput

- rows/sec:  1785
- bytes/sec: 7310545 (~6.97 MB/s payload-only)

## Pass / Fail

- ✅ zero messages dead-lettered (transient PG issues)
- ✅ conservation OK (distinct=697998 >= flusher.inserted=697998, dedup_collisions=0)
- ✅ WAL drained to <1% of peak in 39s
- ✅ no WAL ratcheting across cycles (peak stable / decreasing)
- ✅ mode transitions 7 ≤ 10 (no flapping)

## Per-cycle Stats (oscillation)

| cycle | slow_start_mode | slow_end_mode | clean_end_mode | wal_start | wal_peak | wal_clean_end | inserted | batch@slow_end |
|------:|:----------------|:--------------|:---------------|---------:|--------:|-------------:|--------:|--------------:|
|     1 | normal | critical | critical | 0 | 134196 | 124464 | 15400 | 5000 |
|     2 | critical | critical | critical | 124464 | 142164 | 12896 | 310000 | 5000 |
|     3 | critical | critical | critical | 12896 | 53324 | 53324 | 125000 | 5000 |

## Mode Transitions

Sampled every second. Rows emitted on mode change OR on a `note` (toxic add/remove, end of phase).

| t (s) | mode | batch | ring | wal | paused | note |
|------:|:-----|------:|-----:|----:|-------:|:-----|
|     0 |  | 0 | 0 | 0 | 0 | warmup start |
|     1 | normal | 1000 | 96 | 0 | 0 |  |
|    20 | normal | 1000 | 0 | 0 | 0 | cycle 1 toxic ON |
|    26 | critical | 5000 | 11760 | 0 | 0 |  |
|    33 | elevated | 5000 | 24900 | 860 | 0 |  |
|    39 | critical | 5000 | 20016 | 5277 | 0 |  |
|    42 | elevated | 5000 | 20472 | 5277 | 0 |  |
|    47 | critical | 5000 | 24900 | 13860 | 0 |  |
|    65 | critical | 5000 | 24900 | 49816 | 0 | cycle 1 toxic OFF |
|   110 | critical | 5000 | 5008 | 124464 | 0 | cycle 2 toxic ON |
|   155 | critical | 5000 | 10521 | 139216 | 0 | cycle 2 toxic OFF |
|   200 | critical | 5000 | 576 | 12896 | 0 | cycle 3 toxic ON |
|   246 | critical | 5000 | 24900 | 16860 | 0 | cycle 3 toxic OFF |
|   351 | critical | 5000 | 10745 | 61933 | 0 | load stopped |
|   352 | elevated | 5000 | 10745 | 61933 | 0 |  |
|   353 | critical | 5000 | 745 | 61933 | 0 |  |
|   390 | critical | 5000 | 0 | 0 | 0 | drain complete |

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
  flusher.flushed                858
  flusher.inserted               39840
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

### cycle_1_end — t+1m50s

```
  buffer.depth                   5281
  buffer.dequeued                80508
  buffer.enqueued                85789
  buffer.rejected                134196
  buffer.spilled                 134196
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             5000
  flusher.dead_lettered          0
  flusher.errors                 4
  flusher.flushed                868
  flusher.inserted               55240
  flusher.mode                   2
  flusher.requeued               0
  flusher.retries                128
  sub.acked                      219985
  sub.paused                     0
  sub.pauses_total               3
  sub.received                   219985
  wal.appended                   134196
  wal.drained                    9732
```

### cycle_2_end — t+3m21s

```
  buffer.depth                   1592
  buffer.dequeued                247528
  buffer.enqueued                249120
  buffer.rejected                151896
  buffer.spilled                 151896
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             5000
  flusher.dead_lettered          0
  flusher.errors                 4
  flusher.flushed                930
  flusher.inserted               365240
  flusher.mode                   2
  flusher.requeued               0
  flusher.retries                128
  sub.acked                      401016
  sub.paused                     0
  sub.pauses_total               5
  sub.received                   401016
  wal.appended                   151896
  wal.drained                    139000
```

### cycle_3_end — t+4m51s

```
  buffer.depth                   13112
  buffer.dequeued                364977
  buffer.enqueued                378089
  buffer.rejected                203999
  buffer.spilled                 203999
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             5000
  flusher.dead_lettered          0
  flusher.errors                 4
  flusher.flushed                955
  flusher.inserted               490240
  flusher.mode                   2
  flusher.requeued               0
  flusher.retries                128
  sub.acked                      582088
  sub.paused                     0
  sub.pauses_total               11
  sub.received                   582088
  wal.appended                   203999
  wal.drained                    150675
```

### drain_complete — t+6m30s

```
  buffer.depth                   0
  buffer.dequeued                467015
  buffer.enqueued                467015
  buffer.rejected                230983
  buffer.spilled                 230983
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             5000
  flusher.dead_lettered          0
  flusher.errors                 4
  flusher.flushed                997
  flusher.inserted               697998
  flusher.mode                   2
  flusher.requeued               0
  flusher.retries                128
  sub.acked                      697998
  sub.paused                     0
  sub.pauses_total               13
  sub.received                   697998
  wal.appended                   230983
  wal.drained                    230983
```

