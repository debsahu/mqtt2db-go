# slowdown stress: oscillation-slow

Started: 2026-05-02T20:25:11Z
Ended:   2026-05-02T20:31:04Z
Duration: 5m53s

## Headline

- Target rate:    2000 msg/s
- Sent:           700013
- Publish errors: 0
- Persisted (rows in PG): 699990
- Distinct dedup_keys: 699990
- Flusher inserted (after ON CONFLICT): 699990
- Dedup collisions: 23 (collapsed by unique index — by design)
- WAL high-water: 59156
- WAL drained fully: true
- Drain time:     2s

## Pass / Fail

- ✅ zero messages dead-lettered (transient PG issues)
- ✅ conservation OK (distinct=699990 >= flusher.inserted=699990, dedup_collisions=23)
- ✅ WAL drained to <1% of peak in 2s
- ✅ no WAL ratcheting across cycles (peak stable / decreasing)
- ✅ mode transitions 7 ≤ 10 (no flapping)

## Per-cycle Stats (oscillation)

| cycle | slow_start_mode | slow_end_mode | clean_end_mode | wal_start | wal_peak | wal_clean_end | inserted | batch@slow_end |
|------:|:----------------|:--------------|:---------------|---------:|--------:|-------------:|--------:|--------------:|
|     1 | normal | critical | critical | 0 | 59156 | 23940 | 155609 | 5000 |
|     2 | critical | critical | normal | 23940 | 56048 | 0 | 204545 | 5000 |
|     3 | normal | critical | elevated | 0 | 0 | 0 | 179400 | 5000 |

## Mode Transitions

Sampled every second. Rows emitted on mode change OR on a `note` (toxic add/remove, end of phase).

| t (s) | mode | batch | ring | wal | paused | note |
|------:|:-----|------:|-----:|----:|-------:|:-----|
|     0 |  | 0 | 0 | 0 | 0 | warmup start |
|     1 | normal | 1000 | 176 | 0 | 0 |  |
|    20 | normal | 1000 | 0 | 0 | 0 | cycle 1 toxic ON |
|    26 | critical | 5000 | 11580 | 0 | 0 |  |
|    33 | elevated | 5000 | 25580 | 0 | 0 |  |
|    38 | critical | 5000 | 20580 | 0 | 0 |  |
|    65 | critical | 5000 | 74580 | 0 | 0 | cycle 1 toxic OFF |
|   110 | critical | 5000 | 640 | 23940 | 0 | cycle 2 toxic ON |
|   155 | critical | 5000 | 89024 | 0 | 0 | cycle 2 toxic OFF |
|   195 | normal | 1000 | 192 | 0 | 0 |  |
|   200 | normal | 1000 | 32 | 0 | 0 | cycle 3 toxic ON |
|   226 | critical | 5000 | 20792 | 0 | 0 |  |
|   245 | critical | 5000 | 53792 | 0 | 0 | cycle 3 toxic OFF |
|   252 | elevated | 5000 | 368 | 0 | 0 |  |
|   313 | normal | 1000 | 0 | 0 | 0 |  |
|   350 | normal | 1000 | 64 | 0 | 0 | load stopped |
|   352 | normal | 1000 | 0 | 0 | 0 | drain complete |

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
  buffer.dequeued                39984
  buffer.enqueued                39984
  buffer.rejected                0
  buffer.spilled                 0
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 0
  flusher.flushed                682
  flusher.inserted               39808
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                0
  sub.acked                      39984
  sub.paused                     0
  sub.pauses_total               0
  sub.received                   39984
  wal.appended                   0
  wal.drained                    0
```

### cycle_1_end — t+1m50s

```
  buffer.depth                   672
  buffer.dequeued                160204
  buffer.enqueued                160876
  buffer.rejected                59156
  buffer.spilled                 59156
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             5000
  flusher.dead_lettered          0
  flusher.errors                 7
  flusher.flushed                720
  flusher.inserted               195417
  flusher.mode                   2
  flusher.requeued               0
  flusher.retries                656
  sub.acked                      220032
  sub.paused                     0
  sub.pauses_total               1
  sub.received                   220032
  wal.appended                   59156
  wal.drained                    35216
```

### cycle_2_end — t+3m20s

```
  buffer.depth                   80
  buffer.dequeued                284764
  buffer.enqueued                284844
  buffer.rejected                115204
  buffer.spilled                 115204
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 13
  flusher.flushed                1062
  flusher.inserted               399962
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                2324
  sub.acked                      400048
  sub.paused                     0
  sub.pauses_total               3
  sub.received                   400048
  wal.appended                   115204
  wal.drained                    115204
```

### cycle_3_end — t+4m50s

```
  buffer.depth                   720
  buffer.dequeued                464172
  buffer.enqueued                464892
  buffer.rejected                115204
  buffer.spilled                 115204
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             5000
  flusher.dead_lettered          0
  flusher.errors                 13
  flusher.flushed                1564
  flusher.inserted               579362
  flusher.mode                   1
  flusher.requeued               0
  flusher.retries                2324
  sub.acked                      580096
  sub.paused                     0
  sub.pauses_total               3
  sub.received                   580096
  wal.appended                   115204
  wal.drained                    115204
```

### drain_complete — t+5m53s

```
  buffer.depth                   0
  buffer.dequeued                584809
  buffer.enqueued                584809
  buffer.rejected                115204
  buffer.spilled                 115204
  dlq.errors                     0
  dlq.written                    0
  flusher.batch_size             1000
  flusher.dead_lettered          0
  flusher.errors                 13
  flusher.flushed                3824
  flusher.inserted               699990
  flusher.mode                   0
  flusher.requeued               0
  flusher.retries                2324
  sub.acked                      700013
  sub.paused                     0
  sub.pauses_total               3
  sub.received                   700013
  wal.appended                   115204
  wal.drained                    115204
```

