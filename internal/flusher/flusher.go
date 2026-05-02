// Package flusher pulls batches from the ring (and the WAL) and pushes
// them to PostgreSQL.
//
// Three modes per CLAUDE.md "Adaptive flusher":
//
//	Mode      Trigger                                   Effect
//	-----     --------                                  ------
//	Normal    p95 latency < ElevatedLatencyThreshold    Default batch / interval.
//	Elevated  p95 latency >= ElevatedLatencyThreshold   Larger batch, longer interval.
//	Critical  p95 latency >= CriticalLatencyThreshold   Stop pulling; only retry the
//	          OR a recent flush returned an error       outstanding batch with backoff.
//
// Recovery: when the recent latency window stays under the elevated
// threshold for `RecoveryWindow`, the flusher reverts to normal.
//
// Retries: a batch that errors is retried up to MaxRetries times with
// exponential backoff (1s, 2s, 4s, 8s, 16s). After the last retry, every
// row in the batch is handed to the dead-letter sink (one S3 object per
// row) and the messages are dropped from this pipeline. The next read
// from the ring/WAL proceeds.
package flusher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/debsahu/mqtt2db-go/internal/buffer"
	"github.com/debsahu/mqtt2db-go/internal/config"
	"github.com/debsahu/mqtt2db-go/internal/metrics"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
)

// Source is the read side of the buffer pipeline (ring + WAL combined or
// either alone). Tests substitute a fake.
type Source interface {
	Dequeue(n int) []postgres.Message
	State() buffer.State
}

// WALSource is the optional WAL handle. The flusher pulls from it after
// the ring is empty in a single tick, and re-enqueues batches into it
// when a flush fails terminally for a transient reason (so the next
// pullBatch retries from the WAL after critical mode lifts).
type WALSource interface {
	Drain(limit int) ([]postgres.Message, error)
	Append(msg postgres.Message) ([]byte, error)
}

// DeadLetter is the sink for messages that exhausted retries.
type DeadLetter interface {
	Write(ctx context.Context, msg postgres.Message, lastErr error) error
}

// Mode is the adaptive state.
type Mode int

const (
	ModeNormal Mode = iota
	ModeElevated
	ModeCritical
)

func (m Mode) String() string {
	switch m {
	case ModeNormal:
		return "normal"
	case ModeElevated:
		return "elevated"
	case ModeCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// Flusher orchestrates the adaptive batch loop.
type Flusher struct {
	cfg    config.FlusherConfig
	pg     postgres.Inserter
	src    Source
	wal    WALSource // optional
	dl     DeadLetter
	m      *metrics.FlusherMetrics
	logger *slog.Logger

	// adaptive state
	mu             sync.Mutex
	mode           Mode
	belowSince     time.Time       // when latency dropped below elevated threshold
	criticalUntil  time.Time       // earliest time we may try again from critical
	latencyWindow  []time.Duration // recent flush latencies for p95
	windowCapacity int             // size of the rolling window

	// walMu serializes WAL.Drain calls across workers so two workers
	// don't read the same key range and double-flush. CopyMessages
	// itself is parallelism-safe (each call acquires its own pool conn,
	// and PG's unique index on dedup_key collapses any incidental
	// duplicates).
	walMu sync.Mutex

	// allow injecting a clock for tests
	now func() time.Time
}

// New constructs a Flusher. dl is required; pass a no-op implementation in
// tests if you want to skip the dead-letter path.
func New(
	cfg config.FlusherConfig,
	pg postgres.Inserter,
	src Source,
	wal WALSource,
	dl DeadLetter,
	m *metrics.FlusherMetrics,
	logger *slog.Logger,
) (*Flusher, error) {
	if pg == nil {
		return nil, errors.New("flusher: postgres inserter required")
	}
	if src == nil {
		return nil, errors.New("flusher: source required")
	}
	if dl == nil {
		return nil, errors.New("flusher: dead-letter sink required")
	}
	if m == nil {
		return nil, errors.New("flusher: metrics required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Flusher{
		cfg:            cfg,
		pg:             pg,
		src:            src,
		wal:            wal,
		dl:             dl,
		m:              m,
		logger:         logger.With("component", "flusher"),
		windowCapacity: 64,
		now:            time.Now,
	}, nil
}

// Run blocks until ctx is canceled. Spawns cfg.Workers parallel flush
// goroutines (default 1). Each worker independently pulls a batch from
// the ring or WAL and runs CopyMessages against its own pgxpool
// connection. Mode transitions and the critical-mode gate stay
// serialized through f.mu, so workers cooperate on global state but
// parallelize the I/O path.
func (f *Flusher) Run(ctx context.Context) error {
	workers := f.cfg.Workers
	if workers <= 0 {
		workers = 1
	}

	f.m.Mode.Set(float64(ModeNormal))
	f.m.BatchSize.Set(float64(f.cfg.BatchSize))
	f.logger.Info("started", "event", "started",
		"batch_size", f.cfg.BatchSize,
		"flush_interval", f.cfg.FlushInterval.AsDuration(),
		"workers", workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.workerLoop(ctx)
		}()
	}
	wg.Wait()
	f.logger.Info("stopped", "event", "stopped")
	return nil
}

// workerLoop is the per-goroutine flush loop. Concurrent calls to
// pullBatch are safe (the ring and WAL are individually thread-safe;
// pullBatch serializes WAL.Drain via f.walMu so two workers don't
// duplicate-read the same key range). flushWithRetry uses its own
// pgxpool acquisition so workers don't queue on a single connection.
func (f *Flusher) workerLoop(ctx context.Context) {
	timer := time.NewTimer(f.flushIntervalForMode())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		if f.modeIs(ModeCritical) && f.now().Before(f.criticalGate()) {
			timer.Reset(f.flushIntervalForMode())
			continue
		}

		batch := f.pullBatch()
		if len(batch) == 0 {
			timer.Reset(f.flushIntervalForMode())
			continue
		}

		f.flushWithRetry(ctx, batch)
		timer.Reset(f.flushIntervalForMode())
	}
}

// pullBatch reads from the ring first and tops up from the WAL if there's
// headroom. Bounded by the active batch size. WAL drains are serialized
// across workers so two parallel flushers don't read the same WAL keys.
func (f *Flusher) pullBatch() []postgres.Message {
	batchSize := f.batchSizeForMode()
	out := f.src.Dequeue(batchSize)
	if f.wal != nil && len(out) < batchSize {
		f.walMu.Lock()
		walBatch, err := f.wal.Drain(batchSize - len(out))
		f.walMu.Unlock()
		if err != nil {
			f.logger.Warn("wal drain failed", "event", "wal_drain_error", "err", err.Error())
		} else {
			out = append(out, walBatch...)
		}
	}
	return out
}

// flushWithRetry calls pg.CopyMessages with backoff; on terminal failure
// hands every message to the dead-letter sink.
//
// Note: we do NOT wrap each attempt with an extra context timeout. pgx
// already enforces `connect_timeout` for new connections and TCP-level
// keep-alives for existing ones. Adding a layer-7 timeout on top causes
// false retries during slow-but-healthy recovery (every flush that
// takes more than the chosen threshold gets a context.DeadlineExceeded
// even if PG would have completed it shortly after). Mode transitions
// are driven by recordLatency() observing actual completed flush
// durations — that handles "slow but working" correctly.
func (f *Flusher) flushWithRetry(ctx context.Context, batch []postgres.Message) {
	delay := time.Second
	var lastErr error
	for attempt := 0; attempt <= f.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			f.m.RetriesTotal.Add(float64(len(batch)))
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			delay *= 2
		}

		start := f.now()
		inserted, err := f.pg.CopyMessages(ctx, batch)
		latency := f.now().Sub(start)
		f.m.FlushLatency.Observe(latency.Seconds())

		if err == nil {
			f.m.Flushed.Inc()
			f.m.Inserted.Add(float64(inserted))
			// Order matters: maybeRecover *first* clears critical-from-
			// error so subsequent recordLatency can promote based on the
			// actual measured latency. The previous order
			// (recordLatency → maybeRecover) caused mode to oscillate
			// elevated→critical→elevated for every successful but slow
			// flush, because maybeRecover would always undo a fresh
			// latency-based critical promotion. Net result: the
			// gauge reads "elevated" between flushes even when p95 is
			// well above the critical threshold.
			f.maybeRecover()
			f.recordLatency(latency)
			return
		}

		lastErr = err
		f.m.FlushErrors.Inc()
		f.logger.Warn("flush error",
			"event", "flush_error",
			"attempt", attempt,
			"batch", len(batch),
			"err", err.Error())
		f.enterCritical()
	}

	// Out of retries. Distinguish transient (connection/latency) errors
	// from poison (per-row constraint or data-exception) errors:
	//
	//   • transient + WAL available -> push the batch back into the WAL
	//     and let the next critical-mode-recovery cycle pick it up. This
	//     is the at-least-once contract: connection blips must NOT
	//     terminate as DLQ. See docs/adr/0005-sustained-slowdown-stress-test.md.
	//   • poison, or transient with no WAL -> dead-letter, because
	//     replaying the same payload will hit the same error.
	if !isPoisonError(lastErr) && f.wal != nil {
		f.logger.Warn("retries exhausted; re-enqueuing batch to WAL",
			"event", "requeue_to_wal",
			"batch", len(batch),
			"err", lastErr.Error())
		var requeueErr error
		for _, msg := range batch {
			if _, err := f.wal.Append(msg); err != nil {
				requeueErr = err
				break
			}
			f.m.Requeued.Inc()
		}
		if requeueErr == nil {
			return
		}
		// WAL itself is failing — fall through to dead-letter as last resort.
		f.logger.Error("WAL re-enqueue failed; dead-lettering instead",
			"event", "wal_requeue_failed",
			"err", requeueErr.Error())
	}

	f.logger.Error("retries exhausted; dead-lettering batch",
		"event", "dead_letter",
		"batch", len(batch),
		"err", lastErr.Error())
	for _, msg := range batch {
		if err := f.dl.Write(ctx, msg, lastErr); err != nil {
			f.logger.Error("dead-letter write failed",
				"event", "dead_letter_write_error",
				"dedup_key", msg.DedupKey,
				"err", err.Error())
			continue
		}
		f.m.DeadLettered.Inc()
	}
}

// isPoisonError reports whether err is a deterministic per-row failure
// that will not succeed on replay regardless of how long we wait.
// PostgreSQL SQLSTATE classes:
//
//	22xxx  Data Exception           (e.g. invalid encoding)
//	23xxx  Integrity Constraint     (e.g. dedup_key collision is not
//	                                 here — it's collapsed by ON CONFLICT
//	                                 before we ever see it; CHECK / FK
//	                                 violations would be)
//
// Everything else (connect refused, timeout, network unreachable, server
// shutdown) is transient and worth retrying via the WAL.
func isPoisonError(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "23") {
			return true
		}
	}
	return false
}

// --- mode + latency tracking ------------------------------------------------

func (f *Flusher) modeIs(want Mode) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mode == want
}

func (f *Flusher) criticalGate() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.criticalUntil
}

func (f *Flusher) batchSizeForMode() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mode == ModeNormal {
		return f.cfg.BatchSize
	}
	return f.cfg.ElevatedBatchSize
}

func (f *Flusher) flushIntervalForMode() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mode == ModeNormal {
		return f.cfg.FlushInterval.AsDuration()
	}
	return f.cfg.ElevatedFlushInterval.AsDuration()
}

func (f *Flusher) recordLatency(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.latencyWindow = append(f.latencyWindow, d)
	if len(f.latencyWindow) > f.windowCapacity {
		f.latencyWindow = f.latencyWindow[len(f.latencyWindow)-f.windowCapacity:]
	}

	p95 := f.p95Locked()

	switch f.mode {
	case ModeNormal:
		if p95 >= f.cfg.ElevatedLatencyThreshold.AsDuration() {
			f.setModeLocked(ModeElevated)
		}
	case ModeElevated:
		switch {
		case p95 >= f.cfg.CriticalLatencyThreshold.AsDuration():
			f.setModeLocked(ModeCritical)
		case p95 < f.cfg.ElevatedLatencyThreshold.AsDuration():
			if f.belowSince.IsZero() {
				f.belowSince = f.now()
			} else if f.now().Sub(f.belowSince) >= f.cfg.RecoveryWindow.AsDuration() {
				f.setModeLocked(ModeNormal)
				f.belowSince = time.Time{}
			}
		default:
			f.belowSince = time.Time{}
		}
	case ModeCritical:
		if p95 < f.cfg.ElevatedLatencyThreshold.AsDuration() {
			f.setModeLocked(ModeElevated)
		}
	}
}

// maybeRecover lets a successful flush in critical mode immediately
// downgrade to elevated, since the gate was an error rather than latency.
func (f *Flusher) maybeRecover() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mode == ModeCritical {
		f.setModeLocked(ModeElevated)
	}
}

func (f *Flusher) enterCritical() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setModeLocked(ModeCritical)
	f.criticalUntil = f.now().Add(f.cfg.RecoveryWindow.AsDuration())
}

func (f *Flusher) setModeLocked(m Mode) {
	if f.mode == m {
		return
	}
	f.mode = m
	f.m.Mode.Set(float64(m))
	switch m {
	case ModeNormal:
		f.m.BatchSize.Set(float64(f.cfg.BatchSize))
	case ModeElevated, ModeCritical:
		f.m.BatchSize.Set(float64(f.cfg.ElevatedBatchSize))
	}
	f.logger.Info("mode change", "event", "mode_change", "mode", m.String())
}

// p95Locked computes the p95 of the rolling window. Caller must hold mu.
// Implementation is the simple "sort copy, pick index" — the window cap
// is small (64) so allocation is cheap and we avoid pulling in a heavy
// percentile library.
func (f *Flusher) p95Locked() time.Duration {
	n := len(f.latencyWindow)
	if n == 0 {
		return 0
	}
	cp := make([]time.Duration, n)
	copy(cp, f.latencyWindow)
	insertionSort(cp)
	idx := int(0.95 * float64(n))
	if idx >= n {
		idx = n - 1
	}
	return cp[idx]
}

// insertionSort: tiny window, simpler than pulling sort.Slice and a closure.
func insertionSort(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j-1] > d[j]; j-- {
			d[j-1], d[j] = d[j], d[j-1]
		}
	}
}

// Mode returns the current adaptive mode. Useful for tests.
func (f *Flusher) Mode() Mode {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mode
}

// guard against unused import in version-skew situations.
var _ = fmt.Sprintf
