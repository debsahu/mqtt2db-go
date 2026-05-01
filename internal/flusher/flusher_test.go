package flusher_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/debsahu/mqtt2db-go/internal/buffer"
	"github.com/debsahu/mqtt2db-go/internal/config"
	"github.com/debsahu/mqtt2db-go/internal/flusher"
	"github.com/debsahu/mqtt2db-go/internal/metrics"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
)

// fakeInserter records calls and lets tests sequence success/failure.
type fakeInserter struct {
	mu       sync.Mutex
	calls    [][]postgres.Message
	results  []fakeResult
	resIdx   int
	maxCalls int
	stopped  atomic.Bool
}

type fakeResult struct {
	inserted int64
	err      error
	delay    time.Duration
}

func (f *fakeInserter) CopyMessages(_ context.Context, msgs []postgres.Message) (int64, error) {
	f.mu.Lock()
	if f.maxCalls > 0 && len(f.calls) >= f.maxCalls {
		f.stopped.Store(true)
	}
	f.calls = append(f.calls, append([]postgres.Message(nil), msgs...))
	res := fakeResult{inserted: int64(len(msgs))}
	if f.resIdx < len(f.results) {
		res = f.results[f.resIdx]
		f.resIdx++
	}
	f.mu.Unlock()
	if res.delay > 0 {
		time.Sleep(res.delay)
	}
	return res.inserted, res.err
}

func (f *fakeInserter) snapshot() [][]postgres.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]postgres.Message, len(f.calls))
	for i, c := range f.calls {
		out[i] = append([]postgres.Message(nil), c...)
	}
	return out
}

// fakeWAL implements flusher.WALSource for the re-enqueue tests.
type fakeWAL struct {
	mu        sync.Mutex
	drainErr  error
	appendErr error
	queue     []postgres.Message
	requeued  []postgres.Message
}

func (w *fakeWAL) Drain(limit int) ([]postgres.Message, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.drainErr != nil {
		return nil, w.drainErr
	}
	if len(w.queue) == 0 {
		return nil, nil
	}
	if limit > len(w.queue) {
		limit = len(w.queue)
	}
	out := append([]postgres.Message(nil), w.queue[:limit]...)
	w.queue = w.queue[limit:]
	return out, nil
}
func (w *fakeWAL) Append(m postgres.Message) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.appendErr != nil {
		return nil, w.appendErr
	}
	w.requeued = append(w.requeued, m)
	w.queue = append(w.queue, m)
	return []byte(fmt.Sprintf("k%d", len(w.requeued))), nil
}
func (w *fakeWAL) snapshotRequeued() []postgres.Message {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]postgres.Message(nil), w.requeued...)
}

// fakeSource is a Dequeue-only queue that returns msgs in chunks.
type fakeSource struct {
	mu   sync.Mutex
	msgs []postgres.Message
}

func (s *fakeSource) push(m ...postgres.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, m...)
}
func (s *fakeSource) Dequeue(n int) []postgres.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.msgs) == 0 {
		return nil
	}
	if n > len(s.msgs) {
		n = len(s.msgs)
	}
	out := append([]postgres.Message(nil), s.msgs[:n]...)
	s.msgs = s.msgs[n:]
	return out
}
func (s *fakeSource) State() buffer.State { return buffer.StateNormal }
func (s *fakeSource) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs)
}

// recordingDLQ captures every dead-letter call.
type recordingDLQ struct {
	mu   sync.Mutex
	msgs []postgres.Message
	errs []error
}

func (d *recordingDLQ) Write(_ context.Context, m postgres.Message, err error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.msgs = append(d.msgs, m)
	d.errs = append(d.errs, err)
	return nil
}
func (d *recordingDLQ) Snapshot() []postgres.Message {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]postgres.Message(nil), d.msgs...)
}

func makeMsg(seq int) postgres.Message {
	return postgres.Message{
		TenantID:   "acme",
		DeviceUUID: uuid.New(),
		Topic:      "t/acme/d/x/evt/state",
		Payload:    []byte(fmt.Sprintf(`{"seq":%d}`, seq)),
		ReceivedAt: time.Now(),
		DedupKey:   fmt.Sprintf("acme:%d", seq),
	}
}

func defaultFlusherCfg() config.FlusherConfig {
	return config.FlusherConfig{
		BatchSize:                10,
		FlushInterval:            config.Duration(20 * time.Millisecond),
		ElevatedLatencyThreshold: config.Duration(50 * time.Millisecond),
		ElevatedBatchSize:        20,
		ElevatedFlushInterval:    config.Duration(40 * time.Millisecond),
		CriticalLatencyThreshold: config.Duration(200 * time.Millisecond),
		RecoveryWindow:           config.Duration(100 * time.Millisecond),
		MaxRetries:               2,
	}
}

func newFlusher(t *testing.T, cfg config.FlusherConfig, pg postgres.Inserter, src flusher.Source, dl flusher.DeadLetter) (*flusher.Flusher, *metrics.FlusherMetrics) {
	t.Helper()
	reg := metrics.NewRegistry()
	m := metrics.NewFlusherMetrics(reg)
	f, err := flusher.New(cfg, pg, src, nil, dl, m, nil)
	require.NoError(t, err)
	return f, m
}

func TestNew_RequiresDeps(t *testing.T) {
	cfg := defaultFlusherCfg()
	reg := metrics.NewRegistry()
	m := metrics.NewFlusherMetrics(reg)
	src := &fakeSource{}
	pg := &fakeInserter{}
	dl := &recordingDLQ{}

	_, err := flusher.New(cfg, nil, src, nil, dl, m, nil)
	require.Error(t, err)
	_, err = flusher.New(cfg, pg, nil, nil, dl, m, nil)
	require.Error(t, err)
	_, err = flusher.New(cfg, pg, src, nil, nil, m, nil)
	require.Error(t, err)
	_, err = flusher.New(cfg, pg, src, nil, dl, nil, nil)
	require.Error(t, err)
}

func TestRun_FlushesBatch(t *testing.T) {
	src := &fakeSource{}
	pg := &fakeInserter{}
	dl := &recordingDLQ{}
	f, m := newFlusher(t, defaultFlusherCfg(), pg, src, dl)

	for i := 0; i < 25; i++ {
		src.push(makeMsg(i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = f.Run(ctx); close(done) }()

	require.Eventually(t, func() bool {
		return src.Len() == 0
	}, 2*time.Second, 20*time.Millisecond)

	cancel()
	<-done

	calls := pg.snapshot()
	var total int
	for _, c := range calls {
		total += len(c)
	}
	assert.Equal(t, 25, total)
	assert.Equal(t, float64(25), testutil.ToFloat64(m.Inserted))
	assert.Empty(t, dl.Snapshot())
}

func TestRun_RetriesAndFinallyDeadLetters(t *testing.T) {
	src := &fakeSource{}
	pg := &fakeInserter{
		results: []fakeResult{
			{err: errors.New("transient")},
			{err: errors.New("transient")},
			{err: errors.New("transient")}, // 3rd attempt also fails (MaxRetries=2 -> 1 initial + 2 retries = 3)
		},
	}
	dl := &recordingDLQ{}

	cfg := defaultFlusherCfg()
	cfg.MaxRetries = 2
	f, m := newFlusher(t, cfg, pg, src, dl)

	src.push(makeMsg(0), makeMsg(1), makeMsg(2))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = f.Run(ctx); close(done) }()

	require.Eventually(t, func() bool {
		return len(dl.Snapshot()) == 3
	}, 15*time.Second, 50*time.Millisecond, "all 3 messages should land in DLQ")

	cancel()
	<-done

	assert.Equal(t, float64(3), testutil.ToFloat64(m.DeadLettered))
	assert.Greater(t, testutil.ToFloat64(m.RetriesTotal), float64(0))
}

func TestRun_RecoversFromTransientFailure(t *testing.T) {
	src := &fakeSource{}
	pg := &fakeInserter{
		results: []fakeResult{
			{err: errors.New("blip")}, // first attempt fails
			{inserted: 3},             // retry succeeds
		},
	}
	dl := &recordingDLQ{}
	cfg := defaultFlusherCfg()
	cfg.MaxRetries = 3
	f, m := newFlusher(t, cfg, pg, src, dl)

	src.push(makeMsg(0), makeMsg(1), makeMsg(2))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = f.Run(ctx); close(done) }()

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(m.Inserted) == 3
	}, 5*time.Second, 20*time.Millisecond)

	cancel()
	<-done

	assert.Empty(t, dl.Snapshot())
	assert.Equal(t, float64(1), testutil.ToFloat64(m.FlushErrors))
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	src := &fakeSource{}
	pg := &fakeInserter{}
	dl := &recordingDLQ{}
	f, _ := newFlusher(t, defaultFlusherCfg(), pg, src, dl)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = f.Run(ctx); close(done) }()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return on cancel")
	}
}

// TestRun_TransientFailureReenqueuesToWAL: when MaxRetries is exhausted
// against a connection-level error AND the flusher has a WAL, the batch
// is re-enqueued to the WAL instead of dead-lettered. This is the fix
// captured in ADR 0005 for the sustained-slowdown stress test.
func TestRun_TransientFailureReenqueuesToWAL(t *testing.T) {
	src := &fakeSource{}
	pg := &fakeInserter{
		results: []fakeResult{
			{err: errors.New("connection refused")},
			{err: errors.New("connection refused")},
			{err: errors.New("connection refused")},
		},
	}
	wal := &fakeWAL{}
	dl := &recordingDLQ{}

	cfg := defaultFlusherCfg()
	cfg.MaxRetries = 2

	reg := metrics.NewRegistry()
	m := metrics.NewFlusherMetrics(reg)
	f, err := flusher.New(cfg, pg, src, wal, dl, m, nil)
	require.NoError(t, err)

	src.push(makeMsg(0), makeMsg(1), makeMsg(2))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = f.Run(ctx); close(done) }()

	require.Eventually(t, func() bool {
		return len(wal.snapshotRequeued()) == 3
	}, 30*time.Second, 50*time.Millisecond, "all 3 messages should be re-enqueued to WAL")

	cancel()
	<-done

	assert.Empty(t, dl.Snapshot(), "transient failure must not dead-letter when WAL is available")
	assert.Equal(t, float64(3), testutil.ToFloat64(m.Requeued))
	assert.Equal(t, float64(0), testutil.ToFloat64(m.DeadLettered))
}

// TestRun_PoisonErrorAlwaysDeadLetters: a SQLSTATE 23xxx (constraint
// violation) is deterministic poison — replaying it will fail the same
// way. Even with a WAL available, this batch must dead-letter.
func TestRun_PoisonErrorAlwaysDeadLetters(t *testing.T) {
	src := &fakeSource{}
	pgErr := &pgconn.PgError{Code: "23505", Message: "unique_violation"}
	pg := &fakeInserter{
		results: []fakeResult{
			{err: pgErr},
			{err: pgErr},
			{err: pgErr},
		},
	}
	wal := &fakeWAL{}
	dl := &recordingDLQ{}

	cfg := defaultFlusherCfg()
	cfg.MaxRetries = 2

	reg := metrics.NewRegistry()
	m := metrics.NewFlusherMetrics(reg)
	f, err := flusher.New(cfg, pg, src, wal, dl, m, nil)
	require.NoError(t, err)

	src.push(makeMsg(0), makeMsg(1))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = f.Run(ctx); close(done) }()

	require.Eventually(t, func() bool {
		return len(dl.Snapshot()) == 2
	}, 30*time.Second, 50*time.Millisecond, "poison messages must DLQ even with a WAL")

	cancel()
	<-done

	assert.Empty(t, wal.snapshotRequeued(), "poison must not be re-enqueued")
	assert.Equal(t, float64(2), testutil.ToFloat64(m.DeadLettered))
	assert.Equal(t, float64(0), testutil.ToFloat64(m.Requeued))
}

// TestRun_TransientFailureWALFullFallsBackToDLQ: when the WAL itself is
// failing (full / corrupt / disk-out), we fall back to the dead-letter
// sink as the last-resort durability boundary. This must not silently
// drop.
func TestRun_TransientFailureWALFullFallsBackToDLQ(t *testing.T) {
	src := &fakeSource{}
	pg := &fakeInserter{
		results: []fakeResult{
			{err: errors.New("conn reset")},
			{err: errors.New("conn reset")},
			{err: errors.New("conn reset")},
		},
	}
	wal := &fakeWAL{appendErr: errors.New("disk full")}
	dl := &recordingDLQ{}

	cfg := defaultFlusherCfg()
	cfg.MaxRetries = 2

	reg := metrics.NewRegistry()
	m := metrics.NewFlusherMetrics(reg)
	f, err := flusher.New(cfg, pg, src, wal, dl, m, nil)
	require.NoError(t, err)

	src.push(makeMsg(0), makeMsg(1))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = f.Run(ctx); close(done) }()

	require.Eventually(t, func() bool {
		return len(dl.Snapshot()) == 2
	}, 30*time.Second, 50*time.Millisecond,
		"with a failing WAL, transient failure must fall back to DLQ")

	cancel()
	<-done

	assert.Empty(t, wal.snapshotRequeued())
	assert.Equal(t, float64(2), testutil.ToFloat64(m.DeadLettered))
}
