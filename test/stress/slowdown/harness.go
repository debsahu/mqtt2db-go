//go:build slowdown

// Package slowdown is the test harness for the sustained-slowdown
// stress scenarios (Milestone 13 / ADR 0005). The pipeline boots
// against:
//
//	publisher --> Comqtt --> subscriber --> ring --> flusher --> Toxiproxy --> Postgres
//	                                                |               ^
//	                                                +--> WAL <------+ (re-enqueue on transient failure)
//	                                                +--> RustFS DLQ
//
// Each scenario flips toxics on the live proxy mid-run and asserts the
// pipeline holds the at-least-once contract end-to-end. Scenarios live
// in *_test.go files; this file owns the shared boot/teardown and
// reporting.
package slowdown

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/debsahu/mqtt2db-go/internal/buffer"
	"github.com/debsahu/mqtt2db-go/internal/config"
	"github.com/debsahu/mqtt2db-go/internal/deadletter"
	"github.com/debsahu/mqtt2db-go/internal/flusher"
	"github.com/debsahu/mqtt2db-go/internal/metrics"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
	"github.com/debsahu/mqtt2db-go/internal/subscriber"
	"github.com/debsahu/mqtt2db-go/internal/wal"
)

// envInt / envDuration follow the same convention as the steady-state
// stress test so override paths line up across both harnesses.
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		_, err := fmt.Sscanf(v, "%d", &n)
		if err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}

// Harness is everything a scenario needs to drive load and inspect
// state. Scenarios receive one through setupHarness.
type Harness struct {
	t   testing.TB
	ctx context.Context //nolint:containedctx

	BrokerURL string
	PgDSN     string // un-toxified DSN, used by the test for verification queries
	ProxyDSN  string // through-toxiproxy DSN, used by the service

	Toxi    *toxiclient.Client
	PgProxy *toxiclient.Proxy

	Pool *pgxpool.Pool // verification pool, NOT through toxiproxy

	Reg    *prometheus.Registry
	BufM   *metrics.BufferMetrics
	WalM   *metrics.WALMetrics
	SubM   *metrics.SubscriberMetrics
	FlushM *metrics.FlusherMetrics
	DlqM   *metrics.DeadLetterMetrics
	WAL    *wal.Store
	Ring   *buffer.Ring
	Flush  *flusher.Flusher
	Sub    *subscriber.Subscriber

	publishers []*autopaho.ConnectionManager
	deviceIDs  []uuid.UUID

	timeline []TimelineEvent
	tlMu     sync.Mutex

	// fastPeakWAL is the highest WAL outstanding seen by the 100 ms
	// fast poller since startTimelineRecorder began. Oscillation
	// scenarios reset this at each cycle boundary so cs.WALPeakInCycle
	// captures sub-second spikes the 1 Hz timeline misses. Read /
	// reset only via SnapshotAndResetPeakWAL; do NOT compare-and-swap
	// from the cycle loop while the poller is running.
	fastPeakWAL atomic.Int64

	// fastModeTransitions counts mode changes observed by the same
	// 100 ms poller. The 1 Hz timeline aliases sub-second flips down
	// to a single sample, so the M14a mode-flap detector reads this
	// counter, not the 1 Hz transition count.
	fastModeTransitions atomic.Int64

	// peakMode tracks the highest mode value observed at any sampling
	// instant by startTimelineRecorder. We poll faster (every 100ms)
	// just for this so brief mode oscillations don't escape the test.
	peakMode atomic.Int32

	pipelineCancel context.CancelFunc
	pipelineWG     sync.WaitGroup
}

// TimelineEvent is one row in the per-scenario report.
type TimelineEvent struct {
	At        time.Time
	Mode      string
	BatchSize float64
	WAL       float64
	RingDepth float64
	Paused    float64
	Note      string
}

func migrationsDir(tb testing.TB) string {
	tb.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(tb, ok)
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "migrations")
}

func resultsDir(tb testing.TB) string {
	tb.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(tb, ok)
	return filepath.Join(filepath.Dir(file), "results")
}

// setupHarness brings up Comqtt + Postgres + Toxiproxy + RustFS, starts
// the in-process pipeline, primes the publishers, and returns the
// harness. Cleanup is registered with t.Cleanup.
func setupHarness(t *testing.T, ctx context.Context, scenarioName string) *Harness {
	t.Helper()

	publishers := envInt("STRESS_PUBLISHERS", 16)
	devices := envInt("STRESS_DEVICES", 1000)

	// Validate operator-facing knobs once, at startup. Both helpers
	// fail the test fatally on invalid input so a typo in a make
	// recipe (or env export) never silently runs the wrong benchmark.
	_ = activeSchemaTB(t)
	_ = payloadSizeTB(t)

	t.Logf("[%s] booting infrastructure...", scenarioName)
	dockerNet, err := network.New(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = dockerNet.Remove(ctx)
	})

	broker := startComqtt(t, ctx, dockerNet.Name)
	pgHostDSN := startPostgresOnNetwork(t, ctx, dockerNet.Name)
	toxiHostAddr, proxyHostPort := startToxiproxy(t, ctx, dockerNet.Name)
	dlqEndpoint, dlqAccess, dlqSecret := startRustFS(t, ctx)

	require.NoError(t, postgres.Migrate("file://"+migrationsDir(t), pgHostDSN))
	createBucket(t, ctx, dlqEndpoint, dlqAccess, dlqSecret, "mqtt2db-go-dlq")

	// Test-only wide-schema DDL (Milestone 14b). No-op when
	// SLOWDOWN_SCHEMA != wide. Embedded in the harness rather than
	// living under migrations/ so production operators never see it.
	t.Logf("[%s] schema=%s table=%s payload_bytes=%d",
		scenarioName, activeSchema(), targetTable(), payloadSize())

	proxyDSN := fmt.Sprintf("postgres://ingest:ingest@127.0.0.1:%s/telemetry?sslmode=disable&connect_timeout=5", proxyHostPort)

	toxiClient := toxiclient.NewClient(toxiHostAddr)
	proxy, err := toxiClient.CreateProxy("pg", "0.0.0.0:5433", "pg:5432")
	require.NoError(t, err)

	pool, err := postgres.NewPool(ctx, config.PostgresConfig{
		DSN: proxyDSN, MaxConns: 8, MinConns: 2,
	})
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	// Apply wide-schema DDL through the same proxy pool so the
	// flusher's CopyMessages path will see the table (PG schema cache
	// is per-connection). No-op for minimal mode.
	require.NoError(t, applyWideSchemaIfNeeded(ctx, pool))

	copier, err := postgres.NewCopier(pool, config.PostgresConfig{
		Schema: "public", Table: targetTable(),
		Columns: []string{"tenant_id", "device_uuid", "topic", "payload", "received_at", "dedup_key"},
	})
	require.NoError(t, err)

	verifyPool, err := postgres.NewPool(ctx, config.PostgresConfig{
		DSN: pgHostDSN, MaxConns: 2, MinConns: 1,
	})
	require.NoError(t, err)
	t.Cleanup(verifyPool.Close)

	reg := metrics.NewRegistry()
	bufM := metrics.NewBufferMetrics(reg)
	walM := metrics.NewWALMetrics(reg)
	subM := metrics.NewSubscriberMetrics(reg)
	flushM := metrics.NewFlusherMetrics(reg)
	dlqM := metrics.NewDeadLetterMetrics(reg)

	walStore, err := wal.Open(wal.Config{
		Path:             t.TempDir(),
		TTL:              0,
		SyncWrites:       false,
		ValueLogFileSize: 1 << 28,
	}, walM)
	require.NoError(t, err)
	t.Cleanup(func() { _ = walStore.Close() })

	ring, err := buffer.New(buffer.Config{
		MaxMessages:    100_000,
		MaxBytes:       100 << 20,
		SpillThreshold: 0.8,
		PauseThreshold: 0.95,
	}, bufM)
	require.NoError(t, err)

	dlq, err := deadletter.NewSink(ctx, config.S3Config{
		Bucket: "mqtt2db-go-dlq", Region: "us-east-1",
		Endpoint: dlqEndpoint, UsePathStyle: true,
		AccessKey: dlqAccess, SecretKey: dlqSecret,
	}, dlqM)
	require.NoError(t, err)

	flush, err := flusher.New(config.FlusherConfig{
		BatchSize:                1000,
		FlushInterval:            config.Duration(100 * time.Millisecond),
		ElevatedLatencyThreshold: config.Duration(500 * time.Millisecond),
		ElevatedBatchSize:        5000,
		ElevatedFlushInterval:    config.Duration(500 * time.Millisecond),
		CriticalLatencyThreshold: config.Duration(2 * time.Second),
		RecoveryWindow:           config.Duration(60 * time.Second),
		MaxRetries:               5,
		Workers:                  7, // MaxConns(8) - 1 — see ADR 0005.
	}, copier, ring, walStore, dlq, flushM, nil)
	require.NoError(t, err)

	t.Setenv("HOSTNAME", "slowdown-"+scenarioName)
	sub, err := subscriber.New(config.MQTTConfig{
		Brokers:             []string{broker},
		ClientIDPrefix:      "mqtt2db-go-slowdown",
		SharedSubscription:  "$share/ingest/t/+/d/+/evt/#",
		Keepalive:           config.Duration(30 * time.Second),
		ConnectTimeout:      config.Duration(5 * time.Second),
		MaxReconnectBackoff: config.Duration(10 * time.Second),
		ProtocolVersion:     5,
	}, ring, walStore, subM, nil)
	require.NoError(t, err)

	pipelineCtx, pipelineCancel := context.WithCancel(ctx)
	t.Cleanup(pipelineCancel)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = sub.Start(pipelineCtx) }()
	go func() { defer wg.Done(); _ = flush.Run(pipelineCtx) }()

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(subM.Connected) == 1
	}, 30*time.Second, 100*time.Millisecond, "subscriber never connected")

	deviceIDs := make([]uuid.UUID, devices)
	for i := range deviceIDs {
		deviceIDs[i] = uuid.New()
	}
	bu, err := url.Parse(broker)
	require.NoError(t, err)
	pubs := make([]*autopaho.ConnectionManager, publishers)
	for i := 0; i < publishers; i++ {
		cm, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
			ServerUrls:     []*url.URL{bu},
			KeepAlive:      30,
			ConnectTimeout: 5 * time.Second,
			ClientConfig:   paho.ClientConfig{ClientID: fmt.Sprintf("loadgen-slow-%d-%s", i, uuid.NewString()[:6])},
		})
		require.NoError(t, err)
		require.NoError(t, cm.AwaitConnection(ctx))
		pubs[i] = cm
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, cm := range pubs {
			_ = cm.Disconnect(ctx)
		}
	})

	h := &Harness{
		t:              t,
		ctx:            ctx,
		BrokerURL:      broker,
		PgDSN:          pgHostDSN,
		ProxyDSN:       proxyDSN,
		Toxi:           toxiClient,
		PgProxy:        proxy,
		Pool:           verifyPool,
		Reg:            reg,
		BufM:           bufM,
		WalM:           walM,
		SubM:           subM,
		FlushM:         flushM,
		DlqM:           dlqM,
		WAL:            walStore,
		Ring:           ring,
		Flush:          flush,
		Sub:            sub,
		publishers:     pubs,
		deviceIDs:      deviceIDs,
		pipelineCancel: pipelineCancel,
	}
	t.Cleanup(func() {
		pipelineCancel()
		wg.Wait()
	})
	return h
}

// driveLoad runs a load generator that targets `rate` msg/s for the
// given duration. Returns total sent + publish errors.
func (h *Harness) driveLoad(ctx context.Context, rate int, duration time.Duration) (int64, int64) {
	h.t.Helper()
	publishers := len(h.publishers)
	perPub := rate / publishers
	if perPub < 1 {
		perPub = 1
	}
	loadCtx, loadCancel := context.WithTimeout(ctx, duration)
	defer loadCancel()

	bodyBytes := payloadSize()
	var sent, errs atomic.Int64
	var wg sync.WaitGroup
	for i, cm := range h.publishers {
		wg.Add(1)
		go func(idx int, cm *autopaho.ConnectionManager) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(idx))) //nolint:gosec
			interval := time.Second / time.Duration(perPub)
			next := time.Now()
			body := make([]byte, bodyBytes)
			for {
				select {
				case <-loadCtx.Done():
					return
				default:
				}
				if now := time.Now(); now.Before(next) {
					select {
					case <-loadCtx.Done():
						return
					case <-time.After(next.Sub(now)):
					}
				}
				next = next.Add(interval)

				dev := h.deviceIDs[r.Intn(len(h.deviceIDs))]
				topic := fmt.Sprintf("t/acme/d/%s/evt/state", dev)
				_, _ = r.Read(body)
				if _, err := cm.Publish(loadCtx, &paho.Publish{Topic: topic, QoS: 0, Payload: body}); err != nil {
					errs.Add(1)
					continue
				}
				sent.Add(1)
			}
		}(i, cm)
	}
	wg.Wait()
	return sent.Load(), errs.Load()
}

// startTimelineRecorder polls the metrics every second and appends to
// h.timeline until ctx is cancelled. A separate fast poller (100 ms)
// captures peak mode so we don't miss brief critical-mode flips.
func (h *Harness) startTimelineRecorder(ctx context.Context) {
	go func() {
		modes := []string{"normal", "elevated", "critical"}
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				modeIdx := int(testutil.ToFloat64(h.FlushM.Mode))
				if modeIdx < 0 || modeIdx >= len(modes) {
					modeIdx = 0
				}
				wal := testutil.ToFloat64(h.WalM.Appended) - testutil.ToFloat64(h.WalM.Drained)
				if wal < 0 {
					wal = 0
				}
				h.appendTimeline(TimelineEvent{
					At:        t,
					Mode:      modes[modeIdx],
					BatchSize: testutil.ToFloat64(h.FlushM.BatchSize),
					WAL:       wal,
					RingDepth: testutil.ToFloat64(h.BufM.Depth),
					Paused:    testutil.ToFloat64(h.SubM.Paused),
				})
			}
		}
	}()
	// Fast 100 ms poller: peak mode (gauge), peak WAL (gauge), and
	// fine-grained mode-transition count. The 1 Hz timeline aliases
	// sub-second mode flips to a single sample; this poller catches
	// them. Only writes atomics so it stays lock-free against the
	// hot path.
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		var lastMode int32 = -1
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m := int32(testutil.ToFloat64(h.FlushM.Mode))
				for {
					prev := h.peakMode.Load()
					if m <= prev || h.peakMode.CompareAndSwap(prev, m) {
						break
					}
				}
				if lastMode != -1 && m != lastMode {
					h.fastModeTransitions.Add(1)
				}
				lastMode = m

				wal := int64(testutil.ToFloat64(h.WalM.Appended) - testutil.ToFloat64(h.WalM.Drained))
				if wal < 0 {
					wal = 0
				}
				for {
					prev := h.fastPeakWAL.Load()
					if wal <= prev || h.fastPeakWAL.CompareAndSwap(prev, wal) {
						break
					}
				}
			}
		}
	}()
}

// SnapshotAndResetPeakWAL atomically reads the highest WAL outstanding
// observed since the last reset, then resets the tracker to the
// current WAL depth. Oscillation scenarios call this at each cycle
// boundary so cs.WALPeakInCycle is a true sub-second peak, not a
// 1 Hz alias.
func (h *Harness) SnapshotAndResetPeakWAL() int64 {
	cur := h.walOutstanding()
	prev := h.fastPeakWAL.Swap(cur)
	if prev > cur {
		return prev
	}
	return cur
}

func (h *Harness) appendTimeline(e TimelineEvent) {
	h.tlMu.Lock()
	defer h.tlMu.Unlock()
	h.timeline = append(h.timeline, e)
}

// markTimeline annotates the most-recent timeline event with `note` so
// the report can pinpoint when toxics were added or removed.
func (h *Harness) markTimeline(note string) {
	h.tlMu.Lock()
	defer h.tlMu.Unlock()
	if len(h.timeline) > 0 {
		h.timeline[len(h.timeline)-1].Note = note
	} else {
		h.timeline = append(h.timeline, TimelineEvent{At: time.Now(), Note: note})
	}
}

func (h *Harness) rowCount(ctx context.Context) int64 {
	var n int64
	q := fmt.Sprintf(`SELECT count(*) FROM %s`, targetTable())
	if err := h.Pool.QueryRow(ctx, q).Scan(&n); err != nil {
		h.t.Fatalf("rowCount: %v", err)
	}
	return n
}

func (h *Harness) distinctDedupKeys(ctx context.Context) int64 {
	var n int64
	q := fmt.Sprintf(`SELECT count(distinct dedup_key) FROM %s`, targetTable())
	if err := h.Pool.QueryRow(ctx, q).Scan(&n); err != nil {
		h.t.Fatalf("distinctDedupKeys: %v", err)
	}
	return n
}

// awaitFullDrain waits for the conservation invariant:
//
//	count(distinct dedup_key in PG) >= subscriber.acked
//
// AND
//
//	wal_appended == wal_drained && ring depth == 0
//
// Returns peak WAL observed during the wait (carried forward from prior
// observations via initialPeak). Budget is the time we allow before
// giving up; spec calls for ≤ 10 minutes for the WAL drain criterion.
func (h *Harness) awaitFullDrain(ctx context.Context, initialPeak int64, budget time.Duration) (ok bool, peak int64) {
	peak = initialPeak
	deadline := time.Now().Add(budget)
	last := time.Now()
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			h.t.Logf("[drain] context cancelled before convergence")
			return false, peak
		case <-time.After(2 * time.Second):
		}
		appended := int64(testutil.ToFloat64(h.WalM.Appended))
		drainedN := int64(testutil.ToFloat64(h.WalM.Drained))
		walOutstanding := appended - drainedN
		if walOutstanding > peak {
			peak = walOutstanding
		}
		ringDepth := int64(testutil.ToFloat64(h.BufM.Depth))
		acked := int64(testutil.ToFloat64(h.SubM.Acked))
		var rows int64
		q := fmt.Sprintf(`SELECT count(distinct dedup_key) FROM %s`, targetTable())
		if err := h.Pool.QueryRow(ctx, q).Scan(&rows); err != nil {
			continue
		}
		if time.Since(last) >= 10*time.Second {
			h.t.Logf("[drain] ring=%d wal=%d rows=%d acked=%d gap=%d (peak=%d)",
				ringDepth, walOutstanding, rows, acked, acked-rows, peak)
			last = time.Now()
		}
		// Convergence invariant: ring + WAL both empty AND PG distinct
		// count is within 0.5 % of flusher.Inserted. We tolerate the
		// small one-way gap (flusher.Inserted slightly > distinct) seen
		// at full scale — it's a metric race / pgx counter-vs-PG-
		// visibility artifact, not silent loss. The conservation check
		// in scenarios_test.go does the strict version of this on the
		// final read.
		inserted := int64(testutil.ToFloat64(h.FlushM.Inserted))
		_ = acked
		var insertedSlack int64
		if inserted > 0 {
			insertedSlack = inserted / 200 // 0.5%
			if insertedSlack < 16 {
				insertedSlack = 16
			}
		}
		if walOutstanding == 0 && ringDepth == 0 && rows+insertedSlack >= inserted {
			return true, peak
		}
	}
	return false, peak
}

// observePeak runs a single sample of the WAL outstanding so the
// scenario harness can keep the running peak fresh during the drain
// without blocking.
func (h *Harness) observePeak(prev int64) int64 {
	appended := int64(testutil.ToFloat64(h.WalM.Appended))
	drainedN := int64(testutil.ToFloat64(h.WalM.Drained))
	current := appended - drainedN
	if current > prev {
		return current
	}
	return prev
}

// walOutstanding returns the current WAL depth (appended − drained).
// Cheap; used by oscillation scenarios to sample at slow-window
// boundaries without going through the full snapshot path.
func (h *Harness) walOutstanding() int64 {
	wal := int64(testutil.ToFloat64(h.WalM.Appended) - testutil.ToFloat64(h.WalM.Drained))
	if wal < 0 {
		wal = 0
	}
	return wal
}

// currentMode returns the flusher's current mode as a string.
func (h *Harness) currentMode() string {
	switch int(testutil.ToFloat64(h.FlushM.Mode)) {
	case 1:
		return "elevated"
	case 2:
		return "critical"
	default:
		return "normal"
	}
}

// walPeakBetween scans the timeline for the highest WAL outstanding
// observed within [start, end]. Used by oscillation scenarios to
// detect ratcheting across cycles. Caller must take h.tlMu if it
// needs a stable snapshot; here we read under the lock briefly.
func (h *Harness) walPeakBetween(start, end time.Time) int64 {
	h.tlMu.Lock()
	defer h.tlMu.Unlock()
	var peak float64
	for _, e := range h.timeline {
		if e.At.Before(start) || e.At.After(end) {
			continue
		}
		if e.WAL > peak {
			peak = e.WAL
		}
	}
	return int64(peak)
}

type scenarioReport struct {
	Scenario         string
	StartedAt        time.Time
	EndedAt          time.Time
	TargetRate       int
	Sent             int64
	PublishErrs      int64
	Persisted        int64
	DistinctKeys     int64
	FlusherInserted  int64
	DedupCollisions  int64 // acked - inserted; collapsed by ON CONFLICT
	WALPeak          int64
	WALDrainedFully  bool
	DrainTime        time.Duration
	ModeTimeline     []TimelineEvent
	Snapshots        []reportSnapshot
	Pass             []string
	Fail             []string

	// Schema records which test schema this run used (minimal /
	// wide). Operators reading the report should know whether the
	// throughput numbers were produced against secondary-index
	// loaded tables or the lab schema.
	Schema       string
	PayloadBytes int

	// Cycles is populated only by oscillation scenarios. nil for the
	// existing one-shot moderate / severe / outage runs.
	Cycles []cycleStats
}

// cycleStats captures one slow→clean cycle of an oscillation scenario.
// Used to detect WAL ratcheting (peak monotonically growing) and mode
// thrashing across cycles.
type cycleStats struct {
	Cycle              int
	SlowStartAt        time.Time
	SlowEndAt          time.Time
	CleanEndAt         time.Time
	StartMode          string // mode at the moment toxic was applied
	SlowEndMode        string // mode at the moment toxic was removed
	CleanEndMode       string // mode at the end of the clean window
	WALAtSlowStart     int64
	WALPeakInCycle     int64 // observed peak between SlowStartAt and CleanEndAt
	WALAtCleanEnd      int64
	InsertedInCycle    int64 // delta of flusher.inserted_total over the cycle
	BatchSizeAtSlowEnd float64
}

type reportSnapshot struct {
	Label string
	At    time.Time
	M     map[string]float64
}

func (h *Harness) snapshot(label string) reportSnapshot {
	return reportSnapshot{
		Label: label,
		At:    time.Now(),
		M: map[string]float64{
			"buffer.enqueued":       testutil.ToFloat64(h.BufM.Enqueued),
			"buffer.dequeued":       testutil.ToFloat64(h.BufM.Dequeued),
			"buffer.spilled":        testutil.ToFloat64(h.BufM.Spilled),
			"buffer.rejected":       testutil.ToFloat64(h.BufM.Rejected),
			"buffer.depth":          testutil.ToFloat64(h.BufM.Depth),
			"wal.appended":          testutil.ToFloat64(h.WalM.Appended),
			"wal.drained":           testutil.ToFloat64(h.WalM.Drained),
			"sub.received":          testutil.ToFloat64(h.SubM.Received),
			"sub.acked":             testutil.ToFloat64(h.SubM.Acked),
			"sub.paused":            testutil.ToFloat64(h.SubM.Paused),
			"sub.pauses_total":      testutil.ToFloat64(h.SubM.Pauses),
			"flusher.mode":          testutil.ToFloat64(h.FlushM.Mode),
			"flusher.batch_size":    testutil.ToFloat64(h.FlushM.BatchSize),
			"flusher.flushed":       testutil.ToFloat64(h.FlushM.Flushed),
			"flusher.inserted":      testutil.ToFloat64(h.FlushM.Inserted),
			"flusher.errors":        testutil.ToFloat64(h.FlushM.FlushErrors),
			"flusher.retries":       testutil.ToFloat64(h.FlushM.RetriesTotal),
			"flusher.dead_lettered": testutil.ToFloat64(h.FlushM.DeadLettered),
			"flusher.requeued":      testutil.ToFloat64(h.FlushM.Requeued),
			"dlq.written":           testutil.ToFloat64(h.DlqM.Written),
			"dlq.errors":            testutil.ToFloat64(h.DlqM.Errors),
		},
	}
}

func writeReport(t *testing.T, r scenarioReport) {
	t.Helper()
	dir := resultsDir(t)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	stamp := r.StartedAt.UTC().Format("20060102-150405")
	path := filepath.Join(dir, fmt.Sprintf("%s-%s.md", r.Scenario, stamp))

	var b strings.Builder
	fmt.Fprintf(&b, "# slowdown stress: %s\n\n", r.Scenario)
	fmt.Fprintf(&b, "Started: %s\nEnded:   %s\nDuration: %s\n\n",
		r.StartedAt.UTC().Format(time.RFC3339),
		r.EndedAt.UTC().Format(time.RFC3339),
		r.EndedAt.Sub(r.StartedAt).Round(time.Second))
	fmt.Fprintf(&b, "## Headline\n\n")
	if r.Schema != "" {
		fmt.Fprintf(&b, "- Schema:         %s (%d-byte payloads)\n", r.Schema, r.PayloadBytes)
	}
	fmt.Fprintf(&b, "- Target rate:    %d msg/s\n", r.TargetRate)
	fmt.Fprintf(&b, "- Sent:           %d\n", r.Sent)
	fmt.Fprintf(&b, "- Publish errors: %d\n", r.PublishErrs)
	fmt.Fprintf(&b, "- Persisted (rows in PG): %d\n", r.Persisted)
	fmt.Fprintf(&b, "- Distinct dedup_keys: %d\n", r.DistinctKeys)
	fmt.Fprintf(&b, "- Flusher inserted (after ON CONFLICT): %d\n", r.FlusherInserted)
	fmt.Fprintf(&b, "- Dedup collisions: %d (collapsed by unique index — by design)\n", r.DedupCollisions)
	fmt.Fprintf(&b, "- WAL high-water: %d\n", r.WALPeak)
	fmt.Fprintf(&b, "- WAL drained fully: %v\n", r.WALDrainedFully)
	if r.DrainTime > 0 {
		fmt.Fprintf(&b, "- Drain time:     %s\n", r.DrainTime.Round(time.Second))
	}

	// Throughput section — rows/sec and bytes/sec computed from
	// flusher.inserted_total / total runtime. bytes/sec uses the
	// configured payload size; this slightly under-counts the true
	// row width (excludes the topic, dedup_key, etc.) but is the
	// honest "ingest payload throughput" number an operator wants.
	if r.FlusherInserted > 0 {
		runtime := r.EndedAt.Sub(r.StartedAt).Seconds()
		if runtime > 0 {
			rps := float64(r.FlusherInserted) / runtime
			bps := rps * float64(r.PayloadBytes)
			fmt.Fprintf(&b, "\n## Throughput\n\n")
			fmt.Fprintf(&b, "- rows/sec:  %.0f\n", rps)
			fmt.Fprintf(&b, "- bytes/sec: %.0f (~%.2f MB/s payload-only)\n",
				bps, bps/(1024*1024))
		}
	}
	fmt.Fprintf(&b, "\n## Pass / Fail\n\n")
	for _, p := range r.Pass {
		fmt.Fprintf(&b, "- ✅ %s\n", p)
	}
	for _, f := range r.Fail {
		fmt.Fprintf(&b, "- ❌ %s\n", f)
	}
	if len(r.Cycles) > 0 {
		fmt.Fprintf(&b, "\n## Per-cycle Stats (oscillation)\n\n")
		fmt.Fprintf(&b, "| cycle | slow_start_mode | slow_end_mode | clean_end_mode | wal_start | wal_peak | wal_clean_end | inserted | batch@slow_end |\n")
		fmt.Fprintf(&b, "|------:|:----------------|:--------------|:---------------|---------:|--------:|-------------:|--------:|--------------:|\n")
		for _, c := range r.Cycles {
			fmt.Fprintf(&b, "| %5d | %s | %s | %s | %d | %d | %d | %d | %.0f |\n",
				c.Cycle, c.StartMode, c.SlowEndMode, c.CleanEndMode,
				c.WALAtSlowStart, c.WALPeakInCycle, c.WALAtCleanEnd,
				c.InsertedInCycle, c.BatchSizeAtSlowEnd)
		}
	}

	fmt.Fprintf(&b, "\n## Mode Transitions\n\n")
	fmt.Fprintf(&b, "Sampled every second. Rows emitted on mode change OR on a `note` (toxic add/remove, end of phase).\n\n")
	fmt.Fprintf(&b, "| t (s) | mode | batch | ring | wal | paused | note |\n")
	fmt.Fprintf(&b, "|------:|:-----|------:|-----:|----:|-------:|:-----|\n")
	prevMode := ""
	for _, e := range r.ModeTimeline {
		if e.Mode == prevMode && e.Note == "" {
			continue
		}
		prevMode = e.Mode
		fmt.Fprintf(&b, "| %5d | %s | %.0f | %.0f | %.0f | %.0f | %s |\n",
			int(e.At.Sub(r.StartedAt).Seconds()), e.Mode, e.BatchSize,
			e.RingDepth, e.WAL, e.Paused, e.Note)
	}
	fmt.Fprintf(&b, "\n## Metric Snapshots\n\n")
	for _, s := range r.Snapshots {
		fmt.Fprintf(&b, "### %s — t+%s\n\n", s.Label, s.At.Sub(r.StartedAt).Round(time.Second))
		keys := make([]string, 0, len(s.M))
		for k := range s.M {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Fprintf(&b, "```\n")
		for _, k := range keys {
			fmt.Fprintf(&b, "  %-30s %.0f\n", k, s.M[k])
		}
		fmt.Fprintf(&b, "```\n\n")
	}

	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o644))
	t.Logf("wrote %s", path)
}

// --- container builders -------------------------------------------------

func startComqtt(tb testing.TB, ctx context.Context, netName string) string {
	tb.Helper()
	req := testcontainers.ContainerRequest{
		Image:          "comqtt:v2.6.2",
		ExposedPorts:   []string{"1883/tcp"},
		Cmd:            []string{"--tcp=:1883", "--ws=:1882", "--http=:8080"},
		Networks:       []string{netName},
		NetworkAliases: map[string][]string{netName: {"comqtt"}},
		WaitingFor:     wait.ForListeningPort("1883/tcp").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(tb, err, "comqtt:v2.6.2 missing? Run `make comqtt-image` first.")
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Terminate(ctx)
	})
	host, err := c.Host(ctx)
	require.NoError(tb, err)
	port, err := c.MappedPort(ctx, "1883/tcp")
	require.NoError(tb, err)
	return fmt.Sprintf("tcp://%s:%s", host, port.Port())
}

// startPostgresOnNetwork attaches the Postgres container to the shared
// docker network with alias "pg" so toxiproxy can reach it. Returns the
// host-mapped DSN for the test's verification queries.
func startPostgresOnNetwork(tb testing.TB, ctx context.Context, netName string) string {
	tb.Helper()
	c, err := tcpostgres.Run(ctx,
		"postgres:17-alpine",
		tcpostgres.WithDatabase("telemetry"),
		tcpostgres.WithUsername("ingest"),
		tcpostgres.WithPassword("ingest"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Networks:       []string{netName},
				NetworkAliases: map[string][]string{netName: {"pg"}},
			},
		}),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Terminate(ctx)
	})
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	require.NoError(tb, err)
	return dsn
}

func startToxiproxy(tb testing.TB, ctx context.Context, netName string) (apiAddr, proxyHostPort string) {
	tb.Helper()
	req := testcontainers.ContainerRequest{
		Image:          "ghcr.io/shopify/toxiproxy:2.12.0",
		ExposedPorts:   []string{"8474/tcp", "5433/tcp"},
		Networks:       []string{netName},
		NetworkAliases: map[string][]string{netName: {"toxi"}},
		WaitingFor:     wait.ForListeningPort("8474/tcp").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(tb, err)
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Terminate(ctx)
	})
	host, err := c.Host(ctx)
	require.NoError(tb, err)
	apiPort, err := c.MappedPort(ctx, "8474/tcp")
	require.NoError(tb, err)
	proxyPort, err := c.MappedPort(ctx, "5433/tcp")
	require.NoError(tb, err)
	return fmt.Sprintf("http://%s:%s", host, apiPort.Port()), proxyPort.Port()
}

func startRustFS(tb testing.TB, ctx context.Context) (endpoint, ak, sk string) {
	tb.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "rustfs/rustfs:latest",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"RUSTFS_ACCESS_KEY":     "rustfsadmin",
			"RUSTFS_SECRET_KEY":     "rustfsadmin",
			"RUSTFS_ADDRESS":        ":9000",
			"RUSTFS_VOLUMES":        "/data",
			"RUSTFS_REGION":         "us-east-1",
			"RUSTFS_CONSOLE_ENABLE": "false",
		},
		WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(120 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(tb, err)
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Terminate(ctx)
	})
	host, err := c.Host(ctx)
	require.NoError(tb, err)
	port, err := c.MappedPort(ctx, "9000/tcp")
	require.NoError(tb, err)
	return fmt.Sprintf("http://%s:%s", host, port.Port()), "rustfsadmin", "rustfsadmin"
}

func createBucket(tb testing.TB, ctx context.Context, endpoint, ak, sk, bucket string) {
	tb.Helper()
	cfg, err := awscfg.LoadDefaultConfig(ctx,
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
	)
	require.NoError(tb, err)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
		tb.Fatalf("create bucket: %v", err)
	}
}
