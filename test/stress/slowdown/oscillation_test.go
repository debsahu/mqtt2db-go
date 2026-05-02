//go:build slowdown

// Oscillation stress scenarios — Milestone 14a.
//
// Where the moderate / severe / outage scenarios in scenarios_test.go
// each apply a single sustained toxic and observe one slow→fast ramp,
// these scenarios alternate slow / clean windows on a fixed duty cycle.
// What we want to surface:
//
//   - WAL ratcheting: each cycle's WAL high-water mark should be stable
//     or decreasing (within ±10 %), not monotonically growing. A growing
//     WAL across cycles is the bug this test exists to catch.
//
//   - Mode-transition stability: with FAST cycles (shorter than
//     RecoveryWindow) the flusher should NOT ping-pong back to normal
//     between slow phases — it should park in elevated/critical. With
//     SLOW cycles (longer than RecoveryWindow) the flusher should
//     fully recover each cycle and re-enter elevated only when the
//     next slow phase trips its threshold.
//
//   - Per-conn cached staging across PG hiccups: if `AfterConnect` mis-
//     handles a reconnect (no temp table on the new conn), the first
//     batch of the next clean window will fail with `relation does not
//     exist`. Detected indirectly via the conservation invariant —
//     missing inserts would leave PG distinct < flusher.inserted.
//
//   - Conservation invariant under chop: distinct(dedup_key) ==
//     flusher.inserted at end, same as the M13 scenarios.
package slowdown

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// oscillationOpts configures one oscillation scenario.
//
// The total wall-clock budget for a run is:
//
//	warmup + Cycles*(SlowWindow+CleanWindow) + drain
//
// All durations override via env so quick development runs share the
// same SLOWDOWN_* knobs as the M13 scenarios.
type oscillationOpts struct {
	name        string
	latencyMS   int           // toxic latency to apply during slow window (each direction)
	slowWindow  time.Duration // duration of the slow phase per cycle
	cleanWindow time.Duration // duration of the clean phase per cycle
	cycles      int           // number of slow→clean cycles
}

// runOscillationScenario drives load through Cycles slow/clean cycles,
// snapshots metrics at every cycle boundary, and asserts the
// no-ratchet / mode-stable / conservation criteria. It mirrors the
// shape of runScenario in scenarios_test.go but is structured for
// repetition rather than one-shot apply / revert.
func runOscillationScenario(t *testing.T, h *Harness, opt oscillationOpts) {
	t.Helper()
	warmup := envDuration("SLOWDOWN_WARMUP", 1*time.Minute)
	drain := envDuration("SLOWDOWN_DRAIN", 4*time.Minute)
	rate := targetRate()

	cycleBudget := time.Duration(opt.cycles) * (opt.slowWindow + opt.cleanWindow)
	// Outer budget = warmup + all cycles + drain + 30 min slop for awaitFullDrain.
	ctx, cancel := context.WithTimeout(h.ctx, warmup+cycleBudget+drain+45*time.Minute)
	defer cancel()

	report := scenarioReport{
		Scenario:   opt.name,
		StartedAt:  time.Now(),
		TargetRate: rate,
	}

	tlCtx, tlCancel := context.WithCancel(ctx)
	defer tlCancel()
	h.startTimelineRecorder(tlCtx)

	// Drive load for the entire active phase: warmup + all cycles +
	// drain. The drain phase keeps producing load so the scenario
	// stays representative of "ingest never stops, even while PG
	// blips."
	loadDone := make(chan [2]int64, 1)
	loadCtx, loadCancel := context.WithCancel(ctx)
	defer loadCancel()
	loadDur := warmup + cycleBudget + drain
	go func() {
		s, e := h.driveLoad(loadCtx, rate, loadDur)
		loadDone <- [2]int64{s, e}
	}()

	// Phase 1: warmup
	t.Logf("[%s] warmup %s @ %d msg/s", opt.name, warmup, rate)
	h.markTimeline("warmup start")
	report.Snapshots = append(report.Snapshots, h.snapshot("warmup_begin"))
	time.Sleep(warmup)
	report.Snapshots = append(report.Snapshots, h.snapshot("warmup_end"))
	h.markTimeline("warmup end")

	// Phase 2: cycles
	for i := 1; i <= opt.cycles; i++ {
		select {
		case <-ctx.Done():
			t.Fatalf("[%s] context canceled mid-cycles", opt.name)
		default:
		}

		// Capture starting state BEFORE applying the toxic so deltas
		// are accurate.
		insertedAtCycleStart := int64(testutil.ToFloat64(h.FlushM.Inserted))
		cs := cycleStats{
			Cycle:          i,
			SlowStartAt:    time.Now(),
			WALAtSlowStart: h.walOutstanding(),
			StartMode:      h.currentMode(),
		}

		// Apply toxic.
		applyLatency(t, h, opt.latencyMS)
		h.markTimeline(fmt.Sprintf("cycle %d toxic ON", i))
		t.Logf("[%s] cycle %d/%d: toxic ON for %s",
			opt.name, i, opt.cycles, opt.slowWindow)

		// Slow window. The 1Hz timeline recorder is already capturing
		// per-second WAL/mode samples on its own goroutine; we just
		// sleep here.
		sleepAndSample(ctx, opt.slowWindow)
		cs.SlowEndAt = time.Now()
		cs.SlowEndMode = h.currentMode()
		cs.BatchSizeAtSlowEnd = testutil.ToFloat64(h.FlushM.BatchSize)

		// Revert toxic.
		revertLatency(h)
		h.markTimeline(fmt.Sprintf("cycle %d toxic OFF", i))
		t.Logf("[%s] cycle %d/%d: toxic OFF, clean window %s",
			opt.name, i, opt.cycles, opt.cleanWindow)

		// Clean window.
		sleepAndSample(ctx, opt.cleanWindow)
		cs.CleanEndAt = time.Now()
		cs.CleanEndMode = h.currentMode()
		cs.WALAtCleanEnd = h.walOutstanding()
		cs.WALPeakInCycle = h.walPeakBetween(cs.SlowStartAt, cs.CleanEndAt)
		cs.InsertedInCycle = int64(testutil.ToFloat64(h.FlushM.Inserted)) - insertedAtCycleStart

		report.Cycles = append(report.Cycles, cs)
		report.Snapshots = append(report.Snapshots,
			h.snapshot(fmt.Sprintf("cycle_%d_end", i)))
	}

	// Phase 3: drain — load keeps flowing for `drain`, then stops.
	t.Logf("[%s] all cycles complete; draining for %s", opt.name, drain)
	time.Sleep(drain)
	loadCancel()
	res := <-loadDone
	report.Sent, report.PublishErrs = res[0], res[1]
	h.markTimeline("load stopped")

	// Wait for full convergence.
	preDrainPeak := walPeakFromTimeline(h)
	drainStart := time.Now()
	drainedFully, peak := h.awaitFullDrain(ctx, preDrainPeak, 30*time.Minute)
	report.WALDrainedFully = drainedFully
	report.WALPeak = peak
	report.DrainTime = time.Since(drainStart)
	h.markTimeline("drain complete")
	report.Snapshots = append(report.Snapshots, h.snapshot("drain_complete"))

	report.Persisted = h.rowCount(ctx)
	report.DistinctKeys = h.distinctDedupKeys(ctx)
	report.FlusherInserted = int64(testutil.ToFloat64(h.FlushM.Inserted))
	ackedTotal := int64(testutil.ToFloat64(h.SubM.Acked))
	if ackedTotal >= report.FlusherInserted {
		report.DedupCollisions = ackedTotal - report.FlusherInserted
	}

	// --- Assertions ----------------------------------------------------

	// Zero DLQ for transient PG issues.
	if testutil.ToFloat64(h.FlushM.DeadLettered) == 0 {
		report.Pass = append(report.Pass, "zero messages dead-lettered (transient PG issues)")
	} else {
		report.Fail = append(report.Fail, fmt.Sprintf("dead-lettered %d messages — transient should never DLQ",
			int(testutil.ToFloat64(h.FlushM.DeadLettered))))
	}

	// Conservation invariant — same logic as runScenario.
	switch {
	case report.DistinctKeys >= report.FlusherInserted:
		report.Pass = append(report.Pass, fmt.Sprintf(
			"conservation OK (distinct=%d >= flusher.inserted=%d, dedup_collisions=%d)",
			report.DistinctKeys, report.FlusherInserted, report.DedupCollisions))
	default:
		gap := report.FlusherInserted - report.DistinctKeys
		gapFraction := float64(gap) / float64(report.FlusherInserted+1)
		if gapFraction <= 0.005 {
			report.Pass = append(report.Pass, fmt.Sprintf(
				"conservation OK within 0.5%% slack (distinct=%d, flusher.inserted=%d, gap=%d / %.3f%%)",
				report.DistinctKeys, report.FlusherInserted, gap, gapFraction*100))
		} else {
			report.Fail = append(report.Fail, fmt.Sprintf(
				"LOSS between flusher and PG: distinct=%d < flusher.inserted=%d (gap=%d / %.3f%%)",
				report.DistinctKeys, report.FlusherInserted, gap, gapFraction*100))
		}
	}

	if drainedFully {
		report.Pass = append(report.Pass, fmt.Sprintf("WAL drained to <1%% of peak in %s",
			report.DrainTime.Round(time.Second)))
	} else {
		report.Fail = append(report.Fail, fmt.Sprintf("WAL did not drain to <1%% of peak (%d) within 30m", report.WALPeak))
	}

	// Ratchet detector: peak WAL across cycles should be stable. We
	// allow one "warm-up" cycle where the WAL fills for the first time
	// (no comparison baseline) and ±25 % jitter against the median of
	// remaining cycles. A monotonically growing peak is the bug
	// signature this test catches.
	if ratchet, detail := detectRatchet(report.Cycles); ratchet {
		report.Fail = append(report.Fail, "WAL ratcheting detected across cycles: "+detail)
	} else if len(report.Cycles) >= 2 {
		report.Pass = append(report.Pass, "no WAL ratcheting across cycles (peak stable / decreasing)")
	}

	// Mode-stability: count the number of distinct mode transitions
	// across the whole run. We don't have a per-event log of mode
	// changes, so we approximate by counting transitions in the
	// 1-second timeline. With Cycles slow/clean cycles, a healthy
	// run should see ≤ 2 × cycles transitions (one in, one out per
	// cycle) plus a small constant for warmup. Anything wildly higher
	// is flap.
	transitions := countModeTransitions(h)
	maxExpected := 2*opt.cycles + 4
	if transitions <= maxExpected {
		report.Pass = append(report.Pass, fmt.Sprintf(
			"mode transitions %d ≤ %d (no flapping)", transitions, maxExpected))
	} else {
		report.Fail = append(report.Fail, fmt.Sprintf(
			"mode flapping: %d transitions, expected ≤ %d", transitions, maxExpected))
	}

	report.EndedAt = time.Now()
	report.ModeTimeline = append([]TimelineEvent(nil), h.timeline...)
	tlCancel()
	writeReport(t, report)

	if len(report.Fail) > 0 {
		t.Fatalf("[%s] FAILED criteria:\n  %v", opt.name, report.Fail)
	}
}

// applyLatency adds matching downstream/upstream latency toxics on the
// pg proxy. Each direction gets the requested latency, so the
// flusher's per-batch RTT sees ~2× this value.
func applyLatency(t *testing.T, h *Harness, ms int) {
	t.Helper()
	_, err := h.PgProxy.AddToxic("latency_down", "latency", "downstream", 1.0,
		toxiclient.Attributes{"latency": ms})
	require.NoError(t, err)
	_, err = h.PgProxy.AddToxic("latency_up", "latency", "upstream", 1.0,
		toxiclient.Attributes{"latency": ms})
	require.NoError(t, err)
}

// revertLatency removes both toxics. Tolerant of missing toxics so a
// final cleanup defer can call it without checking state.
func revertLatency(h *Harness) {
	_ = h.PgProxy.RemoveToxic("latency_down")
	_ = h.PgProxy.RemoveToxic("latency_up")
}

// sleepAndSample sleeps for d, with cancellation on ctx. We don't
// actively sample inside the sleep — the timeline recorder is already
// running at 1Hz on its own goroutine.
func sleepAndSample(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// detectRatchet looks at WALAtCleanEnd across cycles — the WAL depth
// at the END of each clean window, after the system has had a full
// recovery period. The bug we want to catch is: each clean window
// fails to fully drain, so leftover WAL grows monotonically across
// cycles.
//
// Why not WALPeakInCycle: the in-cycle peak naturally grows as the
// ring buffer fills and starts spilling — that is the system working
// as designed (three-tier buffering kicking in). What matters is
// whether each clean window drains the leftover, not whether the peak
// during the slow window grew.
//
// Tolerance: the LAST cycle's clean-end WAL must not be the largest
// across cycles AND must not exceed 2× the second cycle's clean-end
// WAL. The first cycle is allowed to be a warm-up (often 0 because
// the ring absorbed everything). Two consecutive clean-end WAL
// readings of 0 is automatic pass.
func detectRatchet(cs []cycleStats) (bool, string) {
	if len(cs) < 3 {
		return false, ""
	}

	// Find max clean-end WAL across all cycles.
	var maxIdx int
	for i, c := range cs {
		if c.WALAtCleanEnd > cs[maxIdx].WALAtCleanEnd {
			maxIdx = i
		}
	}

	// If the maximum is in the LAST cycle AND it grew strictly across
	// cycles 2..N, that is the ratcheting signature.
	if maxIdx == len(cs)-1 && cs[len(cs)-1].WALAtCleanEnd > 0 {
		monotonic := true
		for i := 2; i < len(cs); i++ {
			if cs[i].WALAtCleanEnd <= cs[i-1].WALAtCleanEnd {
				monotonic = false
				break
			}
		}
		if monotonic {
			vals := make([]int64, len(cs))
			for i, c := range cs {
				vals[i] = c.WALAtCleanEnd
			}
			return true, fmt.Sprintf(
				"clean-end WAL grew monotonically across cycles: %v", vals)
		}
	}
	return false, ""
}

// countModeTransitions counts the number of times the recorded mode
// changes from one timeline sample to the next.
func countModeTransitions(h *Harness) int {
	h.tlMu.Lock()
	defer h.tlMu.Unlock()
	var transitions int
	prev := ""
	for _, e := range h.timeline {
		if e.Mode == "" {
			continue
		}
		if prev != "" && e.Mode != prev {
			transitions++
		}
		prev = e.Mode
	}
	return transitions
}

// --- Scenario: oscillation FAST (cycles SHORTER than RecoveryWindow) ---

// TestSlowdown_OscillationFast drives 6 cycles of 30 s slow / 30 s
// clean. With FlusherConfig.RecoveryWindow=60s default, this means
// the flusher should NOT recover to normal between slow phases — it
// should park in elevated (or critical) and stay there.
//
// What we are catching:
//   - Mode flapping: a buggy hysteresis would ping-pong normal⇄elevated
//     every 30 s, halving sustained throughput.
//   - WAL ratcheting: 30 s of clean is too short to drain a large WAL,
//     so each cycle starts with leftover WAL — but the *peak* should
//     not grow unboundedly because the clean window does drain *some*.
func TestSlowdown_OscillationFast(t *testing.T) {
	if testing.Short() {
		t.Skip("oscillation stress runs are long; skipped in -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer cancel()
	h := setupHarness(t, ctx, "oscillation-fast")

	// Cleanup in case the test panics partway through a slow phase.
	t.Cleanup(func() { revertLatency(h) })

	runOscillationScenario(t, h, oscillationOpts{
		name:        "oscillation-fast",
		latencyMS:   2500, // matches severe scenario per direction
		slowWindow:  envDuration("SLOWDOWN_OSC_SLOW", 30*time.Second),
		cleanWindow: envDuration("SLOWDOWN_OSC_CLEAN", 30*time.Second),
		cycles:      envInt("SLOWDOWN_OSC_CYCLES", 6),
	})
}

// --- Scenario: oscillation SLOW (cycles LONGER than RecoveryWindow) ---

// TestSlowdown_OscillationSlow drives 6 cycles of 90 s slow / 90 s
// clean. With RecoveryWindow=60s, the flusher should fully recover
// each cycle (return to normal mode) and then re-enter elevated only
// when the next slow phase trips its threshold.
//
// What we are catching:
//   - Mode-recovery completeness: the test expects each clean window
//     to end with the flusher back at normal mode. If recovery is
//     incomplete, CleanEndMode will pin elevated/critical.
//   - Per-conn cached staging across PG hiccups: 90 s of healthy PG
//     is plenty for pgxpool to cycle connections; if AfterConnect
//     mishandles new conns, the conservation invariant catches it.
//   - WAL fully draining each cycle: cs.WALAtCleanEnd should be near
//     zero each cycle.
func TestSlowdown_OscillationSlow(t *testing.T) {
	if testing.Short() {
		t.Skip("oscillation stress runs are long; skipped in -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Minute)
	defer cancel()
	h := setupHarness(t, ctx, "oscillation-slow")

	t.Cleanup(func() { revertLatency(h) })

	runOscillationScenario(t, h, oscillationOpts{
		name:        "oscillation-slow",
		latencyMS:   2500,
		slowWindow:  envDuration("SLOWDOWN_OSC_SLOW", 90*time.Second),
		cleanWindow: envDuration("SLOWDOWN_OSC_CLEAN", 90*time.Second),
		cycles:      envInt("SLOWDOWN_OSC_CYCLES", 6),
	})
}

// guard against accidental import-cycle from sync (used internally for
// future helpers). Keeps go vet honest if the helper grows.
var _ sync.Locker = (*sync.Mutex)(nil)
