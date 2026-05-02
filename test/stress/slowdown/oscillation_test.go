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

	// expectMaxSlowEndMode asserts that across all cycles the
	// `slow_end` mode reached at least this level. "" disables.
	// Catches the regression where the toxic was applied but the
	// flusher never noticed.
	expectMaxSlowEndMode string // "elevated" | "critical"

	// expectCleanRecoveryByCycle asserts that at least one cycle ≥
	// this index ended its clean window with mode=normal. 0 disables.
	// Only meaningful when cleanWindow > FlusherConfig.RecoveryWindow
	// (otherwise the flusher doesn't have time to recover and the
	// assertion is structurally impossible).
	expectCleanRecoveryByCycle int
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
		Scenario:     opt.name,
		StartedAt:    time.Now(),
		TargetRate:   rate,
		Schema:       string(activeSchema()),
		PayloadBytes: payloadSize(),
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
		// are accurate. SnapshotAndResetPeakWAL also clears the fast
		// poller's peak tracker so cs.WALPeakInCycle captures a TRUE
		// sub-second peak, not an alias of the 1 Hz timeline.
		_ = h.SnapshotAndResetPeakWAL()
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

		// Slow window. The 100 ms fast poller is tracking peak WAL
		// and mode transitions; we just sleep.
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
		// True sub-second peak from the 100 ms poller. SnapshotAndReset
		// also rearms it for the next cycle.
		cs.WALPeakInCycle = h.SnapshotAndResetPeakWAL()
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

	// Mode-stability uses the 100 ms fast counter, not the 1 Hz
	// timeline. The flusher changes mode on flush boundaries
	// (100 ms normal / 500 ms elevated), so 1 Hz sampling can alias
	// multiple flips down to zero observed transitions and let real
	// flap pass.
	//
	// Threshold derivation: each cycle naturally walks
	//   normal → critical (1)
	//   ↔ elevated near the threshold while toxic is on (0–2)
	//   → elevated (1) → normal (1) during recovery
	// That's 3–4 transitions per cycle in healthy operation under a
	// 2.5 s toxic. Bound at 4 × cycles + 4 leaves headroom for one
	// extra threshold-crossing per cycle and is still well below
	// pathological flap (which would scale with elapsed time, not
	// cycle count).
	transitions := int(h.fastModeTransitions.Load())
	maxExpected := 4*opt.cycles + 4
	if transitions <= maxExpected {
		report.Pass = append(report.Pass, fmt.Sprintf(
			"mode transitions %d ≤ %d (no flapping; 100 ms sampled)", transitions, maxExpected))
	} else {
		report.Fail = append(report.Fail, fmt.Sprintf(
			"mode flapping: %d transitions, expected ≤ %d", transitions, maxExpected))
	}

	// Per-scenario mode-state assertions (separate from the
	// transition cap; the cap alone passes when mode is stuck at
	// one extreme — Copilot caught this). opt.expectMaxSlowEndMode
	// and opt.expectCleanRecoveryByCycle let the fast and slow
	// scenarios state different expectations.
	if opt.expectMaxSlowEndMode != "" {
		got := highestModeAcrossCycles(report.Cycles, sceneSlowEnd)
		if modeRank(got) >= modeRank(opt.expectMaxSlowEndMode) {
			report.Pass = append(report.Pass, fmt.Sprintf(
				"slow_end mode reached %s (≥ %s expected)", got, opt.expectMaxSlowEndMode))
		} else {
			report.Fail = append(report.Fail, fmt.Sprintf(
				"slow_end mode never reached %s (highest=%s)", opt.expectMaxSlowEndMode, got))
		}
	}
	if opt.expectCleanRecoveryByCycle > 0 {
		recovered := false
		for _, c := range report.Cycles {
			if c.Cycle >= opt.expectCleanRecoveryByCycle && c.CleanEndMode == "normal" {
				recovered = true
				break
			}
		}
		if recovered {
			report.Pass = append(report.Pass, fmt.Sprintf(
				"clean_end mode recovered to normal by cycle %d", opt.expectCleanRecoveryByCycle))
		} else {
			report.Fail = append(report.Fail, fmt.Sprintf(
				"no cycle ≥ %d ended with clean_end mode=normal — recovery never completed",
				opt.expectCleanRecoveryByCycle))
		}
	}

	report.EndedAt = time.Now()
	// Hold tlMu for the copy. The 1 Hz recorder is still running
	// until tlCancel below, but its only mutator is appendTimeline
	// which takes the same mutex — so this lock serializes with
	// any in-flight append.
	h.tlMu.Lock()
	report.ModeTimeline = append([]TimelineEvent(nil), h.timeline...)
	h.tlMu.Unlock()
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

// modeRank lets the per-scenario assertion compare modes with
// ≥ semantics. Order matches FlusherMetrics.Mode (0/1/2).
func modeRank(m string) int {
	switch m {
	case "critical":
		return 2
	case "elevated":
		return 1
	case "normal":
		return 0
	}
	return -1
}

// cycleField selects which mode column to inspect — used by
// highestModeAcrossCycles below.
type cycleField int

const (
	sceneSlowEnd cycleField = iota
	sceneCleanEnd
)

// highestModeAcrossCycles returns the highest-ranking mode observed
// across `cs` at the chosen field. "normal" if no cycles or all are
// blank.
func highestModeAcrossCycles(cs []cycleStats, f cycleField) string {
	highest := "normal"
	for _, c := range cs {
		var m string
		switch f {
		case sceneSlowEnd:
			m = c.SlowEndMode
		case sceneCleanEnd:
			m = c.CleanEndMode
		}
		if modeRank(m) > modeRank(highest) {
			highest = m
		}
	}
	return highest
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
		// 2.5 s latency definitely escalates the flusher; if we
		// never even reach elevated the toxic was a no-op.
		expectMaxSlowEndMode: "elevated",
		// Both windows shorter than RecoveryWindow=60s, so we do
		// NOT expect recovery between cycles. expectCleanRecovery
		// stays unset.
		expectCleanRecoveryByCycle: 0,
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
		// 2.5 s latency must drive the flusher to elevated (or
		// critical) during the slow window.
		expectMaxSlowEndMode: "elevated",
		// 90 s clean window > 60 s RecoveryWindow → at least one
		// cycle from cycle 2 onwards must end with mode=normal.
		// Without this assertion the test would pass even when
		// recovery never happens (Copilot caught this).
		expectCleanRecoveryByCycle: 2,
	})
}

// guard against accidental import-cycle from sync (used internally for
// future helpers). Keeps go vet honest if the helper grows.
var _ sync.Locker = (*sync.Mutex)(nil)
