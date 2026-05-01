//go:build slowdown

package slowdown

import (
	"context"
	"fmt"
	"testing"
	"time"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// Phase durations. Each scenario is composed of:
//
//	warmup  -- healthy traffic (mode should stay normal)
//	toxic   -- the fault is injected
//	recovery + drain
//
// The full scenario is at least 10 minutes per the milestone spec.
// Override knobs land in environment variables for short development
// runs (the per-scenario report records the override).
func phaseDurations() (warmup, toxic, drain time.Duration) {
	warmup = envDuration("SLOWDOWN_WARMUP", 1*time.Minute)
	toxic = envDuration("SLOWDOWN_TOXIC", 5*time.Minute)
	drain = envDuration("SLOWDOWN_DRAIN", 4*time.Minute)
	return
}

// targetRate is shared across scenarios. The spec calls for 10K msg/s.
func targetRate() int { return envInt("SLOWDOWN_RATE", 10_000) }

// runScenario is the shared driver. tox / untox are called when the
// toxic window opens and closes. peakWALCapMul is the soft assertion
// for WAL high-water relative to ring capacity (e.g. 0.5 for the
// moderate scenario, 1.5 for the severe one).
type scenarioOpts struct {
	name             string
	toxicDuration    time.Duration
	expectMode       string  // mode we require to have been observed during toxic window
	expectPaused     bool    // whether subscriber.paused must increment
	maxWALMultiplier float64 // assertion: peakWAL <= MaxMessages * this (best-effort)
	apply            func(t *testing.T, h *Harness)
	revert           func(t *testing.T, h *Harness)
}

func runScenario(t *testing.T, h *Harness, opt scenarioOpts) {
	t.Helper()
	warmup, _, drain := phaseDurations()
	rate := targetRate()
	// Parent ctx budget = warmup + toxic + drain + 15 min for the
	// awaitFullDrain wait (10 min spec + 5 min slop).
	ctx, cancel := context.WithTimeout(h.ctx, warmup+opt.toxicDuration+drain+15*time.Minute)
	defer cancel()

	report := scenarioReport{
		Scenario:   opt.name,
		StartedAt:  time.Now(),
		TargetRate: rate,
	}

	tlCtx, tlCancel := context.WithCancel(ctx)
	defer tlCancel()
	h.startTimelineRecorder(tlCtx)

	// Async load generator. We drive load throughout warmup + toxic +
	// drain so the pipeline never goes idle during the run.
	loadDone := make(chan [2]int64, 1)
	loadCtx, loadCancel := context.WithCancel(ctx)
	defer loadCancel()
	go func() {
		s, e := h.driveLoad(loadCtx, rate, warmup+opt.toxicDuration+drain)
		loadDone <- [2]int64{s, e}
	}()

	// Phase 1: warmup
	t.Logf("[%s] warmup %s @ %d msg/s", opt.name, warmup, rate)
	h.markTimeline("warmup start")
	report.Snapshots = append(report.Snapshots, h.snapshot("after_warmup_begin"))
	time.Sleep(warmup)
	report.Snapshots = append(report.Snapshots, h.snapshot("warmup_end"))
	h.markTimeline("warmup end")

	// Phase 2: apply toxic
	t.Logf("[%s] applying toxic for %s", opt.name, opt.toxicDuration)
	opt.apply(t, h)
	h.markTimeline("toxic ON")
	report.Snapshots = append(report.Snapshots, h.snapshot("toxic_applied"))

	mid := time.Now().Add(opt.toxicDuration / 2)
	time.Sleep(time.Until(mid))
	report.Snapshots = append(report.Snapshots, h.snapshot("toxic_mid"))

	time.Sleep(time.Until(mid.Add(opt.toxicDuration / 2)))
	report.Snapshots = append(report.Snapshots, h.snapshot("toxic_end"))

	// Phase 3: revert
	t.Logf("[%s] reverting toxic; entering drain phase", opt.name)
	opt.revert(t, h)
	h.markTimeline("toxic OFF")

	// Phase 4: drain — keep producing load for `drain`, then stop and
	// wait for WAL to drain.
	time.Sleep(drain)
	loadCancel()
	res := <-loadDone
	report.Sent, report.PublishErrs = res[0], res[1]
	h.markTimeline("load stopped")

	// Wait for the conservation invariant to hold: every message the
	// subscriber acked has reached PG (count distinct >= acked) AND the
	// ring + WAL are empty. This is the real "WAL drained to <1%" plus a
	// PG-side flush wait that the previous WAL-only check missed.
	preDrainPeak := walPeakFromTimeline(h)
	drainStart := time.Now()
	drainedFully, peak := h.awaitFullDrain(ctx, preDrainPeak, 10*time.Minute)
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

	// --- Assertions / pass-fail capture --------------------------------
	maxObservedMode := observedMaxMode(h)
	switch opt.expectMode {
	case "elevated":
		if maxObservedMode >= 1 {
			report.Pass = append(report.Pass, "flusher entered elevated mode during toxic window")
		} else {
			report.Fail = append(report.Fail, "flusher never reached elevated mode")
		}
	case "critical":
		if maxObservedMode >= 2 {
			report.Pass = append(report.Pass, "flusher entered critical mode during toxic window")
		} else {
			report.Fail = append(report.Fail, "flusher never reached critical mode")
		}
	}
	if opt.expectPaused {
		if testutil.ToFloat64(h.SubM.Pauses) > 0 {
			report.Pass = append(report.Pass, "subscriber Paused gauge incremented")
		} else {
			report.Fail = append(report.Fail, "subscriber Paused gauge never incremented")
		}
	}
	if testutil.ToFloat64(h.FlushM.DeadLettered) == 0 {
		report.Pass = append(report.Pass, "zero messages dead-lettered (transient PG issues)")
	} else {
		report.Fail = append(report.Fail, fmt.Sprintf("dead-lettered %d messages — transient should never DLQ",
			int(testutil.ToFloat64(h.FlushM.DeadLettered))))
	}

	// Conservation invariant for this test:
	//   count(distinct dedup_key in PG) == flusher.inserted_total
	// flusher.inserted is the number of rows the flusher committed via
	// CopyMessages → INSERT ... ON CONFLICT DO NOTHING. The acked_total
	// counter is higher when there are dedup collisions on
	// same-device-same-nanosecond (collapsed by the unique index by
	// design, see the steady-state stress test for the same observation).
	if report.DistinctKeys == report.FlusherInserted {
		report.Pass = append(report.Pass, fmt.Sprintf(
			"conservation OK (distinct=%d == flusher.inserted=%d, dedup_collisions=%d)",
			report.DistinctKeys, report.FlusherInserted, report.DedupCollisions))
	} else if report.DistinctKeys > report.FlusherInserted {
		report.Pass = append(report.Pass, fmt.Sprintf(
			"conservation OK with margin (distinct=%d >= flusher.inserted=%d)",
			report.DistinctKeys, report.FlusherInserted))
	} else {
		report.Fail = append(report.Fail, fmt.Sprintf(
			"LOSS between flusher and PG: distinct=%d < flusher.inserted=%d",
			report.DistinctKeys, report.FlusherInserted))
	}
	if drainedFully {
		report.Pass = append(report.Pass, fmt.Sprintf("WAL drained to <1%% of peak in %s",
			report.DrainTime.Round(time.Second)))
	} else {
		report.Fail = append(report.Fail, fmt.Sprintf("WAL did not drain to <1%% of peak (%d) within 10m", report.WALPeak))
	}
	if opt.maxWALMultiplier > 0 {
		// Best-effort: peak should be bounded by ring + spillage budget.
		// MaxMessages = 100_000 in setupHarness.
		ringMax := int64(100_000)
		bound := int64(float64(ringMax) * opt.maxWALMultiplier)
		if peak <= bound {
			report.Pass = append(report.Pass, fmt.Sprintf("WAL peak %d ≤ %d (%.0fx ring)", peak, bound, opt.maxWALMultiplier))
		} else {
			report.Fail = append(report.Fail, fmt.Sprintf("WAL peak %d exceeded %d (%.0fx ring)", peak, bound, opt.maxWALMultiplier))
		}
	}

	report.EndedAt = time.Now()
	report.ModeTimeline = append([]TimelineEvent(nil), h.timeline...)
	tlCancel()
	writeReport(t, report)

	// Hard fail the test if any criterion missed.
	if len(report.Fail) > 0 {
		t.Fatalf("[%s] FAILED criteria:\n  %v", opt.name, report.Fail)
	}
}

// observedMaxMode returns the highest mode observed in the harness's
// timeline. 0=normal, 1=elevated, 2=critical.
func observedMaxMode(h *Harness) int {
	h.tlMu.Lock()
	defer h.tlMu.Unlock()
	max := 0
	for _, e := range h.timeline {
		switch e.Mode {
		case "elevated":
			if max < 1 {
				max = 1
			}
		case "critical":
			max = 2
		}
	}
	return max
}

// walPeakFromTimeline returns the peak WAL outstanding observed across
// every recorded sample. Source of truth for the high-water assertion.
func walPeakFromTimeline(h *Harness) int64 {
	h.tlMu.Lock()
	defer h.tlMu.Unlock()
	var peak float64
	for _, e := range h.timeline {
		if e.WAL > peak {
			peak = e.WAL
		}
	}
	return int64(peak)
}

// --- Scenario A: moderate slowdown -----------------------------------

func TestSlowdown_Moderate(t *testing.T) {
	if testing.Short() {
		t.Skip("slowdown stress runs are long; skipped in -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	h := setupHarness(t, ctx, "moderate")

	var lat *toxiclient.Toxic
	runScenario(t, h, scenarioOpts{
		name:             "moderate",
		toxicDuration:    envDuration("SLOWDOWN_TOXIC", 5*time.Minute),
		expectMode:       "elevated",
		expectPaused:     false,
		maxWALMultiplier: 0.5,
		apply: func(t *testing.T, h *Harness) {
			t.Helper()
			var err error
			// 300 ms each direction => ~600 ms RTT seen by the flusher.
			lat, err = h.PgProxy.AddToxic("latency_down", "latency", "downstream", 1.0,
				toxiclient.Attributes{"latency": 300})
			require.NoError(t, err)
			_, err = h.PgProxy.AddToxic("latency_up", "latency", "upstream", 1.0,
				toxiclient.Attributes{"latency": 300})
			require.NoError(t, err)
		},
		revert: func(t *testing.T, h *Harness) {
			t.Helper()
			_ = h.PgProxy.RemoveToxic("latency_down")
			_ = h.PgProxy.RemoveToxic("latency_up")
			_ = lat // kept for symmetry; toxic IDs are managed by name above
		},
	})
}

// --- Scenario B: severe slowdown -------------------------------------

func TestSlowdown_Severe(t *testing.T) {
	if testing.Short() {
		t.Skip("slowdown stress runs are long; skipped in -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	h := setupHarness(t, ctx, "severe")

	runScenario(t, h, scenarioOpts{
		name:             "severe",
		toxicDuration:    envDuration("SLOWDOWN_TOXIC", 5*time.Minute),
		expectMode:       "critical",
		expectPaused:     true,
		maxWALMultiplier: 1.5, // severe scenarios may parc more in WAL
		apply: func(t *testing.T, h *Harness) {
			t.Helper()
			_, err := h.PgProxy.AddToxic("latency_down", "latency", "downstream", 1.0,
				toxiclient.Attributes{"latency": 2500})
			require.NoError(t, err)
			_, err = h.PgProxy.AddToxic("latency_up", "latency", "upstream", 1.0,
				toxiclient.Attributes{"latency": 2500})
			require.NoError(t, err)
		},
		revert: func(t *testing.T, h *Harness) {
			t.Helper()
			_ = h.PgProxy.RemoveToxic("latency_down")
			_ = h.PgProxy.RemoveToxic("latency_up")
		},
	})
}

// --- Scenario C: outright outage --------------------------------------

func TestSlowdown_Outage(t *testing.T) {
	if testing.Short() {
		t.Skip("slowdown stress runs are long; skipped in -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	h := setupHarness(t, ctx, "outage")

	runScenario(t, h, scenarioOpts{
		name:             "outage",
		toxicDuration:    envDuration("SLOWDOWN_TOXIC", 3*time.Minute),
		expectMode:       "critical",
		expectPaused:     true,
		maxWALMultiplier: 2.0, // outage is the worst case
		apply: func(t *testing.T, h *Harness) {
			t.Helper()
			require.NoError(t, h.PgProxy.Disable())
		},
		revert: func(t *testing.T, h *Harness) {
			t.Helper()
			require.NoError(t, h.PgProxy.Enable())
		},
	})
}
