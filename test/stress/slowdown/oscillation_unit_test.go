//go:build slowdown

package slowdown

import "testing"

// detectRatchet is the heart of the oscillation no-ratchet criterion;
// keep it covered by direct unit tests so a regression doesn't sneak
// in via a passing scenario where it should have failed.
func TestDetectRatchet_Cases(t *testing.T) {
	cases := []struct {
		name     string
		clean    []int64 // WALAtCleanEnd per cycle
		wantFail bool
	}{
		{
			// Healthy oscillation: WAL drains fully between cycles
			// (ring absorbs everything; all clean-end WAL = 0).
			name:     "healthy_all_zero",
			clean:    []int64{0, 0, 0, 0},
			wantFail: false,
		},
		{
			// One transient spike that recovers — not a ratchet.
			name:     "transient_spike",
			clean:    []int64{0, 19612, 0},
			wantFail: false,
		},
		{
			// Real ratchet: each cycle leaves more in the WAL than
			// the previous, peaking on the LAST cycle. This is the
			// bug signature.
			name:     "true_ratchet",
			clean:    []int64{0, 1000, 5000, 25000},
			wantFail: true,
		},
		{
			// Max is in the middle, not the last cycle. Likely
			// noise in the recovery, not ratcheting.
			name:     "max_in_middle",
			clean:    []int64{0, 1000, 50000, 5000},
			wantFail: false,
		},
		{
			// Plateau — same level every cycle. Not a ratchet (system
			// is at steady-state load).
			name:     "plateau",
			clean:    []int64{0, 5000, 5000, 5000},
			wantFail: false,
		},
		{
			// Below threshold for cycle count.
			name:     "too_few_cycles",
			clean:    []int64{0, 99999},
			wantFail: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cs := make([]cycleStats, len(tc.clean))
			for i, v := range tc.clean {
				cs[i] = cycleStats{Cycle: i + 1, WALAtCleanEnd: v}
			}
			got, detail := detectRatchet(cs)
			if got != tc.wantFail {
				t.Fatalf("detectRatchet(%v) = (%v, %q), want fail=%v",
					tc.clean, got, detail, tc.wantFail)
			}
		})
	}
}
