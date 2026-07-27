package indexlanethroughput

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// envInt reads a sizing override, so the same harness serves a fast in-suite
// smoke run and a full sweep without two code paths.
func envInt(key string, fallback int) int {
	if raw := os.Getenv(key); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			return v
		}
	}
	return fallback
}

// TestLaneSweepSmoke keeps the harness honest in ordinary test runs: it proves
// the measurement actually measures something before anyone trusts a sweep from
// it. It stays small enough not to slow the suite.
func TestLaneSweepSmoke(t *testing.T) {
	corpus := Corpus{Prefixes: 8, ObjectsPerPrefix: 200, PageSize: 100}
	res, err := Run(context.Background(), RunSpec{
		Corpus: corpus, Lanes: 4, Concurrency: 8, Root: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.ObjectsObserved != int64(corpus.totalObjects()) {
		t.Fatalf("observed %d objects, want %d", res.ObjectsObserved, corpus.totalObjects())
	}
	if res.JournalsSealed != 4 {
		t.Fatalf("sealed %d journals, want 4", res.JournalsSealed)
	}
	if res.PeakConcurrentLanes < 2 {
		t.Fatalf("peak concurrent lanes = %d; the harness cannot distinguish parallel lanes from serial ones",
			res.PeakConcurrentLanes)
	}
	if res.CrawlObjectsPerSec() <= 0 {
		t.Fatal("no crawl throughput measured")
	}
}

// TestAggregateListConcurrencyCeiling asserts the run-global listing ceiling on
// a real multi-lane build rather than on the budget in isolation.
//
// No rate limit here, deliberately. A limiter slow enough to space requests
// further apart than a listing takes makes overlap impossible, and the ceiling
// would then hold for a reason having nothing to do with the permit budget. The
// rate ceiling gets its own run below.
func TestAggregateListConcurrencyCeiling(t *testing.T) {
	const concurrency = 3

	res, err := Run(context.Background(), RunSpec{
		Corpus:      Corpus{Prefixes: 12, ObjectsPerPrefix: 200, PageSize: 100, PageLatency: 5 * time.Millisecond},
		Lanes:       6,
		Concurrency: concurrency,
		Root:        t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if res.PeakListConcurrency > concurrency {
		t.Fatalf("aggregate listing concurrency reached %d across lanes, ceiling is %d",
			res.PeakListConcurrency, concurrency)
	}
	if res.PeakListConcurrency < 2 {
		t.Fatalf("aggregate listing concurrency peaked at %d; the ceiling assertion above proves nothing if lanes never overlap",
			res.PeakListConcurrency)
	}
	if res.PeakConcurrentLanes < 2 {
		t.Fatalf("peak concurrent lanes = %d; lanes must genuinely overlap", res.PeakConcurrentLanes)
	}
	t.Logf("listing ceiling: peak_list_concurrency=%d (ceiling %d) peak_concurrent_lanes=%d journals=%d",
		res.PeakListConcurrency, concurrency, res.PeakConcurrentLanes, res.JournalsSealed)
}

// TestAggregateRequestRateCeiling asserts the run-global request-rate ceiling
// across lanes. A per-lane limiter would let N lanes each issue the configured
// rate, multiplying the operator's ceiling rather than honouring it.
func TestAggregateRequestRateCeiling(t *testing.T) {
	const rateLimit = 200.0

	res, err := Run(context.Background(), RunSpec{
		Corpus:      Corpus{Prefixes: 12, ObjectsPerPrefix: 400, PageSize: 100},
		Lanes:       6,
		Concurrency: 8,
		RateLimit:   rateLimit,
		Root:        t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// A token-bucket limiter admits burst immediately and rate*elapsed after, so
	// the ceiling accounts for both rather than sustained rate alone.
	const burst = 1
	allowed := rateLimit*res.RequestWindow.Seconds() + burst
	if float64(res.ListStarts) > allowed {
		t.Fatalf("aggregate request starts %d exceed the run-global ceiling %.1f over %s",
			res.ListStarts, allowed, res.RequestWindow)
	}
	// The limiter must actually have bound, or the ceiling held vacuously.
	if float64(res.ListStarts) < 0.5*allowed {
		t.Fatalf("request starts %d are far below the ceiling %.1f; the limiter never bound, so this proves nothing",
			res.ListStarts, allowed)
	}
	t.Logf("rate ceiling: request_starts=%d allowed=%.1f window=%s peak_concurrent_lanes=%d",
		res.ListStarts, allowed, res.RequestWindow, res.PeakConcurrentLanes)
}

// TestLaneCapDoesNotReduceListBudgetUtilization is the utilization control:
// when listing dominates, capping lanes below the crawl concurrency must not
// leave listing permits idle. Lanes bound journal writers, not listing capacity.
func TestLaneCapDoesNotReduceListBudgetUtilization(t *testing.T) {
	const concurrency = 8
	corpus := Corpus{Prefixes: 16, ObjectsPerPrefix: 200, PageSize: 100, PageLatency: 5 * time.Millisecond}

	for _, lanes := range []int{1, 2, 8} {
		res, err := Run(context.Background(), RunSpec{
			Corpus: corpus, Lanes: lanes, Concurrency: concurrency, Root: t.TempDir(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if res.PeakListConcurrency < concurrency {
			t.Errorf("lanes=%d: peak listing concurrency %d did not reach the configured budget %d; the lane cap reduced listing utilization",
				lanes, res.PeakListConcurrency, concurrency)
		}
		t.Logf("utilization: lanes=%d peak_list_concurrency=%d (budget %d)", lanes, res.PeakListConcurrency, concurrency)
	}
}

// TestLaneSweep is the D1 evidence run. It is env-gated because it is a
// measurement, not a pass/fail gate: it prints a table for the record.
//
//	GONIMBUS_LANE_SWEEP=1 go test ./test/indexlanethroughput/ -run TestLaneSweep -v -timeout 3600s
func TestLaneSweep(t *testing.T) {
	if os.Getenv("GONIMBUS_LANE_SWEEP") == "" {
		t.Skip("GONIMBUS_LANE_SWEEP not set (lane throughput sweep)")
	}
	prefixes := envInt("GONIMBUS_LANE_SWEEP_PREFIXES", 32)
	perPrefix := envInt("GONIMBUS_LANE_SWEEP_OBJECTS", 5000)
	pageSize := envInt("GONIMBUS_LANE_SWEEP_PAGE", 1000)
	concurrency := envInt("GONIMBUS_LANE_SWEEP_CONCURRENCY", 8)
	repeats := envInt("GONIMBUS_LANE_SWEEP_REPEATS", 3)

	// The two regimes need different page shapes, not just different latencies.
	// A prefix served in one page spends almost no time listing however slow that
	// page is, so a "list-bound" control built only by raising latency can still
	// be writer-bound — and would then show lanes helping and be read as a failed
	// null result. The control uses many small pages so listing genuinely
	// dominates, and the listing share is reported per cell rather than assumed.
	cases := []struct {
		name     string
		pageSize int
		latency  time.Duration
	}{
		{"writer-bound (0ms/page)", pageSize, 0},
		{"list-bound control (20ms/page, small pages)", envInt("GONIMBUS_LANE_SWEEP_CONTROL_PAGE", 250), 20 * time.Millisecond},
	}
	laneCounts := []int{1, 2, 4, 8}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("corpus: %d prefixes x %d objects = %d, page=%d, latency=%s, concurrency=%d, median of %d",
				prefixes, perPrefix, prefixes*perPrefix, tc.pageSize, tc.latency, concurrency, repeats)
			var baseline float64
			for _, lanes := range laneCounts {
				runs := make([]RunResult, 0, repeats)
				for r := 0; r < repeats; r++ {
					res, err := Run(context.Background(), RunSpec{
						Corpus: Corpus{
							Prefixes:         prefixes,
							ObjectsPerPrefix: perPrefix,
							PageSize:         tc.pageSize,
							PageLatency:      tc.latency,
						},
						Lanes:       lanes,
						Concurrency: concurrency,
						Root:        t.TempDir(),
					})
					if err != nil {
						t.Fatal(err)
					}
					runs = append(runs, res)
				}

				// Report one representative run in full rather than a median rate
				// beside another run's metadata. A row whose throughput came from one
				// repeat and whose duration and counters came from another is not an
				// auditable measurement, however close the numbers look.
				rep := medianRun(runs)
				got := rep.CrawlObjectsPerSec()
				if lanes == 1 {
					baseline = got
				}

				// Modelled floor on listing time: every page costs its latency, and
				// at best `concurrency` of them are in flight. Its share of the crawl
				// is what decides which regime this cell is actually in.
				listFloor := time.Duration(float64(rep.ListStarts) * float64(tc.latency) / float64(concurrency))
				share := 0.0
				if rep.CrawlDuration > 0 {
					share = float64(listFloor) / float64(rep.CrawlDuration)
				}
				t.Logf("lanes=%d journals=%d crawl=%s rate=%.0f obj/s speedup=%.2fx peak_lanes=%d peak_list=%d list_share=%.0f%% pages=%d total=%s [median run %d of %d; all rates %s]",
					lanes, rep.JournalsSealed, rep.CrawlDuration.Round(time.Millisecond),
					got, got/baseline, rep.PeakConcurrentLanes, rep.PeakListConcurrency,
					share*100, rep.ListStarts, rep.TotalDuration.Round(time.Millisecond),
					indexOfRun(runs, rep)+1, len(runs), formatRates(runs))
			}
		})
	}
}

// medianRun returns the repeat whose throughput is the median, so every figure
// in a reported row comes from one run and can be audited as a whole.
func medianRun(runs []RunResult) RunResult {
	ordered := append([]RunResult(nil), runs...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].CrawlObjectsPerSec() < ordered[j].CrawlObjectsPerSec()
	})
	return ordered[len(ordered)/2]
}

func indexOfRun(runs []RunResult, target RunResult) int {
	for i, r := range runs {
		if r.CrawlDuration == target.CrawlDuration && r.ListStarts == target.ListStarts {
			return i
		}
	}
	return -1
}

// formatRates prints every repeat's throughput, so a reader can see the spread
// the representative row was drawn from rather than trusting a single number.
func formatRates(runs []RunResult) string {
	parts := make([]string, 0, len(runs))
	for _, r := range runs {
		parts = append(parts, fmt.Sprintf("%.0f", r.CrawlObjectsPerSec()))
	}
	return "[" + strings.Join(parts, " ") + "]"
}
