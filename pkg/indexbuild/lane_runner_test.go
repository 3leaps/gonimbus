package indexbuild

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// laneTestConfig is a scoped multi-prefix build: lanes divide crawl-plan
// entries, so a plan with several entries is what makes lanes reachable at all.
func laneTestConfig(t *testing.T, name string, sitePrefixes []string) Config {
	t.Helper()
	cfg := testConfig(t, name)
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)

	objs := make([]provider.ObjectSummary, 0, len(sitePrefixes)*2)
	plan := make([]string, 0, len(sitePrefixes))
	coverage := make([]CoverageAttestation, 0, len(sitePrefixes))
	for i, site := range sitePrefixes {
		plan = append(plan, site)
		coverage = append(coverage, CoverageAttestation{
			Scope:    &Scope{Prefix: site},
			Basis:    CoverageBasisConfirmed,
			Complete: true,
		})
		objs = append(objs,
			provider.ObjectSummary{Key: site + "a.xml", Size: int64(10 + i), ETag: `"a"`, LastModified: base.Add(-time.Hour), StorageClass: "STANDARD"},
			provider.ObjectSummary{Key: site + "b.xml", Size: int64(20 + i), ETag: `"b"`, LastModified: base.Add(-time.Minute), StorageClass: "STANDARD"},
		)
	}
	cfg.Source = Source{Provider: fakeProvider{objects: objs}, ProviderName: "s3"}
	cfg.CrawlPrefixes = plan
	cfg.Coverage = coverage
	return cfg
}

// laneSitePrefixes are plan entries under the base prefix, which is what a
// scoped build's crawl plan actually contains.
func laneSitePrefixes(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, "data/site"+string(rune('A'+i))+"/")
	}
	return out
}

func readSealedJournals(t *testing.T, journalDir string) []indexsubstrate.JournalHeader {
	t.Helper()
	entries, err := os.ReadDir(journalDir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".jsonl" {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	headers := make([]indexsubstrate.JournalHeader, 0, len(names))
	for _, name := range names {
		summary, err := indexsubstrate.ValidateJournalBounded(
			filepath.Join(journalDir, name),
			indexsubstrate.DefaultSpillMergeBudget().MaxRecordBytes,
		)
		require.NoError(t, err, "journal %s must be sealed and integrity-valid", name)
		headers = append(headers, summary.Header)
	}
	return headers
}

// TestMultiLaneBuildSealsLaneLocalJournals is the write-side counterpart to the
// recovery rules: a multi-lane run must seal one journal per lane, each carrying
// only its own plan subset under an explicit lane-local mode, and the subsets
// must partition the run plan exactly.
//
// This is the control that makes the recovery-side union meaningful. If lanes
// sealed the whole run plan in every header, a single supplied journal would
// still attest whole-run coverage and the omitted-lane refusal would never fire
// in production.
func TestMultiLaneBuildSealsLaneLocalJournals(t *testing.T) {
	plan := laneSitePrefixes(4)
	cfg := laneTestConfig(t, "lanes-multi", plan)
	cfg.MaxJournalLanes = 4
	cfg.Crawl.Concurrency = 4

	summary, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(8), summary.ObjectsObserved)

	headers := readSealedJournals(t, cfg.Paths.JournalDir)
	require.Len(t, headers, 4, "each lane must seal its own journal")

	union := make([]string, 0, len(plan))
	seen := map[string]string{}
	for i, h := range headers {
		require.Equal(t, indexsubstrate.CrawlPlanModeLaneLocal, h.CrawlPlanMode,
			"a multi-lane journal must declare lane-local provenance")
		require.NotEmpty(t, h.CrawlPrefixes)
		require.Equal(t, "jrn_run_test_000"+string(rune('1'+i)), h.JournalID)
		require.Equal(t, "shard-000"+string(rune('1'+i)), h.Shard)
		require.Equal(t, "data/", h.Scope.Prefix, "scope stays the build scope, never lane membership")
		for _, p := range h.CrawlPrefixes {
			if prior, dup := seen[p]; dup {
				t.Fatalf("plan entry %q claimed by both %s and %s", p, prior, h.JournalID)
			}
			seen[p] = h.JournalID
			union = append(union, p)
		}
	}
	sort.Strings(union)
	require.Equal(t, plan, union, "lane subsets must partition the run plan exactly")
}

// TestSingleLaneBuildKeepsPreLaneJournalForm proves the compatibility case: one
// lane records the whole run plan with no mode field, under the original journal
// and shard names.
func TestSingleLaneBuildKeepsPreLaneJournalForm(t *testing.T) {
	cfg := laneTestConfig(t, "lanes-single", laneSitePrefixes(4))
	cfg.MaxJournalLanes = 1
	cfg.Crawl.Concurrency = 4

	_, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)

	headers := readSealedJournals(t, cfg.Paths.JournalDir)
	require.Len(t, headers, 1)
	require.Empty(t, headers[0].CrawlPlanMode, "the single-journal form records no mode")
	require.Equal(t, "jrn_run_test_0001", headers[0].JournalID)
	require.Equal(t, "shard-0001", headers[0].Shard)
	require.Equal(t, laneSitePrefixes(4), headers[0].CrawlPrefixes)
	require.FileExists(t, filepath.Join(cfg.Paths.JournalDir, "shard-0001.jsonl"))
}

// TestUnscopedBuildStaysSingleLane proves lanes require an explicit multi-entry
// plan. A build deriving prefixes from the matcher has one synthesized plan
// entry, which is one unit of coverage authority and cannot be divided.
func TestUnscopedBuildStaysSingleLane(t *testing.T) {
	cfg := testConfig(t, "lanes-unscoped")
	cfg.MaxJournalLanes = 8

	_, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)

	headers := readSealedJournals(t, cfg.Paths.JournalDir)
	require.Len(t, headers, 1)
	require.Empty(t, headers[0].CrawlPlanMode)
}

// TestEffectiveLanesBoundedByPlanAndConcurrency pins the min() rule: lanes never
// exceed the plan entries or the crawl concurrency, so no lane can be admitted
// with an empty plan.
func TestEffectiveLanesBoundedByPlanAndConcurrency(t *testing.T) {
	for _, tc := range []struct {
		name        string
		planEntries int
		concurrency int
		maxLanes    int
		wantLanes   int
	}{
		{"plan binds", 2, 8, 8, 2},
		{"concurrency binds", 8, 2, 8, 2},
		{"max lanes binds", 8, 8, 3, 3},
		{"single entry cannot split", 1, 8, 8, 1},
		{"lanes disabled", 8, 8, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := laneTestConfig(t, "lanes-bound", laneSitePrefixes(tc.planEntries))
			cfg.MaxJournalLanes = tc.maxLanes
			cfg.Crawl.Concurrency = tc.concurrency

			_, err := NewRunner(cfg).Build(context.Background())
			require.NoError(t, err)

			headers := readSealedJournals(t, cfg.Paths.JournalDir)
			require.Len(t, headers, tc.wantLanes)
			for _, h := range headers {
				require.NotEmpty(t, h.CrawlPrefixes, "no lane may be admitted with an empty plan")
			}
		})
	}
}

// TestMaxJournalLanesAboveCeilingRefusesBeforeSideEffects proves an out-of-range
// lane count is refused before any artifact exists, rather than clamped.
func TestMaxJournalLanesAboveCeilingRefusesBeforeSideEffects(t *testing.T) {
	cfg := laneTestConfig(t, "lanes-ceiling", laneSitePrefixes(4))
	cfg.MaxJournalLanes = MaxJournalLanesCeiling + 1

	_, err := NewRunner(cfg).Build(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "max journal lanes must not exceed")
	require.NoFileExists(t, cfg.Paths.LatestPath)
	require.NoDirExists(t, cfg.Paths.JournalDir)
}

// TestNegativeMaxJournalLanesRefused pins the lower bound of the control.
func TestNegativeMaxJournalLanesRefused(t *testing.T) {
	cfg := laneTestConfig(t, "lanes-negative", laneSitePrefixes(2))
	cfg.MaxJournalLanes = -1

	_, err := NewRunner(cfg).Build(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "must not be negative")
	require.NoFileExists(t, cfg.Paths.LatestPath)
}

// TestMultiLaneRunPublishesAndRecoversFromFullSet proves the write and recovery
// halves compose: a multi-lane run publishes, and a recovery over its complete
// sealed set republishes under the same whole-run coverage.
func TestMultiLaneRunPublishesAndRecoversFromFullSet(t *testing.T) {
	plan := laneSitePrefixes(4)
	cfg := laneTestConfig(t, "lanes-recover", plan)
	cfg.MaxJournalLanes = 4
	cfg.Crawl.Concurrency = 4

	_, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)

	journalPaths := make([]string, 0, 4)
	for i := 1; i <= 4; i++ {
		journalPaths = append(journalPaths, filepath.Join(cfg.Paths.JournalDir, laneJournalFileName(i)))
	}
	boundPlan, err := boundCrawlPlanFromJournals(journalPaths, indexsubstrate.DefaultSpillMergeBudget().MaxRecordBytes)
	require.NoError(t, err)
	require.Equal(t, plan, boundPlan, "the full sealed set unions back to the whole run plan")
	require.NoError(t, validateCoverageMatchesCrawlPlan(cfg.BaseURI, boundPlan, cfg.Coverage))
}

// TestMultiLaneOmittedJournalRefusesWholeRunCoverage is the production-shaped
// authority exploit: journals sealed by a real multi-lane build, one withheld.
func TestMultiLaneOmittedJournalRefusesWholeRunCoverage(t *testing.T) {
	cfg := laneTestConfig(t, "lanes-omitted", laneSitePrefixes(4))
	cfg.MaxJournalLanes = 4
	cfg.Crawl.Concurrency = 4

	_, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)

	partial := []string{
		filepath.Join(cfg.Paths.JournalDir, laneJournalFileName(1)),
		filepath.Join(cfg.Paths.JournalDir, laneJournalFileName(2)),
		filepath.Join(cfg.Paths.JournalDir, laneJournalFileName(3)),
	}
	boundPlan, err := boundCrawlPlanFromJournals(partial, indexsubstrate.DefaultSpillMergeBudget().MaxRecordBytes)
	require.NoError(t, err)
	require.Len(t, boundPlan, 3, "an omitted lane must narrow the derived plan")

	err = validateCoverageMatchesCrawlPlan(cfg.BaseURI, boundPlan, cfg.Coverage)
	require.Error(t, err, "whole-run coverage over an incomplete journal set must refuse")
	require.Contains(t, err.Error(), "not in the crawl prefix plan")
}

// TestLaneCrawlFailureBlocksPublicationForWholeRun proves seal-all-or-none: one
// failing lane leaves no published artifact and no sealed journal set.
func TestLaneCrawlFailureBlocksPublicationForWholeRun(t *testing.T) {
	cfg := laneTestConfig(t, "lanes-fail", laneSitePrefixes(4))
	cfg.MaxJournalLanes = 4
	cfg.Crawl.Concurrency = 4
	cfg.Source = Source{Provider: fakeProvider{listErr: os.ErrPermission}, ProviderName: "s3"}

	_, err := NewRunner(cfg).Build(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "crawl failed")
	require.NoFileExists(t, cfg.Paths.LatestPath)
	require.NoFileExists(t, cfg.Paths.ManifestPath)

	// Every journal that was created must be unsealed: a sealed journal is a
	// completeness claim, and this run completed nothing.
	entries, err := os.ReadDir(cfg.Paths.JournalDir)
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		_, err := indexsubstrate.ValidateJournalBounded(
			filepath.Join(cfg.Paths.JournalDir, e.Name()),
			indexsubstrate.DefaultSpillMergeBudget().MaxRecordBytes,
		)
		require.Error(t, err, "journal %s must not be sealed after a failed run", e.Name())
	}
}

// countingSink records how often it is closed and what run-scope records it
// received, so shared-sink ownership can be asserted rather than assumed.
type countingSink struct {
	mu        sync.Mutex
	closes    int
	objects   int
	progress  []output.ProgressRecord
	summaries []output.SummaryRecord
}

func (s *countingSink) WriteObject(context.Context, *output.ObjectRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects++
	return nil
}

func (s *countingSink) WriteProgress(_ context.Context, p *output.ProgressRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p != nil {
		s.progress = append(s.progress, *p)
	}
	return nil
}

func (s *countingSink) WriteSummary(_ context.Context, sum *output.SummaryRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sum != nil {
		s.summaries = append(s.summaries, *sum)
	}
	return nil
}

func (s *countingSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes++
	return nil
}

func (s *countingSink) WriteError(context.Context, *output.ErrorRecord) error         { return nil }
func (s *countingSink) WritePrefix(context.Context, *output.PrefixRecord) error       { return nil }
func (s *countingSink) WritePreflight(context.Context, *output.PreflightRecord) error { return nil }
func (s *countingSink) WriteTransfer(context.Context, *output.TransferRecord) error   { return nil }
func (s *countingSink) WriteSkip(context.Context, *output.SkipRecord) error           { return nil }

// TestSharedSinkClosedExactlyOnceAcrossLanes proves the runner owns the shared
// sinks. Closing per lane would close a caller's sink N times.
func TestSharedSinkClosedExactlyOnceAcrossLanes(t *testing.T) {
	sink := &countingSink{}
	cfg := laneTestConfig(t, "lanes-sink", laneSitePrefixes(4))
	cfg.MaxJournalLanes = 4
	cfg.Crawl.Concurrency = 4
	cfg.ObservationSinks = []output.Writer{sink}

	_, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)

	require.Len(t, readSealedJournals(t, cfg.Paths.JournalDir), 4, "the sink must not have suppressed lanes")
	require.Equal(t, 1, sink.closes, "a shared sink is closed once per run, not once per lane")
	require.Equal(t, 8, sink.objects, "every lane's observations reach the shared sink")
}

// TestRunScopeSummaryEmittedOnce proves a caller receives one summary describing
// the run rather than N describing fractions of it.
func TestRunScopeSummaryEmittedOnce(t *testing.T) {
	sink := &countingSink{}
	cfg := laneTestConfig(t, "lanes-summary", laneSitePrefixes(4))
	cfg.MaxJournalLanes = 4
	cfg.Crawl.Concurrency = 4
	cfg.ObservationSinks = []output.Writer{sink}

	_, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.Len(t, sink.summaries, 1, "exactly one run-scope summary")
	require.Equal(t, int64(8), sink.summaries[0].ObjectsMatched, "the summary counts the whole run, not one lane")

	// Progress truthfulness is asserted in full by
	// TestRunScopeProgressIsDeliveredInOrder; this test owns the summary.
}

// TestDurableOnlyConfigurationGetsLanes guards the production path itself. The
// durable-only adapter installs a progress writer as an observation sink; a rule
// that inferred lane suppression from sink population would pin the only path
// that can use lanes to a single one, shipping the feature in a state nothing
// exercises.
func TestDurableOnlyConfigurationGetsLanes(t *testing.T) {
	cfg := laneTestConfig(t, "lanes-durable-only", laneSitePrefixes(4))
	cfg.MaxJournalLanes = 0 // resolved default, as the durable-only adapter leaves it
	cfg.Crawl.Concurrency = 4
	cfg.ObservationSinks = []output.Writer{&countingSink{}}

	_, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)

	headers := readSealedJournals(t, cfg.Paths.JournalDir)
	require.Len(t, headers, DefaultMaxJournalLanes,
		"the durable path with a progress sink must genuinely get lanes")
	for _, h := range headers {
		require.Equal(t, indexsubstrate.CrawlPlanModeLaneLocal, h.CrawlPlanMode)
	}
}

// TestLaneAssignmentIsDeterministic proves lane membership does not vary between
// runs, so journal identity and cross-journal ordering are reproducible.
func TestLaneAssignmentIsDeterministic(t *testing.T) {
	first := planLanes(laneSitePrefixes(7), 8, 3)
	for i := 0; i < 20; i++ {
		require.Equal(t, first, planLanes(laneSitePrefixes(7), 8, 3))
	}
	// Caller order must not change assignment: the assignment view is sorted.
	shuffled := []string{"data/siteC/", "data/siteA/", "data/siteB/"}
	require.Equal(t, planLanes([]string{"data/siteA/", "data/siteB/", "data/siteC/"}, 8, 2), planLanes(shuffled, 8, 2))
}

// TestAuthoritativePlanOrderPreserved proves the sorted view is only ever a copy:
// a valid caller plan supplied out of order is recorded as given.
func TestAuthoritativePlanOrderPreserved(t *testing.T) {
	unsorted := []string{"data/siteC/", "data/siteA/", "data/siteB/"}
	cfg := laneTestConfig(t, "lanes-order", laneSitePrefixes(3))
	cfg.CrawlPrefixes = unsorted
	cfg.MaxJournalLanes = 1
	cfg.Crawl.Concurrency = 4

	_, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)

	headers := readSealedJournals(t, cfg.Paths.JournalDir)
	require.Len(t, headers, 1)
	require.Equal(t, unsorted, headers[0].CrawlPrefixes,
		"the authoritative plan keeps the caller's exact order")
}

// TestLanePlanningDoesNotReorderCallerInput proves the assignment view is a copy
// even on the multi-lane path, which is the only path that sorts at all.
//
// Sorting in place would reorder the caller's own slice — input the engine does
// not own — and with it the crawl plan every later stage reads. The single-lane
// test above cannot reach this: planning returns before the assignment copy is
// ever made, so only a genuinely multi-lane run exercises it.
func TestLanePlanningDoesNotReorderCallerInput(t *testing.T) {
	callerPlan := []string{"data/siteD/", "data/siteB/", "data/siteA/", "data/siteC/"}
	original := append([]string(nil), callerPlan...)

	cfg := laneTestConfig(t, "lanes-no-reorder", laneSitePrefixes(4))
	cfg.CrawlPrefixes = callerPlan
	cfg.MaxJournalLanes = 4
	cfg.Crawl.Concurrency = 4

	_, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)

	require.Equal(t, original, callerPlan,
		"lane assignment must sort a copy; the caller's slice is not the engine's to reorder")
	require.Len(t, readSealedJournals(t, cfg.Paths.JournalDir), 4,
		"the run must actually have taken the multi-lane path for this to prove anything")
}

// barrierProgressSink blocks the first progress delivery until released, so
// delivery ordering can be asserted deterministically rather than raced for.
type barrierProgressSink struct {
	mu       sync.Mutex
	progress []output.ProgressRecord
	summary  *output.SummaryRecord

	// first is a plain flag, not a sync.Once: Once.Do blocks every later caller
	// until the first completes, which would serialize deliveries inside the sink
	// itself and make this barrier unable to observe whether the engine serializes
	// them. Only the first caller may block here.
	first   bool
	release chan struct{}
	entered chan struct{}
}

func newBarrierProgressSink() *barrierProgressSink {
	return &barrierProgressSink{release: make(chan struct{}), entered: make(chan struct{})}
}

func (s *barrierProgressSink) WriteProgress(_ context.Context, p *output.ProgressRecord) error {
	s.mu.Lock()
	isFirst := !s.first
	s.first = true
	s.mu.Unlock()
	if isFirst {
		close(s.entered)
		<-s.release
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if p != nil {
		s.progress = append(s.progress, *p)
	}
	return nil
}

func (s *barrierProgressSink) WriteSummary(_ context.Context, sum *output.SummaryRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sum != nil {
		cp := *sum
		s.summary = &cp
	}
	return nil
}

func (s *barrierProgressSink) WriteObject(context.Context, *output.ObjectRecord) error { return nil }
func (s *barrierProgressSink) WriteError(context.Context, *output.ErrorRecord) error   { return nil }
func (s *barrierProgressSink) WritePrefix(context.Context, *output.PrefixRecord) error { return nil }
func (s *barrierProgressSink) WritePreflight(context.Context, *output.PreflightRecord) error {
	return nil
}
func (s *barrierProgressSink) WriteTransfer(context.Context, *output.TransferRecord) error {
	return nil
}
func (s *barrierProgressSink) WriteSkip(context.Context, *output.SkipRecord) error { return nil }
func (s *barrierProgressSink) Close() error                                        { return nil }

// TestRunScopeProgressIsDeliveredInOrder is the deterministic ordering probe.
//
// The aggregate used to be computed under the lock and delivered after it, so a
// lane that computed a newer total could overtake a lane still delivering an
// older one and a caller would watch the run's counts move backwards. Here the
// first delivery is held inside the sink while other lanes keep observing: no
// second record may arrive until the first is released.
func TestRunScopeProgressIsDeliveredInOrder(t *testing.T) {
	sink := newBarrierProgressSink()
	cfg := laneTestConfig(t, "lanes-progress-order", laneSitePrefixes(4))
	cfg.MaxJournalLanes = 4
	cfg.Crawl.Concurrency = 4
	cfg.Crawl.ProgressEvery = 1
	cfg.ObservationSinks = []output.Writer{sink}

	done := make(chan error, 1)
	go func() {
		_, err := NewRunner(cfg).Build(context.Background())
		done <- err
	}()

	<-sink.entered
	// While the first delivery is held, no other lane may deliver past it.
	time.Sleep(100 * time.Millisecond)
	sink.mu.Lock()
	delivered := len(sink.progress)
	sink.mu.Unlock()
	require.Zero(t, delivered, "no progress may be delivered while an earlier delivery is still in flight")

	close(sink.release)
	require.NoError(t, <-done)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.NotEmpty(t, sink.progress)

	// Counts must never regress, and exactly one terminal record may exist.
	var prevFound, prevMatched, prevBytes int64
	terminals := 0
	for i, p := range sink.progress {
		require.GreaterOrEqual(t, p.ObjectsFound, prevFound, "objects_found regressed at record %d", i)
		require.GreaterOrEqual(t, p.ObjectsMatched, prevMatched, "objects_matched regressed at record %d", i)
		require.GreaterOrEqual(t, p.BytesTotal, prevBytes, "bytes_total regressed at record %d", i)
		prevFound, prevMatched, prevBytes = p.ObjectsFound, p.ObjectsMatched, p.BytesTotal
		if p.Phase == output.PhaseComplete {
			terminals++
			require.Equal(t, len(sink.progress)-1, i, "a terminal record must be the last one")
		}
	}
	require.Equal(t, 1, terminals, "exactly one run-scope terminal progress record")

	final := sink.progress[len(sink.progress)-1]
	require.Equal(t, int64(8), final.ObjectsMatched, "the final progress must account for the whole run")
	require.NotNil(t, sink.summary)
	require.Equal(t, sink.summary.ObjectsMatched, final.ObjectsMatched,
		"the final progress and the run summary must agree")
}

// TestRunScopeProgressNeverReportsCompleteEarly proves a lane finishing its own
// prefixes cannot announce the run as complete while peers are still listing.
func TestRunScopeProgressNeverReportsCompleteEarly(t *testing.T) {
	sink := &countingSink{}
	cfg := laneTestConfig(t, "lanes-progress-phase", laneSitePrefixes(8))
	cfg.MaxJournalLanes = 8
	cfg.Crawl.Concurrency = 8
	cfg.Crawl.ProgressEvery = 1
	cfg.ObservationSinks = []output.Writer{sink}

	_, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.NotEmpty(t, sink.progress)
	for i, p := range sink.progress[:len(sink.progress)-1] {
		require.NotEqual(t, output.PhaseComplete, p.Phase,
			"record %d reports the run complete while lanes were still working", i)
	}
	require.Equal(t, output.PhaseComplete, sink.progress[len(sink.progress)-1].Phase)
	require.Equal(t, int64(16), sink.progress[len(sink.progress)-1].ObjectsMatched)
}

// TestMultiLaneSummaryReportsRunWallDuration proves the run summary reports the
// span the run actually occupied. The longest lane's own elapsed time understates
// a staggered run, since lanes do not start together.
func TestMultiLaneSummaryReportsRunWallDuration(t *testing.T) {
	sink := &countingSink{}
	cfg := laneTestConfig(t, "lanes-duration", laneSitePrefixes(4))
	cfg.MaxJournalLanes = 4
	cfg.Crawl.Concurrency = 4
	cfg.ObservationSinks = []output.Writer{sink}

	start := time.Now()
	_, err := NewRunner(cfg).Build(context.Background())
	elapsed := time.Since(start)
	require.NoError(t, err)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.Len(t, sink.summaries, 1)
	got := sink.summaries[0].Duration
	require.Greater(t, got, time.Duration(0), "the run must report a measured duration")
	require.LessOrEqual(t, got, elapsed, "the reported run span cannot exceed the observed build time")
	require.Equal(t, got.String(), sink.summaries[0].DurationHuman,
		"the human duration must describe the same span as the machine one")
}

// TestSingleLaneObservableStreamUnchanged proves the run-scope rewriting applies
// only where there are several lanes to reconcile: a single-lane run's progress
// and summary are its crawler's own records, phases included.
func TestSingleLaneObservableStreamUnchanged(t *testing.T) {
	sink := &countingSink{}
	cfg := laneTestConfig(t, "lanes-single-stream", laneSitePrefixes(4))
	cfg.MaxJournalLanes = 1
	cfg.Crawl.Concurrency = 4
	cfg.Crawl.ProgressEvery = 1
	cfg.ObservationSinks = []output.Writer{sink}

	_, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.Len(t, sink.summaries, 1)
	require.Equal(t, int64(8), sink.summaries[0].ObjectsMatched)
	// The crawler's own terminal record reaches the caller unrewritten.
	require.Equal(t, output.PhaseComplete, sink.progress[len(sink.progress)-1].Phase)
}
