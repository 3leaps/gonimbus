package indexbuild

// U4 coverage-authority remediation regressions. Coverage authorizes tombstones
// over verified-parent rows, so the plan that bounds coverage must be the exact
// observation universe — not a caller-forgeable value. These prove:
//   - the crawl-plan bytes used for the equality gate are the exact bytes crawled
//     (no lossy normalization opening a false-tombstone window);
//   - a scoped build's observation selector cannot silently reduce below the plan;
//   - ineligible coverage is refused before any side effect, not after a crawl;
//   - public Retry binds coverage to sealed-journal provenance and cannot widen
//     the tombstone universe, with legacy/disagreeing journals failing closed.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// scopedCountingConfig is scopedContConfig with a List-counting provider so a
// pre-side-effect refusal can be proven (zero List calls, no journal, no latest).
func scopedCountingConfig(setRoot, runID string, objs []provider.ObjectSummary, prefixes []string, started time.Time) (Config, *countingListProvider) {
	cfg := scopedContConfig(setRoot, runID, objs, prefixes, started)
	prov := &countingListProvider{inner: fakeProvider{objects: objs}}
	cfg.Source = Source{Provider: prov, ProviderName: "s3"}
	return cfg, prov
}

func requireRefusedBeforeSideEffects(t *testing.T, cfg Config, prov *countingListProvider, err error, wantErr string) {
	t.Helper()
	require.Error(t, err)
	require.Contains(t, err.Error(), wantErr)
	require.Zero(t, prov.lists.Load(), "refusal must precede any provider crawl")
	require.NoDirExists(t, cfg.Paths.JournalDir, "refusal must not create a journal")
	require.NoFileExists(t, cfg.Paths.LatestPath, "refusal must not publish latest")
}

// TestBuildRefusesNonCanonicalCrawlPlan proves the equality gate compares the
// exact bytes that drive LIST: a leading-slash or whitespace-padded plan entry
// (which would normalize to a different string than it crawls) is refused before
// any side effect, closing the lossy-normalization false-tombstone window.
func TestBuildRefusesNonCanonicalCrawlPlan(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	objs := []provider.ObjectSummary{obj("data/siteA/a1.xml", `"a1"`, 1, base)}
	cases := []struct {
		name string
		plan []string
	}{
		{"leading slash", []string{"/data/siteA/"}},
		{"trailing whitespace", []string{"data/siteA/ "}},
		{"leading whitespace", []string{" data/siteA/"}},
		{"empty entry", []string{""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRoot := t.TempDir()
			cfg, prov := scopedCountingConfig(setRoot, "run1", objs, tc.plan, base)
			_, err := NewRunner(cfg).Build(ctx)
			requireRefusedBeforeSideEffects(t, cfg, prov, err, "crawl prefix plan")
		})
	}
}

// TestBuildRefusesScopedSelectorReduction proves a scoped build refuses any
// match/filter that would observe fewer objects than its plan attests complete,
// mirroring the CLI adapter's faithful-coverage gate at the engine seam so a
// direct-library caller cannot authorize false tombstones by filtering.
func TestBuildRefusesScopedSelectorReduction(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	objs := []provider.ObjectSummary{obj("data/siteA/a1.xml", `"a1"`, 1, base)}
	cases := []struct {
		name string
		mut  func(cfg *Config)
	}{
		{"excludes", func(cfg *Config) { cfg.Match.Excludes = []string{"**/*.tmp"} }},
		{"include_hidden", func(cfg *Config) { cfg.Match.IncludeHidden = true }},
		{"non-default include", func(cfg *Config) { cfg.Match.Includes = []string{"data/siteA/**"} }},
		{"post-list filter", func(cfg *Config) { cfg.Filter = &match.CompositeFilter{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRoot := t.TempDir()
			cfg, prov := scopedCountingConfig(setRoot, "run1", objs, []string{"data/siteA/"}, base)
			tc.mut(&cfg)
			_, err := NewRunner(cfg).Build(ctx)
			requireRefusedBeforeSideEffects(t, cfg, prov, err, "scoped build")
		})
	}
}

// TestBuildRefusesIneligibleCoverageForCrawlPlan proves exact-prefix but
// ineligible coverage (inferred / incomplete / gapped) refuses before the crawl
// and journal, not after — the faithful-attestation boundary is pre-side-effect.
func TestBuildRefusesIneligibleCoverageForCrawlPlan(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	objs := []provider.ObjectSummary{obj("data/siteA/a1.xml", `"a1"`, 1, base)}
	cases := []struct {
		name    string
		cov     CoverageAttestation
		wantErr string
	}{
		{"inferred basis", CoverageAttestation{Scope: &Scope{Prefix: "data/siteA/"}, Basis: CoverageBasisInferred, Complete: true}, "confirmed and complete"},
		{"incomplete", CoverageAttestation{Scope: &Scope{Prefix: "data/siteA/"}, Basis: CoverageBasisConfirmed, Complete: false}, "confirmed and complete"},
		{"gapped", CoverageAttestation{Scope: &Scope{Prefix: "data/siteA/"}, Basis: CoverageBasisConfirmed, Complete: true, Gaps: []Scope{{Prefix: "data/siteA/sub/"}}}, "must not declare gaps"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRoot := t.TempDir()
			cfg, prov := scopedCountingConfig(setRoot, "run1", objs, []string{"data/siteA/"}, base)
			cfg.Coverage = []CoverageAttestation{tc.cov}
			_, err := NewRunner(cfg).Build(ctx)
			requireRefusedBeforeSideEffects(t, cfg, prov, err, tc.wantErr)
		})
	}
}

// TestRetryRefusesCoverageWiderThanJournalPlan proves public Retry binds its
// coverage authority to the sealed journal's crawl-plan provenance: a siteA-only
// journal retried with widened base coverage refuses (latest byte-identical, the
// unobserved siteB parent row never tombstoned), while exact-plan coverage
// succeeds and retains siteB active.
func TestRetryRefusesCoverageWiderThanJournalPlan(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
		obj("data/siteB/b1.xml", `"b1"`, 2, base),
	}, base)).Build(ctx)
	require.NoError(t, err)

	// run2 scoped to siteA -> journal sealed with plan ["data/siteA/"].
	run2Start := base.Add(time.Hour)
	sum2, err := NewRunner(scopedContConfig(setRoot, "run2", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1x"`, 9, run2Start),
	}, []string{"data/siteA/"}, run2Start)).Build(ctx)
	require.NoError(t, err)

	latestBefore, err := os.ReadFile(latestPath)
	require.NoError(t, err)

	// Widened base coverage over the siteA-only journal must refuse.
	widen := retryConfigFor(scopedContConfig(setRoot, "run2", nil, []string{"data/siteA/"}, run2Start), sum2)
	widen.Coverage = []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true}}
	_, err = Retry(ctx, widen)
	require.Error(t, err)
	require.Contains(t, err.Error(), "retry coverage authority")

	latestAfter, err := os.ReadFile(latestPath)
	require.NoError(t, err)
	require.Equal(t, latestBefore, latestAfter, "refused Retry must not advance latest")
	_, rows, err := ReadLatest(latestPath)
	require.NoError(t, err)
	require.Nil(t, rowByKey(rows, "siteB/b1.xml").DeletedAt, "widened Retry must not tombstone the unobserved siteB parent")

	// Exact siteA coverage (matching the sealed plan) succeeds; siteB retained.
	_, err = Retry(ctx, retryConfigFor(scopedContConfig(setRoot, "run2", nil, []string{"data/siteA/"}, run2Start), sum2))
	require.NoError(t, err)
	_, rows2, err := ReadLatest(latestPath)
	require.NoError(t, err)
	require.Nil(t, rowByKey(rows2, "siteB/b1.xml").DeletedAt, "exact-plan Retry retains the unobserved siteB parent active")
}

// writeSealedJournalWithPlan writes a minimal valid sealed journal carrying the
// given crawl-plan provenance (nil = legacy / pre-provenance).
func writeSealedJournalWithPlan(t *testing.T, path string, crawlPrefixes []string) {
	t.Helper()
	started := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	jw, err := indexsubstrate.CreateJournal(path, indexsubstrate.JournalHeader{
		Type:               indexsubstrate.JournalHeaderType,
		JournalID:          "jrn_test_0001",
		IndexSetID:         "idx_cont",
		RunID:              "run_test",
		Shard:              "shard-0001",
		CrawlPrefixes:      crawlPrefixes,
		IndexSchemaVersion: indexsubstrate.IndexSchemaVersion,
		StartedAt:          started,
	})
	require.NoError(t, err)
	_, err = jw.Append(indexsubstrate.ObjectRecord{
		Op:         indexsubstrate.ObjectRecordOpObserve,
		RelKey:     "siteA/a1.xml",
		ObservedAt: started,
	})
	require.NoError(t, err)
	require.NoError(t, jw.Seal(started))
	require.NoError(t, jw.Close())
}

// TestBoundCrawlPlanFromJournalsFailsClosedOnLegacyJournal proves a journal with
// no recorded plan (pre-provenance) fails closed — recovery cannot prove the
// observation universe, so caller coverage is never accepted as tombstone
// authority over verified-parent rows.
func TestBoundCrawlPlanFromJournalsFailsClosedOnLegacyJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard-0001.jsonl")
	writeSealedJournalWithPlan(t, path, nil)
	_, err := boundCrawlPlanFromJournals([]string{path})
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)
	require.Contains(t, err.Error(), "predates crawl-plan provenance")
}

// TestBoundCrawlPlanFromJournalsRefusesDisagreement proves multiple journals
// must agree on their sealed plan (order-independent); disagreement fails closed.
func TestBoundCrawlPlanFromJournalsRefusesDisagreement(t *testing.T) {
	dir := t.TempDir()
	p1 := filepath.Join(dir, "a", "shard-0001.jsonl")
	p2 := filepath.Join(dir, "b", "shard-0001.jsonl")
	writeSealedJournalWithPlan(t, p1, []string{"data/siteA/"})
	writeSealedJournalWithPlan(t, p2, []string{"data/siteB/"})
	_, err := boundCrawlPlanFromJournals([]string{p1, p2})
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)
	require.Contains(t, err.Error(), "disagree")

	// Agreement (recorded in either order) returns the shared plan.
	p3 := filepath.Join(dir, "c", "shard-0001.jsonl")
	writeSealedJournalWithPlan(t, p3, []string{"data/siteA/"})
	plan, err := boundCrawlPlanFromJournals([]string{p1, p3})
	require.NoError(t, err)
	require.Equal(t, []string{"data/siteA/"}, plan)
}
