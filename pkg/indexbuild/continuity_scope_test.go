package indexbuild

// Scoped coverage-merge regressions: a build whose crawl-prefix plan covers
// only part of the verified parent's rows must retain every out-of-coverage
// prior row verbatim (state, first-seen lineage, HEAD enrichment, existing
// tombstones) and may tombstone only keys inside the current run's
// confirmed-complete coverage. The published coverage must equal the crawl
// plan exactly — never widened toward the parent's coverage — and a plan that
// disagrees with the attestation refuses before any side effect.

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/durableindex"
	"github.com/3leaps/gonimbus/pkg/indexenrich"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// scopedContConfig is contConfig with an explicit crawl-prefix plan and its
// matching per-prefix coverage attestation (provider-key space, like the CLI
// adapter passes them). When a published latest already exists at setRoot and
// the scoped plan reduces coverage relative to that parent, scopedContConfig
// attaches a pinned LossAcknowledgment so existing continuity-merge tests
// exercise the acknowledged scope-loss path.
func scopedContConfig(setRoot, runID string, objs []provider.ObjectSummary, prefixes []string, started time.Time) Config {
	cfg := contConfig(setRoot, runID, objs, started)
	cfg.CrawlPrefixes = append([]string(nil), prefixes...)
	coverage := make([]CoverageAttestation, 0, len(prefixes))
	for _, prefix := range prefixes {
		coverage = append(coverage, CoverageAttestation{
			Scope:    &Scope{Prefix: prefix},
			Basis:    CoverageBasisConfirmed,
			Complete: true,
		})
	}
	cfg.Coverage = coverage
	if ack := maybeAcknowledgeScopeLossForTest(cfg.Paths.LatestPath, cfg.IndexSetID, cfg.BaseURI, cfg.Coverage, started); ack != nil {
		cfg.LossAcknowledgment = ack
	}
	return cfg
}

func maybeAcknowledgeScopeLossForTest(latestPath, indexSetID, baseURI string, coverage []CoverageAttestation, started time.Time) *durableindex.TransitionAcknowledgment {
	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	if err != nil {
		return nil
	}
	normCov, err := normalizeCoverageForBaseURI(baseURI, coverage)
	if err != nil {
		return nil
	}
	currScopes, err := durableScopesFromCoverage(normCov)
	if err != nil {
		return nil
	}
	_, _, currDigest, err := durableindex.BuildIndexPlan(indexSetID, currScopes)
	if err != nil {
		return nil
	}
	parentScopes, err := durableScopesFromSubstrateCoverage(snap.Manifest.Coverage)
	if err != nil {
		return nil
	}
	lost, err := durableindex.HasScopeLoss(parentScopes, currScopes)
	if err != nil || !lost {
		return nil
	}
	ackAt, err := durableindex.FormatInstant(started.UTC())
	if err != nil {
		return nil
	}
	return &durableindex.TransitionAcknowledgment{
		ParentSHA256:   snap.Complete.ManifestSHA256,
		PlanDigest:     currDigest,
		AcknowledgedBy: "operator-test",
		AcknowledgedAt: ackAt,
	}
}

// manifestCoveragePrefixes reads the published manifest's coverage prefixes
// (rel_key space) and asserts every entry is confirmed, complete, gap-free,
// and non-windowed.
func manifestCoveragePrefixes(t *testing.T, latestPath string) []string {
	t.Helper()
	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	out := make([]string, 0, len(snap.Manifest.Coverage))
	for _, entry := range snap.Manifest.Coverage {
		require.NotNil(t, entry.Scope, "published coverage scope must be explicit")
		require.Nil(t, entry.Scope.Window, "published coverage must not carry a temporal window")
		require.Equal(t, indexsubstrate.CoverageBasisConfirmed, entry.Basis)
		require.True(t, entry.Complete)
		require.Empty(t, entry.Gaps)
		out = append(out, entry.Scope.Prefix)
	}
	return out
}

// TestBuildScopedContinuityRetainsOutOfScopeRows proves the scope-reduced
// coverage merge: a scoped child over a full-coverage parent applies
// add/change/delete semantics inside its attested plan and retains every
// out-of-coverage parent row verbatim, while the published coverage lists
// exactly the crawl plan (never the parent's wider coverage).
func TestBuildScopedContinuityRetainsOutOfScopeRows(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	// run1: full-base coverage over two site prefixes.
	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
		obj("data/siteA/a2.xml", `"a2"`, 2, base),
		obj("data/siteB/b1.xml", `"b1"`, 3, base),
		obj("data/siteB/b2.xml", `"b2"`, 4, base),
	}, base)).Build(ctx)
	require.NoError(t, err)

	// run2: crawl plan and coverage reduced to siteA. Inside the plan: a1
	// changed, a2 deleted, a3 added. siteB is not crawled and must be retained.
	run2Start := base.Add(time.Hour)
	summary, err := NewRunner(scopedContConfig(setRoot, "run2", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1x"`, 9, run2Start),
		obj("data/siteA/a3.xml", `"a3"`, 5, run2Start),
		// Provider-side siteB rows exist but are outside the crawl plan.
		obj("data/siteB/b1.xml", `"b1x"`, 30, run2Start),
	}, []string{"data/siteA/"}, run2Start)).Build(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"data/siteA/"}, summary.PrefixesCrawled)

	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.NotNil(t, snap.Manifest.Lineage)
	require.False(t, snap.Manifest.Lineage.Baseline)
	require.Equal(t, 2, snap.Manifest.Lineage.Generation)
	require.NotNil(t, snap.Manifest.StateParent)
	require.Equal(t, "run1", snap.Manifest.StateParent.RunID)

	_, rows, err := ReadLatest(latestPath)
	require.NoError(t, err)
	byKey := map[string]ObjectState{}
	for _, r := range rows {
		byKey[r.RelKey] = r
	}

	// In-plan semantics: change, delete, add.
	a1 := byKey["siteA/a1.xml"]
	require.Equal(t, `"a1x"`, a1.ETag)
	require.Nil(t, a1.DeletedAt)
	require.Equal(t, "run2", a1.LastChangedRunID)
	require.Equal(t, "run1", a1.FirstSeenRunID)
	a2 := byKey["siteA/a2.xml"]
	require.NotNil(t, a2.DeletedAt, "unobserved in-plan key must be tombstoned under confirmed-complete coverage")
	require.True(t, a2.DeletedAt.Equal(run2Start))
	a3 := byKey["siteA/a3.xml"]
	require.Nil(t, a3.DeletedAt)
	require.Equal(t, "run2", a3.FirstSeenRunID)

	// Out-of-coverage retention: siteB rows carried verbatim — still active,
	// original bytes and lineage, untouched last-seen (the provider-side b1
	// change was outside the plan and must not appear).
	for key, wantETag := range map[string]string{"siteB/b1.xml": `"b1"`, "siteB/b2.xml": `"b2"`} {
		row := byKey[key]
		require.Equal(t, wantETag, row.ETag, key)
		require.Nil(t, row.DeletedAt, "out-of-coverage prior row must not be tombstoned: %s", key)
		require.Equal(t, "run1", row.FirstSeenRunID, key)
		require.Equal(t, "run1", row.LastSeenRunID, "out-of-coverage prior row must not claim observation: %s", key)
	}

	// Published coverage equals the crawl plan exactly (rel_key space) — not
	// rolled up to the parent's base coverage.
	require.Equal(t, []string{"siteA/"}, manifestCoveragePrefixes(t, latestPath))
}

// TestBuildScopedContinuityPreservesOutOfScopeTombstone proves an existing
// out-of-coverage tombstone is carried verbatim (same deleted-at instant) and
// is neither resurrected nor re-stamped by a scoped child.
func TestBuildScopedContinuityPreservesOutOfScopeTombstone(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
		obj("data/siteB/b1.xml", `"b1"`, 2, base),
		obj("data/siteB/b2.xml", `"b2"`, 3, base),
	}, base)).Build(ctx)
	require.NoError(t, err)

	// run2 (full coverage): b2 deleted -> tombstoned at run2Start.
	run2Start := base.Add(time.Hour)
	_, err = NewRunner(contConfig(setRoot, "run2", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
		obj("data/siteB/b1.xml", `"b1"`, 2, base),
	}, run2Start)).Build(ctx)
	require.NoError(t, err)

	// run3 scoped to siteA: the siteB tombstone and active row are out of
	// coverage and must be retained exactly.
	run3Start := base.Add(2 * time.Hour)
	_, err = NewRunner(scopedContConfig(setRoot, "run3", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1y"`, 7, run3Start),
	}, []string{"data/siteA/"}, run3Start)).Build(ctx)
	require.NoError(t, err)

	_, rows, err := ReadLatest(latestPath)
	require.NoError(t, err)
	b2 := rowByKey(rows, "siteB/b2.xml")
	require.NotNil(t, b2.DeletedAt, "carried tombstone must survive the scoped child")
	require.True(t, b2.DeletedAt.Equal(run2Start), "carried tombstone must keep its original deleted-at instant")
	b1 := rowByKey(rows, "siteB/b1.xml")
	require.Nil(t, b1.DeletedAt)
	require.Equal(t, "run2", b1.LastSeenRunID)

	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.Equal(t, 3, snap.Manifest.Lineage.Generation)
}

// TestBuildScopedContinuityPreservesOutOfScopeEnrichment proves HEAD
// enrichment on out-of-coverage rows survives a scoped build over the enriched
// latest (a verified pre-continuity state source).
func TestBuildScopedContinuityPreservesOutOfScopeEnrichment(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	objs := []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
		obj("data/siteB/b1.xml", `"b1"`, 2, base),
	}
	archived := "ARCHIVED"
	prov := &listHeadProvider{
		objects: objs,
		metas: map[string]*provider.ObjectMeta{
			"data/siteA/a1.xml": {ObjectSummary: obj("data/siteA/a1.xml", `"a1"`, 1, base), ContentType: "application/xml"},
			"data/siteB/b1.xml": {ObjectSummary: obj("data/siteB/b1.xml", `"b1"`, 2, base), ContentType: "text/plain", ArchiveStatus: archived},
		},
	}

	cfg1 := contConfig(setRoot, "run1", objs, base)
	cfg1.Source = Source{Provider: prov, ProviderName: "s3"}
	_, err := NewRunner(cfg1).Build(ctx)
	require.NoError(t, err)

	_, err = indexenrich.Run(ctx, indexenrich.Config{
		IndexSetID:     "idx_cont",
		BaseURI:        "s3://bucket/data/",
		Provider:       prov,
		SegmentSetRoot: setRoot,
		JournalRoot:    filepath.Join(setRoot, "enrich-journals"),
	})
	require.NoError(t, err)

	// Scoped build over the enriched latest: siteA re-observed, siteB retained.
	cfg3 := scopedContConfig(setRoot, "run3", objs, []string{"data/siteA/"}, base.Add(2*time.Hour))
	cfg3.Source = Source{Provider: prov, ProviderName: "s3"}
	_, err = NewRunner(cfg3).Build(ctx)
	require.NoError(t, err)

	_, rows, err := ReadLatest(latestPath)
	require.NoError(t, err)
	b1 := rowByKey(rows, "siteB/b1.xml")
	require.NotNil(t, b1.ContentType, "out-of-coverage HEAD enrichment must be retained")
	require.Equal(t, "text/plain", *b1.ContentType)
	require.NotNil(t, b1.ArchiveStatus)
	require.Equal(t, archived, *b1.ArchiveStatus)
	require.NotNil(t, b1.HeadEnrichedAt)
	require.Equal(t, "run1", b1.FirstSeenRunID)
	require.Nil(t, b1.DeletedAt)
	a1 := rowByKey(rows, "siteA/a1.xml")
	require.NotNil(t, a1.ContentType, "in-plan unchanged row keeps its enrichment")
	require.Equal(t, "application/xml", *a1.ContentType)
}

// TestBuildRepeatedExactStaticScopeThreeRunLifecycle proves the
// add/change/delete/reappear lifecycle across three builds that repeat the
// same exact multi-prefix static plan, with faithful per-plan coverage on
// every generation.
func TestBuildRepeatedExactStaticScopeThreeRunLifecycle(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")
	plan := []string{"data/siteA/", "data/siteB/"}

	_, err := NewRunner(scopedContConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
		obj("data/siteB/b1.xml", `"b1"`, 2, base),
	}, plan, base)).Build(ctx)
	require.NoError(t, err)

	// run2: a1 changed, b1 deleted.
	run2Start := base.Add(time.Hour)
	_, err = NewRunner(scopedContConfig(setRoot, "run2", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1x"`, 9, run2Start),
	}, plan, run2Start)).Build(ctx)
	require.NoError(t, err)
	_, rows2, err := ReadLatest(latestPath)
	require.NoError(t, err)
	require.NotNil(t, rowByKey(rows2, "siteB/b1.xml").DeletedAt)

	// run3: b1 reappears.
	run3Start := base.Add(2 * time.Hour)
	_, err = NewRunner(scopedContConfig(setRoot, "run3", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1x"`, 9, run2Start),
		obj("data/siteB/b1.xml", `"b1r"`, 12, run3Start),
	}, plan, run3Start)).Build(ctx)
	require.NoError(t, err)

	_, rows3, err := ReadLatest(latestPath)
	require.NoError(t, err)
	b1 := rowByKey(rows3, "siteB/b1.xml")
	require.Nil(t, b1.DeletedAt, "reappeared key clears its tombstone")
	require.Equal(t, `"b1r"`, b1.ETag)
	a1 := rowByKey(rows3, "siteA/a1.xml")
	require.Equal(t, "run2", a1.LastChangedRunID, "unchanged in-plan row keeps its change lineage")

	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.Equal(t, 3, snap.Manifest.Lineage.Generation)
	require.ElementsMatch(t, []string{"siteA/", "siteB/"}, manifestCoveragePrefixes(t, latestPath))
}

// TestBuildRefusesUnfaithfulCoverageForCrawlPlan proves the fail-closed set
// equality between a supplied crawl-prefix plan and the coverage attestation:
// roll-up, extra, missing, duplicate, and windowed coverage all refuse before
// any provider crawl, journal creation, or publish side effect.
func TestBuildRefusesUnfaithfulCoverageForCrawlPlan(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	plan := []string{"data/siteA/", "data/siteB/"}

	coverageFor := func(prefixes ...string) []CoverageAttestation {
		out := make([]CoverageAttestation, 0, len(prefixes))
		for _, prefix := range prefixes {
			out = append(out, CoverageAttestation{
				Scope:    &Scope{Prefix: prefix},
				Basis:    CoverageBasisConfirmed,
				Complete: true,
			})
		}
		return out
	}

	cases := []struct {
		name     string
		coverage []CoverageAttestation
		wantErr  string
	}{
		{
			name:     "roll-up to base prefix",
			coverage: coverageFor("data/"),
			wantErr:  "is not in the crawl prefix plan",
		},
		{
			name:     "extra out-of-plan prefix",
			coverage: coverageFor("data/siteA/", "data/siteB/", "data/siteC/"),
			wantErr:  "is not in the crawl prefix plan",
		},
		{
			name:     "missing plan prefix",
			coverage: coverageFor("data/siteA/"),
			wantErr:  "coverage is missing crawl prefix plan entry",
		},
		{
			name:     "duplicate coverage prefix",
			coverage: coverageFor("data/siteA/", "data/siteA/", "data/siteB/"),
			wantErr:  "duplicate prefix",
		},
		{
			name:     "empty coverage",
			coverage: nil,
			wantErr:  "coverage attestation is required",
		},
		{
			name: "windowed coverage entry",
			coverage: append(coverageFor("data/siteA/"), CoverageAttestation{
				Scope:    &Scope{Prefix: "data/siteB/", Window: &Window{From: "2026-07-01", To: "2026-07-10"}},
				Basis:    CoverageBasisConfirmed,
				Complete: true,
			}),
			wantErr: "must not set a temporal window",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRoot := t.TempDir()
			prov := &countingListProvider{inner: fakeProvider{objects: []provider.ObjectSummary{
				obj("data/siteA/a1.xml", `"a1"`, 1, base),
			}}}
			cfg := scopedContConfig(setRoot, "run1", nil, plan, base)
			cfg.Source = Source{Provider: prov, ProviderName: "s3"}
			cfg.Coverage = tc.coverage

			_, err := NewRunner(cfg).Build(ctx)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
			require.Zero(t, prov.lists.Load(), "refusal must happen before any provider crawl")
			require.NoDirExists(t, cfg.Paths.JournalDir, "refusal must not create a journal")
			require.NoFileExists(t, filepath.Join(setRoot, "latest.json"), "refusal must not publish")
		})
	}

	t.Run("exact plan coverage is accepted", func(t *testing.T) {
		setRoot := t.TempDir()
		cfg := scopedContConfig(setRoot, "run1", []provider.ObjectSummary{
			obj("data/siteA/a1.xml", `"a1"`, 1, base),
			obj("data/siteB/b1.xml", `"b1"`, 2, base),
		}, plan, base)
		_, err := NewRunner(cfg).Build(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"siteA/", "siteB/"},
			manifestCoveragePrefixes(t, filepath.Join(setRoot, "latest.json")))
	})
}

func TestBuildRefusesUnacknowledgedScopeLossBeforeSideEffects(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)

	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
		obj("data/siteB/b1.xml", `"b1"`, 2, base),
	}, base)).Build(ctx)
	require.NoError(t, err)

	prov := &countingListProvider{inner: fakeProvider{objects: []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
	}}}
	cfg2 := scopedContConfig(setRoot, "run2", nil, []string{"data/siteA/"}, base.Add(time.Hour))
	cfg2.Source = Source{Provider: prov, ProviderName: "s3"}
	cfg2.LossAcknowledgment = nil

	_, err = NewRunner(cfg2).Build(ctx)
	require.Error(t, err)
	reason, ok := durableindex.RefusalReason(err)
	require.True(t, ok)
	require.Equal(t, "scope_loss", reason)
	require.Zero(t, prov.lists.Load(), "unacknowledged scope_loss must refuse before any provider crawl")
	require.NoDirExists(t, cfg2.Paths.JournalDir, "unacknowledged scope_loss must not create journals")

	snapAfter, err := indexsubstrate.OpenLatestPublishedSnapshot(filepath.Join(setRoot, "latest.json"))
	require.NoError(t, err)
	require.Equal(t, "run1", snapAfter.Manifest.RunID, "latest.json must remain at run1")
}

func TestBuildAcknowledgedScopeLossAndRetainedUnobservedRows(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	sum1, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
		obj("data/siteB/b1.xml", `"b1"`, 2, base),
		obj("data/siteB/b2.xml", `"b2"`, 3, base),
	}, base)).Build(ctx)
	require.NoError(t, err)
	require.Equal(t, durableindex.AdmissionBaselineNotCompared, sum1.Manifest.Admission)
	require.Equal(t, 0, sum1.Manifest.RetainedUnobservedRows)

	// run2 with acknowledged scope loss (siteA only): retains 2 unobserved rows from siteB.
	cfg2 := scopedContConfig(setRoot, "run2", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
	}, []string{"data/siteA/"}, base.Add(time.Hour))
	sum2, err := NewRunner(cfg2).Build(ctx)
	require.NoError(t, err)
	require.Equal(t, durableindex.AdmissionLossAcknowledged, sum2.Manifest.Admission)
	require.Equal(t, 2, sum2.Manifest.RetainedUnobservedRows)

	// run3 with identical siteA scope: no_loss relative to run2, still retains 2 unobserved rows.
	cfg3 := scopedContConfig(setRoot, "run3", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
	}, []string{"data/siteA/"}, base.Add(2*time.Hour))
	require.Nil(t, cfg3.LossAcknowledgment, "same scope must not require loss acknowledgment")
	sum3, err := NewRunner(cfg3).Build(ctx)
	require.NoError(t, err)
	require.Equal(t, durableindex.AdmissionNoLoss, sum3.Manifest.Admission)
	require.Equal(t, 2, sum3.Manifest.RetainedUnobservedRows)

	// Supplying a LossAcknowledgment when there is no scope_loss must refuse with unexpected_acknowledgment.
	cfgUnexpected := scopedContConfig(setRoot, "run4", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
	}, []string{"data/siteA/"}, base.Add(3*time.Hour))
	ackAt, err := durableindex.FormatInstant(base.Add(3 * time.Hour).UTC())
	require.NoError(t, err)
	cfgUnexpected.LossAcknowledgment = &durableindex.TransitionAcknowledgment{
		ParentSHA256:   sum3.ManifestSHA256,
		PlanDigest:     sum3.Manifest.PlanDigest,
		AcknowledgedBy: "operator-test",
		AcknowledgedAt: ackAt,
	}
	_, err = NewRunner(cfgUnexpected).Build(ctx)
	require.Error(t, err)
	reason, ok := durableindex.RefusalReason(err)
	require.True(t, ok)
	require.Equal(t, "unexpected_acknowledgment", reason)

	snapLatest, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.Equal(t, "run3", snapLatest.Manifest.RunID)
}

func TestRetryEnforcesSealedJournalLossAcknowledgment(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)

	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
		obj("data/siteB/b1.xml", `"b1"`, 2, base),
	}, base)).Build(ctx)
	require.NoError(t, err)

	cfg2 := scopedContConfig(setRoot, "run2", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
	}, []string{"data/siteA/"}, base.Add(time.Hour))
	sum2, err := NewRunner(cfg2).Build(ctx)
	require.NoError(t, err)

	// Retry of run2 without passing LossAcknowledgment on RetryConfig succeeds because the
	// acknowledgment is sealed inside the journal headers.
	retrySum, err := Retry(ctx, RetryConfig{
		IndexSetID:           cfg2.IndexSetID,
		RunID:                cfg2.RunID,
		BaseURI:              cfg2.BaseURI,
		Paths:                cfg2.Paths,
		JournalPaths:         sum2.JournalPaths,
		Coverage:             cfg2.Coverage,
		RunStartedAt:         cfg2.RunStartedAt,
		ManifestCreatedAt:    cfg2.ManifestCreatedAt,
		TargetRowsPerSegment: cfg2.TargetRowsPerSegment,
	})
	require.NoError(t, err)
	require.Equal(t, durableindex.AdmissionLossAcknowledged, retrySum.Manifest.Admission)

	// Retry with a mismatched LossAcknowledgment on RetryConfig must refuse with acknowledgment_mismatch.
	badAck := *cfg2.LossAcknowledgment
	badAck.AcknowledgedBy = "forged-actor"
	_, err = Retry(ctx, RetryConfig{
		IndexSetID:           cfg2.IndexSetID,
		RunID:                cfg2.RunID,
		BaseURI:              cfg2.BaseURI,
		Paths:                cfg2.Paths,
		JournalPaths:         sum2.JournalPaths,
		Coverage:             cfg2.Coverage,
		LossAcknowledgment:   &badAck,
		RunStartedAt:         cfg2.RunStartedAt,
		ManifestCreatedAt:    cfg2.ManifestCreatedAt,
		TargetRowsPerSegment: cfg2.TargetRowsPerSegment,
	})
	require.Error(t, err)
	reason, ok := durableindex.RefusalReason(err)
	require.True(t, ok)
	require.Equal(t, "acknowledgment_mismatch", reason)
}

func TestBuildAndRetryRejectLiteralDotRootSentinelCollision(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)

	sum1, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/alpha/a.txt", `"a1"`, 1, base),
	}, base)).Build(ctx)
	require.NoError(t, err)

	// Build with CrawlPrefixes=["data/."] must be refused before any journal or publish side effect.
	cfgDot := contConfig(setRoot, "run2", []provider.ObjectSummary{}, base.Add(time.Hour))
	cfgDot.CrawlPrefixes = []string{"data/."}
	cfgDot.Coverage = []CoverageAttestation{{
		Scope:    &Scope{Prefix: "data/."},
		Basis:    CoverageBasisConfirmed,
		Complete: true,
	}}
	_, err = NewRunner(cfgDot).Build(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "relative-root sentinel")

	// Hand-crafted sealed journal with CrawlPrefixes=["data/."] must also be refused by Retry.
	badJrnPath := filepath.Join(t.TempDir(), "bad.ndjson")
	jw, err := indexsubstrate.CreateJournal(badJrnPath, indexsubstrate.JournalHeader{
		Type:               indexsubstrate.JournalHeaderType,
		JournalID:          "jrn_run2_0001",
		IndexSetID:         cfgDot.IndexSetID,
		RunID:              "run2",
		Shard:              "shard-0001",
		Scope:              &indexsubstrate.Scope{Prefix: "data/"},
		CrawlPrefixes:      []string{"data/."},
		IndexSchemaVersion: indexsubstrate.IndexSchemaVersion,
		StartedAt:          base.Add(time.Hour),
	})
	require.NoError(t, err)
	require.NoError(t, jw.Seal(base.Add(time.Hour)))
	require.NoError(t, jw.Close())

	_, err = Retry(ctx, RetryConfig{
		IndexSetID:           cfgDot.IndexSetID,
		RunID:                "run2",
		BaseURI:              cfgDot.BaseURI,
		Paths:                cfgDot.Paths,
		JournalPaths:         []string{badJrnPath},
		Coverage:             cfgDot.Coverage,
		RunStartedAt:         base.Add(time.Hour),
		ManifestCreatedAt:    base.Add(time.Hour),
		TargetRowsPerSegment: cfgDot.TargetRowsPerSegment,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "relative-root sentinel")

	// Verify run1 remains intact and alpha/a.txt is still active.
	snapAfter, err := indexsubstrate.OpenLatestPublishedSnapshot(filepath.Join(setRoot, "latest.json"))
	require.NoError(t, err)
	require.Equal(t, sum1.RunID, snapAfter.Manifest.RunID)
	require.Equal(t, 1, snapAfter.Manifest.Counts.ActiveRows)
}
