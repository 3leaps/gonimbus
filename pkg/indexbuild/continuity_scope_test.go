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
	"github.com/3leaps/gonimbus/pkg/indexenrich"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// scopedContConfig is contConfig with an explicit crawl-prefix plan and its
// matching per-prefix coverage attestation (provider-key space, like the CLI
// adapter passes them).
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
	return cfg
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
