package indexbuild

// Trust-seam regressions for the verified-parent plan: inconsistent plan
// shapes, same-run re-publish locus binding, deep-ancestor revalidation on
// recovery, idempotent continuous re-publish, and off-layout latest pointers.
// Every refusal must leave latest byte-identical with no publish artifacts.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// buildContinuousChain publishes run1 -> run2 -> run3 as a continuous lineage
// chain under one set root and returns run3's build config and summary.
func buildContinuousChain(t *testing.T, ctx context.Context, setRoot string, base time.Time) (Config, Summary) {
	t.Helper()
	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
		obj("data/b.xml", `"b1"`, 2, base),
	}, base)).Build(ctx)
	require.NoError(t, err)
	_, err = NewRunner(contConfig(setRoot, "run2", []provider.ObjectSummary{
		obj("data/a.xml", `"a2"`, 3, base.Add(time.Hour)),
		obj("data/b.xml", `"b1"`, 2, base),
	}, base.Add(time.Hour))).Build(ctx)
	require.NoError(t, err)
	cfg3 := contConfig(setRoot, "run3", []provider.ObjectSummary{
		obj("data/a.xml", `"a2"`, 3, base.Add(time.Hour)),
		obj("data/c.xml", `"c1"`, 4, base.Add(2*time.Hour)),
	}, base.Add(2*time.Hour))
	sum3, err := NewRunner(cfg3).Build(ctx)
	require.NoError(t, err)
	return cfg3, sum3
}

// retryConfigFor reproduces a completed build's publication inputs for Retry at
// the run's recorded artifact locus.
func retryConfigFor(cfg Config, sum Summary) RetryConfig {
	return RetryConfig{
		IndexSetID:           cfg.IndexSetID,
		RunID:                cfg.RunID,
		BaseURI:              cfg.BaseURI,
		Paths:                cfg.Paths,
		JournalPaths:         sum.JournalPaths,
		Coverage:             cfg.Coverage,
		RunStartedAt:         cfg.RunStartedAt,
		CreatedAt:            cfg.CreatedAt,
		TargetRowsPerSegment: cfg.TargetRowsPerSegment,
	}
}

// TestRetryWithLeaseRefusesInconsistentParentPlan proves the typed plan fails
// closed as one invariant before any token or continuity use. A token without a
// captured snapshot, a snapshot without a token, and a token disagreeing with
// the captured identity or coverage digest are each refused inside
// retryWithLease — never degraded to a baseline first publication or a
// token/continuity disagreement — with no artifacts written and latest
// byte-identical.
func TestRetryWithLeaseRefusesInconsistentParentPlan(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
	}, base)).Build(ctx)
	require.NoError(t, err)
	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	covDigest, err := indexsubstrate.CoverageSHA256(snap.Manifest.Coverage)
	require.NoError(t, err)
	liveToken := func() *ParentToken {
		return &ParentToken{
			IndexSetID:     snap.Complete.IndexSetID,
			RunID:          snap.Complete.RunID,
			ManifestSHA256: snap.Complete.ManifestSHA256,
			CoverageSHA256: covDigest,
		}
	}
	wrongRun := liveToken()
	wrongRun.RunID = "run_other"
	wrongCoverage := liveToken()
	wrongCoverage.CoverageSHA256 = "not-the-captured-coverage-digest"

	cases := map[string]*verifiedParentPlan{
		"token-without-snapshot":   {expected: liveToken()},
		"snapshot-without-token":   {snapshot: &snap},
		"identity-mismatch":        {snapshot: &snap, expected: wrongRun},
		"coverage-digest-mismatch": {snapshot: &snap, expected: wrongCoverage},
	}
	for name, plan := range cases {
		plan := plan
		t.Run(name, func(t *testing.T) {
			latestBefore, err := os.ReadFile(latestPath)
			require.NoError(t, err)
			authority, err := indexcoord.Acquire(ctx, setRoot, "idx_cont", "trust-test")
			require.NoError(t, err)
			defer func() { _ = authority.Release() }()
			lease, err := indexsubstrate.AcquireWriteLease(setRoot, "idx_cont", "trust-test", 0)
			require.NoError(t, err)
			defer func() { _ = lease.Release() }()

			childPaths := contRunPaths(setRoot, "run2")
			_, err = retryWithLease(ctx, RetryConfig{
				IndexSetID:   "idx_cont",
				RunID:        "run2",
				BaseURI:      "s3://bucket/data/",
				Paths:        childPaths,
				JournalPaths: []string{filepath.Join(setRoot, "runs", "run2", "journals", "shard-0001.jsonl")},
			}, plan, authority, lease)
			require.Error(t, err)
			require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)

			// Refused before any publication work: no child artifacts, latest
			// byte-identical.
			require.NoFileExists(t, childPaths.CompletePath)
			require.NoFileExists(t, childPaths.ManifestPath)
			require.NoDirExists(t, childPaths.SegmentDir)
			latestAfter, err := os.ReadFile(latestPath)
			require.NoError(t, err)
			require.Equal(t, latestBefore, latestAfter)
		})
	}
}

// TestBuildRefusesSameRunIDAtAlternateLocus proves an idempotent re-publish is
// recovery only at the captured run's exact artifact locus: a Build reusing a
// published run id with artifact paths under a different run directory (same
// latest) is refused stale, never writing a second artifact identity for that
// run id — no publish artifacts at the alternate locus and latest
// byte-identical.
func TestBuildRefusesSameRunIDAtAlternateLocus(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	objs := []provider.ObjectSummary{obj("data/a.xml", `"a1"`, 1, base)}
	_, err := NewRunner(contConfig(setRoot, "run1", objs, base)).Build(ctx)
	require.NoError(t, err)
	latestBefore, err := os.ReadFile(latestPath)
	require.NoError(t, err)
	canonicalManifest, err := os.ReadFile(filepath.Join(setRoot, "runs", "run1", "manifest.json"))
	require.NoError(t, err)

	// Same set/run id, artifact paths relocated under a shadow run directory.
	cfg := contConfig(setRoot, "run1", objs, base.Add(time.Hour))
	cfg.Paths = contRunPaths(setRoot, "run1-shadow")
	_, err = NewRunner(cfg).Build(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)
	require.Contains(t, err.Error(), "different artifact locus")

	// No publication reached the alternate locus and nothing advanced.
	shadowPaths := contRunPaths(setRoot, "run1-shadow")
	require.NoFileExists(t, shadowPaths.CompletePath)
	require.NoFileExists(t, shadowPaths.ManifestPath)
	require.NoDirExists(t, shadowPaths.SegmentDir)
	latestAfter, err := os.ReadFile(latestPath)
	require.NoError(t, err)
	require.Equal(t, latestBefore, latestAfter)
	canonicalAfter, err := os.ReadFile(filepath.Join(setRoot, "runs", "run1", "manifest.json"))
	require.NoError(t, err)
	require.Equal(t, canonicalManifest, canonicalAfter)
}

// TestRetryRefusesContinuousRePublishOverCorruptDeepAncestor proves a same-run
// recovery re-publish revalidates the run's own bounded ancestry with the same
// fail-closed contract as extension: with run1 -> run2 -> run3 continuous and
// run1's manifest corrupted, Retry of run3 at its correct locus is refused and
// latest stays byte-identical, so recovery cannot re-emit a continuous claim
// over a broken deep ancestor.
func TestRetryRefusesContinuousRePublishOverCorruptDeepAncestor(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	cfg3, sum3 := buildContinuousChain(t, ctx, setRoot, base)
	latestBefore, err := os.ReadFile(latestPath)
	require.NoError(t, err)

	// Corrupt the deep ancestor's manifest so its digest no longer matches its
	// complete marker (and run2's state_parent).
	run1Manifest := filepath.Join(setRoot, "runs", "run1", "manifest.json")
	data, err := os.ReadFile(run1Manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(run1Manifest, append(data, ' '), 0o600))

	_, err = Retry(ctx, retryConfigFor(cfg3, sum3))
	require.Error(t, err, "same-run recovery must fail closed on corrupt deep ancestry")
	require.Contains(t, err.Error(), "verify re-publish ancestry")

	latestAfter, err := os.ReadFile(latestPath)
	require.NoError(t, err)
	require.Equal(t, latestBefore, latestAfter, "latest unchanged after refused re-publish")
}

// TestRetryReproducesContinuousRunIdempotently proves the nontrivial recovery
// branch: a continuous run (non-nil state_parent) re-published by Retry at its
// correct locus reproduces its recorded continuity verbatim — identical rows,
// manifest digest and bytes, lineage generation, state_parent, and
// parent_manifests — sourcing parent rows from its recorded parent rather than
// extending itself into a self-cycle.
func TestRetryReproducesContinuousRunIdempotently(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	cfg3, sum3 := buildContinuousChain(t, ctx, setRoot, base)
	manifestBefore, err := os.ReadFile(cfg3.Paths.ManifestPath)
	require.NoError(t, err)
	latestBefore, err := os.ReadFile(latestPath)
	require.NoError(t, err)
	_, rowsBefore, err := ReadLatest(latestPath)
	require.NoError(t, err)

	retrySum, err := Retry(ctx, retryConfigFor(cfg3, sum3))
	require.NoError(t, err, "same-locus continuous re-publish is idempotent recovery")
	require.Equal(t, sum3.Manifest, retrySum.Manifest)
	require.Equal(t, sum3.ManifestSHA256, retrySum.ManifestSHA256)

	manifestAfter, err := os.ReadFile(cfg3.Paths.ManifestPath)
	require.NoError(t, err)
	require.Equal(t, manifestBefore, manifestAfter, "manifest bytes reproduced")
	// latest carries an advisory wall-clock updated_at outside the identity
	// contract; the re-publish must leave every identity field — set, run, and
	// the complete-marker locus — unchanged.
	latestAfter, err := os.ReadFile(latestPath)
	require.NoError(t, err)
	var before, after map[string]any
	require.NoError(t, json.Unmarshal(latestBefore, &before))
	require.NoError(t, json.Unmarshal(latestAfter, &after))
	delete(before, "updated_at")
	delete(after, "updated_at")
	require.Equal(t, before, after, "latest identity unchanged by idempotent re-publish")
	_, rowsAfter, err := ReadLatest(latestPath)
	require.NoError(t, err)
	require.Equal(t, rowsBefore, rowsAfter, "rows reproduced verbatim")

	// Continuity metadata reproduced, not re-derived: still generation 3 over
	// run2 — never a self-referential extension of run3.
	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.NotNil(t, snap.Manifest.Lineage)
	require.False(t, snap.Manifest.Lineage.Baseline)
	require.Equal(t, 3, snap.Manifest.Lineage.Generation)
	require.NotNil(t, snap.Manifest.StateParent)
	require.Equal(t, "run2", snap.Manifest.StateParent.RunID, "state parent is the recorded parent, not the run itself")
	require.Len(t, snap.Manifest.ParentManifests, 1)
	require.Equal(t, "run2", snap.Manifest.ParentManifests[0].RunID)
}

// TestBuildRefusesOffLayoutCapturedParentPointer proves a digest-valid latest
// pointer whose complete marker sits outside the canonical
// runs/<run_id>/complete.json layout cannot redirect ancestry lookups to a
// sibling root: when the captured parent carries a state parent, the capture is
// refused before any sink runs, leaving the pointer byte-identical.
func TestBuildRefusesOffLayoutCapturedParentPointer(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	// run2 is continuous over run1, so the captured parent carries a state
	// parent and any continuation must walk ancestry.
	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
	}, base)).Build(ctx)
	require.NoError(t, err)
	_, err = NewRunner(contConfig(setRoot, "run2", []provider.ObjectSummary{
		obj("data/a.xml", `"a2"`, 2, base.Add(time.Hour)),
	}, base.Add(time.Hour))).Build(ctx)
	require.NoError(t, err)

	canonicalComplete, err := os.ReadFile(filepath.Join(setRoot, "runs", "run2", "complete.json"))
	require.NoError(t, err)
	latestOriginal, err := os.ReadFile(latestPath)
	require.NoError(t, err)

	// Each case relocates run2's byte-identical complete marker off the
	// canonical layout and points latest at the copy. The marker still parses
	// and digest-validates; only the layout relationship differs.
	cases := map[string]struct {
		completePath string
		wantMessage  string
	}{
		"sibling-root": {
			completePath: filepath.Join(setRoot, "mirror", "run2", "complete.json"),
			wantMessage:  "canonical runs/ root",
		},
		"renamed-run-dir": {
			completePath: filepath.Join(setRoot, "runs", "run2-evil", "complete.json"),
			wantMessage:  "canonical run directory",
		},
	}
	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			require.NoError(t, os.MkdirAll(filepath.Dir(tc.completePath), 0o700))
			require.NoError(t, os.WriteFile(tc.completePath, canonicalComplete, 0o600))
			var doc map[string]any
			require.NoError(t, json.Unmarshal(latestOriginal, &doc))
			doc["complete_path"] = tc.completePath
			tampered, err := json.Marshal(doc)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(latestPath, tampered, 0o600))

			cfg3 := contConfig(setRoot, "run3", []provider.ObjectSummary{
				obj("data/a.xml", `"a2"`, 2, base.Add(time.Hour)),
			}, base.Add(2*time.Hour))
			_, err = NewRunner(cfg3).Build(ctx)
			require.Error(t, err)
			require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)
			require.Contains(t, err.Error(), tc.wantMessage)

			// Refused before sinks: no run3 journals/segments, pointer untouched.
			require.NoDirExists(t, filepath.Join(setRoot, "runs", "run3"))
			latestAfter, err := os.ReadFile(latestPath)
			require.NoError(t, err)
			require.Equal(t, tampered, latestAfter)
		})
	}
}
