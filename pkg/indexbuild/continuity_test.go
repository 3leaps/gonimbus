package indexbuild

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// contConfig builds a per-run continuity config under a shared set root.
func contConfig(setRoot, runID string, objs []provider.ObjectSummary, started time.Time) Config {
	return Config{
		IndexSetID:           "idx_cont",
		RunID:                runID,
		BaseURI:              "s3://bucket/data/",
		Source:               Source{Provider: fakeProvider{objects: objs}, ProviderName: "s3"},
		Match:                MatchConfig{Includes: []string{"**"}},
		Paths:                contRunPaths(setRoot, runID),
		Coverage:             []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true}},
		RunStartedAt:         started,
		TargetRowsPerSegment: 10,
	}
}

// contRunPaths lays out one run under a shared set root in the canonical
// runs/<run_id>/ layout so successive builds share latest.json and the ancestry
// lookup can resolve prior runs.
func contRunPaths(setRoot, runID string) PathConfig {
	runDir := filepath.Join(setRoot, "runs", runID)
	return PathConfig{
		JournalDir:   filepath.Join(runDir, "journals"),
		SegmentDir:   filepath.Join(runDir, "segments"),
		ManifestPath: filepath.Join(runDir, "manifest.json"),
		CompletePath: filepath.Join(runDir, "complete.json"),
		LatestPath:   filepath.Join(setRoot, "latest.json"),
		IndexDBDir:   filepath.Join(setRoot, "indexes"),
	}
}

func obj(key, etag string, size int64, at time.Time) provider.ObjectSummary {
	return provider.ObjectSummary{Key: key, ETag: etag, Size: size, LastModified: at, StorageClass: "STANDARD"}
}

// TestBuildContinuityExtendsBaselineWithLineageAndTombstones proves multi-run
// continuity across two builds of the same set: the first is a baseline
// generation 1; the
// second loads the verified parent's rows, publishes a non-baseline generation 2
// bound to the parent by digest, and merges coverage — changing a changed key,
// tombstoning a deleted key, preserving an unchanged key, and adding a new key.
func TestBuildContinuityExtendsBaselineWithLineageAndTombstones(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)

	mkcfg := func(runID string, objs []provider.ObjectSummary, started time.Time) Config {
		return Config{
			IndexSetID:           "idx_cont",
			RunID:                runID,
			BaseURI:              "s3://bucket/data/",
			Source:               Source{Provider: fakeProvider{objects: objs}, ProviderName: "s3"},
			Match:                MatchConfig{Includes: []string{"**"}},
			Paths:                contRunPaths(setRoot, runID),
			Coverage:             []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true}},
			RunStartedAt:         started,
			TargetRowsPerSegment: 10,
		}
	}

	// Run 1: a, b, c -> baseline generation 1.
	_, err := NewRunner(mkcfg("run1", []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
		obj("data/b.xml", `"b1"`, 2, base),
		obj("data/c.xml", `"c1"`, 3, base),
	}, base)).Build(ctx)
	require.NoError(t, err)

	latestPath := filepath.Join(setRoot, "latest.json")
	snap1, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.NotNil(t, snap1.Manifest.Lineage)
	require.True(t, snap1.Manifest.Lineage.Baseline, "run1 must be a baseline")
	require.Equal(t, indexsubstrate.LineageBaselineGeneration, snap1.Manifest.Lineage.Generation)
	require.Nil(t, snap1.Manifest.StateParent, "baseline first publication has no state parent")

	// Run 2: a changed, c unchanged, d added; b absent -> tombstoned under
	// confirmed-complete coverage.
	_, err = NewRunner(mkcfg("run2", []provider.ObjectSummary{
		obj("data/a.xml", `"a2"`, 9, base.Add(time.Hour)),
		obj("data/c.xml", `"c1"`, 3, base),
		obj("data/d.xml", `"d1"`, 4, base.Add(time.Hour)),
	}, base.Add(time.Hour))).Build(ctx)
	require.NoError(t, err)

	snap2, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	// Continuous generation 2 bound to run1 by digest.
	require.NotNil(t, snap2.Manifest.Lineage)
	require.False(t, snap2.Manifest.Lineage.Baseline, "run2 is a continuous child")
	require.Equal(t, 2, snap2.Manifest.Lineage.Generation)
	require.NotNil(t, snap2.Manifest.StateParent)
	require.Equal(t, "run1", snap2.Manifest.StateParent.RunID)
	require.Equal(t, snap1.Complete.ManifestSHA256, snap2.Manifest.StateParent.ManifestSHA256)
	require.Len(t, snap2.Manifest.ParentManifests, 1)

	_, rows, err := ReadLatest(latestPath)
	require.NoError(t, err)
	byKey := map[string]ObjectState{}
	for _, r := range rows {
		byKey[r.RelKey] = r
	}
	// rel_key is relative to the base prefix ("data/"). a changed (new etag,
	// active); c unchanged (active); d added (active); b tombstoned under
	// confirmed-complete coverage.
	require.Equal(t, `"a2"`, byKey["a.xml"].ETag)
	require.Nil(t, byKey["a.xml"].DeletedAt)
	require.Equal(t, `"c1"`, byKey["c.xml"].ETag)
	require.Nil(t, byKey["c.xml"].DeletedAt)
	require.Nil(t, byKey["d.xml"].DeletedAt)
	require.NotNil(t, byKey["b.xml"].DeletedAt, "deleted key must be tombstoned")
	// First-seen lineage preserved for the unchanged key across the continuity.
	require.Equal(t, "run1", byKey["c.xml"].FirstSeenRunID)
}

// TestBuildContinuityReappearClearsTombstone proves the add/change/delete/reappear
// lineage across three builds: a key deleted under complete coverage is
// tombstoned, then re-observed in a later build clears the tombstone.
func TestBuildContinuityReappearClearsTombstone(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	// run1: a, b present.
	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
		obj("data/b.xml", `"b1"`, 2, base),
	}, base)).Build(ctx)
	require.NoError(t, err)

	// run2: a only -> b tombstoned.
	_, err = NewRunner(contConfig(setRoot, "run2", []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
	}, base.Add(time.Hour))).Build(ctx)
	require.NoError(t, err)
	_, rows2, err := ReadLatest(latestPath)
	require.NoError(t, err)
	require.NotNil(t, rowByKey(rows2, "b.xml").DeletedAt, "b tombstoned after deletion")

	// run3: a, b again -> b reappears (tombstone cleared).
	_, err = NewRunner(contConfig(setRoot, "run3", []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
		obj("data/b.xml", `"b2"`, 5, base.Add(2*time.Hour)),
	}, base.Add(2*time.Hour))).Build(ctx)
	require.NoError(t, err)
	_, rows3, err := ReadLatest(latestPath)
	require.NoError(t, err)
	b := rowByKey(rows3, "b.xml")
	require.Nil(t, b.DeletedAt, "reappeared key clears its tombstone")
	require.Equal(t, `"b2"`, b.ETag)

	// Generation advanced to 3 across the continuity.
	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.Equal(t, 3, snap.Manifest.Lineage.Generation)
}

// TestBuildContinuityRefusesCorruptAncestry proves a continuous extension fails
// closed when the parent's ancestry chain cannot be verified, leaving latest
// unchanged.
func TestBuildContinuityRefusesCorruptAncestry(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
	}, base)).Build(ctx)
	require.NoError(t, err)
	_, err = NewRunner(contConfig(setRoot, "run2", []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
	}, base.Add(time.Hour))).Build(ctx)
	require.NoError(t, err)
	latestBefore, err := os.ReadFile(latestPath)
	require.NoError(t, err)

	// Corrupt run1's manifest so its digest no longer matches its complete marker
	// (and run2's state_parent). run3 extends run2 -> ancestry walk hits run1.
	run1Manifest := filepath.Join(setRoot, "runs", "run1", "manifest.json")
	data, err := os.ReadFile(run1Manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(run1Manifest, append(data, ' '), 0o600))

	_, err = NewRunner(contConfig(setRoot, "run3", []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
	}, base.Add(2*time.Hour))).Build(ctx)
	require.Error(t, err, "continuous extension must fail closed on corrupt ancestry")

	latestAfter, err := os.ReadFile(latestPath)
	require.NoError(t, err)
	require.Equal(t, latestBefore, latestAfter, "latest unchanged after refused extension")
}

func rowByKey(rows []ObjectState, relKey string) ObjectState {
	for _, r := range rows {
		if r.RelKey == relKey {
			return r
		}
	}
	return ObjectState{}
}
