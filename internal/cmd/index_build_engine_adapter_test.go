package cmd

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexcompare"
	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/indexbuild"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/scope"
	"github.com/3leaps/gonimbus/pkg/uri"
)

func TestIndexBuildEngineAdapterMatchesLibraryManifestAndSegments(t *testing.T) {
	ctx := context.Background()
	libraryCfg := indexBuildEngineAdapterTestConfig(t, "library")
	adapterCfg := indexBuildEngineAdapterTestConfig(t, "adapter")

	librarySummary, err := indexbuild.NewRunner(libraryCfg).Build(ctx)
	require.NoError(t, err)
	adapterSummary, err := runIndexBuildEngine(ctx, adapterCfg)
	require.NoError(t, err)

	libraryManifest, err := os.ReadFile(libraryCfg.Paths.ManifestPath)
	require.NoError(t, err)
	adapterManifest, err := os.ReadFile(adapterCfg.Paths.ManifestPath)
	require.NoError(t, err)
	require.Equal(t, libraryManifest, adapterManifest)
	require.Equal(t, librarySummary.Manifest, adapterSummary.Manifest)
}

func TestIndexBuildExperimentalEngineCommandBuildsSnapshot(t *testing.T) {
	// Hidden --experimental-engine remains a durable alias during the compatibility window.
	testIndexBuildDurableCommandBuildsSnapshot(t, true)
}

func TestIndexBuildFormatDurableDefaultBuildsSnapshot(t *testing.T) {
	testIndexBuildDurableCommandBuildsSnapshot(t, false)
}

func testIndexBuildDurableCommandBuildsSnapshot(t *testing.T, experimentalAlias bool) {
	t.Helper()
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  match:
    includes: ["**"]
  crawl:
    concurrency: 1
    progress_every: 100
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"
	indexBuildExperimentalEngine = experimentalAlias

	var gotSrc *uri.ObjectURI
	var gotOpts providerdispatch.SourceOptions
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(_ context.Context, src *uri.ObjectURI, opts providerdispatch.SourceOptions) (provider.Provider, error) {
		gotSrc = src
		gotOpts = opts
		return indexBuildEngineFakeProvider{objects: indexBuildEngineTestObjects(base)}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	require.NoError(t, runIndexBuild(cmd, nil))

	require.Equal(t, "s3", gotSrc.Provider)
	require.Equal(t, "bucket", gotSrc.Bucket)
	require.Equal(t, operationIndexBuild, gotOpts.Command)

	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
	manifestSummary, rows, err := indexbuild.ReadLatest(latestFiles[0])
	require.NoError(t, err)
	require.Equal(t, 2, manifestSummary.Rows)
	require.Len(t, rows, 2)
	require.Equal(t, "a.xml", rows[0].RelKey)
	require.Equal(t, "b.xml", rows[1].RelKey)
	// Durable-only builds write identity, not index.db.
	indexDBs, err := filepath.Glob(filepath.Join(dataRoot, "indexes", "*", "index.db"))
	require.NoError(t, err)
	require.Empty(t, indexDBs)
}

func TestIndexBuildFormatDurableRejectsDBFlag(t *testing.T) {
	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildFormat = "durable"
	indexBuildDBPath = "/tmp/index.db"
	err := validateIndexBuildFormatFlags("")
	require.ErrorContains(t, err, "does not use --db")
}

func TestIndexBuildFormatDurableRejectsIncludeHidden(t *testing.T) {
	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildFormat = "durable"
	m := &manifest.IndexManifest{
		Build: &manifest.IndexBuildConfig{
			Match: &manifest.IndexMatchConfig{
				Includes:      []string{"**"},
				IncludeHidden: true,
			},
		},
	}
	err := validateIndexBuildFormatManifest(m)
	require.ErrorContains(t, err, "include_hidden")
}

func TestIndexBuildFormatBothAllowsDBFlag(t *testing.T) {
	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildFormat = "both"
	indexBuildDBPath = "/tmp/index.db"
	require.NoError(t, validateIndexBuildFormatFlags(""))
}

func TestIndexBuildFormatBothAllowsDryRunFlag(t *testing.T) {
	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildFormat = "both"
	indexBuildDryRun = true
	require.NoError(t, validateIndexBuildFormatFlags(""))
}

func TestIndexBuildFormatBothUsesSingleObservedProviderStream(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  match:
    includes: ["**"]
  crawl:
    concurrency: 1
    progress_every: 100
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"

	prov := &countingIndexBuildProvider{objects: indexBuildEngineTestObjects(base)}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	var stdout strings.Builder
	cmd.SetOut(&stdout)
	require.NoError(t, runIndexBuild(cmd, nil))
	require.Equal(t, 1, prov.listCalls)

	var report map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(stdout.String())), &report))
	require.Equal(t, "gonimbus.index.compare_result.v1", report["type"])
	semantics, ok := report["projection_semantics"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "LIST-projection fidelity (sqlite vs durable row projection over one crawl)", semantics["certifies"])
	require.Equal(t, "reflow-input readiness (HEAD-enrichment parity)", semantics["does_not_certify"])
	require.Equal(t, []any{"rel_key", "size_bytes", "last_modified", "storage_class"}, semantics["included_fields"])
	require.Contains(t, semantics["content_identity"], "not a portable content hash")
	require.Equal(t, true, report["sqlite_materialized"])
	require.Equal(t, true, report["durable_published"])
	require.Equal(t, true, report["comparison_ran"])
	require.Equal(t, true, report["parity_passed"])

	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
}

// TestIndexBuildFormatBothEmitsDurableLineageIndependentOfSQLite proves that a
// --format both build emits authoritative durable lineage (baseline generation
// 1 + run_started_at) on the durable side while the SQLite and durable row
// projections match over the one crawl. The lineage is produced by the durable
// path alone; SQLite never contributes to it.
//
// NOTE: successive --format both builds of the same set are additionally blocked
// today on the SQLite side — the second build's canonical-DB adoption refuses to
// reopen the prior index.db while quarantined transaction sidecars from the
// first build are present. That is a SQLite-store limitation (pkg/indexstore
// sidecar guard), orthogonal to durable continuity, which is proven end-to-end
// by the durable-only successive-build tests in pkg/indexbuild.
func TestIndexBuildFormatBothEmitsDurableLineageIndependentOfSQLite(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  match:
    includes: ["**"]
  crawl:
    concurrency: 1
    progress_every: 100
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"

	prov := &countingIndexBuildProvider{objects: []provider.ObjectSummary{
		{Key: "data/a.xml", Size: 10, ETag: `"a1"`, LastModified: base, StorageClass: "STANDARD"},
		{Key: "data/b.xml", Size: 11, ETag: `"b1"`, LastModified: base, StorageClass: "STANDARD"},
	}}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	var stdout strings.Builder
	cmd.SetOut(&stdout)
	require.NoError(t, runIndexBuild(cmd, nil))
	require.Equal(t, 1, prov.listCalls, "one crawl feeds both sinks")

	var report map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(stdout.String())), &report))
	require.Equal(t, true, report["parity_passed"], "sqlite and durable projections match over the crawl")

	// Durable side carries authoritative lineage produced independently.
	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestFiles[0])
	require.NoError(t, err)
	require.NotNil(t, snap.Manifest.Lineage, "durable emits lineage under --format both")
	require.True(t, snap.Manifest.Lineage.Baseline)
	require.Equal(t, indexsubstrate.LineageBaselineGeneration, snap.Manifest.Lineage.Generation)
	require.NotNil(t, snap.Manifest.RunStartedAt)
}

// TestIndexBuildFormatBothSuccessiveScopedRunsFormDurableContinuity proves the
// case that used to fail: a second scoped --format both build of the same set
// no longer contends for canonical index.db residue, extends durable lineage as
// a continuous child, and passes per-run parity with a run-bound receipt. The
// same two-run sequence replayed through --format durable in a fresh data root
// must produce an identical current-state row projection, proving the durable
// side of `both` is independent of its SQLite verification sink.
func TestIndexBuildFormatBothSuccessiveScopedRunsFormDurableContinuity(t *testing.T) {
	base := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  scope:
    type: prefix_list
    prefixes: ["hot/", "cold/"]
  match:
    includes: ["**"]
  crawl:
    concurrency: 1
    progress_every: 100
`), 0o600))

	run1Objects := []provider.ObjectSummary{
		{Key: "data/hot/a.xml", Size: 10, ETag: `"a1"`, LastModified: base, StorageClass: "STANDARD"},
		{Key: "data/cold/b.xml", Size: 11, ETag: `"b1"`, LastModified: base, StorageClass: "STANDARD"},
	}
	run2Objects := []provider.ObjectSummary{
		{Key: "data/hot/a.xml", Size: 20, ETag: `"a2"`, LastModified: base.Add(time.Hour), StorageClass: "STANDARD"},
		{Key: "data/hot/a2.xml", Size: 12, ETag: `"n1"`, LastModified: base.Add(time.Hour), StorageClass: "STANDARD"},
	}
	prov := &countingIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	restore := withIndexBuildExperimentalEngineTestState(t)
	runBothBuild := func() indexBuildResultRecord {
		t.Helper()
		restore()
		indexBuildJobPath = manifestPath
		indexBuildFormat = "both"
		indexBuildJSON = true
		cmd := &cobra.Command{Use: "build"}
		cmd.SetContext(context.Background())
		var stdout strings.Builder
		cmd.SetOut(&stdout)
		require.NoError(t, runIndexBuild(cmd, nil))
		lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
		require.Len(t, lines, 2, "compare report then terminal receipt")
		var report map[string]any
		require.NoError(t, json.Unmarshal([]byte(lines[0]), &report))
		require.Equal(t, true, report["parity_passed"])
		var rec indexBuildResultRecord
		require.NoError(t, json.Unmarshal([]byte(lines[1]), &rec))
		require.Equal(t, "success", rec.Status)
		require.Equal(t, []string{"durable-v2"}, rec.FormatsCommitted)
		require.NotNil(t, rec.Verification)
		require.True(t, rec.Verification.ParityPassed)
		require.True(t, rec.Verification.ProjectionMaterialized)
		require.True(t, rec.Verification.ProjectionClosed)
		require.Equal(t, rec.RunID, rec.Verification.ObservationRunID, "verification must bind the producing run")
		// Report and receipt share the actual compared artifact identity: an
		// opaque attempt ID plus a set-relative locator naming the run-scoped
		// projection (never the canonical index.db path, never host-absolute).
		sqliteArtifact, _ := report["sqlite_artifact"].(map[string]any)
		require.NotNil(t, sqliteArtifact)
		require.Equal(t, rec.Verification.ArtifactID, sqliteArtifact["id"])
		require.Equal(t, rec.Verification.ArtifactLocator, sqliteArtifact["path"])
		require.Equal(t, "verification/"+rec.Verification.ArtifactID+"/index.db", rec.Verification.ArtifactLocator)
		require.False(t, filepath.IsAbs(rec.Verification.ArtifactLocator))
		locatorPath := filepath.Join(os.Getenv("GONIMBUS_DATA_DIR"), "cache", "segments", rec.IndexSetID, rec.Verification.ArtifactLocator)
		require.FileExists(t, locatorPath, "locator must name the projection that exists through comparison and close")
		return rec
	}
	currentRows := func(dataRoot string) (indexsubstrate.InternalManifest, map[string]indexbuild.ObjectState, string) {
		t.Helper()
		latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
		require.NoError(t, err)
		require.Len(t, latestFiles, 1)
		snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestFiles[0])
		require.NoError(t, err)
		_, rows, err := indexbuild.ReadLatest(latestFiles[0])
		require.NoError(t, err)
		byKey := map[string]indexbuild.ObjectState{}
		for _, r := range rows {
			byKey[r.RelKey] = r
		}
		return snap.Manifest, byKey, latestFiles[0]
	}

	// --- both path: two successive scoped runs in one data root, no cleanup ---
	resetAppDataRootTestState(t)
	bothRoot := filepath.Join(t.TempDir(), "gonimbus-data-both")
	t.Setenv("GONIMBUS_DATA_DIR", bothRoot)

	prov.objects = run1Objects
	rec1 := runBothBuild()
	manifest1, _, latestPath := currentRows(bothRoot)
	require.NotNil(t, manifest1.Lineage)
	require.True(t, manifest1.Lineage.Baseline)

	// Shape 2: no canonical consumer index.db is created by `both`; canonical
	// identity is published; the SQLite side lives on run-scoped verification
	// paths (one per run, discoverable residue, never adopted).
	dbGlobs, err := filepath.Glob(filepath.Join(bothRoot, "indexes", "idx_*", "index.db"))
	require.NoError(t, err)
	require.Empty(t, dbGlobs, "both must not create a canonical index.db")
	identityGlobs, err := filepath.Glob(filepath.Join(bothRoot, "indexes", "idx_*", "identity.json"))
	require.NoError(t, err)
	require.Len(t, identityGlobs, 1)

	prov.objects = run2Objects
	rec2 := runBothBuild()
	require.NotEqual(t, rec1.RunID, rec2.RunID)
	require.Equal(t, rec1.IndexSetID, rec2.IndexSetID)

	manifest2, bothByKey, _ := currentRows(bothRoot)
	require.NotNil(t, manifest2.Lineage)
	require.False(t, manifest2.Lineage.Baseline, "second scoped both build is a continuous child")
	require.Equal(t, 2, manifest2.Lineage.Generation)
	require.NotNil(t, manifest2.StateParent)
	require.Equal(t, rec1.RunID, manifest2.StateParent.RunID)
	require.Equal(t, `"a2"`, bothByKey["hot/a.xml"].ETag)
	require.Nil(t, bothByKey["hot/a.xml"].DeletedAt)
	require.Nil(t, bothByKey["hot/a2.xml"].DeletedAt)
	require.NotNil(t, bothByKey["cold/b.xml"].DeletedAt, "unobserved in-plan key must be tombstoned")

	verificationDBs, err := filepath.Glob(filepath.Join(bothRoot, "cache", "segments", "*", "verification", "run_*", "index.db"))
	require.NoError(t, err)
	require.Len(t, verificationDBs, 2, "one run-scoped verification projection per both run")
	_ = latestPath

	// --- durable-only twin: identical two-run sequence in a fresh data root ---
	resetAppDataRootTestState(t)
	durableRoot := filepath.Join(t.TempDir(), "gonimbus-data-durable")
	t.Setenv("GONIMBUS_DATA_DIR", durableRoot)
	runDurableBuild := func() {
		t.Helper()
		restore()
		indexBuildJobPath = manifestPath
		indexBuildFormat = "durable"
		cmd := &cobra.Command{Use: "build"}
		cmd.SetContext(context.Background())
		cmd.SetOut(&strings.Builder{})
		require.NoError(t, runIndexBuild(cmd, nil))
	}
	prov.objects = run1Objects
	runDurableBuild()
	prov.objects = run2Objects
	runDurableBuild()

	durableManifest, durableByKey, _ := currentRows(durableRoot)
	require.Equal(t, manifest2.Counts, durableManifest.Counts)
	require.Equal(t, manifest2.Lineage.Generation, durableManifest.Lineage.Generation)
	require.Equal(t, len(bothByKey), len(durableByKey))
	for key, want := range durableByKey {
		got, ok := bothByKey[key]
		require.True(t, ok, "row %s missing from both-path state", key)
		require.Equal(t, want.SizeBytes, got.SizeBytes, key)
		require.Equal(t, want.ETag, got.ETag, key)
		require.Equal(t, want.DeletedAt == nil, got.DeletedAt == nil, key)
	}
}

// TestIndexBuildFormatBothDiscoveryPlanRetainsOutOfPlanRowWithParity pins the
// retained-row leg of the comparator predicate through the adapter: under one
// discovery-driven scope identity, run 1 observes rows under two discovered
// prefixes and run 2's compiled plan covers only one of them. The row outside
// run 2's plan must stay active and lineage-stable in durable current state
// (never tombstoned, never re-stamped), while run 2's SQLite verification
// projection carries only current-run rows and parity still passes because the
// comparator applies the observation-run predicate to the durable side.
func TestIndexBuildFormatBothDiscoveryPlanRetainsOutOfPlanRowWithParity(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  scope:
    type: date_partitions
    discover:
      segments:
        - index: 0
    date:
      segment_index: 1
      format: "2006-01-02"
      range:
        after: "2026-04-01"
        before: "2026-04-02"
  match:
    includes: ["**"]
  crawl:
    concurrency: 1
    progress_every: 100
`), 0o600))

	prov := &discoveringIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	restore := withIndexBuildExperimentalEngineTestState(t)
	runBothBuild := func() indexBuildResultRecord {
		t.Helper()
		restore()
		indexBuildJobPath = manifestPath
		indexBuildFormat = "both"
		indexBuildJSON = true
		cmd := &cobra.Command{Use: "build"}
		cmd.SetContext(context.Background())
		var stdout strings.Builder
		cmd.SetOut(&stdout)
		require.NoError(t, runIndexBuild(cmd, nil))
		lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
		require.Len(t, lines, 2)
		var report map[string]any
		require.NoError(t, json.Unmarshal([]byte(lines[0]), &report))
		require.Equal(t, true, report["parity_passed"])
		var rec indexBuildResultRecord
		require.NoError(t, json.Unmarshal([]byte(lines[1]), &rec))
		require.Equal(t, "success", rec.Status)
		require.NotNil(t, rec.Verification)
		require.True(t, rec.Verification.ParityPassed)
		require.Equal(t, rec.RunID, rec.Verification.ObservationRunID)
		return rec
	}

	// Run 1: discovery surfaces site-a and site-b; both rows observed.
	prov.objects = []provider.ObjectSummary{
		{Key: "data/site-a/2026-04-01/a.xml", Size: 10, ETag: `"a1"`, LastModified: base, StorageClass: "STANDARD"},
		{Key: "data/site-b/2026-04-01/b.xml", Size: 11, ETag: `"b1"`, LastModified: base, StorageClass: "STANDARD"},
	}
	rec1 := runBothBuild()
	require.EqualValues(t, 2, rec1.Verification.ProjectionRows)

	// Run 2: same manifest identity, but discovery now surfaces only site-a,
	// so the compiled plan excludes site-b entirely. site-b's row is outside
	// run 2's attested coverage.
	prov.objects = []provider.ObjectSummary{
		{Key: "data/site-a/2026-04-01/a.xml", Size: 20, ETag: `"a2"`, LastModified: base.Add(time.Hour), StorageClass: "STANDARD"},
	}
	rec2 := runBothBuild()
	require.Equal(t, rec1.IndexSetID, rec2.IndexSetID, "discovery narrowing must not change identity")
	require.NotEqual(t, rec1.RunID, rec2.RunID)
	// The verification projection carries only current-run rows; without the
	// symmetric observation-run predicate the retained durable row would read
	// as SQLite-missing and parity (asserted green above) would fail.
	require.EqualValues(t, 1, rec2.Verification.ProjectionRows)

	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestFiles[0])
	require.NoError(t, err)
	require.NotNil(t, snap.Manifest.Lineage)
	require.Equal(t, 2, snap.Manifest.Lineage.Generation, "run 2 extends run 1 continuously")
	_, rows, err := indexbuild.ReadLatest(latestFiles[0])
	require.NoError(t, err)
	byKey := map[string]indexbuild.ObjectState{}
	for _, r := range rows {
		byKey[r.RelKey] = r
	}
	require.Len(t, byKey, 2)

	observed := byKey["site-a/2026-04-01/a.xml"]
	require.Equal(t, `"a2"`, observed.ETag)
	require.Equal(t, rec2.RunID, observed.LastSeenRunID)
	require.Nil(t, observed.DeletedAt)

	retained := byKey["site-b/2026-04-01/b.xml"]
	require.Nil(t, retained.DeletedAt, "out-of-plan row must never be tombstoned")
	require.Equal(t, `"b1"`, retained.ETag)
	require.Equal(t, rec1.RunID, retained.LastSeenRunID, "out-of-plan row must not be re-stamped by run 2")
	require.Equal(t, rec1.RunID, retained.FirstSeenRunID)
}

// TestIndexBuildFormatBothRefusesPlantedVerificationSymlink reproduces the
// review probe: a planted <set>/verification symlink must refuse before any
// provider work or SQLite mutation, leaving the outside directory
// byte-identical and emitting no receipt.
func TestIndexBuildFormatBothRefusesPlantedVerificationSymlink(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	manifestPath := writeScopedPrefixManifest(t, []string{"hot/"})

	m, err := manifest.LoadIndexManifest(manifestPath)
	require.NoError(t, err)
	scopeHash, err := scope.HashConfig(m.Build.Scope)
	require.NoError(t, err)
	indexSetID := computeTestIndexSetID(t, m, scopeHash)

	outside := filepath.Join(t.TempDir(), "outside")
	require.NoError(t, os.MkdirAll(outside, 0o700))
	sentinel := filepath.Join(outside, "sentinel.txt")
	require.NoError(t, os.WriteFile(sentinel, []byte("untouched\n"), 0o600))
	segmentRoot := filepath.Join(dataRoot, "cache", "segments", indexSetID)
	require.NoError(t, os.MkdirAll(segmentRoot, 0o700))
	require.NoError(t, os.Symlink(outside, filepath.Join(segmentRoot, "verification")))

	prov := &countingIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"
	indexBuildJSON = true
	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	var stdout strings.Builder
	cmd.SetOut(&stdout)
	err = runIndexBuild(cmd, nil)
	require.ErrorIs(t, err, indexreader.ErrVerificationProjectionTarget)
	require.Zero(t, prov.listCalls, "refusal must happen before provider work")
	require.Empty(t, strings.TrimSpace(stdout.String()), "no report or receipt on refusal")

	entries, err := os.ReadDir(outside)
	require.NoError(t, err)
	require.Len(t, entries, 1, "no files may be created through the planted symlink")
	sentinelBytes, err := os.ReadFile(sentinel)
	require.NoError(t, err)
	require.Equal(t, []byte("untouched\n"), sentinelBytes)

	latest, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Empty(t, latest, "no durable publication on refusal")
}

func TestIndexBuildBothFormatsFailureReportCarriesProjectionSemantics(t *testing.T) {
	report := indexBuildBothFormatsFailureReport(nil, nil, indexcompare.Artifact{Path: "/tmp/index.db"}, indexbuild.PathConfig{ManifestPath: "/tmp/manifest.json"}, true, false)
	require.Equal(t, "gonimbus.index.compare_result.v1", report.Type)
	require.Equal(t, "LIST-projection fidelity (sqlite vs durable row projection over one crawl)", report.ProjectionSemantics.Certifies)
	require.Equal(t, "reflow-input readiness (HEAD-enrichment parity)", report.ProjectionSemantics.DoesNotCertify)
	require.Equal(t, []string{"rel_key", "size_bytes", "last_modified", "storage_class"}, report.ProjectionSemantics.IncludedFields)
	require.Contains(t, report.ProjectionSemantics.ContentIdentity, "not a portable content hash")
	require.Len(t, report.ProjectionSemantics.ExcludedFields, 4)
	require.False(t, report.ComparisonRan)
	require.False(t, report.ParityPassed)
}

func TestIndexBuildFormatBothAcceptsMultiPrefixScopeWithFaithfulCoverage(t *testing.T) {
	// Scoped --format both: one crawl, dual writers, coverage prefixes == plan
	// prefixes exactly, Window nil, parity PASS.
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  scope:
    type: prefix_list
    prefixes: ["hot/", "cold/"]
  match:
    includes: ["**"]
  crawl:
    concurrency: 1
    progress_every: 100
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"

	objects := []provider.ObjectSummary{
		{Key: "data/hot/a.xml", Size: 10, ETag: `"a"`, LastModified: base.Add(-time.Hour), StorageClass: "STANDARD"},
		{Key: "data/cold/b.xml", Size: 11, ETag: `"b"`, LastModified: base.Add(-time.Minute), StorageClass: "STANDARD"},
		// Outside scope — must not be observed.
		{Key: "data/other/c.xml", Size: 12, ETag: `"c"`, LastModified: base, StorageClass: "STANDARD"},
	}
	prov := &countingIndexBuildProvider{objects: objects}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	var stdout strings.Builder
	cmd.SetOut(&stdout)
	require.NoError(t, runIndexBuild(cmd, nil))
	// Single crawl over two plan prefixes (not a third full-base list).
	require.Equal(t, 2, prov.listCalls)

	var report map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(stdout.String())), &report))
	require.Equal(t, true, report["parity_passed"])
	require.Equal(t, true, report["durable_published"])

	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
	manifestSummary, rows, err := indexbuild.ReadLatest(latestFiles[0])
	require.NoError(t, err)
	require.Equal(t, 2, manifestSummary.Rows)
	require.Len(t, rows, 2)

	// Coverage on the durable manifest must list exactly the plan prefixes
	// (relative after base-uri normalize) with Window nil.
	manifestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "runs", "*", "manifest.json"))
	require.NoError(t, err)
	require.Len(t, manifestFiles, 1)
	manifestDoc, err := indexsubstrate.ReadInternalManifestFile(manifestFiles[0])
	require.NoError(t, err)
	require.Len(t, manifestDoc.Coverage, 2)
	got := make([]string, 0, len(manifestDoc.Coverage))
	for _, entry := range manifestDoc.Coverage {
		require.NotNil(t, entry.Scope)
		require.Nil(t, entry.Scope.Window)
		require.True(t, entry.Complete)
		require.Equal(t, indexsubstrate.CoverageBasisConfirmed, entry.Basis)
		got = append(got, entry.Scope.Prefix)
	}
	sort.Strings(got)
	require.Equal(t, []string{"cold/", "hot/"}, got)
}

func TestIndexBuildFormatDurableRepeatedScopeContinuityMergesCoverage(t *testing.T) {
	// Repeated exact static scope through the CLI adapter: the second scoped
	// build of the same identity extends the first as a continuous child,
	// applies add/change/delete inside the attested plan, and publishes
	// coverage equal to the plan on every generation.
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  scope:
    type: prefix_list
    prefixes: ["hot/", "cold/"]
  match:
    includes: ["**"]
  crawl:
    concurrency: 1
    progress_every: 100
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"

	prov := &countingIndexBuildProvider{objects: []provider.ObjectSummary{
		{Key: "data/hot/a.xml", Size: 10, ETag: `"a1"`, LastModified: base, StorageClass: "STANDARD"},
		{Key: "data/cold/b.xml", Size: 11, ETag: `"b1"`, LastModified: base, StorageClass: "STANDARD"},
	}}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	runBuild := func() {
		t.Helper()
		cmd := &cobra.Command{Use: "build"}
		cmd.SetContext(context.Background())
		require.NoError(t, runIndexBuild(cmd, nil))
	}
	runBuild()

	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
	latestPath := latestFiles[0]
	snap1, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.True(t, snap1.Manifest.Lineage.Baseline)

	// Second build of the same scoped identity: hot/a changed, hot/a2 added,
	// cold/b deleted (in-plan -> tombstone).
	prov.objects = []provider.ObjectSummary{
		{Key: "data/hot/a.xml", Size: 20, ETag: `"a2"`, LastModified: base.Add(time.Hour), StorageClass: "STANDARD"},
		{Key: "data/hot/a2.xml", Size: 12, ETag: `"n1"`, LastModified: base.Add(time.Hour), StorageClass: "STANDARD"},
	}
	runBuild()

	snap2, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.NotNil(t, snap2.Manifest.Lineage)
	require.False(t, snap2.Manifest.Lineage.Baseline, "second scoped build is a continuous child")
	require.Equal(t, 2, snap2.Manifest.Lineage.Generation)
	require.NotNil(t, snap2.Manifest.StateParent)
	require.Equal(t, snap1.Complete.RunID, snap2.Manifest.StateParent.RunID)

	_, rows, err := indexbuild.ReadLatest(latestPath)
	require.NoError(t, err)
	byKey := map[string]indexbuild.ObjectState{}
	for _, r := range rows {
		byKey[r.RelKey] = r
	}
	require.Equal(t, `"a2"`, byKey["hot/a.xml"].ETag)
	require.Nil(t, byKey["hot/a.xml"].DeletedAt)
	require.Nil(t, byKey["hot/a2.xml"].DeletedAt)
	require.NotNil(t, byKey["cold/b.xml"].DeletedAt, "unobserved in-plan key must be tombstoned")

	// Coverage on generation 2 equals the plan exactly (never rolled up).
	got := make([]string, 0, len(snap2.Manifest.Coverage))
	for _, entry := range snap2.Manifest.Coverage {
		require.NotNil(t, entry.Scope)
		require.Nil(t, entry.Scope.Window)
		require.True(t, entry.Complete)
		got = append(got, entry.Scope.Prefix)
	}
	sort.Strings(got)
	require.Equal(t, []string{"cold/", "hot/"}, got)
}

func TestIndexBuildFormatBothRejectsScopePlusMatchExcludes(t *testing.T) {
	// Scope-relax must not open the match-predicate door.
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  scope:
    type: prefix_list
    prefixes: ["hot/"]
  match:
    includes: ["**"]
    excludes: ["tmp/**"]
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"

	prov := &countingIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	err := runIndexBuild(cmd, nil)
	require.ErrorContains(t, err, "--format both does not support build.match.excludes in this slice")
	require.Zero(t, prov.listCalls)
}

func TestIndexBuildEngineCoverageFaithfulSetEquality(t *testing.T) {
	// Happy path: multi-prefix plan → exact coverage.
	coverage, err := indexBuildEngineCoverageFromCrawl("data/", []string{"data/hot/", "data/cold/"})
	require.NoError(t, err)
	require.Len(t, coverage, 2)
	for _, entry := range coverage {
		require.NotNil(t, entry.Scope)
		require.Nil(t, entry.Scope.Window)
		require.True(t, entry.Complete)
	}

	// Unscoped base fallback.
	coverage, err = indexBuildEngineCoverageFromCrawl("data/", nil)
	require.NoError(t, err)
	require.Len(t, coverage, 1)
	require.Equal(t, "data/", coverage[0].Scope.Prefix)
	require.Nil(t, coverage[0].Scope.Window)

	// Unscoped bucket-root base_uri (empty provider key) must not fail validation.
	coverage, err = indexBuildEngineCoverageFromCrawl("", nil)
	require.NoError(t, err)
	require.Len(t, coverage, 1)
	require.Equal(t, indexsubstrate.RelativeRootScopePrefix, coverage[0].Scope.Prefix)
	require.Nil(t, coverage[0].Scope.Window)

	// Roll-up to base must fail (coverage claims base while crawl is sub-prefixes).
	rolled := []indexbuild.CoverageAttestation{{
		Scope:    &indexbuild.Scope{Prefix: "data/"},
		Basis:    indexbuild.CoverageBasisConfirmed,
		Complete: true,
	}}
	err = validateFaithfulIndexBuildCoverage("data/", []string{"data/hot/", "data/cold/"}, rolled)
	require.ErrorContains(t, err, "not in the crawl plan")

	// Missing plan prefix must fail.
	missing := []indexbuild.CoverageAttestation{{
		Scope:    &indexbuild.Scope{Prefix: "data/hot/"},
		Basis:    indexbuild.CoverageBasisConfirmed,
		Complete: true,
	}}
	err = validateFaithfulIndexBuildCoverage("data/", []string{"data/hot/", "data/cold/"}, missing)
	require.ErrorContains(t, err, "must equal crawl plan prefixes")

	// Windowed coverage must fail early at build.
	windowed := []indexbuild.CoverageAttestation{{
		Scope: &indexbuild.Scope{
			Prefix: "data/hot/",
			Window: &indexbuild.Window{From: "2026-01-01", To: "2026-01-31"},
		},
		Basis:    indexbuild.CoverageBasisConfirmed,
		Complete: true,
	}}
	err = validateFaithfulIndexBuildCoverage("data/", []string{"data/hot/"}, windowed)
	require.ErrorContains(t, err, "must not set a temporal window")
}

func TestIndexBuildFormatDurableUnscopedBucketRootSucceeds(t *testing.T) {
	// Entarch: base_uri s3://bucket/ yields empty basePrefix; durable default
	// must still publish via relative-root coverage, not fail prefix-required.
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  match:
    includes: ["**"]
  crawl:
    concurrency: 1
    progress_every: 100
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"

	objects := []provider.ObjectSummary{
		{Key: "a.xml", Size: 10, ETag: `"a"`, LastModified: base.Add(-time.Hour), StorageClass: "STANDARD"},
		{Key: "b.xml", Size: 11, ETag: `"b"`, LastModified: base.Add(-time.Minute), StorageClass: "STANDARD"},
	}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return indexBuildEngineFakeProvider{objects: objects}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	require.NoError(t, runIndexBuild(cmd, nil))

	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
	_, rows, err := indexbuild.ReadLatest(latestFiles[0])
	require.NoError(t, err)
	require.Len(t, rows, 2)

	manifestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "runs", "*", "manifest.json"))
	require.NoError(t, err)
	require.Len(t, manifestFiles, 1)
	manifestDoc, err := indexsubstrate.ReadInternalManifestFile(manifestFiles[0])
	require.NoError(t, err)
	require.Len(t, manifestDoc.Coverage, 1)
	require.Equal(t, indexsubstrate.RelativeRootScopePrefix, manifestDoc.Coverage[0].Scope.Prefix)
	require.Nil(t, manifestDoc.Coverage[0].Scope.Window)
}

func TestIndexBuildFormatBothRejectsNarrowMatchIncludesBeforeDurablePublish(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  match:
    includes: ["hot/**"]
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"

	prov := &countingIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	err := runIndexBuild(cmd, nil)
	require.ErrorContains(t, err, `--format both supports only default build.match.includes "**" in this slice`)
	require.Zero(t, prov.listCalls)

	latestFiles, globErr := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, globErr)
	require.Empty(t, latestFiles)
}

func TestIndexBuildFormatBothRejectsWhitespacePaddedDefaultIncludeBeforeDurablePublish(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  match:
    includes: [" ** "]
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"

	prov := &countingIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	err := runIndexBuild(cmd, nil)
	require.ErrorContains(t, err, `--format both supports only default build.match.includes "**" in this slice`)
	require.Zero(t, prov.listCalls)

	latestFiles, globErr := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, globErr)
	require.Empty(t, latestFiles)
}

func TestIndexBuildFormatBothRejectsMatchExcludesBeforeDurablePublish(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  match:
    includes: ["**"]
    excludes: ["tmp/**"]
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"

	prov := &countingIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	err := runIndexBuild(cmd, nil)
	require.ErrorContains(t, err, "--format both does not support build.match.excludes in this slice")
	require.Zero(t, prov.listCalls)

	latestFiles, globErr := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, globErr)
	require.Empty(t, latestFiles)
}

func TestIndexBuildFormatBothRejectsMatchFiltersBeforeDurablePublish(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  match:
    includes: ["**"]
    filters:
      size:
        min: "1B"
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"

	prov := &countingIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	err := runIndexBuild(cmd, nil)
	require.ErrorContains(t, err, "--format both does not support build.match.filters in this slice")
	require.Zero(t, prov.listCalls)

	latestFiles, globErr := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, globErr)
	require.Empty(t, latestFiles)
}

func TestIndexBuildExperimentalEngineRejectsNarrowMatchBeforeDurablePublish(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  match:
    includes: ["**"]
    excludes: ["tmp/**"]
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildExperimentalEngine = true

	prov := &countingIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	err := runIndexBuild(cmd, nil)
	require.ErrorContains(t, err, "--format durable does not support build.match.excludes in this slice")
	require.Zero(t, prov.listCalls)

	latestFiles, globErr := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, globErr)
	require.Empty(t, latestFiles)
}

func TestIndexBuildFormatBothRejectsExperimentalEngineFlag(t *testing.T) {
	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildFormat = "both"
	indexBuildExperimentalEngine = true

	err := validateIndexBuildFormatFlags("")
	require.ErrorContains(t, err, "--format both is not compatible with --experimental-engine")
}

func indexBuildEngineAdapterTestConfig(t *testing.T, name string) indexbuild.Config {
	t.Helper()
	root := filepath.Join(t.TempDir(), name)
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	return indexbuild.Config{
		IndexSetID: "idx_test",
		RunID:      "run_test",
		BaseURI:    "s3://bucket/data/",
		Source: indexbuild.Source{
			Provider:     indexBuildEngineFakeProvider{objects: indexBuildEngineTestObjects(base)},
			ProviderName: "s3",
		},
		Match: indexbuild.MatchConfig{Includes: []string{"**"}},
		Paths: indexbuild.PathConfig{
			JournalDir:   filepath.Join(root, "journals"),
			SegmentDir:   filepath.Join(root, "segments"),
			ManifestPath: filepath.Join(root, "manifest.json"),
			CompletePath: filepath.Join(root, "complete.json"),
			LatestPath:   filepath.Join(root, "latest.json"),
			IndexDBDir:   filepath.Join(root, "indexes", "idx_test"),
		},
		Coverage: []indexbuild.CoverageAttestation{{
			Scope:    &indexbuild.Scope{Prefix: "data/"},
			Basis:    indexbuild.CoverageBasisConfirmed,
			Complete: true,
		}},
		RunStartedAt:         base,
		CreatedAt:            base.Add(time.Minute),
		Clock:                func() time.Time { return base.Add(2 * time.Minute) },
		TargetRowsPerSegment: 1,
	}
}

func indexBuildEngineTestObjects(base time.Time) []provider.ObjectSummary {
	return []provider.ObjectSummary{
		{Key: "data/a.xml", Size: 10, ETag: `"a"`, LastModified: base.Add(-time.Hour), StorageClass: "STANDARD"},
		{Key: "data/b.xml", Size: 11, ETag: `"b"`, LastModified: base.Add(-time.Minute), StorageClass: "STANDARD"},
	}
}

type indexBuildEngineFakeProvider struct {
	objects []provider.ObjectSummary
}

func (p indexBuildEngineFakeProvider) List(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	var out []provider.ObjectSummary
	for _, obj := range p.objects {
		if strings.HasPrefix(obj.Key, opts.Prefix) {
			out = append(out, obj)
		}
	}
	return &provider.ListResult{Objects: out}, nil
}

func (indexBuildEngineFakeProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (indexBuildEngineFakeProvider) Close() error { return nil }

type countingIndexBuildProvider struct {
	objects   []provider.ObjectSummary
	listCalls int
}

func (p *countingIndexBuildProvider) List(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	p.listCalls++
	var out []provider.ObjectSummary
	for _, obj := range p.objects {
		if strings.HasPrefix(obj.Key, opts.Prefix) {
			out = append(out, obj)
		}
	}
	return &provider.ListResult{Objects: out}, nil
}

func (*countingIndexBuildProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (*countingIndexBuildProvider) Close() error { return nil }

// discoveringIndexBuildProvider adds delimiter-based prefix discovery derived
// from the fake object set, so discovery-driven scopes compile plans that
// track the objects present at each run.
type discoveringIndexBuildProvider struct {
	countingIndexBuildProvider
}

func (p *discoveringIndexBuildProvider) ListCommonPrefixes(_ context.Context, opts provider.ListCommonPrefixesOptions) (*provider.ListCommonPrefixesResult, error) {
	delimiter := opts.Delimiter
	if delimiter == "" {
		delimiter = "/"
	}
	seen := map[string]struct{}{}
	var prefixes []string
	for _, obj := range p.objects {
		if !strings.HasPrefix(obj.Key, opts.Prefix) {
			continue
		}
		rest := strings.TrimPrefix(obj.Key, opts.Prefix)
		i := strings.Index(rest, delimiter)
		if i < 0 {
			continue
		}
		cp := opts.Prefix + rest[:i+1]
		if _, ok := seen[cp]; ok {
			continue
		}
		seen[cp] = struct{}{}
		prefixes = append(prefixes, cp)
	}
	sort.Strings(prefixes)
	return &provider.ListCommonPrefixesResult{Prefixes: prefixes}, nil
}

func withIndexBuildExperimentalEngineTestState(t *testing.T) func() {
	t.Helper()
	oldJobPath := indexBuildJobPath
	oldDBPath := indexBuildDBPath
	oldDryRun := indexBuildDryRun
	oldBackground := indexBuildBackground
	oldDedupe := indexBuildDedupe
	oldManagedJobID := indexBuildManagedJobID
	oldStorageProv := indexBuildStorageProv
	oldCloudProv := indexBuildCloudProv
	oldRegionKind := indexBuildRegionKind
	oldRegion := indexBuildRegion
	oldEndpointHost := indexBuildEndpointHost
	oldScopeWarnPrefix := indexBuildScopeWarnPrefix
	oldScopeMaxPrefix := indexBuildScopeMaxPrefix
	oldName := indexBuildName
	oldSummary := indexBuildSummary
	oldResumeRun := indexBuildResumeRun
	oldSince := indexBuildSince
	oldFormat := indexBuildFormat
	oldExperimentalEngine := indexBuildExperimentalEngine
	oldJSON := indexBuildJSON
	oldIdentityGuardHook := indexBuildAfterIdentityGuard
	t.Cleanup(func() {
		indexBuildJobPath = oldJobPath
		indexBuildDBPath = oldDBPath
		indexBuildDryRun = oldDryRun
		indexBuildBackground = oldBackground
		indexBuildDedupe = oldDedupe
		indexBuildManagedJobID = oldManagedJobID
		indexBuildStorageProv = oldStorageProv
		indexBuildCloudProv = oldCloudProv
		indexBuildRegionKind = oldRegionKind
		indexBuildRegion = oldRegion
		indexBuildEndpointHost = oldEndpointHost
		indexBuildScopeWarnPrefix = oldScopeWarnPrefix
		indexBuildScopeMaxPrefix = oldScopeMaxPrefix
		indexBuildName = oldName
		indexBuildSummary = oldSummary
		indexBuildResumeRun = oldResumeRun
		indexBuildSince = oldSince
		indexBuildFormat = oldFormat
		indexBuildExperimentalEngine = oldExperimentalEngine
		indexBuildJSON = oldJSON
		indexBuildAfterIdentityGuard = oldIdentityGuardHook
	})
	return func() {
		indexBuildJobPath = ""
		indexBuildDBPath = ""
		indexBuildDryRun = false
		indexBuildBackground = false
		indexBuildDedupe = false
		indexBuildManagedJobID = ""
		indexBuildStorageProv = ""
		indexBuildCloudProv = ""
		indexBuildRegionKind = ""
		indexBuildRegion = ""
		indexBuildEndpointHost = ""
		indexBuildScopeWarnPrefix = 10000
		indexBuildScopeMaxPrefix = 50000
		indexBuildName = ""
		indexBuildSummary = false
		indexBuildResumeRun = ""
		indexBuildSince = ""
		indexBuildFormat = "durable"
		indexBuildExperimentalEngine = false
		indexBuildJSON = false
		indexBuildAfterIdentityGuard = nil
	}
}
