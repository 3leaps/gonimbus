package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/indexbuild"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/scope"
	"github.com/3leaps/gonimbus/pkg/uri"
)

// TestMixedHistoryListSelectionTrap proves distinct scopes yield distinct sets
// and that format-aware list surfaces durable-only siblings (not SQLite-only).
func TestMixedHistoryListSelectionTrap(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC)

	objects := []provider.ObjectSummary{
		{Key: "data/site-a/2026-04-01/a.xml", Size: 10, ETag: `"a"`, LastModified: base, StorageClass: "STANDARD"},
		{Key: "data/site-a/2026-06-01/b.xml", Size: 11, ETag: `"b"`, LastModified: base.Add(time.Hour), StorageClass: "STANDARD"},
	}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return indexBuildEngineFakeProvider{objects: objects}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	manifestA := writeScopedPrefixManifest(t, []string{"site-a/2026-04-01/"})
	manifestB := writeScopedPrefixManifest(t, []string{"site-a/2026-06-01/"})

	mA, err := manifest.LoadIndexManifest(manifestA)
	require.NoError(t, err)
	mB, err := manifest.LoadIndexManifest(manifestB)
	require.NoError(t, err)
	hashA, err := scope.HashConfig(mA.Build.Scope)
	require.NoError(t, err)
	hashB, err := scope.HashConfig(mB.Build.Scope)
	require.NoError(t, err)
	require.NotEmpty(t, hashA)
	require.NotEmpty(t, hashB)
	require.NotEqual(t, hashA, hashB)

	idA := computeTestIndexSetID(t, mA, hashA)
	idB := computeTestIndexSetID(t, mB, hashB)
	require.NotEqual(t, idA, idB)

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestA
	indexBuildFormat = "both"
	cmdA := &cobra.Command{Use: "build"}
	cmdA.SetContext(context.Background())
	var outA strings.Builder
	cmdA.SetOut(&outA)
	require.NoError(t, runIndexBuild(cmdA, nil))

	restore()
	indexBuildJobPath = manifestB
	indexBuildFormat = "durable"
	indexBuildJSON = true
	cmdB := &cobra.Command{Use: "build"}
	cmdB.SetContext(context.Background())
	var outB strings.Builder
	cmdB.SetOut(&outB)
	require.NoError(t, runIndexBuild(cmdB, nil))

	var receiptB indexBuildResultRecord
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(outB.String())), &receiptB))
	require.Equal(t, indexBuildResultType, receiptB.Type)
	require.Equal(t, "success", receiptB.Status)
	require.Equal(t, "durable", receiptB.RequestedFormat)
	require.Equal(t, []string{"durable-v2"}, receiptB.FormatsCommitted)
	require.Equal(t, idB, receiptB.IndexSetID)
	require.Equal(t, hashB, receiptB.ScopeHash)
	require.NotEmpty(t, receiptB.RunID)
	require.NotEmpty(t, receiptB.ManifestSHA256)
	require.NotNil(t, receiptB.Rows)
	require.Equal(t, 1, *receiptB.Rows)

	latestB := filepath.Join(dataRoot, "cache", "segments", idB, "latest.json")
	_, err = os.Stat(latestB)
	require.NoError(t, err)
	completeB := filepath.Join(dataRoot, "cache", "segments", idB, "runs", receiptB.RunID, "complete.json")
	_, err = os.Stat(completeB)
	require.NoError(t, err)

	dbA, err := filepath.Glob(filepath.Join(dataRoot, "indexes", "*", "index.db"))
	require.NoError(t, err)
	require.Len(t, dbA, 1)
	dbB := filepath.Join(dataRoot, "indexes", "idx_"+strings.TrimPrefix(idB, "idx_")[:16], "index.db")
	_, err = os.Stat(dbB)
	require.True(t, os.IsNotExist(err), "durable-only B must not create index.db")

	// Exact-epoch cleanup retains discoverable quarantine captures that block
	// later readers until recovery. Multi-step fixtures clear temp-dir residue
	// only (not a production API).
	clearSQLiteQuarantineResidueUnderDataRoot(t, dataRoot)

	// Legacy SQLite-only discovery still sees only A (trap for list-based rediscovery).
	legacy, err := loadIndexEntriesWithPaths(context.Background())
	require.NoError(t, err)
	require.Len(t, legacy, 1, "SQLite-only discovery must still see only both/SQLite set A")
	require.Equal(t, idA, legacy[0].Info.IndexSetID)

	// Format-aware list surfaces durable-only B as well.
	opts, err := indexReaderResolveOptions()
	require.NoError(t, err)
	listed, err := indexreader.ListIndexReaders(context.Background(), opts)
	require.NoError(t, err)
	listed = preferListedIndexes(listed)
	ids := make(map[string]string)
	for _, item := range listed {
		ids[item.Meta.IndexSetID] = string(item.Meta.Format)
	}
	require.Contains(t, ids, idA)
	require.Contains(t, ids, idB, "format-aware list must surface durable-only set B")
	require.Equal(t, string(indexreader.FormatSQLiteV1), ids[idA])
	require.Equal(t, string(indexreader.FormatDurableV2), ids[idB])

	snap, err := loadLocalDurableSnapshotForExport(idB, receiptB.RunID)
	require.NoError(t, err)
	require.Equal(t, receiptB.ManifestSHA256, snap.ManifestSHA)
	require.Equal(t, idB, snap.Manifest.IndexSetID)
	require.Equal(t, receiptB.RunID, snap.Manifest.RunID)
}

func TestBuildJSONReceiptDurableAndDryRunIdentity(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	manifestPath := writeScopedPrefixManifest(t, []string{"hot/"})

	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return indexBuildEngineFakeProvider{objects: []provider.ObjectSummary{
			{Key: "data/hot/a.xml", Size: 10, ETag: `"a"`, LastModified: base, StorageClass: "STANDARD"},
		}}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	m, err := manifest.LoadIndexManifest(manifestPath)
	require.NoError(t, err)
	scopeHash, err := computeScopeHash(m)
	require.NoError(t, err)
	identity := buildEffectiveIdentity(m)
	buildFilters, err := computeIndexBuildFilters(m)
	require.NoError(t, err)
	params := buildIndexSetParams(m, identity, buildFilters.FiltersHash, scopeHash)
	identityResult, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)

	cmdPlan := &cobra.Command{Use: "build"}
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	err = showIndexBuildPlan(context.Background(), cmdPlan, m, identity, buildFilters, &indexBuildSincePlan{}, identityResult, scopeHash)
	require.NoError(t, w.Close())
	os.Stdout = oldStdout
	require.NoError(t, err)
	planBytes, readErr := io.ReadAll(r)
	_ = r.Close()
	require.NoError(t, readErr)
	planText := string(planBytes)
	require.Contains(t, planText, "index_set_id: "+identityResult.IndexSetID)
	require.Contains(t, planText, "scope_hash: "+scopeHash)
	latestBefore, _ := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.Empty(t, latestBefore)

	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"
	indexBuildJSON = true
	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	var stdout strings.Builder
	cmd.SetOut(&stdout)
	require.NoError(t, runIndexBuild(cmd, nil))

	raw := strings.TrimSpace(stdout.String())
	var rec indexBuildResultRecord
	require.NoError(t, json.Unmarshal([]byte(raw), &rec))
	require.Equal(t, indexBuildResultType, rec.Type)
	require.Equal(t, indexBuildResultVersion, rec.SchemaVersion)
	require.Equal(t, "success", rec.Status)
	require.Equal(t, identityResult.IndexSetID, rec.IndexSetID)
	require.Equal(t, scopeHash, rec.ScopeHash)
	require.NotEmpty(t, rec.RunID)
	require.NotEmpty(t, rec.ManifestSHA256)
	require.Equal(t, []string{"durable-v2"}, rec.FormatsCommitted)
	assertRawJSONHasKeys(t, raw, "rows", "active_rows", "tombstones", "segments", "objects_observed", "manifest_sha256")
	require.NotContains(t, raw, "base_uri")
	require.NotContains(t, raw, "s3://")
	require.NotContains(t, raw, "endpoint")
	require.NotContains(t, raw, "profile")
}

func TestBuildJSONReceiptEmptyDurableKeepsZeroCounts(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	manifestPath := writeScopedPrefixManifest(t, []string{"empty/"})

	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return indexBuildEngineFakeProvider{objects: nil}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"
	indexBuildJSON = true
	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	var stdout strings.Builder
	cmd.SetOut(&stdout)
	require.NoError(t, runIndexBuild(cmd, nil))

	raw := strings.TrimSpace(stdout.String())
	var rec indexBuildResultRecord
	require.NoError(t, json.Unmarshal([]byte(raw), &rec))
	require.Equal(t, "success", rec.Status)
	require.NotNil(t, rec.Rows)
	require.Equal(t, 0, *rec.Rows)
	require.NotNil(t, rec.ActiveRows)
	require.Equal(t, 0, *rec.ActiveRows)
	require.NotNil(t, rec.Tombstones)
	require.Equal(t, 0, *rec.Tombstones)
	require.NotNil(t, rec.ObjectsObserved)
	require.EqualValues(t, 0, *rec.ObjectsObserved)
	require.NotEmpty(t, rec.ManifestSHA256)
	assertRawJSONHasKeys(t, raw, "rows", "active_rows", "tombstones", "segments", "objects_observed", "manifest_sha256")
	// Zero must appear in raw JSON (not dropped by omitempty).
	require.Contains(t, raw, `"rows":0`)
	require.Contains(t, raw, `"objects_observed":0`)
	_ = dataRoot
}

func TestBuildJSONReceiptBothEmitsCompareThenReceipt(t *testing.T) {
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
	indexBuildJSON = true

	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return indexBuildEngineFakeProvider{objects: indexBuildEngineTestObjects(base)}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	var stdout strings.Builder
	cmd.SetOut(&stdout)
	require.NoError(t, runIndexBuild(cmd, nil))

	sc := bufio.NewScanner(strings.NewReader(strings.TrimSpace(stdout.String())))
	var lines []string
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) != "" {
			lines = append(lines, sc.Text())
		}
	}
	require.NoError(t, sc.Err())
	require.Len(t, lines, 2)

	var compare map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &compare))
	require.Equal(t, "gonimbus.index.compare_result.v1", compare["type"])

	var rec indexBuildResultRecord
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &rec))
	require.Equal(t, indexBuildResultType, rec.Type)
	require.Equal(t, "success", rec.Status)
	require.Equal(t, "both", rec.RequestedFormat)
	require.Equal(t, []string{"sqlite-v1", "durable-v2"}, rec.FormatsCommitted)
	require.NotEmpty(t, rec.ManifestSHA256)
	require.NotNil(t, rec.Rows)
	require.Equal(t, 2, *rec.Rows)
	require.NotNil(t, rec.ObjectsIngested)
	assertRawJSONHasKeys(t, lines[1], "rows", "objects_ingested", "objects_observed", "manifest_sha256")
}

func TestBuildJSONReceiptSQLitePartialAndFailedSemantics(t *testing.T) {
	// SQLite success + partial emit terminal records; failed emits nothing.
	var successOut bytes.Buffer
	require.NoError(t, emitCommittedIndexBuildJSON(
		&successOut, "sqlite", indexstore.RunStatusSuccess, indexbuild.Summary{}, false,
		"idx_"+strings.Repeat("a", 64), "run_1", "scopehash", 3,
	))
	rawSuccess := strings.TrimSpace(successOut.String())
	var successRec indexBuildResultRecord
	require.NoError(t, json.Unmarshal([]byte(rawSuccess), &successRec))
	require.Equal(t, "success", successRec.Status)
	require.NotNil(t, successRec.ObjectsIngested)
	require.EqualValues(t, 3, *successRec.ObjectsIngested)
	assertRawJSONHasKeys(t, rawSuccess, "objects_ingested", "formats_committed", "index_set_id", "run_id", "status")
	require.Contains(t, rawSuccess, `"objects_ingested":3`)

	var partialOut bytes.Buffer
	require.NoError(t, emitCommittedIndexBuildJSON(
		&partialOut, "sqlite", indexstore.RunStatusPartial, indexbuild.Summary{}, false,
		"idx_"+strings.Repeat("b", 64), "run_2", "scopehash", 1,
	))
	rawPartial := strings.TrimSpace(partialOut.String())
	var partialRec indexBuildResultRecord
	require.NoError(t, json.Unmarshal([]byte(rawPartial), &partialRec))
	require.Equal(t, "partial", partialRec.Status)
	require.NotNil(t, partialRec.ObjectsIngested)
	require.EqualValues(t, 1, *partialRec.ObjectsIngested)
	assertRawJSONHasKeys(t, rawPartial, "objects_ingested", "status")
	require.Contains(t, rawPartial, `"objects_ingested":1`)

	// Empty sqlite still includes zero count key.
	var emptyOut bytes.Buffer
	require.NoError(t, emitCommittedIndexBuildJSON(
		&emptyOut, "sqlite", indexstore.RunStatusSuccess, indexbuild.Summary{}, false,
		"idx_"+strings.Repeat("c", 64), "run_3", "", 0,
	))
	rawEmpty := strings.TrimSpace(emptyOut.String())
	require.Contains(t, rawEmpty, `"objects_ingested":0`)

	// Failed final status: no terminal receipt.
	var failedOut bytes.Buffer
	require.NoError(t, emitCommittedIndexBuildJSON(
		&failedOut, "sqlite", indexstore.RunStatusFailed, indexbuild.Summary{}, false,
		"idx_"+strings.Repeat("d", 64), "run_4", "", 0,
	))
	require.Empty(t, strings.TrimSpace(failedOut.String()))

	// Success without durable digest must fail closed at emit.
	bad := newDurableBuildResultRecord(indexbuild.Summary{
		IndexSetID: "idx_" + strings.Repeat("e", 64),
		RunID:      "run_5",
	}, "", "durable", []string{"durable-v2"})
	bad.ManifestSHA256 = ""
	err := emitIndexBuildResultJSON(io.Discard, bad)
	require.ErrorContains(t, err, "manifest_sha256")
}

func TestBuildJSONReceiptRejectsBackgroundDryRunResume(t *testing.T) {
	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = filepath.Join(t.TempDir(), "x.yaml")
	require.NoError(t, os.WriteFile(indexBuildJobPath, []byte("version: \"1.0\"\n"), 0o600))

	indexBuildBackground = true
	indexBuildJSON = true
	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	require.ErrorContains(t, runIndexBuild(cmd, nil), "--json is not compatible with --background")

	restore()
	indexBuildJobPath = filepath.Join(t.TempDir(), "y.yaml")
	require.NoError(t, os.WriteFile(indexBuildJobPath, []byte("version: \"1.0\"\n"), 0o600))
	indexBuildDryRun = true
	indexBuildJSON = true
	require.ErrorContains(t, runIndexBuild(cmd, nil), "--json is not compatible with --dry-run")

	restore()
	indexBuildResumeRun = "run_123"
	indexBuildJSON = true
	require.ErrorContains(t, runIndexBuild(cmd, nil), "--json is not compatible with --resume-run")
}

func TestBuildJSONReceiptFailureNoTerminalRecord(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	manifestPath := writeScopedPrefixManifest(t, []string{"hot/"})

	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return nil, context.DeadlineExceeded
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"
	indexBuildJSON = true
	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	var stdout strings.Builder
	cmd.SetOut(&stdout)
	err := runIndexBuild(cmd, nil)
	require.Error(t, err)
	require.Empty(t, strings.TrimSpace(stdout.String()), "fatal failure must not emit a build receipt")
}

func TestPinnedRunQueryIgnoresLatestAdvance(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)

	manifestPath := writeScopedPrefixManifest(t, []string{"hot/"})
	objectsB := []provider.ObjectSummary{
		{Key: "data/hot/b-only.xml", Size: 10, ETag: `"b"`, LastModified: base, StorageClass: "STANDARD"},
	}
	objectsC := []provider.ObjectSummary{
		{Key: "data/hot/c-only.xml", Size: 11, ETag: `"c"`, LastModified: base.Add(time.Hour), StorageClass: "STANDARD"},
	}
	var current []provider.ObjectSummary
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return indexBuildEngineFakeProvider{objects: current}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"
	indexBuildJSON = true

	current = objectsB
	cmdB := &cobra.Command{Use: "build"}
	cmdB.SetContext(context.Background())
	var outB strings.Builder
	cmdB.SetOut(&outB)
	require.NoError(t, runIndexBuild(cmdB, nil))
	var recB indexBuildResultRecord
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(outB.String())), &recB))

	time.Sleep(2 * time.Millisecond)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"
	indexBuildJSON = true
	current = objectsC
	cmdC := &cobra.Command{Use: "build"}
	cmdC.SetContext(context.Background())
	var outC strings.Builder
	cmdC.SetOut(&outC)
	require.NoError(t, runIndexBuild(cmdC, nil))
	var recC indexBuildResultRecord
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(outC.String())), &recC))
	require.Equal(t, recB.IndexSetID, recC.IndexSetID)
	require.NotEqual(t, recB.RunID, recC.RunID)

	latestPath := filepath.Join(dataRoot, "cache", "segments", recB.IndexSetID, "latest.json")
	snapLatest, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.Equal(t, recC.RunID, snapLatest.Manifest.RunID)

	reader, err := openIndexReader(context.Background(), "", recB.IndexSetID, recB.RunID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })
	meta := reader.Meta()
	require.Equal(t, indexreader.FormatDurableV2, meta.Format)
	require.Equal(t, recB.RunID, meta.RunID)
	results, _, err := reader.QueryObjects(context.Background(), indexstore.QueryParams{IndexSetID: recB.IndexSetID})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "hot/b-only.xml", results[0].RelKey)

	_, err = openIndexReader(context.Background(), "", recB.IndexSetID, "run_0000000000000000000")
	require.Error(t, err)

	qcmd := &cobra.Command{Use: "query"}
	qcmd.Flags().String("index-set", "", "")
	qcmd.Flags().String("run-id", "run_1", "")
	require.NoError(t, qcmd.Flags().Set("run-id", "run_1"))
	err = runIndexQuery(qcmd, nil)
	require.ErrorContains(t, err, "--run-id requires --index-set")
}

func writeScopedPrefixManifest(t *testing.T, prefixes []string) string {
	t.Helper()
	quoted := make([]string, 0, len(prefixes))
	for _, p := range prefixes {
		quoted = append(quoted, `"`+p+`"`)
	}
	path := filepath.Join(t.TempDir(), "index.yaml")
	body := `
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
    prefixes: [` + strings.Join(quoted, ", ") + `]
  match:
    includes: ["**"]
  crawl:
    concurrency: 1
    progress_every: 100
`
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func computeTestIndexSetID(t *testing.T, m *manifest.IndexManifest, scopeHash string) string {
	t.Helper()
	identity := buildEffectiveIdentity(m)
	buildFilters, err := computeIndexBuildFilters(m)
	require.NoError(t, err)
	params := buildIndexSetParams(m, identity, buildFilters.FiltersHash, scopeHash)
	result, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)
	return result.IndexSetID
}

func assertRawJSONHasKeys(t *testing.T, raw string, keys ...string) {
	t.Helper()
	var obj map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &obj))
	for _, key := range keys {
		_, ok := obj[key]
		require.True(t, ok, "raw JSON missing key %q in %s", key, raw)
	}
}
