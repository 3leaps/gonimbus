package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/indexenrich"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

func TestIndexEnrichWithHeadDurableAdvancesLatestAndPreservesRows(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	manifestPath := writeScopedPrefixManifest(t, []string{"hot/"})

	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return indexBuildEngineFakeProvider{objects: []provider.ObjectSummary{
			{Key: "data/hot/a.xml", Size: 10, ETag: `"a"`, LastModified: base, StorageClass: "STANDARD"},
			{Key: "data/hot/b.xml", Size: 20, ETag: `"b"`, LastModified: base, StorageClass: "STANDARD"},
		}}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"
	indexBuildJSON = true
	buildCmd := &cobra.Command{Use: "build"}
	buildCmd.SetContext(context.Background())
	var buildOut strings.Builder
	buildCmd.SetOut(&buildOut)
	require.NoError(t, runIndexBuild(buildCmd, nil))
	var receipt map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buildOut.String())), &receipt))
	setID, _ := receipt["index_set_id"].(string)
	parentRun, _ := receipt["run_id"].(string)
	require.NotEmpty(t, setID)
	require.NotEmpty(t, parentRun)

	// Capture parent segment bytes for immutability check.
	parentRunDir := filepath.Join(dataRoot, "cache", "segments", setID, "runs", parentRun)
	parentSegBefore, err := filepath.Glob(filepath.Join(parentRunDir, "*.parquet"))
	require.NoError(t, err)
	require.NotEmpty(t, parentSegBefore)
	parentSegBytes := map[string][]byte{}
	for _, p := range parentSegBefore {
		b, readErr := osReadFile(p)
		require.NoError(t, readErr)
		parentSegBytes[p] = b
	}
	parentSnap, err := indexsubstrate.OpenLatestPublishedSnapshot(filepath.Join(dataRoot, "cache", "segments", setID, "latest.json"))
	require.NoError(t, err)
	parentDigest := parentSnap.Complete.ManifestSHA256

	oldEnrichProv := newEnrichHeadProvider
	newEnrichHeadProvider = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return &fakeEnrichProvider{
			metas: map[string]*provider.ObjectMeta{
				"data/hot/a.xml": {
					ObjectSummary: provider.ObjectSummary{Key: "data/hot/a.xml"},
					ContentType:   "application/xml",
					ArchiveStatus: "STANDARD",
					RestoreState:  "none",
				},
			},
		}, nil
	}
	t.Cleanup(func() { newEnrichHeadProvider = oldEnrichProv })

	require.NoError(t, indexEnrichWithHeadCmd.Flags().Set("resume", "false"))
	require.NoError(t, indexEnrichWithHeadCmd.Flags().Set("parallel", "2"))
	require.NoError(t, indexEnrichWithHeadCmd.Flags().Set("pattern", "hot/a.xml"))
	require.NoError(t, indexEnrichWithHeadCmd.Flags().Set("state-out", ""))
	t.Cleanup(func() {
		_ = indexEnrichWithHeadCmd.Flags().Set("pattern", "")
	})

	var out bytes.Buffer
	indexEnrichWithHeadCmd.SetOut(&out)
	indexEnrichWithHeadCmd.SetContext(context.Background())
	require.NoError(t, runIndexEnrichWithHead(indexEnrichWithHeadCmd, []string{setID[:16]}))

	// Parse durable result receipt.
	var enrichRunID string
	var published bool
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) != nil {
			continue
		}
		if rec["type"] == "gonimbus.index.enrich_with_head.durable_result.v1" {
			published, _ = rec["published"].(bool)
			enrichRunID, _ = rec["run_id"].(string)
			require.Equal(t, parentRun, rec["parent_run_id"])
			require.Equal(t, parentDigest, rec["parent_manifest_sha256"])
			require.NoError(t, validateRunID(enrichRunID))
		}
	}
	require.True(t, published)
	require.NotEmpty(t, enrichRunID)
	require.NotEqual(t, parentRun, enrichRunID)

	// Latest advanced to enrich run with valid run id.
	latestPath := filepath.Join(dataRoot, "cache", "segments", setID, "latest.json")
	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.Equal(t, enrichRunID, snap.Complete.RunID)
	require.Equal(t, setID, snap.Complete.IndexSetID)
	require.NoError(t, validateRunID(snap.Complete.RunID))

	// Parent segments immutable.
	for p, before := range parentSegBytes {
		after, readErr := osReadFile(p)
		require.NoError(t, readErr)
		require.Equal(t, before, after, "parent segment must be immutable: %s", p)
	}

	// Both prior rows preserved; only HEAD fields change on enriched key.
	_, rows, err := indexsubstrate.ReadLatestPublishedRows(latestPath)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	byKey := map[string]indexsubstrate.CurrentObjectRow{}
	for _, row := range rows {
		byKey[row.RelKey] = row
	}
	require.Contains(t, byKey, "hot/a.xml")
	require.Contains(t, byKey, "hot/b.xml")
	require.NotNil(t, byKey["hot/a.xml"].HeadEnrichedAt)
	require.NotNil(t, byKey["hot/a.xml"].ContentType)
	require.Equal(t, "application/xml", *byKey["hot/a.xml"].ContentType)
	require.Equal(t, int64(10), byKey["hot/a.xml"].SizeBytes)
	require.Equal(t, `"a"`, byKey["hot/a.xml"].ETag)
	require.Nil(t, byKey["hot/b.xml"].HeadEnrichedAt)
	require.Nil(t, byKey["hot/b.xml"].DeletedAt)
	require.Equal(t, int64(20), byKey["hot/b.xml"].SizeBytes)

	// Query --enriched-after sees enriched object; pinned query accepts enrich run id.
	reader, err := openIndexReader(context.Background(), "", setID, enrichRunID)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	require.Equal(t, indexreader.FormatDurableV2, reader.Meta().Format)
	require.Equal(t, enrichRunID, reader.Meta().RunID)
	enrichedAfter := byKey["hot/a.xml"].HeadEnrichedAt.Add(-time.Second)
	results, _, err := reader.QueryObjects(context.Background(), indexstore.QueryParams{
		IndexSetID:    setID,
		EnrichedAfter: enrichedAfter,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "hot/a.xml", results[0].RelKey)
}

func TestIndexEnrichWithHeadDurableResumeRunRejectedBeforeSQLite(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
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
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"
	indexBuildJSON = true
	require.NoError(t, runIndexBuild(&cobra.Command{Use: "build"}, nil))

	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
	setID := filepath.Base(filepath.Dir(latestFiles[0]))

	require.NoError(t, indexEnrichWithHeadCmd.Flags().Set("resume-run", "run_999"))
	t.Cleanup(func() { _ = indexEnrichWithHeadCmd.Flags().Set("resume-run", "") })
	err = runIndexEnrichWithHead(indexEnrichWithHeadCmd, []string{setID[:16]})
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not support --resume-run")
	require.NotContains(t, err.Error(), "operation checkpoint")
}

func TestIndexEnrichWithHeadDurableStaleParentCAS(t *testing.T) {
	// Stale parent CAS is owned by substrate publish; covered there and by
	// indexenrich integration through the normal enrich path after a concurrent
	// advance. This smoke check proves lease exclusivity blocks a second enrich.
	dir := t.TempDir()
	first, err := indexsubstrate.AcquireWriteLease(dir, "idx_cas", "holder-a", 0)
	require.NoError(t, err)
	_, err = indexsubstrate.AcquireWriteLease(dir, "idx_cas", "holder-b", 0)
	require.ErrorIs(t, err, indexsubstrate.ErrWriteLeaseHeld)
	require.NoError(t, first.Release())
}

func TestDurableWriteLeaseIsExclusive(t *testing.T) {
	dir := t.TempDir()
	first, err := indexsubstrate.AcquireWriteLease(dir, "idx_test", "holder-a", time.Hour)
	require.NoError(t, err)
	_, err = indexsubstrate.AcquireWriteLease(dir, "idx_test", "holder-b", time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrWriteLeaseHeld)
	require.NoError(t, first.Release())
	second, err := indexsubstrate.AcquireWriteLease(dir, "idx_test", "holder-b", time.Hour)
	require.NoError(t, err)
	require.NoError(t, second.Release())
}

func TestIndexEnrichWithHeadDurableLeaseHeldDoesNotEmitEmptySet(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
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
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"
	indexBuildJSON = true
	require.NoError(t, runIndexBuild(&cobra.Command{Use: "build"}, nil))

	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
	setID := filepath.Base(filepath.Dir(latestFiles[0]))
	segRoot := filepath.Dir(latestFiles[0])
	held, err := indexsubstrate.AcquireWriteLease(segRoot, setID, "peer", 0)
	require.NoError(t, err)
	defer func() { _ = held.Release() }()

	oldEnrichProv := newEnrichHeadProvider
	newEnrichHeadProvider = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return &fakeEnrichProvider{}, nil
	}
	t.Cleanup(func() { newEnrichHeadProvider = oldEnrichProv })

	require.NoError(t, indexEnrichWithHeadCmd.Flags().Set("resume", "false"))
	require.NoError(t, indexEnrichWithHeadCmd.Flags().Set("parallel", "1"))
	require.NoError(t, indexEnrichWithHeadCmd.Flags().Set("pattern", ""))
	var out bytes.Buffer
	indexEnrichWithHeadCmd.SetOut(&out)
	indexEnrichWithHeadCmd.SetContext(context.Background())
	err = runIndexEnrichWithHead(indexEnrichWithHeadCmd, []string{setID[:16]})
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrWriteLeaseHeld)
	// Structured records may include set ID (engine base result) but must not claim success.
	if out.Len() > 0 {
		require.NotContains(t, out.String(), `"published":true`)
		require.Contains(t, out.String(), setID)
	}
}

type failAfterNWriter struct {
	n, failAfter int
	buf          bytes.Buffer
}

func (w *failAfterNWriter) Write(p []byte) (int, error) {
	w.n++
	if w.n > w.failAfter {
		return 0, fmt.Errorf("broken pipe")
	}
	return w.buf.Write(p)
}

func TestIndexEnrichWithHeadDurableStdoutFailureAfterCommitSurfacesIdentity(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
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
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"
	indexBuildJSON = true
	require.NoError(t, runIndexBuild(&cobra.Command{Use: "build"}, nil))

	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
	setID := filepath.Base(filepath.Dir(latestFiles[0]))
	parentRun := mustDurableLatestRun(t, latestFiles[0])

	oldEnrichProv := newEnrichHeadProvider
	newEnrichHeadProvider = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return &fakeEnrichProvider{
			metas: map[string]*provider.ObjectMeta{
				"data/hot/a.xml": {ObjectSummary: provider.ObjectSummary{Key: "data/hot/a.xml"}, ContentType: "application/xml"},
			},
		}, nil
	}
	t.Cleanup(func() { newEnrichHeadProvider = oldEnrichProv })

	require.NoError(t, indexEnrichWithHeadCmd.Flags().Set("resume", "false"))
	require.NoError(t, indexEnrichWithHeadCmd.Flags().Set("parallel", "1"))
	require.NoError(t, indexEnrichWithHeadCmd.Flags().Set("pattern", ""))
	// Fail after summary line so durable_result encode hits broken pipe post-commit.
	failOut := &failAfterNWriter{failAfter: 1}
	indexEnrichWithHeadCmd.SetOut(failOut)
	indexEnrichWithHeadCmd.SetContext(context.Background())
	runErr := runIndexEnrichWithHead(indexEnrichWithHeadCmd, []string{setID[:16]})
	require.Error(t, runErr)
	require.Contains(t, runErr.Error(), "latest_advanced=true")
	require.Contains(t, runErr.Error(), setID)
	// Authoritative latest advanced despite stdout failure; error identity must match child.
	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestFiles[0])
	require.NoError(t, err)
	require.NotEqual(t, parentRun, snap.Complete.RunID)
	require.Contains(t, runErr.Error(), "run_id="+snap.Complete.RunID)
	require.Contains(t, runErr.Error(), "manifest_sha256="+snap.Complete.ManifestSHA256)
}

func TestWriteEnrichHeadStateEventExactV1Shape(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "state-*.jsonl")
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	ct := "application/xml"
	enriched := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	require.NoError(t, writeEnrichHeadStateEvent(f, indexenrich.StateEvent{
		IndexSetID:     "idx_test",
		RelKey:         "hot/a.xml",
		FullKey:        "data/hot/a.xml",
		Status:         "success",
		Attempts:       1,
		ContentType:    &ct,
		HeadEnrichedAt: &enriched,
		EventTime:      enriched,
	}))
	require.NoError(t, writeEnrichHeadStateEvent(f, indexenrich.StateEvent{
		IndexSetID:   "idx_test",
		RelKey:       "hot/b.xml",
		FullKey:      "data/hot/b.xml",
		Status:       "failed",
		Attempts:     2,
		ErrorCode:    "access_denied",
		ErrorMessage: "access denied",
		EventTime:    enriched,
	}))
	require.NoError(t, f.Close())

	raw, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	require.Len(t, lines, 2)

	var ok enrichHeadStateRecord
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &ok))
	require.Equal(t, "gonimbus.index.enrich_with_head.state.v1", ok.Type)
	require.Equal(t, enriched.Format(time.RFC3339), ok.TS)
	require.Equal(t, "idx_test", ok.Data.IndexSetID)
	require.Equal(t, "hot/a.xml", ok.Data.RelKey)
	require.Equal(t, "data/hot/a.xml", ok.Data.Key)
	require.Equal(t, "success", ok.Data.Status)
	require.Equal(t, 1, ok.Data.Attempts)
	require.NotNil(t, ok.Data.ContentType)
	require.Equal(t, "application/xml", *ok.Data.ContentType)
	require.NotNil(t, ok.Data.HeadEnrichedAt)

	var fail enrichHeadStateRecord
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &fail))
	require.Equal(t, "gonimbus.index.enrich_with_head.state.v1", fail.Type)
	require.Equal(t, "failed", fail.Data.Status)
	require.Equal(t, "access_denied", fail.Data.ErrorCode)
	require.Equal(t, "access denied", fail.Data.Error)
	require.Equal(t, 2, fail.Data.Attempts)
}

func mustDurableLatestRun(t *testing.T, latestPath string) string {
	t.Helper()
	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	return snap.Complete.RunID
}

func TestIndexEnrichWithHeadDurableObjectFailureDoesNotAdvanceLatest(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	manifestPath := writeScopedPrefixManifest(t, []string{"hot/"})

	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return indexBuildEngineFakeProvider{objects: []provider.ObjectSummary{
			{Key: "data/hot/a.xml", Size: 10, ETag: `"a"`, LastModified: base, StorageClass: "STANDARD"},
			{Key: "data/hot/b.xml", Size: 20, ETag: `"b"`, LastModified: base, StorageClass: "STANDARD"},
		}}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"
	indexBuildJSON = true
	require.NoError(t, runIndexBuild(&cobra.Command{Use: "build"}, nil))

	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
	setID := filepath.Base(filepath.Dir(latestFiles[0]))
	before, err := osReadFile(latestFiles[0])
	require.NoError(t, err)

	oldEnrichProv := newEnrichHeadProvider
	newEnrichHeadProvider = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return &fakeEnrichProvider{
			metas: map[string]*provider.ObjectMeta{
				"data/hot/a.xml": {
					ObjectSummary: provider.ObjectSummary{Key: "data/hot/a.xml"},
					ContentType:   "application/xml",
				},
			},
			errs: map[string]error{
				"data/hot/b.xml": provider.ErrAccessDenied,
			},
		}, nil
	}
	t.Cleanup(func() { newEnrichHeadProvider = oldEnrichProv })

	require.NoError(t, indexEnrichWithHeadCmd.Flags().Set("resume", "false"))
	require.NoError(t, indexEnrichWithHeadCmd.Flags().Set("parallel", "2"))
	require.NoError(t, indexEnrichWithHeadCmd.Flags().Set("pattern", ""))
	var out bytes.Buffer
	indexEnrichWithHeadCmd.SetOut(&out)
	indexEnrichWithHeadCmd.SetContext(context.Background())
	err = runIndexEnrichWithHead(indexEnrichWithHeadCmd, []string{setID[:16]})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failure")
	after, err := osReadFile(latestFiles[0])
	require.NoError(t, err)
	require.Equal(t, before, after, "latest must not advance on object failure")
	require.Contains(t, out.String(), `"published":false`)
}

// osReadFile avoids importing os name clashes in helpers.
func osReadFile(path string) ([]byte, error) {
	return readFileForTest(path)
}

func readFileForTest(path string) ([]byte, error) {
	return os.ReadFile(path) // #nosec G304 -- test helper
}
