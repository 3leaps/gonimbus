package cmd

import (
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

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

func TestIndexListIncludesDurableOnly(t *testing.T) {
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
	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	var buildOut strings.Builder
	cmd.SetOut(&buildOut)
	require.NoError(t, runIndexBuild(cmd, nil))

	// No index.db
	dbs, err := filepath.Glob(filepath.Join(dataRoot, "indexes", "*", "index.db"))
	require.NoError(t, err)
	require.Empty(t, dbs)

	listCmd := &cobra.Command{Use: "list"}
	listCmd.Flags().Bool("json", false, "")
	require.NoError(t, listCmd.Flags().Set("json", "true"))
	listCmd.SetContext(context.Background())
	stdout := captureStdout(t, func() {
		require.NoError(t, runIndexList(listCmd, nil))
	})
	var entries []map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &entries))
	require.Len(t, entries, 1)
	require.Equal(t, "durable-v2", entries[0]["format"])
	require.EqualValues(t, float64(1), entries[0]["object_count"])
	require.NotEmpty(t, entries[0]["index_set_id"])
	require.NotEmpty(t, entries[0]["latest_run_id"])
}

func TestDurableBuildRetainsCanonicalAbsenceProofThroughIdentityPublication(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	manifestPath := writeScopedPrefixManifest(t, []string{"hot/"})
	providerCalls := 0
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		providerCalls++
		return indexBuildEngineFakeProvider{}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })
	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"
	var interposedDB string
	indexBuildAfterIdentityGuard = func(path string) error {
		interposedDB = path
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return err
		}
		return os.WriteFile(path, []byte("appeared after validation\n"), 0o640)
	}
	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	cmd.SetOut(&strings.Builder{})
	err := runIndexBuild(cmd, nil)
	require.ErrorIs(t, err, indexreader.ErrCanonicalSQLiteAdoption)
	require.Zero(t, providerCalls, "identity interposition must fail before provider construction")
	require.NotEmpty(t, interposedDB)
	require.Equal(t, []byte("appeared after validation\n"), mustReadFile(t, interposedDB))
	require.NoFileExists(t, filepath.Join(filepath.Dir(interposedDB), "identity.json"))
	require.NoFileExists(t, filepath.Join(filepath.Dir(interposedDB), "manifest.json"))
	require.NoFileExists(t, interposedDB+"-wal")
	require.NoFileExists(t, interposedDB+"-shm")
	aliases, globErr := filepath.Glob(filepath.Join(filepath.Dir(interposedDB), ".gonimbus-index-write-*.db*"))
	require.NoError(t, globErr)
	require.Empty(t, aliases)
}

func TestDurableBuildRetainsExistingCanonicalProofThroughIdentityPublication(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	manifestPath := writeScopedPrefixManifest(t, []string{"hot/"})
	providerCalls := 0
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		providerCalls++
		return indexBuildEngineFakeProvider{objects: []provider.ObjectSummary{{
			Key: "data/hot/a.xml", Size: 1, ETag: `"a"`,
			LastModified: time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC), StorageClass: "STANDARD",
		}}}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })
	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"
	runBuild := func() error {
		cmd := &cobra.Command{Use: "build"}
		cmd.SetContext(context.Background())
		cmd.SetOut(&strings.Builder{})
		return runIndexBuild(cmd, nil)
	}
	require.NoError(t, runBuild())
	dbs, err := filepath.Glob(filepath.Join(dataRoot, "indexes", "idx_*", "index.db"))
	require.NoError(t, err)
	require.Len(t, dbs, 1)
	dbPath := dbs[0]
	identityPath := filepath.Join(filepath.Dir(dbPath), "identity.json")
	markerBefore, err := os.ReadFile(identityPath)
	require.NoError(t, err)
	markerInfoBefore, err := os.Stat(identityPath)
	require.NoError(t, err)

	providerCalls = 0
	indexBuildFormat = "durable"
	validatedPath := filepath.Join(t.TempDir(), "validated.db")
	indexBuildAfterIdentityGuard = func(path string) error {
		if err := os.Rename(path, validatedPath); err != nil {
			// Native Windows denies replacement while the retained proof is open.
			return err
		}
		return os.WriteFile(path, []byte("unvalidated replacement\n"), 0o640)
	}
	err = runBuild()
	require.Error(t, err)
	require.Zero(t, providerCalls, "replacement must fail before provider construction")
	require.Equal(t, markerBefore, mustReadFile(t, identityPath))
	markerInfoAfter, statErr := os.Stat(identityPath)
	require.NoError(t, statErr)
	require.Equal(t, markerInfoBefore.Mode(), markerInfoAfter.Mode())
	require.Equal(t, markerInfoBefore.ModTime(), markerInfoAfter.ModTime())
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")
}

func TestIndexListSurfacesUntrustedSQLiteIdentityWithoutDBMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status string
		mutate func(t *testing.T, path string)
	}{
		{name: "missing", status: "missing", mutate: func(t *testing.T, path string) {
			require.NoError(t, os.Remove(path))
		}},
		{name: "invalid", status: "invalid", mutate: func(t *testing.T, path string) {
			require.NoError(t, os.WriteFile(path, []byte("{invalid\n"), 0o600))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetAppDataRootTestState(t)
			dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
			t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
			indexesRoot := filepath.Join(dataRoot, "indexes")
			require.NoError(t, os.MkdirAll(indexesRoot, 0o700))
			identity := createTestIndex(t, indexesRoot, "s3://test-bucket/legacy/")
			identityDir := filepath.Join(indexesRoot, identity.DirName)
			require.NoError(t, writeIndexIdentityFile(identityDir, identity))
			dbPath := filepath.Join(identityDir, "index.db")
			tc.mutate(t, filepath.Join(identityDir, "identity.json"))
			before, err := os.ReadFile(dbPath)
			require.NoError(t, err)
			beforeInfo, err := os.Stat(dbPath)
			require.NoError(t, err)

			newListCommand := func(jsonOutput bool) *cobra.Command {
				cmd := &cobra.Command{Use: "list"}
				cmd.Flags().Bool("json", jsonOutput, "")
				cmd.SetContext(context.Background())
				return cmd
			}
			var textOut string
			textErr := captureStderr(t, func() {
				textOut = captureStdout(t, func() {
					require.NoError(t, runIndexList(newListCommand(false), nil))
				})
			})
			require.NotContains(t, textErr, "No indexes found")
			require.Contains(t, textOut, "sqlite-v1")
			require.Contains(t, textOut, tc.status)
			lines := strings.Split(strings.TrimSpace(textOut), "\n")
			require.Len(t, lines, 2)
			fields := strings.Fields(lines[1])
			require.GreaterOrEqual(t, len(fields), 12)
			require.Equal(t, []string{"-", "-", "-"}, fields[4:7], "untrusted objects/size/runs must render as unknown")

			var jsonOut string
			jsonErr := captureStderr(t, func() {
				jsonOut = captureStdout(t, func() {
					require.NoError(t, runIndexList(newListCommand(true), nil))
				})
			})
			require.NotContains(t, jsonErr, "No indexes found")
			var entries []map[string]any
			require.NoError(t, json.Unmarshal([]byte(jsonOut), &entries))
			require.Len(t, entries, 1)
			require.Equal(t, "sqlite-v1", entries[0]["format"])
			require.Equal(t, tc.status, entries[0]["identity_status"])
			require.NotEmpty(t, entries[0]["identity_diagnostic"])
			require.Equal(t, false, entries[0]["metadata_trusted"])
			for _, key := range []string{"created_at", "object_count", "total_size_bytes", "run_count"} {
				_, present := entries[0][key]
				require.False(t, present, "untrusted JSON must omit %s", key)
			}

			after, err := os.ReadFile(dbPath)
			require.NoError(t, err)
			afterInfo, err := os.Stat(dbPath)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, beforeInfo.Mode(), afterInfo.Mode())
			require.Equal(t, beforeInfo.ModTime(), afterInfo.ModTime())
			require.NoFileExists(t, dbPath+"-wal")
			require.NoFileExists(t, dbPath+"-shm")
			require.NoFileExists(t, dbPath+"-journal")
		})
	}
}

func TestIndexListSurfacesBothSQLiteIdentityMismatchClasses(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, indexesRoot string) string
	}{
		{name: "marker versus directory", setup: func(t *testing.T, indexesRoot string) string {
			identity := createTestIndex(t, indexesRoot, "s3://test-bucket/directory-mismatch/")
			sourceDir := filepath.Join(indexesRoot, identity.DirName)
			require.NoError(t, writeIndexIdentityFile(sourceDir, identity))
			targetDir := filepath.Join(indexesRoot, "idx_0000000000000000")
			require.NotEqual(t, sourceDir, targetDir)
			require.NoError(t, os.Rename(sourceDir, targetDir))
			return filepath.Join(targetDir, "index.db")
		}},
		{name: "marker versus database", setup: func(t *testing.T, indexesRoot string) string {
			dbIdentity := createTestIndex(t, indexesRoot, "s3://test-bucket/database-a/")
			markerIdentity := createTestIndex(t, t.TempDir(), "s3://test-bucket/database-b/")
			sourceDir := filepath.Join(indexesRoot, dbIdentity.DirName)
			targetDir := filepath.Join(indexesRoot, markerIdentity.DirName)
			require.NoError(t, os.Rename(sourceDir, targetDir))
			require.NoError(t, writeIndexIdentityFile(targetDir, markerIdentity))
			return filepath.Join(targetDir, "index.db")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetAppDataRootTestState(t)
			dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
			t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
			indexesRoot := filepath.Join(dataRoot, "indexes")
			require.NoError(t, os.MkdirAll(indexesRoot, 0o700))
			dbPath := tc.setup(t, indexesRoot)
			assertReportOnlySQLiteList(t, dbPath, "mismatch")
		})
	}
}

func assertReportOnlySQLiteList(t *testing.T, dbPath, wantStatus string) {
	t.Helper()
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	beforeInfo, err := os.Stat(dbPath)
	require.NoError(t, err)
	newListCommand := func(jsonOutput bool) *cobra.Command {
		cmd := &cobra.Command{Use: "list"}
		cmd.Flags().Bool("json", jsonOutput, "")
		cmd.SetContext(context.Background())
		return cmd
	}
	textOut := captureStdout(t, func() {
		require.NoError(t, runIndexList(newListCommand(false), nil))
	})
	lines := strings.Split(strings.TrimSpace(textOut), "\n")
	require.Len(t, lines, 2)
	fields := strings.Fields(lines[1])
	require.GreaterOrEqual(t, len(fields), 12)
	require.Equal(t, []string{"-", "-", "-"}, fields[4:7])
	require.Equal(t, wantStatus, fields[11])

	jsonOut := captureStdout(t, func() {
		require.NoError(t, runIndexList(newListCommand(true), nil))
	})
	var entries []map[string]any
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &entries))
	require.Len(t, entries, 1)
	require.Equal(t, wantStatus, entries[0]["identity_status"])
	require.Equal(t, false, entries[0]["metadata_trusted"])
	for _, key := range []string{"index_set_id", "created_at", "object_count", "total_size_bytes", "run_count"} {
		_, present := entries[0][key]
		require.False(t, present, "report-only JSON must omit %s", key)
	}
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	afterInfo, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, beforeInfo.Mode(), afterInfo.Mode())
	require.Equal(t, beforeInfo.ModTime(), afterInfo.ModTime())
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")
	require.NoFileExists(t, dbPath+"-journal")
}

func TestSQLiteRebuildPublishesTrustedIdentityVisibleToList(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	manifestPath := writeScopedPrefixManifest(t, []string{"hot/"})

	providerCalls := 0
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		providerCalls++
		return indexBuildEngineFakeProvider{objects: []provider.ObjectSummary{
			{Key: "data/hot/rebuilt.xml", Size: 10, ETag: `"rebuilt"`, LastModified: base, StorageClass: "STANDARD"},
		}}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })
	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	// The dual-format rebuild uses the injected engine source while still
	// publishing the canonical SQLite compatibility artifact and identity.
	indexBuildFormat = "both"
	indexBuildJSON = true
	runBuild := func() error {
		cmd := &cobra.Command{Use: "build"}
		cmd.SetContext(context.Background())
		cmd.SetOut(&strings.Builder{})
		return runIndexBuild(cmd, nil)
	}
	require.NoError(t, runBuild())

	identities, err := filepath.Glob(filepath.Join(dataRoot, "indexes", "idx_*", "identity.json"))
	require.NoError(t, err)
	require.Len(t, identities, 1, "a rebuild must publish canonical identity.json")
	dbs, err := filepath.Glob(filepath.Join(dataRoot, "indexes", "idx_*", "index.db"))
	require.NoError(t, err)
	require.Len(t, dbs, 1)
	identityDir := filepath.Dir(dbs[0])
	identityDoc, err := indexreader.ReadLocalIdentityFile(identities[0], int64(maxHubMarkerBytes))
	require.NoError(t, err)
	setID := identityDoc.IndexSetID
	require.NoError(t, os.Remove(identities[0]))
	before, err := os.ReadFile(dbs[0])
	require.NoError(t, err)
	beforeInfo, err := os.Stat(dbs[0])
	require.NoError(t, err)
	segmentRoot := filepath.Join(dataRoot, "cache", "segments", setID)
	journalRoot := filepath.Join(dataRoot, "journals", "crawl", setID)
	require.NoError(t, os.MkdirAll(journalRoot, 0o700))
	identityBefore := snapshotIndexGCTreeState(t, identityDir)
	segmentBefore := snapshotIndexGCTreeState(t, segmentRoot)
	journalBefore := snapshotIndexGCTreeState(t, journalRoot)

	for _, refusal := range []struct {
		name     string
		format   string
		explicit bool
	}{
		{name: "default durable", format: "durable"},
		{name: "explicit canonical sqlite", format: "sqlite", explicit: true},
		{name: "explicit canonical both", format: "both", explicit: true},
	} {
		t.Run(refusal.name, func(t *testing.T) {
			indexBuildFormat = refusal.format
			indexBuildDBPath = ""
			if refusal.explicit {
				indexBuildDBPath = dbs[0]
			}
			providerCalls = 0
			err = runBuild()
			require.ErrorIs(t, err, indexreader.ErrCanonicalSQLiteAdoption)
			require.Zero(t, providerCalls, "refusal must happen before provider construction")
			afterRefusal, readErr := os.ReadFile(dbs[0])
			require.NoError(t, readErr)
			afterRefusalInfo, statErr := os.Stat(dbs[0])
			require.NoError(t, statErr)
			require.Equal(t, before, afterRefusal)
			require.Equal(t, beforeInfo.Mode(), afterRefusalInfo.Mode())
			require.Equal(t, beforeInfo.ModTime(), afterRefusalInfo.ModTime())
			require.Equal(t, identityBefore, snapshotIndexGCTreeState(t, identityDir))
			require.Equal(t, segmentBefore, snapshotIndexGCTreeState(t, segmentRoot))
			require.Equal(t, journalBefore, snapshotIndexGCTreeState(t, journalRoot))
			require.NoFileExists(t, filepath.Join(identityDir, "identity.json"))
			require.NoFileExists(t, dbs[0]+"-wal")
			require.NoFileExists(t, dbs[0]+"-shm")
			require.NoFileExists(t, dbs[0]+"-journal")
			aliases, globErr := filepath.Glob(filepath.Join(identityDir, ".gonimbus-index-write-*.db*"))
			require.NoError(t, globErr)
			require.Empty(t, aliases)
		})
	}

	backupDir := filepath.Join(t.TempDir(), filepath.Base(identityDir))
	require.NoError(t, os.Rename(identityDir, backupDir))
	require.FileExists(t, filepath.Join(backupDir, "index.db"))
	backupBytes, err := os.ReadFile(filepath.Join(backupDir, "index.db"))
	require.NoError(t, err)
	require.Equal(t, before, backupBytes)
	indexBuildFormat = "both"
	indexBuildDBPath = ""
	require.NoError(t, runBuild())
	require.Greater(t, providerCalls, 0)
	identities, err = filepath.Glob(filepath.Join(dataRoot, "indexes", "idx_*", "identity.json"))
	require.NoError(t, err)
	require.Len(t, identities, 1)
	dbs, err = filepath.Glob(filepath.Join(dataRoot, "indexes", "idx_*", "index.db"))
	require.NoError(t, err)
	require.Len(t, dbs, 1)
	require.Equal(t, before, backupBytes, "fresh rebuild must not mutate the preserved legacy backup")

	// Exact-epoch cleanup retains discoverable quarantine captures that block
	// later readers until recovery. Multi-step fixtures clear temp-dir residue
	// only (not a production API).
	clearSQLiteQuarantineResidueUnderDataRoot(t, dataRoot)

	listCmd := &cobra.Command{Use: "list"}
	listCmd.Flags().Bool("json", true, "")
	listCmd.SetContext(context.Background())
	stdout := captureStdout(t, func() {
		require.NoError(t, runIndexList(listCmd, nil))
	})
	var entries []map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &entries))
	require.Len(t, entries, 1)
	require.Equal(t, "sqlite-v1", entries[0]["format"])
	require.Equal(t, "ok", entries[0]["identity_status"])
	require.Equal(t, true, entries[0]["metadata_trusted"])
	require.NotEmpty(t, entries[0]["index_set_id"])
}

func TestIndexBuildExplicitExternalSQLiteRetainsCallerOwnedBehavior(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	manifestPath := writeScopedPrefixManifest(t, []string{"hot/"})
	externalDir := t.TempDir()
	externalDB := filepath.Join(externalDir, "caller-owned.db")

	providerCalls := 0
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		providerCalls++
		return indexBuildEngineFakeProvider{objects: []provider.ObjectSummary{
			{Key: "data/hot/external.xml", Size: 10, ETag: `"external"`, LastModified: time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)},
		}}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })
	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"
	indexBuildDBPath = externalDB

	runBuild := func() error {
		cmd := &cobra.Command{Use: "build"}
		cmd.SetContext(context.Background())
		return runIndexBuild(cmd, nil)
	}
	require.NoError(t, runBuild())
	require.FileExists(t, externalDB)
	require.NoFileExists(t, filepath.Join(externalDir, "identity.json"))
	firstCalls := providerCalls
	require.Positive(t, firstCalls)
	require.NoError(t, runBuild(), "an existing external DB remains caller-owned and does not require canonical identity.json")
	require.Greater(t, providerCalls, firstCalls)
	require.NoFileExists(t, filepath.Join(externalDir, "identity.json"))
}

func TestIndexStatsDurableAndPrefixesRejected(t *testing.T) {
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
	require.NoError(t, runIndexBuild(&cobra.Command{Use: "build"}, nil))

	statsCmd := &cobra.Command{Use: "stats"}
	statsCmd.Flags().Bool("json", false, "")
	statsCmd.Flags().Bool("prefixes", false, "")
	statsCmd.Flags().Bool("runs", false, "")
	require.NoError(t, statsCmd.Flags().Set("json", "true"))
	statsCmd.SetContext(context.Background())
	stdout := captureStdout(t, func() {
		require.NoError(t, runIndexStats(statsCmd, []string{"s3://bucket/data/"}))
	})
	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &doc))
	require.Equal(t, "durable-v2", doc["format"])
	objects := doc["objects"].(map[string]any)
	require.EqualValues(t, float64(2), objects["active"])
	require.Equal(t, "segment_file_bytes", objects["size_semantics"])
	// Publication times only — no SQLite started_at coercion.
	require.Contains(t, doc["time_semantics"], "publication")
	pub := doc["published_runs"].(map[string]any)
	latest := pub["latest"].(map[string]any)
	_, hasStarted := latest["started_at"]
	require.False(t, hasStarted, "durable stats must not emit started_at")
	_, hasPublished := latest["published_at"]
	require.True(t, hasPublished || latest["run_id"] != "", "durable latest must carry run id")

	require.NoError(t, statsCmd.Flags().Set("prefixes", "true"))
	err := runIndexStats(statsCmd, []string{"s3://bucket/data/"})
	require.ErrorContains(t, err, "--prefixes is not supported on durable-v2")
}

func TestIndexDoctorDurableOnly(t *testing.T) {
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
	cmd := &cobra.Command{Use: "build"}
	var out strings.Builder
	cmd.SetOut(&out)
	require.NoError(t, runIndexBuild(cmd, nil))
	var receipt map[string]any
	require.NoError(t, json.Unmarshal([]byte(out.String()), &receipt))
	setID, _ := receipt["index_set_id"].(string)
	require.NotEmpty(t, setID)

	stdout, stderr, err := executeIndexDoctorCommand(t, "--stats", "--json", setID[:12])
	require.NoError(t, err, stderr)
	require.Empty(t, stderr)
	var entries []map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &entries))
	require.Len(t, entries, 1)
	require.Equal(t, "durable-v2", entries[0]["format"])
	require.Equal(t, true, entries[0]["durable_marker_ok"])
	require.EqualValues(t, float64(1), entries[0]["active_object_count"])
	require.Equal(t, true, entries[0]["identity_ok"])
}

func TestPreferListedIndexesPrefersSQLite(t *testing.T) {
	listed := []indexreader.ListedIndex{
		{Meta: indexreader.Meta{IndexSetID: "idx_a", Format: indexreader.FormatDurableV2, SourcePath: "/d/latest.json"}},
		{Meta: indexreader.Meta{IndexSetID: "idx_a", Format: indexreader.FormatSQLiteV1, SourcePath: "/s/index.db"}},
		{Meta: indexreader.Meta{IndexSetID: "idx_b", Format: indexreader.FormatDurableV2, SourcePath: "/d2/latest.json"}},
	}
	got := preferListedIndexes(listed)
	require.Len(t, got, 2)
	byID := map[string]indexreader.Format{}
	for _, g := range got {
		byID[g.Meta.IndexSetID] = g.Meta.Format
	}
	require.Equal(t, indexreader.FormatSQLiteV1, byID["idx_a"])
	require.Equal(t, indexreader.FormatDurableV2, byID["idx_b"])
}

// Doctor must inspect durable even when sqlite also exists for the same set.
func TestIndexDoctorReportsCorruptedDurableAlongsideSQLite(t *testing.T) {
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
  match:
    includes: ["**"]
  crawl:
    concurrency: 1
    progress_every: 100
`), 0o600))

	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return indexBuildEngineFakeProvider{objects: []provider.ObjectSummary{
			{Key: "data/a.xml", Size: 10, ETag: `"a"`, LastModified: base, StorageClass: "STANDARD"},
		}}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"
	require.NoError(t, runIndexBuild(&cobra.Command{Use: "build"}, nil))

	// Corrupt durable latest pointer type so trust fails while sqlite remains healthy.
	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
	require.NoError(t, os.WriteFile(latestFiles[0], []byte(`{"type":"","index_set_id":"x","run_id":"run_1","complete_path":"/nope"}`), 0o600))

	stdout, stderr, err := executeIndexDoctorCommand(t, "--json")
	require.NoError(t, err, stderr)
	var entries []map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &entries))
	formats := map[string]bool{}
	var durable map[string]any
	for _, e := range entries {
		f, _ := e["format"].(string)
		formats[f] = true
		if f == "durable-v2" {
			durable = e
		}
	}
	require.True(t, formats["sqlite-v1"], "doctor must still report sqlite for format-both")
	require.True(t, formats["durable-v2"], "doctor must not suppress durable when sqlite exists")
	require.NotNil(t, durable)
	require.Equal(t, false, durable["durable_marker_ok"])
	notes, _ := durable["notes"].([]any)
	require.NotEmpty(t, notes)
}

// Broken durable-only markers must still appear as structured unhealthy entries.
func TestIndexDoctorReportsBrokenDurableOnlyMarkers(t *testing.T) {
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
	cmd := &cobra.Command{Use: "build"}
	var out strings.Builder
	cmd.SetOut(&out)
	require.NoError(t, runIndexBuild(cmd, nil))
	var receipt map[string]any
	require.NoError(t, json.Unmarshal([]byte(out.String()), &receipt))
	setID, _ := receipt["index_set_id"].(string)
	require.NotEmpty(t, setID)

	// Malformed latest type.
	latestPath := filepath.Join(dataRoot, "cache", "segments", setID, "latest.json")
	require.NoError(t, os.WriteFile(latestPath, []byte(`{"type":"not-a-marker","run_id":"run_1","complete_path":"x"}`), 0o600))

	// Default doctor (no target) must still list the unhealthy durable set.
	stdout, stderr, err := executeIndexDoctorCommand(t, "--json")
	require.NoError(t, err, stderr)
	var entries []map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &entries))
	require.NotEmpty(t, entries, "broken durable-only must not disappear from default doctor")
	require.Equal(t, "durable-v2", entries[0]["format"])
	require.Equal(t, false, entries[0]["durable_marker_ok"])

	// Named lookup returns structured entry, not a pure resolution error.
	stdout, stderr, err = executeIndexDoctorCommand(t, "--json", setID[:16])
	require.NoError(t, err, stderr)
	require.NoError(t, json.Unmarshal([]byte(stdout), &entries))
	require.Len(t, entries, 1)
	require.Equal(t, false, entries[0]["durable_marker_ok"])

	// Manifest digest mismatch after restoring valid latest pointer structure.
	// Rebuild latest to point at real complete, then corrupt complete digest.
	// Use a second build fixture path: rewrite complete marker digest only.
	// First restore a parseable latest by re-reading complete path from disk.
	completeFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", setID, "runs", "*", "complete.json"))
	require.NoError(t, err)
	require.NotEmpty(t, completeFiles)
	// Point latest at real complete with correct type, then break digest in complete.
	require.NoError(t, os.WriteFile(latestPath, []byte(`{
		"type":"gonimbus.index.latest.v1",
		"index_set_id":"`+setID+`",
		"run_id":"run_broken",
		"updated_at":"2026-07-10T00:00:00Z",
		"complete_path":"`+completeFiles[0]+`"
	}`), 0o600))
	// Force set/run mismatch on complete to fail closed (digest path also covered by substrate tests).
	raw, err := os.ReadFile(completeFiles[0])
	require.NoError(t, err)
	var complete map[string]any
	require.NoError(t, json.Unmarshal(raw, &complete))
	complete["manifest_sha256"] = "0000000000000000000000000000000000000000000000000000000000000000"
	fixed, err := json.Marshal(complete)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(completeFiles[0], fixed, 0o600))
	// Align latest run_id with complete so mismatch is specifically digest.
	completeRun, _ := complete["run_id"].(string)
	require.NoError(t, os.WriteFile(latestPath, []byte(`{
		"type":"gonimbus.index.latest.v1",
		"index_set_id":"`+setID+`",
		"run_id":"`+completeRun+`",
		"updated_at":"2026-07-10T00:00:00Z",
		"complete_path":"`+completeFiles[0]+`"
	}`), 0o600))

	stdout, stderr, err = executeIndexDoctorCommand(t, "--json", setID)
	require.NoError(t, err, stderr)
	require.NoError(t, json.Unmarshal([]byte(stdout), &entries))
	require.Len(t, entries, 1)
	require.Equal(t, false, entries[0]["durable_marker_ok"])
	noteBlob := fmt.Sprint(entries[0]["notes"])
	require.Contains(t, strings.ToLower(noteBlob), "digest")
}

// SQLite-only builds must not synthesize a phantom durable backend from identity alone.
func TestIndexDoctorSQLiteOnlyNoPhantomDurable(t *testing.T) {
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
  match:
    includes: ["**"]
  crawl:
    concurrency: 1
    progress_every: 100
`), 0o600))

	// SQLite path does not use the durable engine source hook; use a local provider
	// via the same fake by forcing format both is wrong. Instead open a pure sqlite
	// build with format sqlite — needs real provider or the build will try AWS.
	// Use the engine adapter path with format both is not SQLite-only.
	// Create sqlite-only fixture via indexstore like doctor unit tests.
	root := filepath.Join(dataRoot, "indexes")
	require.NoError(t, os.MkdirAll(root, 0o755))
	identity := createTestIndex(t, root, "s3://bucket/sqlite-only/")
	require.NoError(t, writeIndexIdentityFile(filepath.Join(root, identity.DirName), identity))
	// Ensure no segment cache artifact for this set.
	segRoot := filepath.Join(dataRoot, "cache", "segments")
	require.NoError(t, os.MkdirAll(segRoot, 0o755))
	entries, err := os.ReadDir(segRoot)
	require.NoError(t, err)
	require.Empty(t, entries)

	stdout, stderr, err := executeIndexDoctorCommand(t, "--json")
	require.NoError(t, err, stderr)
	var docs []map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &docs))
	require.Len(t, docs, 1, "sqlite-only must emit exactly one doctor row")
	require.Equal(t, "sqlite-v1", docs[0]["format"])
	require.Nil(t, docs[0]["durable_marker_ok"], "must not invent durable health")

	// Directory selection / --detail requires exactly one target.
	dirPath := filepath.Join(root, identity.DirName)
	stdout, stderr, err = executeIndexDoctorCommand(t, "--detail", dirPath)
	require.NoError(t, err, stderr)
	var detail map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &detail))
	require.Equal(t, "sqlite-v1", detail["format"])
	_ = base
}

// A real segment-set directory with missing latest.json stays discoverable and unhealthy.
func TestIndexDoctorMissingLatestOnExistingSegmentSet(t *testing.T) {
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
	cmd := &cobra.Command{Use: "build"}
	var out strings.Builder
	cmd.SetOut(&out)
	require.NoError(t, runIndexBuild(cmd, nil))
	var receipt map[string]any
	require.NoError(t, json.Unmarshal([]byte(out.String()), &receipt))
	setID, _ := receipt["index_set_id"].(string)
	require.NotEmpty(t, setID)

	latestPath := filepath.Join(dataRoot, "cache", "segments", setID, "latest.json")
	require.NoError(t, os.Remove(latestPath))
	// Segment set directory must still exist.
	st, err := os.Stat(filepath.Join(dataRoot, "cache", "segments", setID))
	require.NoError(t, err)
	require.True(t, st.IsDir())

	stdout, stderr, err := executeIndexDoctorCommand(t, "--json")
	require.NoError(t, err, stderr)
	var entries []map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &entries))
	require.Len(t, entries, 1)
	require.Equal(t, "durable-v2", entries[0]["format"])
	require.Equal(t, false, entries[0]["durable_marker_ok"])

	stdout, stderr, err = executeIndexDoctorCommand(t, "--json", setID[:16])
	require.NoError(t, err, stderr)
	require.NoError(t, json.Unmarshal([]byte(stdout), &entries))
	require.Len(t, entries, 1)
	require.Equal(t, false, entries[0]["durable_marker_ok"])
}

func TestIndexDoctorDetailRequiresFormatForBoth(t *testing.T) {
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
  match:
    includes: ["**"]
  crawl:
    concurrency: 1
    progress_every: 100
`), 0o600))

	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return indexBuildEngineFakeProvider{objects: []provider.ObjectSummary{
			{Key: "data/a.xml", Size: 10, ETag: `"a"`, LastModified: base, StorageClass: "STANDARD"},
		}}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"
	indexBuildJSON = true
	cmd := &cobra.Command{Use: "build"}
	var out strings.Builder
	cmd.SetOut(&out)
	require.NoError(t, runIndexBuild(cmd, nil))
	var receipt map[string]any
	// both emits compare then optional receipt only with --json after success;
	// for both without parsing, use list of set from segment cache.
	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
	setID := filepath.Base(filepath.Dir(latestFiles[0]))
	_ = receipt

	// Without --format, detail fails closed on dual substrates.
	_, _, err = executeIndexDoctorCommand(t, "--detail", setID[:16])
	require.Error(t, err)
	require.Contains(t, err.Error(), "--format")

	// Durable detail retains identity directory metadata.
	stdout, stderr, err := executeIndexDoctorCommand(t, "--detail", "--format", "durable-v2", setID[:16])
	require.NoError(t, err, stderr)
	var detail map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &detail))
	require.Equal(t, "durable-v2", detail["format"])
	require.Equal(t, true, detail["identity_present"])
	require.NotEmpty(t, detail["identity_path"])

	// Exact-epoch cleanup retains discoverable quarantine captures that block
	// later SQLite readers until recovery. Multi-step fixtures clear temp-dir
	// residue only (not a production API).
	clearSQLiteQuarantineResidueUnderDataRoot(t, dataRoot)

	stdout, stderr, err = executeIndexDoctorCommand(t, "--detail", "--format", "sqlite-v1", setID[:16])
	require.NoError(t, err, stderr)
	require.NoError(t, json.Unmarshal([]byte(stdout), &detail))
	require.Equal(t, "sqlite-v1", detail["format"])
}

func TestBoundedIdentityReadRejectsOversized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.json")
	// Larger than maxHubMarkerBytes (1 MiB).
	require.NoError(t, os.WriteFile(path, []byte(`{"pad":"`+strings.Repeat("x", maxHubMarkerBytes+10)+`"}`), 0o600))
	_, err := indexreader.ReadLocalIdentityFile(path, int64(maxHubMarkerBytes))
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds limit")
	require.Equal(t, "invalid", computeIndexIdentityStatus(path, "idx_deadbeefdeadbeef", "idx_deadbeefdeadbeef"))
}

// Durable stats must report the run selected by latest.json, not the complete
// marker with the newest publication timestamp.
func TestIndexStatsDurableLatestFollowsPointerNotTimestamp(t *testing.T) {
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

	// Publish run B (older complete).
	cmdB := &cobra.Command{Use: "build"}
	cmdB.SetContext(context.Background())
	var outB strings.Builder
	cmdB.SetOut(&outB)
	require.NoError(t, runIndexBuild(cmdB, nil))
	var receiptB map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(outB.String())), &receiptB))
	setID, _ := receiptB["index_set_id"].(string)
	runB, _ := receiptB["run_id"].(string)
	require.NotEmpty(t, setID)
	require.NotEmpty(t, runB)

	// Publish run C (newer complete; advances latest).
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"
	indexBuildJSON = true
	cmdC := &cobra.Command{Use: "build"}
	cmdC.SetContext(context.Background())
	var outC strings.Builder
	cmdC.SetOut(&outC)
	require.NoError(t, runIndexBuild(cmdC, nil))
	var receiptC map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(outC.String())), &receiptC))
	runC, _ := receiptC["run_id"].(string)
	require.NotEmpty(t, runC)
	require.NotEqual(t, runB, runC)
	require.Equal(t, setID, receiptC["index_set_id"])

	segmentRoot := filepath.Join(dataRoot, "cache", "segments", setID)
	completeB := filepath.Join(segmentRoot, "runs", runB, "complete.json")
	completeC := filepath.Join(segmentRoot, "runs", runC, "complete.json")
	require.FileExists(t, completeB)
	require.FileExists(t, completeC)

	// Point latest.json back at B (older pointer selection / non-advanced C case).
	latestPath := filepath.Join(segmentRoot, "latest.json")
	writeJSONFile(t, latestPath, map[string]any{
		"type":          "gonimbus.index.latest.v1",
		"index_set_id":  setID,
		"run_id":        runB,
		"updated_at":    time.Date(2026, 7, 10, 11, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		"complete_path": completeB,
	})

	// Ensure C's complete marker has a strictly newer completed_at so a
	// timestamp heuristic would incorrectly prefer C.
	var completeDoc map[string]any
	rawC, err := os.ReadFile(completeC)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(rawC, &completeDoc))
	completeDoc["completed_at"] = time.Date(2026, 7, 10, 18, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	writeJSONFile(t, completeC, completeDoc)

	statsCmd := &cobra.Command{Use: "stats"}
	statsCmd.Flags().Bool("json", false, "")
	statsCmd.Flags().Bool("prefixes", false, "")
	statsCmd.Flags().Bool("runs", false, "")
	require.NoError(t, statsCmd.Flags().Set("json", "true"))
	require.NoError(t, statsCmd.Flags().Set("runs", "true"))
	statsCmd.SetContext(context.Background())
	stdout := captureStdout(t, func() {
		require.NoError(t, runIndexStats(statsCmd, []string{"s3://bucket/data/"}))
	})
	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &doc))
	pub := doc["published_runs"].(map[string]any)
	latest := pub["latest"].(map[string]any)
	require.Equal(t, runB, latest["run_id"], "stats latest must follow latest.json, not newest complete timestamp")
	require.EqualValues(t, float64(2), pub["total"])

	// History may still list C (and even order it first by time); latest stays B.
	history, _ := doc["publication_history"].([]any)
	require.GreaterOrEqual(t, len(history), 2)
	historyIDs := map[string]bool{}
	for _, item := range history {
		row := item.(map[string]any)
		historyIDs[row["run_id"].(string)] = true
	}
	require.True(t, historyIDs[runB])
	require.True(t, historyIDs[runC])
}

// Doctor --detail must bound job-manifest provenance reads and never emit an
// oversized raw payload.
func TestIndexDoctorDetailRejectsOversizedJobManifest(t *testing.T) {
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
	cmd := &cobra.Command{Use: "build"}
	var out strings.Builder
	cmd.SetOut(&out)
	require.NoError(t, runIndexBuild(cmd, nil))
	var receipt map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(out.String())), &receipt))
	setID, _ := receipt["index_set_id"].(string)
	require.NotEmpty(t, setID)

	identityDir := findIdentityDirForSet(filepath.Join(dataRoot, "indexes"), setID)
	require.NotEmpty(t, identityDir)
	// Job-manifest provenance under the identity directory (not the durable
	// segment manifest). Oversized content must fail closed under --detail.
	jobManifest := filepath.Join(identityDir, "manifest.json")
	oversized := `{"pad":"` + strings.Repeat("x", maxHubMarkerBytes+10) + `"}`
	require.NoError(t, os.WriteFile(jobManifest, []byte(oversized), 0o600))

	stdout, stderr, err := executeIndexDoctorCommand(t, "--detail", "--format", "durable-v2", "--json", setID[:16])
	require.NoError(t, err, stderr)
	var entries []map[string]any
	// doctor --json may emit an array or single object depending on path;
	// normalize either shape.
	trimmed := strings.TrimSpace(stdout)
	if strings.HasPrefix(trimmed, "[") {
		require.NoError(t, json.Unmarshal([]byte(trimmed), &entries))
	} else {
		var one map[string]any
		require.NoError(t, json.Unmarshal([]byte(trimmed), &one))
		entries = []map[string]any{one}
	}
	require.Len(t, entries, 1)
	entry := entries[0]
	require.Equal(t, "durable-v2", entry["format"])
	// Oversized provenance: no raw payload, note records the bound failure.
	_, hasRaw := entry["manifest_raw"]
	require.False(t, hasRaw, "oversized job manifest must not be emitted as detail raw payload")
	require.Equal(t, false, entry["manifest_present"])
	require.Equal(t, false, entry["manifest_valid_json"])
	notes, _ := entry["notes"].([]any)
	joined := fmt.Sprint(notes)
	require.Contains(t, joined, "manifest.json")
	require.Contains(t, joined, "exceeds limit")
	// Response itself must not balloon with the oversized pad.
	require.Less(t, len(stdout), maxHubMarkerBytes)
}
