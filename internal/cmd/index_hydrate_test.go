package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
)

// --- validateFullIndexSetID ---

func TestValidateFullIndexSetID_Valid(t *testing.T) {
	err := validateFullIndexSetID("idx_a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2")
	assert.NoError(t, err)
}

func TestValidateFullIndexSetID_ShortPrefix(t *testing.T) {
	err := validateFullIndexSetID("idx_da038d8171b4a9ba")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "short prefix")
}

func TestValidateFullIndexSetID_NoPrefix(t *testing.T) {
	err := validateFullIndexSetID("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid index set ID")
}

func TestValidateFullIndexSetID_BadChars(t *testing.T) {
	err := validateFullIndexSetID("idx_ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ")
	require.Error(t, err)
}

// --- validateRunID ---

func TestValidateRunID_ValidNumeric(t *testing.T) {
	assert.NoError(t, validateRunID("run_1709654400000000000"))
}

func TestValidateRunID_ValidULID(t *testing.T) {
	assert.NoError(t, validateRunID("run_01HRZ6J5KRBXVNC9G5TNMPQ4YT"))
}

func TestValidateRunID_Malformed(t *testing.T) {
	tests := []struct {
		name  string
		runID string
	}{
		{"empty", ""},
		{"no prefix", "1709654400000000000"},
		{"wrong prefix", "RUN_123"},
		{"path traversal", "run_../../etc/passwd"},
		{"spaces", "run_12 34"},
		{"slashes", "run_12/34"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRunID(tt.runID)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid run ID")
		})
	}
}

// --- resolveLatestRunID ---

func TestResolveLatestRunID(t *testing.T) {
	ctx := context.Background()
	hubDir := t.TempDir()
	hub := &hubDestSpec{Provider: "file", BaseDir: hubDir}

	indexSetID := "idx_" + "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	runID := "run_1709654400000000000"

	// Create latest.json
	latestDir := filepath.Join(hubDir, "index-sets", indexSetID)
	require.NoError(t, os.MkdirAll(latestDir, 0755))
	latestJSON := []byte(`{"version":"1.0","index_set_id":"` + indexSetID + `","run_id":"` + runID + `","updated_at":"2026-03-06T00:00:00Z"}`)
	require.NoError(t, os.WriteFile(filepath.Join(latestDir, "latest.json"), latestJSON, 0644))

	fp, err := providerfile.New(providerfile.Config{BaseDir: hubDir})
	require.NoError(t, err)

	resolved, err := resolveLatestRunID(ctx, fp, hub, indexSetID)
	require.NoError(t, err)
	assert.Equal(t, runID, resolved)
}

func TestResolveLatestRunID_NotFound(t *testing.T) {
	ctx := context.Background()
	hubDir := t.TempDir()
	hub := &hubDestSpec{Provider: "file", BaseDir: hubDir}

	fp, err := providerfile.New(providerfile.Config{BaseDir: hubDir})
	require.NoError(t, err)

	_, err = resolveLatestRunID(ctx, fp, hub, "idx_nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no latest.json found")
	assert.Contains(t, err.Error(), "--run-id")
}

func TestResolveLatestRunID_EmptyRunID(t *testing.T) {
	ctx := context.Background()
	hubDir := t.TempDir()
	hub := &hubDestSpec{Provider: "file", BaseDir: hubDir}

	indexSetID := "idx_abc"
	latestDir := filepath.Join(hubDir, "index-sets", indexSetID)
	require.NoError(t, os.MkdirAll(latestDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(latestDir, "latest.json"), []byte(`{"run_id":""}`), 0644))

	fp, err := providerfile.New(providerfile.Config{BaseDir: hubDir})
	require.NoError(t, err)

	_, err = resolveLatestRunID(ctx, fp, hub, indexSetID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty run_id")
}

func TestResolveLatestRunID_MalformedRunID(t *testing.T) {
	ctx := context.Background()
	hubDir := t.TempDir()
	hub := &hubDestSpec{Provider: "file", BaseDir: hubDir}

	indexSetID := "idx_abc"
	latestDir := filepath.Join(hubDir, "index-sets", indexSetID)
	require.NoError(t, os.MkdirAll(latestDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(latestDir, "latest.json"), []byte(`{"run_id":"../../evil"}`), 0644))

	fp, err := providerfile.New(providerfile.Config{BaseDir: hubDir})
	require.NoError(t, err)

	_, err = resolveLatestRunID(ctx, fp, hub, indexSetID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid run ID")
}

// --- downloadBytes ---

func TestDownloadBytes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	content := []byte(`{"hello":"world"}`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "test.json"), content, 0644))

	fp, err := providerfile.New(providerfile.Config{BaseDir: dir})
	require.NoError(t, err)

	data, err := downloadBytes(ctx, fp, "test.json")
	require.NoError(t, err)
	assert.Equal(t, content, data)
}

func TestDownloadBytes_NotFound(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fp, err := providerfile.New(providerfile.Config{BaseDir: dir})
	require.NoError(t, err)

	_, err = downloadBytes(ctx, fp, "missing.json")
	require.Error(t, err)
	assert.True(t, provider.IsNotFound(err))
}

// --- downloadFile ---

func TestDownloadFile(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	content := []byte("database content here")
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "index.db"), content, 0644))

	fp, err := providerfile.New(providerfile.Config{BaseDir: srcDir})
	require.NoError(t, err)

	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "index.db")
	require.NoError(t, downloadFile(ctx, fp, "index.db", destPath))

	downloaded, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, content, downloaded)
}

func TestDownloadFile_NotFound(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fp, err := providerfile.New(providerfile.Config{BaseDir: dir})
	require.NoError(t, err)

	err = downloadFile(ctx, fp, "missing.db", filepath.Join(dir, "out.db"))
	require.Error(t, err)
	assert.True(t, provider.IsNotFound(err))
}

// --- command-level integration: export then hydrate ---

func TestRunIndexHydrate_FileHub(t *testing.T) {
	ctx := context.Background()

	// Phase 1: Set up a local index and export to a file hub
	idxDir := t.TempDir()
	dbPath := filepath.Join(idxDir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))

	params := testIndexSetParams("s3://bucket/prefix/")
	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusSuccess, nil))

	identityContent := []byte(`{"test": "identity"}`)
	require.NoError(t, os.WriteFile(filepath.Join(idxDir, "identity.json"), identityContent, 0644))

	_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, db.Close())

	// Export to file hub
	hubDir := t.TempDir()
	hubURI := "file://" + hubDir + "/"

	exportCmd := &cobra.Command{Use: "export", RunE: runIndexExport}
	exportCmd.Flags().String("hub", "", "")
	exportCmd.Flags().String("index-set", "", "")
	exportCmd.Flags().String("run-id", "", "")
	exportCmd.Flags().String("db", "", "")
	exportCmd.Flags().String("hub-profile", "", "")
	exportCmd.Flags().String("hub-region", "", "")
	exportCmd.Flags().String("hub-endpoint", "", "")
	exportCmd.SetArgs([]string{
		"--hub", hubURI,
		"--index-set", indexSet.IndexSetID,
		"--run-id", run.RunID,
		"--db", dbPath,
	})
	exportCmd.SetContext(ctx)
	require.NoError(t, exportCmd.Execute())

	// Phase 2: Hydrate from the hub to a fresh directory
	hydrateDir := t.TempDir()

	hydrateCmd := &cobra.Command{Use: "hydrate", RunE: runIndexHydrate}
	hydrateCmd.Flags().String("hub", "", "")
	hydrateCmd.Flags().String("index-set", "", "")
	hydrateCmd.Flags().String("run-id", "", "")
	hydrateCmd.Flags().String("dest", "", "")
	hydrateCmd.Flags().String("hub-profile", "", "")
	hydrateCmd.Flags().String("hub-region", "", "")
	hydrateCmd.Flags().String("hub-endpoint", "", "")
	hydrateCmd.SetArgs([]string{
		"--hub", hubURI,
		"--index-set", indexSet.IndexSetID,
		"--dest", hydrateDir,
	})
	hydrateCmd.SetContext(ctx)
	require.NoError(t, hydrateCmd.Execute())

	// Verify hydrated artifacts
	assert.FileExists(t, filepath.Join(hydrateDir, "index.db"))
	assert.FileExists(t, filepath.Join(hydrateDir, "identity.json"))
	assert.FileExists(t, filepath.Join(hydrateDir, "complete.json"))

	// Verify index.db integrity (matches source)
	srcHash, srcSize, err := hashFile(dbPath)
	require.NoError(t, err)
	hydratedHash, hydratedSize, err := hashFile(filepath.Join(hydrateDir, "index.db"))
	require.NoError(t, err)
	assert.Equal(t, srcHash, hydratedHash)
	assert.Equal(t, srcSize, hydratedSize)

	// Verify identity.json content
	hydratedIdentity, err := os.ReadFile(filepath.Join(hydrateDir, "identity.json"))
	require.NoError(t, err)
	assert.Equal(t, identityContent, hydratedIdentity)

	// Verify complete.json was saved for provenance
	completeData, err := os.ReadFile(filepath.Join(hydrateDir, "complete.json"))
	require.NoError(t, err)
	var completeDoc map[string]interface{}
	require.NoError(t, json.Unmarshal(completeData, &completeDoc))
	assert.Equal(t, indexSet.IndexSetID, completeDoc["index_set_id"])
	assert.Equal(t, run.RunID, completeDoc["run_id"])
}

func TestRunIndexExportHydrate_DurableFileHub(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	ctx := context.Background()

	idxDir := t.TempDir()
	dbPath := filepath.Join(idxDir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))

	params := testIndexSetParams("s3://bucket/prefix/")
	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusSuccess, nil))
	_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, db.Close())

	localManifest := writeLocalDurableSnapshotForHubTest(t, indexSet.IndexSetID, run.RunID)
	hubDir := t.TempDir()
	hubURI := "file://" + hubDir + "/"

	exportCmd := &cobra.Command{Use: "export", RunE: runIndexExport}
	exportCmd.Flags().String("hub", "", "")
	exportCmd.Flags().String("index-set", "", "")
	exportCmd.Flags().String("run-id", "", "")
	exportCmd.Flags().String("db", "", "")
	exportCmd.Flags().String("format", "", "")
	exportCmd.Flags().String("hub-profile", "", "")
	exportCmd.Flags().String("hub-region", "", "")
	exportCmd.Flags().String("hub-endpoint", "", "")
	exportCmd.Flags().String("hub-gcp-project", "", "")
	addLatestPointerFlags(exportCmd)
	exportCmd.SetArgs([]string{
		"--hub", hubURI,
		"--index-set", indexSet.IndexSetID,
		"--run-id", run.RunID,
		"--db", dbPath,
		"--format", "durable",
	})
	exportCmd.SetContext(ctx)
	require.NoError(t, exportCmd.Execute())

	runDir := filepath.Join(hubDir, "index-sets", indexSet.IndexSetID, "runs", run.RunID)
	require.NoFileExists(t, filepath.Join(runDir, "index.db"))
	require.FileExists(t, filepath.Join(runDir, "manifest.json"))
	require.FileExists(t, filepath.Join(runDir, "segments", localManifest.Segments[0].Path))
	completeData, err := os.ReadFile(filepath.Join(runDir, "complete.json"))
	require.NoError(t, err)
	var complete map[string]any
	require.NoError(t, json.Unmarshal(completeData, &complete))
	require.Equal(t, indexHubFormatDurableV2, complete["format"])
	require.NotContains(t, string(completeData), dataRoot)
	require.NotContains(t, string(completeData), "s3://bucket")

	hydrateDir := t.TempDir()
	hydrateCmd := &cobra.Command{Use: "hydrate", RunE: runIndexHydrate}
	hydrateCmd.Flags().String("hub", "", "")
	hydrateCmd.Flags().String("index-set", "", "")
	hydrateCmd.Flags().String("run-id", "", "")
	hydrateCmd.Flags().String("dest", "", "")
	hydrateCmd.Flags().String("hub-profile", "", "")
	hydrateCmd.Flags().String("hub-region", "", "")
	hydrateCmd.Flags().String("hub-endpoint", "", "")
	hydrateCmd.Flags().String("hub-gcp-project", "", "")
	hydrateCmd.SetArgs([]string{
		"--hub", hubURI,
		"--index-set", indexSet.IndexSetID,
		"--run-id", run.RunID,
		"--dest", hydrateDir,
	})
	hydrateCmd.SetContext(ctx)
	require.NoError(t, hydrateCmd.Execute())

	require.NoFileExists(t, filepath.Join(hydrateDir, "index.db"))
	require.FileExists(t, filepath.Join(hydrateDir, "complete.json"))
	hydratedManifest, err := indexsubstrate.ReadInternalManifestFile(filepath.Join(hydrateDir, "manifest.json"))
	require.NoError(t, err)
	require.Equal(t, localManifest, hydratedManifest)
	rows, err := indexsubstrate.ReadManifestRows(filepath.Join(hydrateDir, "segments"), hydratedManifest)
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

// Test hydrate resolves latest.json correctly (no --run-id)
func TestRunIndexHydrate_ResolvesLatest(t *testing.T) {
	ctx := context.Background()

	// Set up and export
	idxDir := t.TempDir()
	dbPath := filepath.Join(idxDir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))

	params := testIndexSetParams("s3://bucket/prefix/")
	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusSuccess, nil))

	_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, db.Close())

	hubDir := t.TempDir()
	hubURI := "file://" + hubDir + "/"

	// Export (creates latest.json)
	exportCmd := &cobra.Command{Use: "export", RunE: runIndexExport}
	exportCmd.Flags().String("hub", "", "")
	exportCmd.Flags().String("index-set", "", "")
	exportCmd.Flags().String("run-id", "", "")
	exportCmd.Flags().String("db", "", "")
	exportCmd.Flags().String("hub-profile", "", "")
	exportCmd.Flags().String("hub-region", "", "")
	exportCmd.Flags().String("hub-endpoint", "", "")
	exportCmd.SetArgs([]string{
		"--hub", hubURI,
		"--index-set", indexSet.IndexSetID,
		"--run-id", run.RunID,
		"--db", dbPath,
	})
	exportCmd.SetContext(ctx)
	require.NoError(t, exportCmd.Execute())

	// Hydrate without --run-id (should resolve from latest.json)
	hydrateDir := t.TempDir()
	hydrateCmd := &cobra.Command{Use: "hydrate", RunE: runIndexHydrate}
	hydrateCmd.Flags().String("hub", "", "")
	hydrateCmd.Flags().String("index-set", "", "")
	hydrateCmd.Flags().String("run-id", "", "")
	hydrateCmd.Flags().String("dest", "", "")
	hydrateCmd.Flags().String("hub-profile", "", "")
	hydrateCmd.Flags().String("hub-region", "", "")
	hydrateCmd.Flags().String("hub-endpoint", "", "")
	hydrateCmd.SetArgs([]string{
		"--hub", hubURI,
		"--index-set", indexSet.IndexSetID,
		"--dest", hydrateDir,
	})
	hydrateCmd.SetContext(ctx)
	require.NoError(t, hydrateCmd.Execute())

	assert.FileExists(t, filepath.Join(hydrateDir, "index.db"))

	// Verify it got the right run via complete.json
	completeData, err := os.ReadFile(filepath.Join(hydrateDir, "complete.json"))
	require.NoError(t, err)
	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(completeData, &doc))
	assert.Equal(t, run.RunID, doc["run_id"])
}

// Test hydrate fails on uncommitted run (no complete.json)
func TestRunIndexHydrate_UncommittedRun(t *testing.T) {
	ctx := context.Background()
	hubDir := t.TempDir()

	indexSetID := "idx_a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	runID := "run_1709654400000000000"

	// Create run dir with index.db but NO complete.json
	runDir := filepath.Join(hubDir, "index-sets", indexSetID, "runs", runID)
	require.NoError(t, os.MkdirAll(runDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "index.db"), []byte("fake"), 0644))

	hydrateDir := t.TempDir()
	hydrateCmd := &cobra.Command{Use: "hydrate", RunE: runIndexHydrate}
	hydrateCmd.Flags().String("hub", "", "")
	hydrateCmd.Flags().String("index-set", "", "")
	hydrateCmd.Flags().String("run-id", "", "")
	hydrateCmd.Flags().String("dest", "", "")
	hydrateCmd.Flags().String("hub-profile", "", "")
	hydrateCmd.Flags().String("hub-region", "", "")
	hydrateCmd.Flags().String("hub-endpoint", "", "")
	hydrateCmd.SetArgs([]string{
		"--hub", "file://" + hubDir + "/",
		"--index-set", indexSetID,
		"--run-id", runID,
		"--dest", hydrateDir,
	})
	hydrateCmd.SetContext(ctx)

	err := hydrateCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not committed")
}

// Test hydrate detects integrity failure
func TestRunIndexHydrate_IntegrityFailure(t *testing.T) {
	ctx := context.Background()
	hubDir := t.TempDir()

	indexSetID := "idx_a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	runID := "run_1709654400000000000"

	// Create run dir with index.db and complete.json with wrong checksum
	runDir := filepath.Join(hubDir, "index-sets", indexSetID, "runs", runID)
	require.NoError(t, os.MkdirAll(runDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "index.db"), []byte("actual content"), 0644))

	wrongHash := "0000000000000000000000000000000000000000000000000000000000000000"
	complete := map[string]interface{}{
		"version":      "1.0",
		"index_set_id": indexSetID,
		"run_id":       runID,
		"completed_at": "2026-03-06T00:00:00Z",
		"artifacts": map[string]interface{}{
			"index_db": map[string]interface{}{
				"size_bytes": len("actual content"),
				"sha256":     wrongHash,
			},
		},
	}
	completeJSON, err := json.Marshal(complete)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "complete.json"), completeJSON, 0644))

	hydrateDir := t.TempDir()
	hydrateCmd := &cobra.Command{Use: "hydrate", RunE: runIndexHydrate}
	hydrateCmd.Flags().String("hub", "", "")
	hydrateCmd.Flags().String("index-set", "", "")
	hydrateCmd.Flags().String("run-id", "", "")
	hydrateCmd.Flags().String("dest", "", "")
	hydrateCmd.Flags().String("hub-profile", "", "")
	hydrateCmd.Flags().String("hub-region", "", "")
	hydrateCmd.Flags().String("hub-endpoint", "", "")
	hydrateCmd.SetArgs([]string{
		"--hub", "file://" + hubDir + "/",
		"--index-set", indexSetID,
		"--run-id", runID,
		"--dest", hydrateDir,
	})
	hydrateCmd.SetContext(ctx)

	err = hydrateCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "integrity check failed")
}

func TestRunIndexHydrate_RejectsUnknownFormat(t *testing.T) {
	ctx := context.Background()
	hubDir := t.TempDir()

	indexSetID := "idx_a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	runID := "run_1709654400000000000"
	runDir := filepath.Join(hubDir, "index-sets", indexSetID, "runs", runID)
	require.NoError(t, os.MkdirAll(runDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "complete.json"), []byte(`{"version":"1.0","format":"future-v9","index_set_id":"`+indexSetID+`","run_id":"`+runID+`","artifacts":{}}`), 0644))

	hydrateCmd := &cobra.Command{Use: "hydrate", RunE: runIndexHydrate}
	hydrateCmd.Flags().String("hub", "", "")
	hydrateCmd.Flags().String("index-set", "", "")
	hydrateCmd.Flags().String("run-id", "", "")
	hydrateCmd.Flags().String("dest", "", "")
	hydrateCmd.Flags().String("hub-profile", "", "")
	hydrateCmd.Flags().String("hub-region", "", "")
	hydrateCmd.Flags().String("hub-endpoint", "", "")
	hydrateCmd.Flags().String("hub-gcp-project", "", "")
	hydrateCmd.SetArgs([]string{
		"--hub", "file://" + hubDir + "/",
		"--index-set", indexSetID,
		"--run-id", runID,
		"--dest", t.TempDir(),
	})
	hydrateCmd.SetContext(ctx)

	err := hydrateCmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), `unsupported index hub format "future-v9"`)
}

func TestRunIndexHydrate_RejectsOversizedCompleteMarker(t *testing.T) {
	ctx := context.Background()
	hubDir := t.TempDir()
	indexSetID := "idx_a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	runID := "run_1709654400000000000"
	runDir := filepath.Join(hubDir, "index-sets", indexSetID, "runs", runID)
	require.NoError(t, os.MkdirAll(runDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "complete.json"), bytesOfSize(maxHubMarkerBytes+1), 0644))

	err := runHydrateFileHubForTest(ctx, hubDir, indexSetID, runID, t.TempDir())
	require.Error(t, err)
	require.Contains(t, err.Error(), "complete.json size")
	require.Contains(t, err.Error(), "exceeds limit")
}

func TestRunIndexHydrate_DurableRejectsOversizedManifestDeclaration(t *testing.T) {
	ctx := context.Background()
	hubDir, indexSetID, runID := writeDurableHubRunForHydrateTest(t, func(complete map[string]any) {
		artifacts := complete["artifacts"].(map[string]any)
		manifest := artifacts["manifest"].(map[string]any)
		manifest["size_bytes"] = float64(maxDurableManifestBytes + 1)
	}, nil)

	err := runHydrateFileHubForTest(ctx, hubDir, indexSetID, runID, t.TempDir())
	require.Error(t, err)
	require.Contains(t, err.Error(), "durable manifest size")
	require.Contains(t, err.Error(), "exceeds limit")
}

func TestRunIndexHydrate_DurableRejectsMarkerContractDrift(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(map[string]any)
		message string
	}{
		{
			name: "missing schema",
			mutate: func(complete map[string]any) {
				delete(complete, "marker_schema_version")
			},
			message: "durable complete marker schema version",
		},
		{
			name: "wrong format version",
			mutate: func(complete map[string]any) {
				complete["format_version"] = "3"
			},
			message: "durable complete marker format_version",
		},
		{
			name: "missing exported by",
			mutate: func(complete map[string]any) {
				delete(complete, "exported_by")
			},
			message: "durable complete marker exported_by is required",
		},
		{
			name: "index identity mismatch",
			mutate: func(complete map[string]any) {
				complete["index_set_id"] = "idx_ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
			},
			message: "durable complete marker index_set_id mismatch",
		},
		{
			name: "run identity mismatch",
			mutate: func(complete map[string]any) {
				complete["run_id"] = "run_999"
			},
			message: "durable complete marker run_id mismatch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			hubDir, indexSetID, runID := writeDurableHubRunForHydrateTest(t, func(complete map[string]any) {
				tt.mutate(complete)
			}, nil)

			err := runHydrateFileHubForTest(ctx, hubDir, indexSetID, runID, t.TempDir())
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.message)
		})
	}
}

func TestRunIndexHydrate_DurableRejectsNonCanonicalSegmentArtifactPath(t *testing.T) {
	ctx := context.Background()
	hubDir, indexSetID, runID := writeDurableHubRunForHydrateTest(t, func(complete map[string]any) {
		artifacts := complete["artifacts"].(map[string]any)
		segments := artifacts["segments"].([]any)
		first := segments[0].(map[string]any)
		first["path"] = "segments/../escape.parquet"
	}, nil)

	err := runHydrateFileHubForTest(ctx, hubDir, indexSetID, runID, t.TempDir())
	require.Error(t, err)
	require.Contains(t, err.Error(), "durable segment artifact path must be canonical")
}

func TestValidateDurableHubManifestBoundsRejectsExcessiveSegments(t *testing.T) {
	manifest := indexsubstrate.InternalManifest{
		Segments: make([]indexsubstrate.SegmentDescriptor, maxDurableHubSegments+1),
	}
	err := validateDurableHubManifestBounds(manifest)
	require.Error(t, err)
	require.Contains(t, err.Error(), "segment count")
	require.Contains(t, err.Error(), "exceeds limit")
}

// Test hydrate rejects short index-set IDs at command level
func TestRunIndexHydrate_RejectsShortIndexSet(t *testing.T) {
	ctx := context.Background()

	hydrateCmd := &cobra.Command{Use: "hydrate", RunE: runIndexHydrate}
	hydrateCmd.Flags().String("hub", "", "")
	hydrateCmd.Flags().String("index-set", "", "")
	hydrateCmd.Flags().String("run-id", "", "")
	hydrateCmd.Flags().String("dest", "", "")
	hydrateCmd.Flags().String("hub-profile", "", "")
	hydrateCmd.Flags().String("hub-region", "", "")
	hydrateCmd.Flags().String("hub-endpoint", "", "")
	hydrateCmd.SetArgs([]string{
		"--hub", "file:///tmp/hub/",
		"--index-set", "idx_da038d8171b4a9ba",
		"--dest", t.TempDir(),
	})
	hydrateCmd.SetContext(ctx)

	err := hydrateCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "short prefix")
}

// Test hydrate rejects malformed --run-id flag
func TestRunIndexHydrate_RejectsMalformedRunID(t *testing.T) {
	ctx := context.Background()

	hydrateCmd := &cobra.Command{Use: "hydrate", RunE: runIndexHydrate}
	hydrateCmd.Flags().String("hub", "", "")
	hydrateCmd.Flags().String("index-set", "", "")
	hydrateCmd.Flags().String("run-id", "", "")
	hydrateCmd.Flags().String("dest", "", "")
	hydrateCmd.Flags().String("hub-profile", "", "")
	hydrateCmd.Flags().String("hub-region", "", "")
	hydrateCmd.Flags().String("hub-endpoint", "", "")
	hydrateCmd.SetArgs([]string{
		"--hub", "file:///tmp/hub/",
		"--index-set", "idx_a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		"--run-id", "../../etc/passwd",
		"--dest", t.TempDir(),
	})
	hydrateCmd.SetContext(ctx)

	err := hydrateCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid run ID")
}

// Test that hashFile result matches what complete.json expects in a real export
func TestHydrate_VerifiesExportedChecksum(t *testing.T) {
	content := []byte("test database content")
	h := sha256.Sum256(content)
	expected := hex.EncodeToString(h[:])

	dir := t.TempDir()
	path := filepath.Join(dir, "index.db")
	require.NoError(t, os.WriteFile(path, content, 0644))

	actual, size, err := hashFile(path)
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
	assert.Equal(t, int64(len(content)), size)
}

func writeLocalDurableSnapshotForHubTest(t *testing.T, indexSetID, runID string) indexsubstrate.InternalManifest {
	t.Helper()
	segmentRoot, err := indexSubstrateSegmentCacheDir(indexSetID)
	require.NoError(t, err)
	runDir := filepath.Join(segmentRoot, "runs", runID)
	base := time.Date(2026, 7, 8, 16, 0, 0, 0, time.UTC)
	storageClass := "STANDARD"
	rows := []indexsubstrate.CurrentObjectRow{
		{
			IndexSetID:       indexSetID,
			RelKey:           "a.xml",
			SizeBytes:        10,
			LastModified:     &base,
			ETag:             `"etag-a"`,
			StorageClass:     &storageClass,
			FirstSeenRunID:   runID,
			FirstSeenAt:      base,
			LastChangedRunID: runID,
			LastChangedAt:    base,
			LastSeenRunID:    runID,
			LastSeenAt:       base,
		},
		{
			IndexSetID:       indexSetID,
			RelKey:           "b.xml",
			SizeBytes:        20,
			LastModified:     ptrTime(base.Add(time.Minute)),
			ETag:             `"etag-b"`,
			StorageClass:     &storageClass,
			FirstSeenRunID:   runID,
			FirstSeenAt:      base,
			LastChangedRunID: runID,
			LastChangedAt:    base,
			LastSeenRunID:    runID,
			LastSeenAt:       base.Add(time.Minute),
		},
	}
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir:                  runDir,
		IndexSetID:           indexSetID,
		RunID:                runID,
		CreatedAt:            base,
		TargetRowsPerSegment: 1,
		Coverage: []indexsubstrate.CoverageAttestation{{
			Scope:    &indexsubstrate.Scope{Prefix: indexsubstrate.RelativeRootScopePrefix},
			Basis:    indexsubstrate.CoverageBasisConfirmed,
			Complete: true,
		}},
	}, rows)
	require.NoError(t, err)
	manifestPath := filepath.Join(runDir, "manifest.json")
	require.NoError(t, indexsubstrate.WriteInternalManifestFile(manifestPath, manifest))
	manifestSHA, _, err := hashFile(manifestPath)
	require.NoError(t, err)
	complete := durableLocalCompleteDoc{
		Type:           "gonimbus.index.complete.v1",
		IndexSetID:     indexSetID,
		RunID:          runID,
		CompletedAt:    base.Format(time.RFC3339),
		ManifestPath:   manifestPath,
		ManifestSHA256: manifestSHA,
		SegmentDir:     runDir,
		Segments:       len(manifest.Segments),
	}
	data, err := json.MarshalIndent(complete, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "complete.json"), append(data, '\n'), 0o600))
	return manifest
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func runHydrateFileHubForTest(ctx context.Context, hubDir, indexSetID, runID, destDir string) error {
	hydrateCmd := &cobra.Command{Use: "hydrate", RunE: runIndexHydrate}
	hydrateCmd.Flags().String("hub", "", "")
	hydrateCmd.Flags().String("index-set", "", "")
	hydrateCmd.Flags().String("run-id", "", "")
	hydrateCmd.Flags().String("dest", "", "")
	hydrateCmd.Flags().String("hub-profile", "", "")
	hydrateCmd.Flags().String("hub-region", "", "")
	hydrateCmd.Flags().String("hub-endpoint", "", "")
	hydrateCmd.Flags().String("hub-gcp-project", "", "")
	hydrateCmd.SetArgs([]string{
		"--hub", "file://" + hubDir + "/",
		"--index-set", indexSetID,
		"--run-id", runID,
		"--dest", destDir,
	})
	hydrateCmd.SetContext(ctx)
	return hydrateCmd.Execute()
}

func writeDurableHubRunForHydrateTest(t *testing.T, mutateComplete func(map[string]any), mutateManifest func(*indexsubstrate.InternalManifest)) (string, string, string) {
	t.Helper()
	indexSetID := "idx_a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	runID := "run_1709654400000000000"
	hubDir := t.TempDir()
	runDir := filepath.Join(hubDir, "index-sets", indexSetID, "runs", runID)
	segmentDir := filepath.Join(runDir, "segments")
	base := time.Date(2026, 7, 8, 16, 0, 0, 0, time.UTC)
	row := indexsubstrate.CurrentObjectRow{
		IndexSetID:       indexSetID,
		RelKey:           "a.xml",
		SizeBytes:        10,
		LastModified:     &base,
		ETag:             `"etag-a"`,
		FirstSeenRunID:   runID,
		FirstSeenAt:      base,
		LastChangedRunID: runID,
		LastChangedAt:    base,
		LastSeenRunID:    runID,
		LastSeenAt:       base,
	}
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir:                  segmentDir,
		IndexSetID:           indexSetID,
		RunID:                runID,
		CreatedAt:            base,
		TargetRowsPerSegment: 1,
		Coverage: []indexsubstrate.CoverageAttestation{{
			Scope:    &indexsubstrate.Scope{Prefix: indexsubstrate.RelativeRootScopePrefix},
			Basis:    indexsubstrate.CoverageBasisConfirmed,
			Complete: true,
		}},
	}, []indexsubstrate.CurrentObjectRow{row})
	require.NoError(t, err)
	if mutateManifest != nil {
		mutateManifest(&manifest)
	}
	manifestPath := filepath.Join(runDir, "manifest.json")
	require.NoError(t, indexsubstrate.WriteInternalManifestFile(manifestPath, manifest))
	manifestSHA, manifestSize, err := hashFile(manifestPath)
	require.NoError(t, err)
	run := &indexstore.IndexRun{RunID: runID, IndexSetID: indexSetID, StartedAt: base, Status: indexstore.RunStatusSuccess}
	indexSet := &indexstore.IndexSet{IndexSetID: indexSetID}
	completeBytes, err := buildDurableCompleteJSON(indexSet, run, durableExportSnapshot{
		Manifest:     manifest,
		ManifestPath: manifestPath,
		ManifestSHA:  manifestSHA,
		ManifestSize: manifestSize,
		SegmentDir:   segmentDir,
	})
	require.NoError(t, err)
	var complete map[string]any
	require.NoError(t, json.Unmarshal(completeBytes, &complete))
	if mutateComplete != nil {
		mutateComplete(complete)
	}
	completeBytes, err = json.MarshalIndent(complete, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "complete.json"), append(completeBytes, '\n'), 0o600))
	return hubDir, indexSetID, runID
}

func bytesOfSize(size int64) []byte {
	out := make([]byte, int(size))
	for i := range out {
		out[i] = 'x'
	}
	return out
}
