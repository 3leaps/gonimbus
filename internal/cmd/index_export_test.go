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

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

// --- parseHubURI ---

func TestParseHubURI_S3(t *testing.T) {
	hub, err := parseHubURI("s3://my-bucket/hub/prefix/")
	require.NoError(t, err)
	assert.Equal(t, "s3", hub.Provider)
	assert.Equal(t, "my-bucket", hub.Bucket)
	assert.Equal(t, "hub/prefix/", hub.Prefix)
}

func TestParseHubURI_S3NoTrailingSlash(t *testing.T) {
	hub, err := parseHubURI("s3://my-bucket/hub/prefix")
	require.NoError(t, err)
	assert.Equal(t, "hub/prefix/", hub.Prefix)
}

func TestParseHubURI_File(t *testing.T) {
	hub, err := parseHubURI("file:///data/index-hub/")
	require.NoError(t, err)
	assert.Equal(t, "file", hub.Provider)
	assert.Equal(t, "/data/index-hub", hub.BaseDir)
}

func TestParseHubURI_FileNoTrailingSlash(t *testing.T) {
	hub, err := parseHubURI("file:///data/index-hub")
	require.NoError(t, err)
	assert.Equal(t, "/data/index-hub", hub.BaseDir)
}

func TestParseHubURI_Empty(t *testing.T) {
	_, err := parseHubURI("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "hub URI is required")
}

func TestParseHubURI_UnsupportedScheme(t *testing.T) {
	_, err := parseHubURI("gs://bucket/prefix/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported hub scheme")
}

func TestParseHubURI_S3NoBucket(t *testing.T) {
	_, err := parseHubURI("s3:///prefix/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing bucket")
}

func TestParseHubURI_S3NoPrefix(t *testing.T) {
	_, err := parseHubURI("s3://bucket")
	require.NoError(t, err) // trailing slash added, prefix becomes ""
}

func TestParseHubURI_FileRelative(t *testing.T) {
	_, err := parseHubURI("file://relative/path")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be absolute")
}

// --- hubArtifactKey ---

func TestHubArtifactKey(t *testing.T) {
	hub := &hubDestSpec{Prefix: "ops/index-hub/"}
	key := hubArtifactKey(hub, "index-sets", "idx_abc", "runs", "run_123", "index.db")
	assert.Equal(t, "ops/index-hub/index-sets/idx_abc/runs/run_123/index.db", key)
}

func TestHubArtifactKey_EmptyPrefix(t *testing.T) {
	hub := &hubDestSpec{Prefix: ""}
	key := hubArtifactKey(hub, "index-sets", "idx_abc", "latest.json")
	assert.Equal(t, "index-sets/idx_abc/latest.json", key)
}

// --- buildCompleteJSON ---

func TestBuildCompleteJSON_Basic(t *testing.T) {
	now := time.Now().UTC()
	endedAt := now.Add(5 * time.Minute)
	indexSet := &indexstore.IndexSet{
		IndexSetID: "idx_" + "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		BaseURI:    "s3://bucket/prefix/",
		Provider:   "s3",
	}
	run := &indexstore.IndexRun{
		RunID:      "run_1709654400000000000",
		IndexSetID: indexSet.IndexSetID,
		StartedAt:  now,
		EndedAt:    &endedAt,
		Status:     indexstore.RunStatusSuccess,
	}
	summary := &indexstore.IndexSetSummary{
		ActiveObjects:  42,
		TotalSizeBytes: 1024,
	}

	data, err := buildCompleteJSON(indexSet, run, summary, "deadbeef"+"deadbeef"+"deadbeef"+"deadbeef"+"deadbeef"+"deadbeef"+"deadbeef"+"deadbeef", 4096, "", 0)
	require.NoError(t, err)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &doc))

	assert.Equal(t, "1.0", doc["version"])
	assert.Equal(t, indexSet.IndexSetID, doc["index_set_id"])
	assert.Equal(t, run.RunID, doc["run_id"])
	assert.NotEmpty(t, doc["completed_at"])
	assert.NotEmpty(t, doc["exported_by"])

	artifacts := doc["artifacts"].(map[string]interface{})
	indexDB := artifacts["index_db"].(map[string]interface{})
	assert.Equal(t, float64(4096), indexDB["size_bytes"])
	assert.Nil(t, artifacts["identity_json"]) // no identity provided

	source := doc["source"].(map[string]interface{})
	assert.Equal(t, "s3://bucket/prefix/", source["base_uri"])
	assert.Equal(t, "success", source["run_status"])
	assert.Equal(t, float64(42), source["object_count"])
	assert.Equal(t, float64(1024), source["total_size_bytes"])
	assert.NotEmpty(t, source["run_ended_at"])
}

func TestBuildCompleteJSON_WithIdentity(t *testing.T) {
	indexSet := &indexstore.IndexSet{
		IndexSetID: "idx_" + "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		BaseURI:    "s3://bucket/prefix/",
		Provider:   "s3",
	}
	run := &indexstore.IndexRun{
		RunID:      "run_1709654400000000000",
		IndexSetID: indexSet.IndexSetID,
		StartedAt:  time.Now().UTC(),
		Status:     indexstore.RunStatusSuccess,
	}

	identitySHA := "cafecafe" + "cafecafe" + "cafecafe" + "cafecafe" + "cafecafe" + "cafecafe" + "cafecafe" + "cafecafe"
	data, err := buildCompleteJSON(indexSet, run, nil, "deadbeef"+"deadbeef"+"deadbeef"+"deadbeef"+"deadbeef"+"deadbeef"+"deadbeef"+"deadbeef", 4096, identitySHA, 256)
	require.NoError(t, err)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &doc))

	artifacts := doc["artifacts"].(map[string]interface{})
	identityJSON := artifacts["identity_json"].(map[string]interface{})
	assert.Equal(t, float64(256), identityJSON["size_bytes"])
	assert.Equal(t, identitySHA, identityJSON["sha256"])
}

func TestBuildCompleteJSON_NoSummary(t *testing.T) {
	indexSet := &indexstore.IndexSet{
		IndexSetID: "idx_" + "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		BaseURI:    "s3://bucket/prefix/",
		Provider:   "s3",
	}
	run := &indexstore.IndexRun{
		RunID:      "run_1709654400000000000",
		IndexSetID: indexSet.IndexSetID,
		StartedAt:  time.Now().UTC(),
		Status:     indexstore.RunStatusPartial,
	}

	data, err := buildCompleteJSON(indexSet, run, nil, "deadbeef"+"deadbeef"+"deadbeef"+"deadbeef"+"deadbeef"+"deadbeef"+"deadbeef"+"deadbeef", 4096, "", 0)
	require.NoError(t, err)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &doc))

	source := doc["source"].(map[string]interface{})
	assert.Equal(t, "partial", source["run_status"])
	assert.Nil(t, source["object_count"])
	assert.Nil(t, source["total_size_bytes"])
}

// --- buildLatestJSON ---

func TestBuildLatestJSON(t *testing.T) {
	indexSet := &indexstore.IndexSet{
		IndexSetID: "idx_" + "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
	}
	run := &indexstore.IndexRun{
		RunID: "run_1709654400000000000",
	}

	data, err := buildLatestJSON(indexSet, run)
	require.NoError(t, err)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &doc))

	assert.Equal(t, "1.0", doc["version"])
	assert.Equal(t, indexSet.IndexSetID, doc["index_set_id"])
	assert.Equal(t, run.RunID, doc["run_id"])
	assert.NotEmpty(t, doc["updated_at"])
	assert.NotEmpty(t, doc["updated_by"])
}

// --- exportableRunStatus ---

func TestExportableRunStatus_Valid(t *testing.T) {
	tests := []struct {
		status   indexstore.RunStatus
		expected string
	}{
		{indexstore.RunStatusSuccess, "success"},
		{indexstore.RunStatusPartial, "partial"},
		{indexstore.RunStatusFailed, "failed"},
		{indexstore.RunStatusFailedResumable, "failed-resumable"},
	}
	for _, tt := range tests {
		s, err := exportableRunStatus(tt.status)
		require.NoError(t, err)
		assert.Equal(t, tt.expected, s)
	}
}

func TestExportableRunStatus_Unknown(t *testing.T) {
	_, err := exportableRunStatus("unknown_status")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported run status")
}

// --- matchIndexSetInDB ---

func TestMatchIndexSetInDB_ExactMatch(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	t.Cleanup(func() {
		_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		_ = db.Close()
	})

	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, testIndexSetParams("s3://bucket/prefix/"))
	require.NoError(t, err)

	cleanHex := indexSet.IndexSetID[4:] // strip idx_
	matched, err := matchIndexSetInDB(ctx, db, cleanHex)
	require.NoError(t, err)
	assert.Equal(t, indexSet.IndexSetID, matched.IndexSetID)
}

func TestMatchIndexSetInDB_PrefixMatch(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	t.Cleanup(func() {
		_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		_ = db.Close()
	})

	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, testIndexSetParams("s3://bucket/prefix/"))
	require.NoError(t, err)

	cleanHex := indexSet.IndexSetID[4:8] // first 4 hex chars
	matched, err := matchIndexSetInDB(ctx, db, cleanHex)
	require.NoError(t, err)
	assert.Equal(t, indexSet.IndexSetID, matched.IndexSetID)
}

func TestMatchIndexSetInDB_NoMatch(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	t.Cleanup(func() {
		_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		_ = db.Close()
	})

	_, _, err = indexstore.FindOrCreateIndexSet(ctx, db, testIndexSetParams("s3://bucket/prefix/"))
	require.NoError(t, err)

	_, err = matchIndexSetInDB(ctx, db, "0000000000000000")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no matching index set")
}

// --- resolveExportRun ---

func TestResolveExportRun_LatestSuccessful(t *testing.T) {
	ctx := context.Background()

	dbPath := filepath.Join(t.TempDir(), "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))

	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, testIndexSetParams("s3://bucket/prefix/"))
	require.NoError(t, err)

	// Failed run
	run1, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run1.RunID, indexstore.RunStatusFailed, nil))

	// Successful run
	run2, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run2.RunID, indexstore.RunStatusSuccess, nil))

	// Partial run (most recent)
	run3, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run3.RunID, indexstore.RunStatusPartial, nil))

	t.Cleanup(func() {
		_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		_ = db.Close()
	})

	// Should pick the successful run, not the partial
	resolved, err := resolveExportRun(ctx, db, indexSet.IndexSetID, "")
	require.NoError(t, err)
	assert.Equal(t, run2.RunID, resolved.RunID)
}

func TestResolveExportRun_ExplicitRunID(t *testing.T) {
	ctx := context.Background()

	dbPath := filepath.Join(t.TempDir(), "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))

	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, testIndexSetParams("s3://bucket/prefix/"))
	require.NoError(t, err)

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusSuccess, nil))

	t.Cleanup(func() {
		_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		_ = db.Close()
	})

	resolved, err := resolveExportRun(ctx, db, indexSet.IndexSetID, run.RunID)
	require.NoError(t, err)
	assert.Equal(t, run.RunID, resolved.RunID)
}

func TestResolveExportRun_WrongIndexSet(t *testing.T) {
	ctx := context.Background()

	dbPath := filepath.Join(t.TempDir(), "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))

	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, testIndexSetParams("s3://bucket/prefix/"))
	require.NoError(t, err)

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusSuccess, nil))

	t.Cleanup(func() {
		_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		_ = db.Close()
	})

	_, err = resolveExportRun(ctx, db, "idx_wrong", run.RunID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "belongs to index set")
}

func TestResolveExportRun_FallbackToPartial(t *testing.T) {
	ctx := context.Background()

	dbPath := filepath.Join(t.TempDir(), "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))

	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, testIndexSetParams("s3://bucket/prefix/"))
	require.NoError(t, err)

	// Only failed and partial — no success
	run1, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run1.RunID, indexstore.RunStatusFailed, nil))

	run2, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run2.RunID, indexstore.RunStatusPartial, nil))

	t.Cleanup(func() {
		_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		_ = db.Close()
	})

	resolved, err := resolveExportRun(ctx, db, indexSet.IndexSetID, "")
	require.NoError(t, err)
	assert.Equal(t, run2.RunID, resolved.RunID)
}

func TestResolveExportRun_NoExportableRun(t *testing.T) {
	ctx := context.Background()

	dbPath := filepath.Join(t.TempDir(), "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))

	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, testIndexSetParams("s3://bucket/prefix/"))
	require.NoError(t, err)

	// Only failed runs
	run1, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run1.RunID, indexstore.RunStatusFailed, nil))

	t.Cleanup(func() {
		_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		_ = db.Close()
	})

	_, err = resolveExportRun(ctx, db, indexSet.IndexSetID, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no exportable run found")
}

// --- hashFile ---

func TestHashFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "testfile")
	content := []byte("hello world")
	require.NoError(t, os.WriteFile(path, content, 0644))

	h := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(h[:])

	hash, size, err := hashFile(path)
	require.NoError(t, err)
	assert.Equal(t, expectedHash, hash)
	assert.Equal(t, int64(len(content)), size)
}

func TestHashFile_NotFound(t *testing.T) {
	_, _, err := hashFile("/nonexistent/file")
	require.Error(t, err)
}

// --- exportedByString ---

func TestExportedByString(t *testing.T) {
	s := exportedByString()
	assert.Contains(t, s, "gonimbus/")
}

// --- integration: command-level file hub export ---

func TestRunIndexExport_FileHub(t *testing.T) {
	ctx := context.Background()

	// Set up a local index with a successful run
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

	// Write identity.json next to index.db
	identityContent := []byte(`{"test": "identity"}`)
	require.NoError(t, os.WriteFile(filepath.Join(idxDir, "identity.json"), identityContent, 0644))

	_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, db.Close())

	// Pre-compute expected checksums for verification
	dbChecksum, dbSize, err := hashFile(dbPath)
	require.NoError(t, err)
	idH := sha256.Sum256(identityContent)
	identityChecksum := hex.EncodeToString(idH[:])

	// Set up file hub destination
	hubDir := t.TempDir()
	hubURI := "file://" + hubDir + "/"

	// Invoke runIndexExport via the cobra command (covers flag wiring)
	cmd := &cobra.Command{Use: "export", RunE: runIndexExport}
	cmd.Flags().String("hub", "", "")
	cmd.Flags().String("index-set", "", "")
	cmd.Flags().String("run-id", "", "")
	cmd.Flags().String("db", "", "")
	cmd.Flags().String("hub-profile", "", "")
	cmd.Flags().String("hub-region", "", "")
	cmd.Flags().String("hub-endpoint", "", "")
	addLatestPointerFlags(cmd)

	cmd.SetArgs([]string{
		"--hub", hubURI,
		"--index-set", indexSet.IndexSetID,
		"--run-id", run.RunID,
		"--db", dbPath,
	})
	cmd.SetContext(ctx)

	require.NoError(t, cmd.Execute())

	// Verify all hub artifacts exist
	runDir := filepath.Join(hubDir, "index-sets", indexSet.IndexSetID, "runs", run.RunID)
	assert.FileExists(t, filepath.Join(runDir, "index.db"))
	assert.FileExists(t, filepath.Join(runDir, "identity.json"))
	assert.FileExists(t, filepath.Join(runDir, "complete.json"))
	assert.FileExists(t, filepath.Join(hubDir, "index-sets", indexSet.IndexSetID, "latest.json"))

	// Verify complete.json content and schema conformance
	completeData, err := os.ReadFile(filepath.Join(runDir, "complete.json"))
	require.NoError(t, err)
	var completeDoc map[string]interface{}
	require.NoError(t, json.Unmarshal(completeData, &completeDoc))
	assert.Equal(t, "1.0", completeDoc["version"])
	assert.Equal(t, indexSet.IndexSetID, completeDoc["index_set_id"])
	assert.Equal(t, run.RunID, completeDoc["run_id"])
	assert.NotEmpty(t, completeDoc["completed_at"])
	assert.NotEmpty(t, completeDoc["exported_by"])

	artifacts := completeDoc["artifacts"].(map[string]interface{})
	indexDBEntry := artifacts["index_db"].(map[string]interface{})
	assert.Equal(t, dbChecksum, indexDBEntry["sha256"])
	assert.Equal(t, float64(dbSize), indexDBEntry["size_bytes"])

	identityEntry := artifacts["identity_json"].(map[string]interface{})
	assert.Equal(t, identityChecksum, identityEntry["sha256"])

	source := completeDoc["source"].(map[string]interface{})
	assert.Equal(t, "s3://bucket/prefix/", source["base_uri"])
	assert.Equal(t, "success", source["run_status"])

	// Verify latest.json points to the exported run
	latestData, err := os.ReadFile(filepath.Join(hubDir, "index-sets", indexSet.IndexSetID, "latest.json"))
	require.NoError(t, err)
	var latestDoc map[string]interface{}
	require.NoError(t, json.Unmarshal(latestData, &latestDoc))
	assert.Equal(t, "1.0", latestDoc["version"])
	assert.Equal(t, indexSet.IndexSetID, latestDoc["index_set_id"])
	assert.Equal(t, run.RunID, latestDoc["run_id"])

	// Verify exported index.db integrity (byte-for-byte match)
	hubDBChecksum, hubDBSize, err := hashFile(filepath.Join(runDir, "index.db"))
	require.NoError(t, err)
	assert.Equal(t, dbChecksum, hubDBChecksum)
	assert.Equal(t, dbSize, hubDBSize)
}

// --- helpers ---

func testIndexSetParams(baseURI string) indexstore.IndexSetParams {
	return indexstore.IndexSetParams{
		BaseURI:         baseURI,
		Provider:        "s3",
		StorageProvider: "aws_s3",
		CloudProvider:   "aws",
		RegionKind:      "aws",
		Region:          "us-east-1",
		BuildParams: indexstore.BuildParams{
			SourceType:      "crawl",
			SchemaVersion:   indexstore.SchemaVersion,
			GonimbusVersion: "test",
			Includes:        []string{"**"},
		},
	}
}
