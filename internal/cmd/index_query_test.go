package cmd

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

// createTestIndex creates a minimal index DB in a temp directory and returns the identity.
func createTestIndex(t *testing.T, root string, baseURI string) *indexstore.IndexSetIdentityResult {
	t.Helper()
	return createTestIndexWithIncludes(t, root, baseURI, []string{"**"})
}

// createTestIndexWithIncludes creates a minimal index DB with custom includes (for distinct hashes).
func createTestIndexWithIncludes(t *testing.T, root string, baseURI string, includes []string) *indexstore.IndexSetIdentityResult {
	t.Helper()

	params := indexstore.IndexSetParams{
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
			Includes:        includes,
		},
	}

	identity, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)

	idxDir := filepath.Join(root, identity.DirName)
	require.NoError(t, os.MkdirAll(idxDir, 0755))

	dbPath := filepath.Join(idxDir, "index.db")
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: dbPath})
	require.NoError(t, err)

	require.NoError(t, indexstore.Migrate(context.Background(), db))
	_, _, err = indexstore.FindOrCreateIndexSet(context.Background(), db, params)
	require.NoError(t, err)

	// Checkpoint WAL before closing so the DB file is self-contained.
	_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, db.Close())

	return identity
}

func createLegacyV4TestIndex(t *testing.T, root string, baseURI string) *indexstore.IndexSetIdentityResult {
	t.Helper()

	params := indexstore.IndexSetParams{
		BaseURI:         baseURI,
		Provider:        "s3",
		StorageProvider: "aws_s3",
		CloudProvider:   "aws",
		RegionKind:      "aws",
		Region:          "us-east-1",
		BuildParams: indexstore.BuildParams{
			SourceType:      "crawl",
			SchemaVersion:   4,
			GonimbusVersion: "legacy-test",
			Includes:        []string{"**"},
		},
	}

	identity, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)

	idxDir := filepath.Join(root, identity.DirName)
	require.NoError(t, os.MkdirAll(idxDir, 0755))

	db, err := indexstore.Open(context.Background(), indexstore.Config{
		Path: filepath.Join(idxDir, "index.db"),
	})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	legacySchema := []string{
		`CREATE TABLE schema_meta (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			schema_version INTEGER NOT NULL
		);`,
		`INSERT INTO schema_meta (id, schema_version) VALUES (1, 4);`,
		`CREATE TABLE index_sets (
			index_set_id TEXT PRIMARY KEY,
			base_uri TEXT NOT NULL,
			provider TEXT NOT NULL,
			storage_provider TEXT,
			cloud_provider TEXT,
			region_kind TEXT,
			region TEXT,
			endpoint TEXT,
			endpoint_host TEXT,
			index_build_hash TEXT NOT NULL,
			created_at TEXT NOT NULL
		);`,
		`CREATE TABLE index_runs (
			run_id TEXT PRIMARY KEY,
			index_set_id TEXT NOT NULL,
			started_at TEXT NOT NULL,
			ended_at TEXT,
			acquired_at TEXT NOT NULL,
			source_type TEXT NOT NULL,
			source_snapshot_at TEXT,
			status TEXT NOT NULL,
			FOREIGN KEY(index_set_id) REFERENCES index_sets(index_set_id)
		);`,
		`CREATE TABLE objects_current (
			index_set_id TEXT NOT NULL,
			rel_key TEXT NOT NULL,
			size_bytes INTEGER NOT NULL,
			last_modified TEXT,
			etag TEXT,
			last_seen_run_id TEXT NOT NULL,
			last_seen_at TEXT NOT NULL,
			deleted_at TEXT,
			PRIMARY KEY(index_set_id, rel_key),
			FOREIGN KEY(index_set_id) REFERENCES index_sets(index_set_id),
			FOREIGN KEY(last_seen_run_id) REFERENCES index_runs(run_id)
		);`,
		`CREATE TABLE prefix_stats (
			index_set_id TEXT NOT NULL,
			run_id TEXT NOT NULL,
			prefix TEXT NOT NULL,
			depth INTEGER NOT NULL,
			objects_direct INTEGER NOT NULL,
			bytes_direct INTEGER NOT NULL,
			common_prefixes INTEGER NOT NULL,
			truncated INTEGER NOT NULL,
			truncated_reason TEXT,
			PRIMARY KEY(index_set_id, run_id, prefix),
			FOREIGN KEY(index_set_id) REFERENCES index_sets(index_set_id),
			FOREIGN KEY(run_id) REFERENCES index_runs(run_id)
		);`,
		`CREATE TABLE index_run_events (
			event_id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL,
			occurred_at TEXT NOT NULL,
			event_type TEXT NOT NULL,
			event_category TEXT NOT NULL,
			detail TEXT,
			key TEXT,
			prefix TEXT,
			error_code TEXT,
			FOREIGN KEY(run_id) REFERENCES index_runs(run_id)
		);`,
	}
	for _, stmt := range legacySchema {
		_, err = db.ExecContext(context.Background(), stmt)
		require.NoError(t, err)
	}

	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	_, err = db.ExecContext(context.Background(), `
		INSERT INTO index_sets (
			index_set_id, base_uri, provider, storage_provider, cloud_provider,
			region_kind, region, endpoint, endpoint_host, index_build_hash, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		identity.IndexSetID, baseURI, "s3", "aws_s3", "aws",
		"aws", "us-east-1", "", "", "legacy-hash", now.Format(time.RFC3339),
	)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `
		INSERT INTO index_runs (
			run_id, index_set_id, started_at, ended_at, acquired_at,
			source_type, source_snapshot_at, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
		"run_legacy", identity.IndexSetID, now.Format(time.RFC3339), now.Format(time.RFC3339), now.Format(time.RFC3339),
		"crawl", nil, string(indexstore.RunStatusSuccess),
	)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `
		INSERT INTO objects_current (
			index_set_id, rel_key, size_bytes, last_modified, etag,
			last_seen_run_id, last_seen_at, deleted_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, NULL);
	`,
		identity.IndexSetID, "legacy-object.xml", int64(123), now.Format(time.RFC3339), "etag-legacy",
		"run_legacy", now.Format(time.RFC3339),
	)
	require.NoError(t, err)

	_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	return identity
}

func TestOpenIndexDBByIDInRoot_ExactDirName(t *testing.T) {
	root := t.TempDir()
	identity := createTestIndex(t, root, "s3://bucket/prefix/")

	db, indexSet, err := openIndexDBByIDInRoot(context.Background(), root, identity.DirName)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.Equal(t, identity.IndexSetID, indexSet.IndexSetID)
	require.Equal(t, "s3://bucket/prefix/", indexSet.BaseURI)
}

func TestOpenIndexDBByIDInRoot_FullID(t *testing.T) {
	root := t.TempDir()
	identity := createTestIndex(t, root, "s3://bucket/prefix/")

	db, indexSet, err := openIndexDBByIDInRoot(context.Background(), root, identity.IndexSetID)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.Equal(t, identity.IndexSetID, indexSet.IndexSetID)
}

func TestOpenIndexDBByIDInRoot_PartialHex(t *testing.T) {
	root := t.TempDir()
	identity := createTestIndex(t, root, "s3://bucket/prefix/")

	// Use first 8 chars of the hex (without idx_ prefix)
	partialHex := identity.CanonicalSHA256[:8]
	db, indexSet, err := openIndexDBByIDInRoot(context.Background(), root, partialHex)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.Equal(t, identity.IndexSetID, indexSet.IndexSetID)
}

func TestOpenIndexDBByIDInRoot_NotFound(t *testing.T) {
	root := t.TempDir()

	_, _, err := openIndexDBByIDInRoot(context.Background(), root, "idx_0000000000000000")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no index found matching ID")
}

func TestOpenIndexDBByIDInRoot_EmptyID(t *testing.T) {
	root := t.TempDir()

	_, _, err := openIndexDBByIDInRoot(context.Background(), root, "idx_")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid index set ID")
}

func TestOpenIndexDBByIDInRoot_MissingRoot(t *testing.T) {
	_, _, err := openIndexDBByIDInRoot(context.Background(), "/nonexistent/path", "idx_abc123")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no indexes found")
}

func TestOpenIndexDBByIDInRoot_NonHexID(t *testing.T) {
	root := t.TempDir()

	_, _, err := openIndexDBByIDInRoot(context.Background(), root, "idx_ZZZZ")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid index set ID")
	require.Contains(t, err.Error(), "must be hex")
}

func TestOpenIndexDBByIDInRoot_OverlongID(t *testing.T) {
	root := t.TempDir()

	// 65 hex chars exceeds the 64-char max for SHA-256
	overlong := "idx_" + "a" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	_, _, err := openIndexDBByIDInRoot(context.Background(), root, overlong)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid index set ID")
}

func TestOpenIndexDBByIDInRoot_OverlongSuffixNoMatch(t *testing.T) {
	root := t.TempDir()
	identity := createTestIndex(t, root, "s3://bucket/prefix/")

	// Valid prefix + extra chars: should NOT match (old code would match via reverse prefix)
	badID := identity.DirName + "ffffffff"
	_, _, err := openIndexDBByIDInRoot(context.Background(), root, badID)
	require.Error(t, err)
	// The overlong input (16 + 8 = 24 hex chars) is still valid hex under 64,
	// but should not match because the dir hex doesn't start with the full cleanID.
	require.Contains(t, err.Error(), "no index found matching ID")
}

func TestOpenIndexDBByIDInRoot_AmbiguousPrefix(t *testing.T) {
	root := t.TempDir()

	// Create one real index, then plant a second empty dir that shares the same prefix.
	identity := createTestIndex(t, root, "s3://bucket/prefix/")
	dirHex := strings.TrimPrefix(identity.DirName, "idx_")

	// Create a sibling dir with the same prefix + different suffix.
	sibling := "idx_" + dirHex + "ff"
	siblingDir := filepath.Join(root, sibling)
	require.NoError(t, os.MkdirAll(siblingDir, 0755))

	// Plant a DB so the dir counts as a match.
	sibDB, err := indexstore.Open(context.Background(), indexstore.Config{
		Path: filepath.Join(siblingDir, "index.db"),
	})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(context.Background(), sibDB))
	_, _ = sibDB.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, sibDB.Close())

	// A prefix that matches both dirs should fail with ambiguity.
	_, _, err = openIndexDBByIDInRoot(context.Background(), root, dirHex[:8])
	require.Error(t, err)
	require.Contains(t, err.Error(), "ambiguous index ID")
	require.Contains(t, err.Error(), identity.DirName)
	require.Contains(t, err.Error(), sibling)
}

func TestOpenIndexDBByIDInRoot_BaseURIFromIndex(t *testing.T) {
	root := t.TempDir()
	identity := createTestIndex(t, root, "s3://my-bucket/data/")

	db, indexSet, err := openIndexDBByIDInRoot(context.Background(), root, identity.DirName)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// The returned IndexSet carries the authoritative base_uri from the DB.
	require.Equal(t, "s3://my-bucket/data/", indexSet.BaseURI)
}

func TestOpenIndexDBByIDInRoot_MigratesLegacyV4ForQuery(t *testing.T) {
	root := t.TempDir()
	identity := createLegacyV4TestIndex(t, root, "s3://bucket/prefix/")

	db, indexSet, err := openIndexDBByIDInRoot(context.Background(), root, identity.DirName)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.Equal(t, identity.IndexSetID, indexSet.IndexSetID)

	var version int
	err = db.QueryRowContext(context.Background(), `SELECT schema_version FROM schema_meta WHERE id = 1`).Scan(&version)
	require.NoError(t, err)
	require.Equal(t, indexstore.SchemaVersion, version)

	var columnCount int
	err = db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM pragma_table_info('objects_current') WHERE name = 'storage_class'`).Scan(&columnCount)
	require.NoError(t, err)
	require.Equal(t, 1, columnCount)

	results, _, err := indexstore.QueryObjects(context.Background(), db, indexstore.QueryParams{
		IndexSetID: indexSet.IndexSetID,
		Limit:      1,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "legacy-object.xml", results[0].RelKey)
	require.Nil(t, results[0].StorageClass)
}

func TestIndexCanonicalQueryRecordShapeIncludesAlternateSizeBytes(t *testing.T) {
	lastModified := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	deletedAt := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	group := indexstore.CanonicalObjectGroup{
		ETag: "etag-shared",
		Canonical: indexstore.QueryResult{
			RelKey:       "canonical.xml",
			SizeBytes:    123,
			LastModified: &lastModified,
			ETag:         "etag-shared",
			StorageClass: stringPtr("GLACIER"),
		},
		Alternates: []indexstore.QueryResult{{
			RelKey:       "alternate.xml",
			SizeBytes:    456,
			LastModified: &lastModified,
			ETag:         "etag-shared",
			DeletedAt:    &deletedAt,
			StorageClass: stringPtr("STANDARD"),
		}},
	}

	record := newIndexCanonicalQueryRecord("s3://bucket/prefix/", "2026-05-19T12:00:00Z", group, indexstore.CanonicalTieBreakMinKey, true)
	b, err := json.Marshal(record)
	require.NoError(t, err)

	require.Contains(t, string(b), `"type":"gonimbus.index.object.canonical.v1"`)
	require.Contains(t, string(b), `"key":"prefix/canonical.xml"`)
	require.Contains(t, string(b), `"storage_class":"GLACIER"`)
	require.Contains(t, string(b), `"alternates_count":1`)
	require.Contains(t, string(b), `"size_bytes":456`)
	require.Contains(t, string(b), `"storage_class":"STANDARD"`)
	require.Contains(t, string(b), `"deleted_at":"2026-05-20T12:00:00Z"`)

	withoutAlternates := newIndexCanonicalQueryRecord("s3://bucket/prefix/", "2026-05-19T12:00:00Z", group, indexstore.CanonicalTieBreakMinKey, false)
	b, err = json.Marshal(withoutAlternates)
	require.NoError(t, err)
	require.Contains(t, string(b), `"alternates_count":1`)
	require.NotContains(t, string(b), `"alternates":[`)
}

func TestIndexQueryHelpDocumentsCanonicalETagCaveat(t *testing.T) {
	help := strings.Join(strings.Fields(indexQueryCmd.Long), " ")

	require.Contains(t, help, "--canonical-by-etag")
	require.Contains(t, help, "ETag is a provider version/fingerprint hint")
	require.Contains(t, help, "not a universal content hash")
	require.Contains(t, help, "docs/user-guide/index-build-mental-model.md")
}

func TestParseStorageClassFilterValues(t *testing.T) {
	values, err := parseStorageClassFilterValues([]string{"STANDARD, GLACIER", "DEEP_ARCHIVE"})
	require.NoError(t, err)
	require.Equal(t, []string{"STANDARD", "GLACIER", "DEEP_ARCHIVE"}, values)

	_, err = parseStorageClassFilterValues([]string{"STANDARD,,GLACIER"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty value")
}

func TestIndexQueryRecordOmitEmptyStorageClass(t *testing.T) {
	record := newIndexQueryRecord("s3://bucket/prefix/", "2026-05-19T12:00:00Z", indexstore.QueryResult{
		RelKey:    "missing.txt",
		SizeBytes: 1,
	})
	b, err := json.Marshal(record)
	require.NoError(t, err)
	require.NotContains(t, string(b), "storage_class")
}

func TestIndexQueryRecordIncludesStorageClass(t *testing.T) {
	record := newIndexQueryRecord("s3://bucket/prefix/", "2026-05-19T12:00:00Z", indexstore.QueryResult{
		RelKey:       "archive.txt",
		SizeBytes:    1,
		StorageClass: stringPtr("DEEP_ARCHIVE"),
	})
	b, err := json.Marshal(record)
	require.NoError(t, err)
	require.Contains(t, string(b), `"storage_class":"DEEP_ARCHIVE"`)
}

func stringPtr(value string) *string {
	return &value
}
