package indexstore

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpsertObject(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))

	// Create an index set first
	params := IndexSetParams{
		BaseURI:  "s3://test-bucket/data/",
		Provider: "s3",
		BuildParams: BuildParams{
			SourceType:    "crawl",
			SchemaVersion: SchemaVersion,
		},
	}
	indexSet, _, err := FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)

	// Create an index run
	run, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	now := time.Now().UTC()
	lastMod := now.Add(-24 * time.Hour)

	t.Run("insert new object", func(t *testing.T) {
		storageClass := "STANDARD_IA"
		obj := ObjectRow{
			IndexSetID:    indexSet.IndexSetID,
			RelKey:        "path/to/file.txt",
			SizeBytes:     1024,
			LastModified:  &lastMod,
			ETag:          "abc123",
			StorageClass:  &storageClass,
			LastSeenRunID: run.RunID,
			LastSeenAt:    run.StartedAt,
		}

		err := UpsertObject(ctx, db, obj)
		require.NoError(t, err)

		// Verify it was inserted
		retrieved, err := GetObject(ctx, db, indexSet.IndexSetID, "path/to/file.txt")
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.Equal(t, int64(1024), retrieved.SizeBytes)
		assert.Equal(t, "abc123", retrieved.ETag)
		require.NotNil(t, retrieved.StorageClass)
		assert.Equal(t, "STANDARD_IA", *retrieved.StorageClass)
		assert.Nil(t, retrieved.DeletedAt)
	})

	t.Run("update existing object", func(t *testing.T) {
		// Update with new size
		obj := ObjectRow{
			IndexSetID:    indexSet.IndexSetID,
			RelKey:        "path/to/file.txt",
			SizeBytes:     2048, // changed
			LastModified:  &lastMod,
			ETag:          "def456", // changed
			LastSeenRunID: run.RunID,
			LastSeenAt:    run.StartedAt,
		}

		err := UpsertObject(ctx, db, obj)
		require.NoError(t, err)

		retrieved, err := GetObject(ctx, db, indexSet.IndexSetID, "path/to/file.txt")
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.Equal(t, int64(2048), retrieved.SizeBytes)
		assert.Equal(t, "def456", retrieved.ETag)
		assert.Nil(t, retrieved.StorageClass)
	})
}

func TestBatchUpsertObjects(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))

	params := IndexSetParams{
		BaseURI:  "s3://test-bucket/data/",
		Provider: "s3",
		BuildParams: BuildParams{
			SourceType:    "crawl",
			SchemaVersion: SchemaVersion,
		},
	}
	indexSet, _, err := FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)

	run, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	objects := []ObjectRow{
		{IndexSetID: indexSet.IndexSetID, RelKey: "file1.txt", SizeBytes: 100, LastSeenRunID: run.RunID, LastSeenAt: run.StartedAt},
		{IndexSetID: indexSet.IndexSetID, RelKey: "file2.txt", SizeBytes: 200, LastSeenRunID: run.RunID, LastSeenAt: run.StartedAt},
		{IndexSetID: indexSet.IndexSetID, RelKey: "file3.txt", SizeBytes: 300, LastSeenRunID: run.RunID, LastSeenAt: run.StartedAt},
	}

	err = BatchUpsertObjects(ctx, db, objects)
	require.NoError(t, err)

	count, err := CountObjects(ctx, db, indexSet.IndexSetID, false)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

func TestBatchUpdateHeadEnrichmentPreservesListFields(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))

	params := IndexSetParams{
		BaseURI:  "s3://test-bucket/data/",
		Provider: "s3",
		BuildParams: BuildParams{
			SourceType:    "crawl",
			SchemaVersion: SchemaVersion,
		},
	}
	indexSet, _, err := FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)
	run, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	lastModified := time.Date(2026, 5, 25, 10, 0, 0, 0, time.UTC)
	storageClass := "GLACIER"
	require.NoError(t, UpsertObject(ctx, db, ObjectRow{
		IndexSetID:    indexSet.IndexSetID,
		RelKey:        "file.txt",
		SizeBytes:     123,
		LastModified:  &lastModified,
		ETag:          "etag-list",
		StorageClass:  &storageClass,
		LastSeenRunID: run.RunID,
		LastSeenAt:    run.StartedAt,
	}))

	archiveStatus := "DEEP_ARCHIVE_ACCESS"
	restoreState := "completed"
	contentType := "application/xml"
	restoreExpiry := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	enrichedAt := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	require.NoError(t, BatchUpdateHeadEnrichment(ctx, db, []HeadEnrichmentUpdate{{
		IndexSetID:     indexSet.IndexSetID,
		RelKey:         "file.txt",
		ArchiveStatus:  &archiveStatus,
		RestoreState:   &restoreState,
		RestoreExpiry:  &restoreExpiry,
		ContentType:    &contentType,
		HeadEnrichedAt: enrichedAt,
	}}))

	retrieved, err := GetObject(ctx, db, indexSet.IndexSetID, "file.txt")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, int64(123), retrieved.SizeBytes)
	require.Equal(t, "etag-list", retrieved.ETag)
	require.NotNil(t, retrieved.StorageClass)
	require.Equal(t, "GLACIER", *retrieved.StorageClass)
	require.NotNil(t, retrieved.ArchiveStatus)
	require.Equal(t, "DEEP_ARCHIVE_ACCESS", *retrieved.ArchiveStatus)
	require.NotNil(t, retrieved.RestoreState)
	require.Equal(t, "completed", *retrieved.RestoreState)
	require.NotNil(t, retrieved.RestoreExpiry)
	require.Equal(t, restoreExpiry, *retrieved.RestoreExpiry)
	require.NotNil(t, retrieved.ContentType)
	require.Equal(t, "application/xml", *retrieved.ContentType)
	require.NotNil(t, retrieved.HeadEnrichedAt)
	require.Equal(t, enrichedAt, *retrieved.HeadEnrichedAt)
}

func TestCountObjects(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))

	params := IndexSetParams{
		BaseURI:  "s3://test-bucket/data/",
		Provider: "s3",
		BuildParams: BuildParams{
			SourceType:    "crawl",
			SchemaVersion: SchemaVersion,
		},
	}
	indexSet, _, err := FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)

	run, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	// Insert some objects
	objects := []ObjectRow{
		{IndexSetID: indexSet.IndexSetID, RelKey: "file1.txt", SizeBytes: 100, LastSeenRunID: run.RunID, LastSeenAt: run.StartedAt},
		{IndexSetID: indexSet.IndexSetID, RelKey: "file2.txt", SizeBytes: 200, LastSeenRunID: run.RunID, LastSeenAt: run.StartedAt},
	}
	err = BatchUpsertObjects(ctx, db, objects)
	require.NoError(t, err)

	t.Run("count all", func(t *testing.T) {
		count, err := CountObjects(ctx, db, indexSet.IndexSetID, true)
		require.NoError(t, err)
		assert.Equal(t, int64(2), count)
	})

	t.Run("count non-deleted", func(t *testing.T) {
		count, err := CountObjects(ctx, db, indexSet.IndexSetID, false)
		require.NoError(t, err)
		assert.Equal(t, int64(2), count)
	})
}

func TestDeriveRelKey(t *testing.T) {
	tests := []struct {
		name     string
		baseURI  string
		fullKey  string
		expected string
	}{
		{
			name:     "simple prefix",
			baseURI:  "s3://bucket/prefix/",
			fullKey:  "prefix/file.txt",
			expected: "file.txt",
		},
		{
			name:     "nested prefix",
			baseURI:  "s3://bucket/a/b/c/",
			fullKey:  "a/b/c/d/e/file.txt",
			expected: "d/e/file.txt",
		},
		{
			name:     "bucket root",
			baseURI:  "s3://bucket/",
			fullKey:  "file.txt",
			expected: "file.txt",
		},
		{
			name:     "key not under prefix",
			baseURI:  "s3://bucket/prefix/",
			fullKey:  "other/file.txt",
			expected: "other/file.txt",
		},
		{
			name:     "ensures no leading slash",
			baseURI:  "s3://bucket/data/",
			fullKey:  "data/subdir/file.txt",
			expected: "subdir/file.txt",
		},
		{
			name:     "exact prefix match with nested path",
			baseURI:  "s3://bucket/logs/2024/",
			fullKey:  "logs/2024/01/app.log",
			expected: "01/app.log",
		},
		{
			name:     "handles base_uri without trailing slash gracefully",
			baseURI:  "s3://bucket/prefix",
			fullKey:  "prefix/file.txt",
			expected: "file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DeriveRelKey(tt.baseURI, tt.fullKey)
			assert.Equal(t, tt.expected, result)
			// Ensure result never starts with "/"
			assert.False(t, strings.HasPrefix(result, "/"), "rel_key should not start with /")
		})
	}
}
