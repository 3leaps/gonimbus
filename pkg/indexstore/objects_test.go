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

func TestBatchUpsertObjectsTracksFirstSeenAndLastChanged(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))
	indexSet, _, err := FindOrCreateIndexSet(ctx, db, IndexSetParams{
		BaseURI:  "s3://test-bucket/data/",
		Provider: "s3",
		BuildParams: BuildParams{
			SourceType:    "crawl",
			SchemaVersion: SchemaVersion,
		},
	})
	require.NoError(t, err)

	lastModified := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	run1, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, BatchUpsertObjects(ctx, db, []ObjectRow{{
		IndexSetID:    indexSet.IndexSetID,
		RelKey:        "file.json",
		SizeBytes:     100,
		LastModified:  &lastModified,
		ETag:          "etag-1",
		LastSeenRunID: run1.RunID,
		LastSeenAt:    run1.StartedAt,
	}}))
	obj, err := GetObject(ctx, db, indexSet.IndexSetID, "file.json")
	require.NoError(t, err)
	require.Equal(t, run1.RunID, obj.FirstSeenRunID)
	require.Equal(t, run1.RunID, obj.LastChangedRunID)

	run2, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, BatchUpsertObjects(ctx, db, []ObjectRow{{
		IndexSetID:    indexSet.IndexSetID,
		RelKey:        "file.json",
		SizeBytes:     100,
		LastModified:  &lastModified,
		ETag:          "etag-1",
		LastSeenRunID: run2.RunID,
		LastSeenAt:    run2.StartedAt,
	}}))
	obj, err = GetObject(ctx, db, indexSet.IndexSetID, "file.json")
	require.NoError(t, err)
	require.Equal(t, run1.RunID, obj.FirstSeenRunID)
	require.Equal(t, run1.RunID, obj.LastChangedRunID)
	require.Equal(t, run2.RunID, obj.LastSeenRunID)

	run3, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, BatchUpsertObjects(ctx, db, []ObjectRow{{
		IndexSetID:    indexSet.IndexSetID,
		RelKey:        "file.json",
		SizeBytes:     101,
		LastModified:  &lastModified,
		ETag:          "etag-2",
		LastSeenRunID: run3.RunID,
		LastSeenAt:    run3.StartedAt,
	}}))
	obj, err = GetObject(ctx, db, indexSet.IndexSetID, "file.json")
	require.NoError(t, err)
	require.Equal(t, run1.RunID, obj.FirstSeenRunID)
	require.Equal(t, run3.RunID, obj.LastChangedRunID)

	_, err = db.ExecContext(ctx, `UPDATE objects_current SET deleted_at = ? WHERE index_set_id = ? AND rel_key = ?`,
		timeString(run3.StartedAt), indexSet.IndexSetID, "file.json")
	require.NoError(t, err)
	run4, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, BatchUpsertObjects(ctx, db, []ObjectRow{{
		IndexSetID:    indexSet.IndexSetID,
		RelKey:        "file.json",
		SizeBytes:     101,
		LastModified:  &lastModified,
		ETag:          "etag-2",
		LastSeenRunID: run4.RunID,
		LastSeenAt:    run4.StartedAt,
	}}))
	obj, err = GetObject(ctx, db, indexSet.IndexSetID, "file.json")
	require.NoError(t, err)
	require.Equal(t, run1.RunID, obj.FirstSeenRunID)
	require.Equal(t, run4.RunID, obj.LastChangedRunID)
	require.Nil(t, obj.DeletedAt)
}

func TestBatchUpsertObjectsLastChangedMatchesObjectRowChangedPredicate(t *testing.T) {
	ctx := context.Background()
	stringPtr := func(value string) *string { return &value }

	baseModified := time.Date(2026, 7, 1, 12, 13, 14, 123456789, time.UTC)
	equalInstantDifferentLocation := time.Date(2026, 7, 1, 7, 13, 14, 123456789, time.FixedZone("offset", -5*60*60))
	changedModified := baseModified.Add(time.Nanosecond)
	baseStorage := "STANDARD"

	base := ObjectRow{
		RelKey:       "file.json",
		SizeBytes:    100,
		LastModified: &baseModified,
		ETag:         "etag-1",
		StorageClass: &baseStorage,
	}

	tests := []struct {
		name      string
		candidate ObjectRow
	}{
		{
			name:      "unchanged",
			candidate: base,
		},
		{
			name: "size changed",
			candidate: func() ObjectRow {
				obj := base
				obj.SizeBytes = 101
				return obj
			}(),
		},
		{
			name: "etag changed",
			candidate: func() ObjectRow {
				obj := base
				obj.ETag = "etag-2"
				return obj
			}(),
		},
		{
			name: "storage class changed",
			candidate: func() ObjectRow {
				obj := base
				obj.StorageClass = stringPtr("GLACIER")
				return obj
			}(),
		},
		{
			name: "storage class removed",
			candidate: func() ObjectRow {
				obj := base
				obj.StorageClass = nil
				return obj
			}(),
		},
		{
			name: "storage class added",
			candidate: func() ObjectRow {
				obj := base
				obj.StorageClass = stringPtr("STANDARD")
				return obj
			}(),
		},
		{
			name: "last modified changed",
			candidate: func() ObjectRow {
				obj := base
				obj.LastModified = &changedModified
				return obj
			}(),
		},
		{
			name: "last modified removed",
			candidate: func() ObjectRow {
				obj := base
				obj.LastModified = nil
				return obj
			}(),
		},
		{
			name: "last modified added",
			candidate: func() ObjectRow {
				obj := base
				obj.LastModified = &baseModified
				return obj
			}(),
		},
		{
			name: "last modified equal instant different location",
			candidate: func() ObjectRow {
				obj := base
				obj.LastModified = &equalInstantDifferentLocation
				return obj
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(ctx, Config{Path: ":memory:"})
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			require.NoError(t, Migrate(ctx, db))

			indexSet, _, err := FindOrCreateIndexSet(ctx, db, IndexSetParams{
				BaseURI:  "s3://test-bucket/data/",
				Provider: "s3",
				BuildParams: BuildParams{
					SourceType:    "crawl",
					SchemaVersion: SchemaVersion,
				},
			})
			require.NoError(t, err)

			initialRun, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
			require.NoError(t, err)
			candidateRun, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
			require.NoError(t, err)

			existing := base
			existing.IndexSetID = indexSet.IndexSetID
			existing.LastSeenRunID = initialRun.RunID
			existing.LastSeenAt = initialRun.StartedAt

			if tt.name == "storage class added" {
				existing.StorageClass = nil
			}
			if tt.name == "last modified added" {
				existing.LastModified = nil
			}

			require.NoError(t, BatchUpsertObjects(ctx, db, []ObjectRow{existing}))

			candidate := tt.candidate
			candidate.IndexSetID = indexSet.IndexSetID
			candidate.RelKey = existing.RelKey
			candidate.LastSeenRunID = candidateRun.RunID
			candidate.LastSeenAt = candidateRun.StartedAt

			expectedChanged := ObjectRowChanged(&existing, candidate)
			require.NoError(t, BatchUpsertObjects(ctx, db, []ObjectRow{candidate}))

			got, err := GetObject(ctx, db, indexSet.IndexSetID, existing.RelKey)
			require.NoError(t, err)
			if expectedChanged {
				require.Equal(t, candidate.LastSeenRunID, got.LastChangedRunID)
			} else {
				require.Equal(t, existing.LastSeenRunID, got.LastChangedRunID)
			}
		})
	}

	// Reappearance from soft-delete is intentionally tracked as a last_changed
	// update by SQL even when the LIST fields themselves are unchanged.
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
