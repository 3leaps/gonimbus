package indexstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarkObjectsDeletedNotSeenInRun(t *testing.T) {
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

	// First run - insert 3 objects
	run1, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	objects1 := []ObjectRow{
		{IndexSetID: indexSet.IndexSetID, RelKey: "file1.txt", SizeBytes: 100, LastSeenRunID: run1.RunID, LastSeenAt: run1.StartedAt},
		{IndexSetID: indexSet.IndexSetID, RelKey: "file2.txt", SizeBytes: 200, LastSeenRunID: run1.RunID, LastSeenAt: run1.StartedAt},
		{IndexSetID: indexSet.IndexSetID, RelKey: "file3.txt", SizeBytes: 300, LastSeenRunID: run1.RunID, LastSeenAt: run1.StartedAt},
	}
	err = BatchUpsertObjects(ctx, db, objects1)
	require.NoError(t, err)

	// Second run - only see file1 and file2 (file3 is "deleted")
	run2, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	objects2 := []ObjectRow{
		{IndexSetID: indexSet.IndexSetID, RelKey: "file1.txt", SizeBytes: 100, LastSeenRunID: run2.RunID, LastSeenAt: run2.StartedAt},
		{IndexSetID: indexSet.IndexSetID, RelKey: "file2.txt", SizeBytes: 200, LastSeenRunID: run2.RunID, LastSeenAt: run2.StartedAt},
	}
	err = BatchUpsertObjects(ctx, db, objects2)
	require.NoError(t, err)

	// Mark objects not seen in run2 as deleted
	deleted, err := MarkObjectsDeletedNotSeenInRun(ctx, db, indexSet.IndexSetID, run2.RunID, run2.StartedAt)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted) // file3 should be marked deleted

	// Verify file3 is marked deleted
	file3, err := GetObject(ctx, db, indexSet.IndexSetID, "file3.txt")
	require.NoError(t, err)
	require.NotNil(t, file3)
	assert.NotNil(t, file3.DeletedAt)

	// Verify file1 and file2 are not deleted
	file1, err := GetObject(ctx, db, indexSet.IndexSetID, "file1.txt")
	require.NoError(t, err)
	assert.Nil(t, file1.DeletedAt)

	file2, err := GetObject(ctx, db, indexSet.IndexSetID, "file2.txt")
	require.NoError(t, err)
	assert.Nil(t, file2.DeletedAt)

	// Count should exclude deleted
	count, err := CountObjects(ctx, db, indexSet.IndexSetID, false)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// Count with deleted should include all
	countAll, err := CountObjects(ctx, db, indexSet.IndexSetID, true)
	require.NoError(t, err)
	assert.Equal(t, int64(3), countAll)
}

func TestPurgeDeletedObjects(t *testing.T) {
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

	// Insert an object
	obj := ObjectRow{
		IndexSetID:    indexSet.IndexSetID,
		RelKey:        "old-file.txt",
		SizeBytes:     100,
		LastSeenRunID: run.RunID,
		LastSeenAt:    run.StartedAt,
	}
	err = UpsertObject(ctx, db, obj)
	require.NoError(t, err)

	// Create a new run that doesn't see the object
	run2, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	// Mark as deleted
	_, err = MarkObjectsDeletedNotSeenInRun(ctx, db, indexSet.IndexSetID, run2.RunID, run2.StartedAt)
	require.NoError(t, err)

	// Verify it's deleted
	retrieved, err := GetObject(ctx, db, indexSet.IndexSetID, "old-file.txt")
	require.NoError(t, err)
	require.NotNil(t, retrieved.DeletedAt)

	// Purge objects deleted before the deleted_at time + 1 second (should purge it)
	purgeTime := retrieved.DeletedAt.Add(time.Second)
	purged, err := PurgeDeletedObjects(ctx, db, indexSet.IndexSetID, purgeTime)
	require.NoError(t, err)
	assert.Equal(t, int64(1), purged)

	// Verify it's gone
	retrieved, err = GetObject(ctx, db, indexSet.IndexSetID, "old-file.txt")
	require.NoError(t, err)
	assert.Nil(t, retrieved)
}

func TestRestoreDeletedObjects(t *testing.T) {
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

	run1, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	obj := ObjectRow{
		IndexSetID:    indexSet.IndexSetID,
		RelKey:        "file.txt",
		SizeBytes:     100,
		LastSeenRunID: run1.RunID,
		LastSeenAt:    run1.StartedAt,
	}
	err = UpsertObject(ctx, db, obj)
	require.NoError(t, err)

	// Create run2 that doesn't see the object
	run2, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	// Mark as deleted
	_, err = MarkObjectsDeletedNotSeenInRun(ctx, db, indexSet.IndexSetID, run2.RunID, run2.StartedAt)
	require.NoError(t, err)

	// Verify deleted
	retrieved, err := GetObject(ctx, db, indexSet.IndexSetID, "file.txt")
	require.NoError(t, err)
	require.NotNil(t, retrieved.DeletedAt)

	// Restore objects deleted after run1.StartedAt
	restored, err := RestoreDeletedObjects(ctx, db, indexSet.IndexSetID, run1.StartedAt)
	require.NoError(t, err)
	assert.Equal(t, int64(1), restored)

	// Verify restored
	retrieved, err = GetObject(ctx, db, indexSet.IndexSetID, "file.txt")
	require.NoError(t, err)
	assert.Nil(t, retrieved.DeletedAt)
}

func TestGetDeletedObjectStats(t *testing.T) {
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

	run1, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	// Insert objects
	objects := []ObjectRow{
		{IndexSetID: indexSet.IndexSetID, RelKey: "file1.txt", SizeBytes: 100, LastSeenRunID: run1.RunID, LastSeenAt: run1.StartedAt},
		{IndexSetID: indexSet.IndexSetID, RelKey: "file2.txt", SizeBytes: 200, LastSeenRunID: run1.RunID, LastSeenAt: run1.StartedAt},
	}
	err = BatchUpsertObjects(ctx, db, objects)
	require.NoError(t, err)

	// No deleted objects yet
	stats, err := GetDeletedObjectStats(ctx, db, indexSet.IndexSetID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats.TotalDeleted)
	assert.Nil(t, stats.OldestDeleted)
	assert.Nil(t, stats.NewestDeleted)

	// Delete one object
	run2, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	// Only see file1
	obj := ObjectRow{IndexSetID: indexSet.IndexSetID, RelKey: "file1.txt", SizeBytes: 100, LastSeenRunID: run2.RunID, LastSeenAt: run2.StartedAt}
	err = UpsertObject(ctx, db, obj)
	require.NoError(t, err)

	_, err = MarkObjectsDeletedNotSeenInRun(ctx, db, indexSet.IndexSetID, run2.RunID, run2.StartedAt)
	require.NoError(t, err)

	// Now should have 1 deleted
	stats, err = GetDeletedObjectStats(ctx, db, indexSet.IndexSetID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.TotalDeleted)
	assert.NotNil(t, stats.OldestDeleted)
	assert.NotNil(t, stats.NewestDeleted)
}
