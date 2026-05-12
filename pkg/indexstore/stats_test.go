package indexstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetTopLevelObjectSummaryForRun_ScopedToRun(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))

	indexSet, _, err := FindOrCreateIndexSet(ctx, db, IndexSetParams{
		BaseURI:  "s3://bucket/base/",
		Provider: "s3",
		BuildParams: BuildParams{
			SourceType:    "crawl",
			SchemaVersion: SchemaVersion,
		},
	})
	require.NoError(t, err)

	oldRun, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	run, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	now := time.Now().UTC()
	require.NoError(t, BatchUpsertObjects(ctx, db, []ObjectRow{
		{IndexSetID: indexSet.IndexSetID, RelKey: "data/a/file.json", SizeBytes: 100, LastSeenRunID: run.RunID, LastSeenAt: now},
		{IndexSetID: indexSet.IndexSetID, RelKey: "data/b/file.json", SizeBytes: 300, LastSeenRunID: run.RunID, LastSeenAt: now},
		{IndexSetID: indexSet.IndexSetID, RelKey: "logs/file.json", SizeBytes: 200, LastSeenRunID: run.RunID, LastSeenAt: now},
		{IndexSetID: indexSet.IndexSetID, RelKey: "root-file.json", SizeBytes: 50, LastSeenRunID: run.RunID, LastSeenAt: now},
		{IndexSetID: indexSet.IndexSetID, RelKey: "old/file.json", SizeBytes: 999, LastSeenRunID: oldRun.RunID, LastSeenAt: now},
	}))

	rows, err := GetTopLevelObjectSummaryForRun(ctx, db, indexSet.IndexSetID, run.RunID)
	require.NoError(t, err)
	require.Equal(t, []TopLevelObjectSummary{
		{Prefix: "data/", ObjectCount: 2, TotalSizeBytes: 400},
		{Prefix: "logs/", ObjectCount: 1, TotalSizeBytes: 200},
		{Prefix: "", ObjectCount: 1, TotalSizeBytes: 50},
	}, rows)
}

func TestGetTopLevelObjectSummaryForRun_Empty(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))

	rows, err := GetTopLevelObjectSummaryForRun(ctx, db, "idx_missing", "run_missing")
	require.NoError(t, err)
	require.Empty(t, rows)
}
