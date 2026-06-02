package indexstore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarkIndexRunResumingRequiresFailedResumable(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.NoError(t, Migrate(ctx, db))

	indexSet, _, err := FindOrCreateIndexSet(ctx, db, IndexSetParams{
		BaseURI:  "s3://bucket/prefix/",
		Provider: "s3",
		BuildParams: BuildParams{
			SourceType:    "crawl",
			SchemaVersion: SchemaVersion,
		},
	})
	require.NoError(t, err)
	run, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "enrich_head")
	require.NoError(t, err)

	require.Error(t, MarkIndexRunResuming(ctx, db, run.RunID))

	require.NoError(t, UpdateIndexRunStatus(ctx, db, run.RunID, RunStatusFailedResumable, nil))
	require.NoError(t, MarkIndexRunResuming(ctx, db, run.RunID))

	got, err := GetIndexRun(ctx, db, run.RunID)
	require.NoError(t, err)
	require.Equal(t, RunStatus(RunStatusRunning), got.Status)
	require.Nil(t, got.EndedAt)
}
