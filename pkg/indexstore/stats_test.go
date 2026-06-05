package indexstore

import (
	"context"
	"database/sql"
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

func TestGetIndexSetSummaryCountsFailedResumableRuns(t *testing.T) {
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

	successRun, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	partialRun, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	failedRun, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	resumableRun, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "enrich_head")
	require.NoError(t, err)

	require.NoError(t, setRunStartedAt(ctx, db, successRun.RunID, time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)))
	require.NoError(t, setRunStartedAt(ctx, db, partialRun.RunID, time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)))
	require.NoError(t, setRunStartedAt(ctx, db, failedRun.RunID, time.Date(2026, 6, 1, 14, 0, 0, 0, time.UTC)))
	require.NoError(t, setRunStartedAt(ctx, db, resumableRun.RunID, time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC)))

	require.NoError(t, UpdateIndexRunStatus(ctx, db, successRun.RunID, RunStatusSuccess, nil))
	require.NoError(t, UpdateIndexRunStatus(ctx, db, partialRun.RunID, RunStatusPartial, nil))
	require.NoError(t, UpdateIndexRunStatus(ctx, db, failedRun.RunID, RunStatusFailed, nil))
	require.NoError(t, UpdateIndexRunStatus(ctx, db, resumableRun.RunID, RunStatusFailedResumable, nil))

	summary, err := GetIndexSetSummary(ctx, db, indexSet.IndexSetID)
	require.NoError(t, err)

	require.Equal(t, 4, summary.TotalRuns)
	require.Equal(t, 1, summary.SuccessfulRuns)
	require.Equal(t, 1, summary.PartialRuns)
	require.Equal(t, 1, summary.FailedRuns)
	require.Equal(t, 1, summary.FailedResumableRuns)
	require.NotNil(t, summary.LatestRun)
	require.Equal(t, resumableRun.RunID, summary.LatestRun.RunID)
	require.Equal(t, RunStatus(RunStatusFailedResumable), summary.LatestRun.Status)
}

func TestGetIndexSetSummaryNoRunsUsesZeroCounts(t *testing.T) {
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

	summary, err := GetIndexSetSummary(ctx, db, indexSet.IndexSetID)
	require.NoError(t, err)

	require.Equal(t, 0, summary.TotalRuns)
	require.Equal(t, 0, summary.SuccessfulRuns)
	require.Equal(t, 0, summary.PartialRuns)
	require.Equal(t, 0, summary.FailedRuns)
	require.Equal(t, 0, summary.FailedResumableRuns)
	require.Nil(t, summary.LatestRun)
}

func TestListIndexSetsWithStatsIncludesLatestRunMetadata(t *testing.T) {
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

	run, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "enrich_head")
	require.NoError(t, err)
	require.NoError(t, UpdateIndexRunStatus(ctx, db, run.RunID, RunStatusFailedResumable, nil))

	entries, err := ListIndexSetsWithStats(ctx, db)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	require.Equal(t, run.RunID, entries[0].LatestRunID)
	require.NotNil(t, entries[0].LatestRunAt)
	require.Equal(t, string(RunStatusFailedResumable), entries[0].LatestStatus)
	require.Equal(t, "enrich_head", entries[0].LatestSourceType)
}

func setRunStartedAt(ctx context.Context, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, runID string, at time.Time) error {
	_, err := db.ExecContext(ctx,
		`UPDATE index_runs SET started_at = ?, acquired_at = ? WHERE run_id = ?`,
		timeString(at), timeString(at), runID)
	return err
}
