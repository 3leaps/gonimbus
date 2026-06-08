package indexstore

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newIndexRunResumeTestDB(t *testing.T) (context.Context, *sql.DB, *IndexRun) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
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
	return ctx, db, run
}

func TestMarkIndexRunResumingRequiresFailedResumable(t *testing.T) {
	ctx, db, run := newIndexRunResumeTestDB(t)

	require.Error(t, MarkIndexRunResuming(ctx, db, run.RunID))

	require.NoError(t, UpdateIndexRunStatus(ctx, db, run.RunID, RunStatusFailedResumable, nil))
	require.NoError(t, MarkIndexRunResuming(ctx, db, run.RunID))

	got, err := GetIndexRun(ctx, db, run.RunID)
	require.NoError(t, err)
	require.Equal(t, RunStatus(RunStatusRunning), got.Status)
	require.Nil(t, got.EndedAt)
}

func TestUpdateIndexRunStatusWithEventsRollsBackStatusOnEventFailure(t *testing.T) {
	ctx, db, run := newIndexRunResumeTestDB(t)
	require.NoError(t, UpdateIndexRunStatus(ctx, db, run.RunID, RunStatusFailedResumable, nil))
	require.NoError(t, RecordRunEvent(ctx, db, RunEvent{
		EventID:       "evt_duplicate",
		RunID:         run.RunID,
		OccurredAt:    time.Now().UTC(),
		EventType:     "existing_event",
		EventCategory: string(EventCategoryInfo),
	}))

	err := UpdateIndexRunStatusWithEvents(ctx, db, run.RunID, RunStatusSuccess, nil, []RunEvent{{
		EventID:       "evt_duplicate",
		RunID:         run.RunID,
		OccurredAt:    time.Now().UTC(),
		EventType:     "resume_completed",
		EventCategory: string(EventCategoryInfo),
	}})
	require.Error(t, err)

	got, err := GetIndexRun(ctx, db, run.RunID)
	require.NoError(t, err)
	require.Equal(t, RunStatus(RunStatusFailedResumable), got.Status)
}

func TestMarkIndexRunResumingWithEventsRollsBackStatusOnEventFailure(t *testing.T) {
	ctx, db, run := newIndexRunResumeTestDB(t)
	require.NoError(t, UpdateIndexRunStatus(ctx, db, run.RunID, RunStatusFailedResumable, nil))
	require.NoError(t, RecordRunEvent(ctx, db, RunEvent{
		EventID:       "evt_duplicate",
		RunID:         run.RunID,
		OccurredAt:    time.Now().UTC(),
		EventType:     "existing_event",
		EventCategory: string(EventCategoryInfo),
	}))

	err := MarkIndexRunResumingWithEvents(ctx, db, run.RunID, []RunEvent{{
		EventID:       "evt_duplicate",
		RunID:         run.RunID,
		OccurredAt:    time.Now().UTC(),
		EventType:     "resume_started",
		EventCategory: string(EventCategoryInfo),
	}})
	require.Error(t, err)

	got, err := GetIndexRun(ctx, db, run.RunID)
	require.NoError(t, err)
	require.Equal(t, RunStatus(RunStatusFailedResumable), got.Status)
	require.NotNil(t, got.EndedAt)
}
