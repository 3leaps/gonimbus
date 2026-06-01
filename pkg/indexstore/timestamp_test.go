package indexstore

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimestampWritesUseFixedWidthRFC3339Text(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))
	seedTimestampTestIndex(t, ctx, db)

	lastModified := mustParseTestTime(t, "2026-01-01T01:00:00Z")
	lastSeen := mustParseTestTime(t, "2026-01-01T02:00:00Z")
	deletedAt := mustParseTestTime(t, "2026-01-02T00:00:00Z")
	occurredAt := mustParseTestTime(t, "2026-01-01T03:00:00Z")
	restoreExpiry := mustParseTestTime(t, "2026-01-04T00:00:00.123456789Z")
	headEnrichedAt := mustParseTestTime(t, "2026-01-03T00:00:00.987654321Z")

	require.NoError(t, UpsertObject(ctx, db, ObjectRow{
		IndexSetID:    "idx_timestamp",
		RelKey:        "alpha/a.json",
		SizeBytes:     10,
		LastModified:  &lastModified,
		ETag:          `"etag-a"`,
		LastSeenRunID: "run_001",
		LastSeenAt:    lastSeen,
	}))
	deleted, err := MarkObjectsDeletedNotSeenInRun(ctx, db, "idx_timestamp", "run_002", deletedAt)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)

	require.NoError(t, RecordRunEvent(ctx, db, RunEvent{
		EventID:       "evt_timestamp",
		RunID:         "run_001",
		OccurredAt:    occurredAt,
		EventType:     string(EventTypeRateLimited),
		EventCategory: string(EventCategoryThrottle),
	}))
	require.NoError(t, BatchUpdateHeadEnrichment(ctx, db, []HeadEnrichmentUpdate{{
		IndexSetID:     "idx_timestamp",
		RelKey:         "alpha/a.json",
		RestoreExpiry:  &restoreExpiry,
		HeadEnrichedAt: headEnrichedAt,
	}}))

	var lastModifiedRaw, lastSeenRaw, deletedAtRaw, restoreExpiryRaw, headEnrichedAtRaw string
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT quote(last_modified), quote(last_seen_at), quote(deleted_at), quote(restore_expiry), quote(head_enriched_at)
		FROM objects_current
		WHERE index_set_id = 'idx_timestamp' AND rel_key = 'alpha/a.json'
	`).Scan(&lastModifiedRaw, &lastSeenRaw, &deletedAtRaw, &restoreExpiryRaw, &headEnrichedAtRaw))
	require.Equal(t, "'2026-01-01T01:00:00.000000000Z'", lastModifiedRaw)
	require.Equal(t, "'2026-01-01T02:00:00.000000000Z'", lastSeenRaw)
	require.Equal(t, "'2026-01-02T00:00:00.000000000Z'", deletedAtRaw)
	require.Equal(t, "'2026-01-04T00:00:00.123456789Z'", restoreExpiryRaw)
	require.Equal(t, "'2026-01-03T00:00:00.987654321Z'", headEnrichedAtRaw)

	events, err := ListRunEvents(ctx, db, "run_001", nil)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.True(t, occurredAt.Equal(events[0].OccurredAt), "occurred_at should parse from stored text")

	var occurredAtRaw string
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT quote(occurred_at)
		FROM index_run_events
		WHERE event_id = 'evt_timestamp'
	`).Scan(&occurredAtRaw))
	require.Equal(t, "'2026-01-01T03:00:00.000000000Z'", occurredAtRaw)
}

func TestTimestampFiltersUseExactSecondBoundaries(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))
	seedTimestampTestIndex(t, ctx, db)

	exact := mustParseTestTime(t, "2026-01-01T01:00:00Z")
	after := mustParseTestTime(t, "2026-01-01T01:00:00.000000001Z")
	require.NoError(t, UpsertObject(ctx, db, ObjectRow{
		IndexSetID:    "idx_timestamp",
		RelKey:        "alpha/exact.json",
		SizeBytes:     10,
		LastModified:  &exact,
		ETag:          `"etag-exact"`,
		LastSeenRunID: "run_001",
		LastSeenAt:    exact,
	}))
	require.NoError(t, UpsertObject(ctx, db, ObjectRow{
		IndexSetID:    "idx_timestamp",
		RelKey:        "alpha/after.json",
		SizeBytes:     10,
		LastModified:  &after,
		ETag:          `"etag-after"`,
		LastSeenRunID: "run_001",
		LastSeenAt:    after,
	}))

	results, _, err := QueryObjects(ctx, db, QueryParams{
		IndexSetID:     "idx_timestamp",
		ModifiedAfter:  exact,
		IncludeDeleted: true,
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, "alpha/after.json", results[0].RelKey)
	require.Equal(t, "alpha/exact.json", results[1].RelKey)

	results, _, err = QueryObjects(ctx, db, QueryParams{
		IndexSetID:     "idx_timestamp",
		ModifiedBefore: exact,
		IncludeDeleted: true,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "alpha/exact.json", results[0].RelKey)

	count, err := QueryObjectCount(ctx, db, QueryParams{
		IndexSetID:     "idx_timestamp",
		Pattern:        "alpha/**",
		ModifiedAfter:  exact,
		IncludeDeleted: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
}

func TestTimestampOrderingUsesChronologicalTextOrder(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))

	exact := mustParseTestTime(t, "2026-01-01T01:00:00Z")
	after := mustParseTestTime(t, "2026-01-01T01:00:00.000000001Z")
	_, err = db.ExecContext(ctx, `INSERT INTO index_sets
		(index_set_id, base_uri, provider, storage_provider, cloud_provider, region_kind, region, endpoint, endpoint_host, index_build_hash, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"idx_order_exact", "s3://example-bucket/exact/", "s3", "", "", "", "", "", "", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", timeString(exact),
		"idx_order_after", "s3://example-bucket/after/", "s3", "", "", "", "", "", "", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", timeString(after))
	require.NoError(t, err)

	sets, err := ListIndexSets(ctx, db, "")
	require.NoError(t, err)
	require.Len(t, sets, 2)
	require.Equal(t, "idx_order_after", sets[0].IndexSetID)
	require.Equal(t, "idx_order_exact", sets[1].IndexSetID)

	for _, run := range []struct {
		id        string
		startedAt time.Time
	}{
		{id: "run_order_exact", startedAt: exact},
		{id: "run_order_after", startedAt: after},
	} {
		_, err = db.ExecContext(ctx, `INSERT INTO index_runs
			(run_id, index_set_id, started_at, acquired_at, source_type, status)
			VALUES (?, ?, ?, ?, ?, ?)`,
			run.id, "idx_order_exact", timeString(run.startedAt), timeString(run.startedAt), "crawl", RunStatusRunning)
		require.NoError(t, err)
	}

	runs, err := ListIndexRuns(ctx, db, "idx_order_exact")
	require.NoError(t, err)
	require.Len(t, runs, 2)
	require.Equal(t, "run_order_after", runs[0].RunID)
	require.Equal(t, "run_order_exact", runs[1].RunID)

	require.NoError(t, RecordRunEvent(ctx, db, RunEvent{
		EventID:       "evt_order_after",
		RunID:         "run_order_exact",
		OccurredAt:    after,
		EventType:     string(EventTypeRunStarted),
		EventCategory: string(EventCategoryInfo),
	}))
	require.NoError(t, RecordRunEvent(ctx, db, RunEvent{
		EventID:       "evt_order_exact",
		RunID:         "run_order_exact",
		OccurredAt:    exact,
		EventType:     string(EventTypeRunStarted),
		EventCategory: string(EventCategoryInfo),
	}))

	events, err := ListRunEvents(ctx, db, "run_order_exact", nil)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, "evt_order_exact", events[0].EventID)
	require.Equal(t, "evt_order_after", events[1].EventID)
}

func seedTimestampTestIndex(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()

	_, err := db.ExecContext(ctx, `INSERT INTO index_sets
		(index_set_id, base_uri, provider, storage_provider, cloud_provider, region_kind, region, endpoint, endpoint_host, index_build_hash, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"idx_timestamp", "s3://example-bucket/data/", "s3", "", "", "", "", "", "",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "2026-01-01T00:00:00Z")
	require.NoError(t, err)

	for _, runID := range []string{"run_001", "run_002"} {
		_, err = db.ExecContext(ctx, `INSERT INTO index_runs
			(run_id, index_set_id, started_at, acquired_at, source_type, status)
			VALUES (?, ?, ?, ?, ?, ?)`,
			runID, "idx_timestamp", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "crawl", RunStatusRunning)
		require.NoError(t, err)
	}
}

func mustParseTestTime(t *testing.T, value string) time.Time {
	t.Helper()

	parsed, err := time.Parse(time.RFC3339, value)
	require.NoError(t, err)
	return parsed
}
