package indexstore

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrateAddsStorageClassColumnAndIndex(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("libsql", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))

	var columnCount int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('objects_current') WHERE name = 'storage_class'`).Scan(&columnCount)
	require.NoError(t, err)
	require.Equal(t, 1, columnCount)

	var indexCount int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_objects_current_storage_class'`).Scan(&indexCount)
	require.NoError(t, err)
	require.Equal(t, 1, indexCount)

	var version int
	err = db.QueryRowContext(ctx, `SELECT schema_version FROM schema_meta WHERE id = 1`).Scan(&version)
	require.NoError(t, err)
	require.Equal(t, SchemaVersion, version)
}

func TestMigrateAddsHeadEnrichmentColumnsAndIndex(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("libsql", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))

	for _, column := range []string{"archive_status", "restore_state", "restore_expiry", "content_type", "head_enriched_at"} {
		var columnCount int
		err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('objects_current') WHERE name = ?`, column).Scan(&columnCount)
		require.NoError(t, err)
		require.Equal(t, 1, columnCount, column)
	}

	var indexCount int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_objects_current_head_enriched_at'`).Scan(&indexCount)
	require.NoError(t, err)
	require.Equal(t, 1, indexCount)
}

func TestMigrateUpgradesVersion4DBWithoutStorageClass(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("libsql", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	legacyStatements := []string{
		`CREATE TABLE schema_meta (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			schema_version INTEGER NOT NULL
		)`,
		`INSERT INTO schema_meta (id, schema_version) VALUES (1, 4)`,
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
		)`,
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
		)`,
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
		)`,
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
		)`,
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
		)`,
	}
	for _, stmt := range legacyStatements {
		_, err = db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}
	legacyStartedAt := "2026-01-01T00:00:00Z"
	_, err = db.ExecContext(ctx, `INSERT INTO index_sets
		(index_set_id, base_uri, provider, storage_provider, cloud_provider, region_kind, region, endpoint, endpoint_host, index_build_hash, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"idx_v4", "s3://example-bucket/data/", "s3", "", "", "", "", "", "",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", legacyStartedAt)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO index_runs
		(run_id, index_set_id, started_at, ended_at, acquired_at, source_type, source_snapshot_at, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"run_v4", "idx_v4", legacyStartedAt, legacyStartedAt, legacyStartedAt, "crawl", nil, RunStatusSuccess)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO objects_current
		(index_set_id, rel_key, size_bytes, last_modified, etag, last_seen_run_id, last_seen_at, deleted_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, NULL)`,
		"idx_v4", "legacy.json", 100, legacyStartedAt, "etag-v4", "run_v4", legacyStartedAt)
	require.NoError(t, err)
	require.NoError(t, Migrate(ctx, db))

	var columnCount int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('objects_current') WHERE name = 'storage_class'`).Scan(&columnCount)
	require.NoError(t, err)
	require.Equal(t, 1, columnCount)

	var indexCount int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_objects_current_storage_class'`).Scan(&indexCount)
	require.NoError(t, err)
	require.Equal(t, 1, indexCount)
	for _, column := range []string{"first_seen_run_id", "first_seen_at", "last_changed_run_id", "last_changed_at"} {
		err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('objects_current') WHERE name = ?`, column).Scan(&columnCount)
		require.NoError(t, err)
		require.Equal(t, 1, columnCount, column)
	}
	var firstSeenRunID, firstSeenAt, lastChangedRunID, lastChangedAt, lastSeenAt, lastModified string
	err = db.QueryRowContext(ctx, `
		SELECT first_seen_run_id, first_seen_at, last_changed_run_id, last_changed_at, last_seen_at, last_modified
		FROM objects_current
		WHERE index_set_id = 'idx_v4' AND rel_key = 'legacy.json'
	`).Scan(&firstSeenRunID, &firstSeenAt, &lastChangedRunID, &lastChangedAt, &lastSeenAt, &lastModified)
	require.NoError(t, err)
	require.Equal(t, "run_v4", firstSeenRunID)
	require.Equal(t, "run_v4", lastChangedRunID)
	require.Equal(t, "'2026-01-01T00:00:00.000000000+0000'", quoted(firstSeenAt))
	require.Equal(t, "'2026-01-01T00:00:00.000000000+0000'", quoted(lastChangedAt))
	require.Equal(t, "'2026-01-01T00:00:00.000000000+0000'", quoted(lastSeenAt))
	require.Equal(t, "'2026-01-01T00:00:00.000000000+0000'", quoted(lastModified))

	baseline, err := GetObjectDeltaBaseline(ctx, db, "idx_v4")
	require.NoError(t, err)
	require.NotNil(t, baseline)
	require.Equal(t, "run_v4", baseline.BaselineRunID)

	var version int
	err = db.QueryRowContext(ctx, `SELECT schema_version FROM schema_meta WHERE id = 1`).Scan(&version)
	require.NoError(t, err)
	require.Equal(t, SchemaVersion, version)
}

func quoted(value string) string {
	return "'" + value + "'"
}

func TestMigrateNormalizesVersion6TimestampText(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("libsql", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))
	_, err = db.ExecContext(ctx, `UPDATE schema_meta SET schema_version = 6 WHERE id = 1`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO index_sets
		(index_set_id, base_uri, provider, storage_provider, cloud_provider, region_kind, region, endpoint, endpoint_host, index_build_hash, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"idx_legacy", "s3://example-bucket/data/", "s3", "", "", "", "", "", "",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "2026-01-01T01:00:00Z")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO index_runs
		(run_id, index_set_id, started_at, ended_at, acquired_at, source_type, source_snapshot_at, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"run_legacy", "idx_legacy", "2026-01-01T01:00:00Z", "2026-01-01T01:00:01Z", "2026-01-01T01:00:00.5Z", "crawl", "2026-01-01T01:00:00.000000001Z", RunStatusRunning,
	)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO objects_current
		(index_set_id, rel_key, size_bytes, last_modified, etag, storage_class, restore_expiry, head_enriched_at, last_seen_run_id, last_seen_at, deleted_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"idx_legacy", "alpha/a.json", 10, "2026-01-01T01:00:00Z", `"etag-a"`, "STANDARD", "2026-01-02T01:00:00Z", "2026-01-01T01:00:00.5Z", "run_legacy", "2026-01-01T01:00:00Z", "2026-01-03T01:00:00Z",
	)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO index_run_events
		(event_id, run_id, occurred_at, event_type, event_category)
		VALUES (?, ?, ?, ?, ?)`,
		"evt_legacy", "run_legacy", "2026-01-01T01:00:00Z", string(EventTypeRunStarted), string(EventCategoryInfo))
	require.NoError(t, err)

	require.NoError(t, Migrate(ctx, db))

	var version int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT schema_version FROM schema_meta WHERE id = 1`).Scan(&version))
	require.Equal(t, SchemaVersion, version)

	assertQuotedTimestamp(t, db, `SELECT quote(created_at) FROM index_sets WHERE index_set_id = 'idx_legacy'`, "'2026-01-01T01:00:00.000000000+0000'")
	assertQuotedTimestamp(t, db, `SELECT quote(started_at) FROM index_runs WHERE run_id = 'run_legacy'`, "'2026-01-01T01:00:00.000000000+0000'")
	assertQuotedTimestamp(t, db, `SELECT quote(acquired_at) FROM index_runs WHERE run_id = 'run_legacy'`, "'2026-01-01T01:00:00.500000000+0000'")
	assertQuotedTimestamp(t, db, `SELECT quote(source_snapshot_at) FROM index_runs WHERE run_id = 'run_legacy'`, "'2026-01-01T01:00:00.000000001+0000'")
	assertQuotedTimestamp(t, db, `SELECT quote(last_modified) FROM objects_current WHERE index_set_id = 'idx_legacy' AND rel_key = 'alpha/a.json'`, "'2026-01-01T01:00:00.000000000+0000'")
	assertQuotedTimestamp(t, db, `SELECT quote(head_enriched_at) FROM objects_current WHERE index_set_id = 'idx_legacy' AND rel_key = 'alpha/a.json'`, "'2026-01-01T01:00:00.500000000+0000'")
	assertQuotedTimestamp(t, db, `SELECT quote(occurred_at) FROM index_run_events WHERE event_id = 'evt_legacy'`, "'2026-01-01T01:00:00.000000000+0000'")
}

func TestMigrateRejectsNewerSchemaVersion(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("libsql", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))
	_, err = db.ExecContext(ctx, `UPDATE schema_meta SET schema_version = ? WHERE id = 1`, SchemaVersion+1)
	require.NoError(t, err)

	err = Migrate(ctx, db)
	require.Error(t, err)
	require.Contains(t, err.Error(), "newer than supported")

	var version int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT schema_version FROM schema_meta WHERE id = 1`).Scan(&version))
	require.Equal(t, SchemaVersion+1, version)
}

func assertQuotedTimestamp(t *testing.T, db *sql.DB, query, expected string) {
	t.Helper()

	var actual string
	require.NoError(t, db.QueryRow(query).Scan(&actual))
	require.Equal(t, expected, actual)
}
