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

func TestMigrateUpgradesVersion4DBWithoutStorageClass(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("libsql", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.ExecContext(ctx, `
		CREATE TABLE schema_meta (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			schema_version INTEGER NOT NULL
		);
		INSERT INTO schema_meta (id, schema_version) VALUES (1, 4);

		CREATE TABLE index_sets (
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
		);

		CREATE TABLE index_runs (
			run_id TEXT PRIMARY KEY,
			index_set_id TEXT NOT NULL,
			started_at TEXT NOT NULL,
			ended_at TEXT,
			acquired_at TEXT NOT NULL,
			source_type TEXT NOT NULL,
			source_snapshot_at TEXT,
			status TEXT NOT NULL,
			FOREIGN KEY(index_set_id) REFERENCES index_sets(index_set_id)
		);

		CREATE TABLE objects_current (
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
		);

		CREATE TABLE prefix_stats (
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
		);

		CREATE TABLE index_run_events (
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
		);
	`)
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

	var version int
	err = db.QueryRowContext(ctx, `SELECT schema_version FROM schema_meta WHERE id = 1`).Scan(&version)
	require.NoError(t, err)
	require.Equal(t, SchemaVersion, version)
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
