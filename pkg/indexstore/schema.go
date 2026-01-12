package indexstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const SchemaVersion = 3

// Migrate creates (or upgrades) the index schema in-place.
//
// v0.1.3 starts with a minimal schema that supports:
// - index set identity + run provenance
// - current object rows (upserted)
// - prefix summary stats (tree-backed)
func Migrate(ctx context.Context, db *sql.DB) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS schema_meta (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			schema_version INTEGER NOT NULL
		);`,
		`INSERT INTO schema_meta (id, schema_version)
			VALUES (1, 0)
			ON CONFLICT(id) DO NOTHING;`,

		`CREATE TABLE IF NOT EXISTS index_sets (
			index_set_id TEXT PRIMARY KEY,
			base_uri TEXT NOT NULL,
			provider TEXT NOT NULL,
			-- storage_provider is the canonical backend/variant (e.g., aws_s3, r2, wasabi).
			storage_provider TEXT,
			-- cloud_provider is the broader cloud (e.g., aws, gcp, azure).
			cloud_provider TEXT,
			-- region_kind disambiguates region naming schemes across clouds.
			region_kind TEXT,
			region TEXT,
			endpoint TEXT,
			-- endpoint_host is derived from endpoint (host[:port]) when applicable.
			endpoint_host TEXT,
			build_params_hash TEXT NOT NULL,
			created_at TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_index_sets_base ON index_sets(base_uri);`,

		`CREATE TABLE IF NOT EXISTS index_runs (
			run_id TEXT PRIMARY KEY,
			index_set_id TEXT NOT NULL,
			started_at TEXT NOT NULL,
			ended_at TEXT,
			acquired_at TEXT NOT NULL,
			source_type TEXT NOT NULL,
			source_snapshot_at TEXT,
			status TEXT NOT NULL,
			FOREIGN KEY(index_set_id) REFERENCES index_sets(index_set_id)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_index_runs_index_set_id ON index_runs(index_set_id);`,
		`CREATE INDEX IF NOT EXISTS idx_index_runs_started_at ON index_runs(started_at);`,

		`CREATE TABLE IF NOT EXISTS objects_current (
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
		);`,
		`CREATE INDEX IF NOT EXISTS idx_objects_current_last_modified ON objects_current(index_set_id, last_modified);`,
		`CREATE INDEX IF NOT EXISTS idx_objects_current_deleted_at ON objects_current(index_set_id, deleted_at);`,

		`CREATE TABLE IF NOT EXISTS prefix_stats (
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
		);`,
		`CREATE INDEX IF NOT EXISTS idx_prefix_stats_prefix ON prefix_stats(index_set_id, prefix);`,
		`CREATE INDEX IF NOT EXISTS idx_prefix_stats_depth ON prefix_stats(index_set_id, depth);`,

		`CREATE TABLE IF NOT EXISTS index_run_events (
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
		);`,
		`CREATE INDEX IF NOT EXISTS idx_index_run_events_run_id ON index_run_events(run_id);`,
		`CREATE INDEX IF NOT EXISTS idx_index_run_events_occurred_at ON index_run_events(occurred_at);`,
	}

	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec schema statement: %w", err)
		}
	}

	var current int
	if err := tx.QueryRowContext(ctx, `SELECT schema_version FROM schema_meta WHERE id=1`).Scan(&current); err != nil {
		return fmt.Errorf("read schema_version: %w", err)
	}

	// v2: add provider identity columns for capability-aware indexing.
	if current < 2 {
		alters := []string{
			`ALTER TABLE index_sets ADD COLUMN storage_provider TEXT;`,
			`ALTER TABLE index_sets ADD COLUMN cloud_provider TEXT;`,
			`ALTER TABLE index_sets ADD COLUMN region_kind TEXT;`,
			`ALTER TABLE index_sets ADD COLUMN endpoint_host TEXT;`,
		}
		for _, stmt := range alters {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				msg := err.Error()
				// SQLite/libsql report duplicate columns as an error; treat as idempotent.
				if strings.Contains(msg, "duplicate column name") || strings.Contains(msg, "already exists") {
					continue
				}
				return fmt.Errorf("exec migration statement: %w", err)
			}
		}
	}

	// v3: add index_run_events table for structured partial reasons.
	// Events table is created in base stmts via CREATE TABLE IF NOT EXISTS.
	// No additional migration steps needed for v3.

	if current != SchemaVersion {
		if _, err := tx.ExecContext(ctx, `UPDATE schema_meta SET schema_version=? WHERE id=1`, SchemaVersion); err != nil {
			return fmt.Errorf("update schema_version: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema tx: %w", err)
	}
	return nil
}
