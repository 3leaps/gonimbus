package indexstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const SchemaVersion = 7

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
			index_build_hash TEXT NOT NULL,
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
			storage_class TEXT,
			archive_status TEXT,
			restore_state TEXT,
			restore_expiry TEXT,
			content_type TEXT,
			head_enriched_at TEXT,
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
	if current > SchemaVersion {
		return fmt.Errorf("index schema version %d is newer than supported version %d; upgrade gonimbus before using this index", current, SchemaVersion)
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

	// v4: rename build_params_hash to index_build_hash.
	if current < 4 {
		err := renameIndexBuildHash(ctx, tx)
		if err != nil {
			return err
		}
	}

	// v5: add LIST-derived storage class to current object rows.
	if current < 5 {
		if err := addObjectsCurrentStorageClass(ctx, tx); err != nil {
			return err
		}
	}
	if err := ensureObjectsCurrentStorageClassIndex(ctx, tx); err != nil {
		return err
	}
	// v6: add HEAD-derived enrichment columns.
	if current < 6 {
		if err := addObjectsCurrentHeadEnrichment(ctx, tx); err != nil {
			return err
		}
	}
	if err := ensureObjectsCurrentHeadEnrichmentIndex(ctx, tx); err != nil {
		return err
	}
	// v7: normalize timestamp TEXT values to fixed-width UTC so SQLite
	// lexicographic comparisons preserve chronological order.
	if current < 7 {
		if err := normalizeTimestampTextColumns(ctx, tx); err != nil {
			return err
		}
	}

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

func addObjectsCurrentStorageClass(ctx context.Context, tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE objects_current ADD COLUMN storage_class TEXT;`); err != nil {
		msg := err.Error()
		if !strings.Contains(msg, "duplicate column name") && !strings.Contains(msg, "already exists") {
			return fmt.Errorf("exec migration statement: %w", err)
		}
	}
	return nil
}

func ensureObjectsCurrentStorageClassIndex(ctx context.Context, tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if _, err := tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_objects_current_storage_class ON objects_current(index_set_id, storage_class);`); err != nil {
		return fmt.Errorf("exec migration statement: %w", err)
	}
	return nil
}

func addObjectsCurrentHeadEnrichment(ctx context.Context, tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	for _, stmt := range []string{
		`ALTER TABLE objects_current ADD COLUMN archive_status TEXT;`,
		`ALTER TABLE objects_current ADD COLUMN restore_state TEXT;`,
		`ALTER TABLE objects_current ADD COLUMN restore_expiry TEXT;`,
		`ALTER TABLE objects_current ADD COLUMN content_type TEXT;`,
		`ALTER TABLE objects_current ADD COLUMN head_enriched_at TEXT;`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			msg := err.Error()
			if !strings.Contains(msg, "duplicate column name") && !strings.Contains(msg, "already exists") {
				return fmt.Errorf("exec migration statement: %w", err)
			}
		}
	}
	return nil
}

func ensureObjectsCurrentHeadEnrichmentIndex(ctx context.Context, tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if _, err := tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_objects_current_head_enriched_at ON objects_current(index_set_id, head_enriched_at);`); err != nil {
		return fmt.Errorf("exec migration statement: %w", err)
	}
	return nil
}

func normalizeTimestampTextColumns(ctx context.Context, tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}

	normalizations := []struct {
		table     string
		column    string
		pkColumns []string
	}{
		{table: "index_sets", column: "created_at", pkColumns: []string{"index_set_id"}},
		{table: "index_runs", column: "started_at", pkColumns: []string{"run_id"}},
		{table: "index_runs", column: "ended_at", pkColumns: []string{"run_id"}},
		{table: "index_runs", column: "acquired_at", pkColumns: []string{"run_id"}},
		{table: "index_runs", column: "source_snapshot_at", pkColumns: []string{"run_id"}},
		{table: "objects_current", column: "last_modified", pkColumns: []string{"index_set_id", "rel_key"}},
		{table: "objects_current", column: "restore_expiry", pkColumns: []string{"index_set_id", "rel_key"}},
		{table: "objects_current", column: "head_enriched_at", pkColumns: []string{"index_set_id", "rel_key"}},
		{table: "objects_current", column: "last_seen_at", pkColumns: []string{"index_set_id", "rel_key"}},
		{table: "objects_current", column: "deleted_at", pkColumns: []string{"index_set_id", "rel_key"}},
		{table: "index_run_events", column: "occurred_at", pkColumns: []string{"event_id"}},
	}

	for _, normalization := range normalizations {
		if err := normalizeTimestampTextColumn(ctx, tx, normalization.table, normalization.column, normalization.pkColumns); err != nil {
			return err
		}
	}
	return nil
}

func normalizeTimestampTextColumn(ctx context.Context, tx *sql.Tx, table, column string, pkColumns []string) error {
	selectColumns := append(append([]string{}, pkColumns...), column)
	// #nosec G201 -- identifiers come from normalizeTimestampTextColumns' fixed table/column allowlist.
	query := fmt.Sprintf(`SELECT %s FROM %s WHERE %s IS NOT NULL AND trim(%s) != ''`, strings.Join(selectColumns, ", "), table, column, column)

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query timestamp column %s.%s: %w", table, column, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		values := make([]sql.NullString, len(selectColumns))
		dest := make([]any, len(values))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("scan timestamp column %s.%s: %w", table, column, err)
		}

		rawValue := values[len(values)-1]
		if !rawValue.Valid {
			continue
		}
		parsed, err := parseDBTimeString(rawValue.String)
		if err != nil {
			return fmt.Errorf("parse timestamp %s.%s: %w", table, column, err)
		}
		normalized := timeString(parsed)
		if normalized == rawValue.String {
			continue
		}

		whereParts := make([]string, len(pkColumns))
		args := make([]any, 0, len(pkColumns)+1)
		args = append(args, normalized)
		for i, pkColumn := range pkColumns {
			if !values[i].Valid {
				return fmt.Errorf("timestamp normalization %s.%s has null primary key column %s", table, column, pkColumn)
			}
			whereParts[i] = fmt.Sprintf("%s = ?", pkColumn)
			args = append(args, values[i].String)
		}

		// #nosec G201 -- identifiers come from normalizeTimestampTextColumns' fixed table/column allowlist.
		update := fmt.Sprintf(`UPDATE %s SET %s = ? WHERE %s`, table, column, strings.Join(whereParts, " AND "))
		if _, err := tx.ExecContext(ctx, update, args...); err != nil {
			return fmt.Errorf("normalize timestamp %s.%s: %w", table, column, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate timestamp column %s.%s: %w", table, column, err)
	}

	return nil
}

func renameIndexBuildHash(ctx context.Context, tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}

	if _, err := tx.ExecContext(ctx, `ALTER TABLE index_sets RENAME COLUMN build_params_hash TO index_build_hash;`); err == nil {
		return nil
	} else {
		msg := err.Error()
		if strings.Contains(msg, "no such column") || strings.Contains(msg, "duplicate column name") || strings.Contains(msg, "already exists") {
			return nil
		}
		if strings.Contains(msg, "syntax error") || strings.Contains(msg, "unsupported") {
			if _, err := tx.ExecContext(ctx, `ALTER TABLE index_sets ADD COLUMN index_build_hash TEXT;`); err != nil {
				addMsg := err.Error()
				if !strings.Contains(addMsg, "duplicate column name") && !strings.Contains(addMsg, "already exists") {
					return fmt.Errorf("exec migration statement: %w", err)
				}
			}
			if _, err := tx.ExecContext(ctx, `UPDATE index_sets SET index_build_hash = COALESCE(index_build_hash, build_params_hash)`); err != nil {
				updateMsg := err.Error()
				if strings.Contains(updateMsg, "no such column") {
					return nil
				}
				return fmt.Errorf("exec migration statement: %w", err)
			}
			return nil
		}
		return fmt.Errorf("exec migration statement: %w", err)
	}
}
