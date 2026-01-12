package indexstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// PrefixStatRow represents a row in the prefix_stats table.
type PrefixStatRow struct {
	IndexSetID      string
	RunID           string
	Prefix          string
	Depth           int
	ObjectsDirect   int64
	BytesDirect     int64
	CommonPrefixes  int64
	Truncated       bool
	TruncatedReason string
}

// ErrInvalidPrefixStat is returned when prefix stat values are invalid.
var ErrInvalidPrefixStat = errors.New("invalid prefix stat: values must be non-negative")

// InsertPrefixStat inserts a prefix stat record.
//
// Prefix stats are per-run (not upserted) to maintain history.
// Returns ErrInvalidPrefixStat if any numeric values are negative.
func InsertPrefixStat(ctx context.Context, db *sql.DB, stat PrefixStatRow) error {
	if ctx == nil {
		ctx = context.Background()
	}

	// Validate non-negative values (important for partial traversal outputs)
	if stat.Depth < 0 || stat.ObjectsDirect < 0 || stat.BytesDirect < 0 || stat.CommonPrefixes < 0 {
		return ErrInvalidPrefixStat
	}

	truncatedInt := 0
	if stat.Truncated {
		truncatedInt = 1
	}

	var truncatedReason *string
	if stat.TruncatedReason != "" {
		truncatedReason = &stat.TruncatedReason
	}

	_, err := db.ExecContext(ctx,
		`INSERT INTO prefix_stats
		 (index_set_id, run_id, prefix, depth, objects_direct, bytes_direct,
		  common_prefixes, truncated, truncated_reason)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		stat.IndexSetID, stat.RunID, stat.Prefix, stat.Depth,
		stat.ObjectsDirect, stat.BytesDirect, stat.CommonPrefixes,
		truncatedInt, truncatedReason)

	if err != nil {
		return fmt.Errorf("insert prefix stat: %w", err)
	}

	return nil
}

// BatchInsertPrefixStats inserts multiple prefix stat records in a single transaction.
//
// Returns ErrInvalidPrefixStat if any record has negative numeric values.
func BatchInsertPrefixStats(ctx context.Context, db *sql.DB, stats []PrefixStatRow) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(stats) == 0 {
		return nil
	}

	// Validate all stats before starting transaction
	for i, stat := range stats {
		if stat.Depth < 0 || stat.ObjectsDirect < 0 || stat.BytesDirect < 0 || stat.CommonPrefixes < 0 {
			return fmt.Errorf("%w: record %d (prefix=%s)", ErrInvalidPrefixStat, i, stat.Prefix)
		}
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO prefix_stats
		 (index_set_id, run_id, prefix, depth, objects_direct, bytes_direct,
		  common_prefixes, truncated, truncated_reason)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare stmt: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, stat := range stats {
		truncatedInt := 0
		if stat.Truncated {
			truncatedInt = 1
		}

		var truncatedReason *string
		if stat.TruncatedReason != "" {
			truncatedReason = &stat.TruncatedReason
		}

		_, err := stmt.ExecContext(ctx,
			stat.IndexSetID, stat.RunID, stat.Prefix, stat.Depth,
			stat.ObjectsDirect, stat.BytesDirect, stat.CommonPrefixes,
			truncatedInt, truncatedReason)
		if err != nil {
			return fmt.Errorf("exec insert for %s: %w", stat.Prefix, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

// GetPrefixStats retrieves prefix stats for a run.
func GetPrefixStats(ctx context.Context, db *sql.DB, indexSetID, runID string) ([]PrefixStatRow, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	rows, err := db.QueryContext(ctx,
		`SELECT index_set_id, run_id, prefix, depth, objects_direct, bytes_direct,
		        common_prefixes, truncated, truncated_reason
		 FROM prefix_stats
		 WHERE index_set_id = ? AND run_id = ?
		 ORDER BY prefix`,
		indexSetID, runID)

	if err != nil {
		return nil, fmt.Errorf("query prefix stats: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var stats []PrefixStatRow
	for rows.Next() {
		var stat PrefixStatRow
		var truncatedInt int
		var truncatedReason sql.NullString

		err := rows.Scan(
			&stat.IndexSetID, &stat.RunID, &stat.Prefix, &stat.Depth,
			&stat.ObjectsDirect, &stat.BytesDirect, &stat.CommonPrefixes,
			&truncatedInt, &truncatedReason)
		if err != nil {
			return nil, fmt.Errorf("scan prefix stat: %w", err)
		}

		stat.Truncated = truncatedInt != 0
		if truncatedReason.Valid {
			stat.TruncatedReason = truncatedReason.String
		}

		stats = append(stats, stat)
	}

	return stats, nil
}

// GetLatestPrefixStats retrieves prefix stats from the most recent successful run.
func GetLatestPrefixStats(ctx context.Context, db *sql.DB, indexSetID string) ([]PrefixStatRow, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Find the most recent successful run
	var latestRunID string
	err := db.QueryRowContext(ctx,
		`SELECT run_id FROM index_runs
		 WHERE index_set_id = ? AND status = 'success'
		 ORDER BY started_at DESC LIMIT 1`,
		indexSetID).Scan(&latestRunID)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find latest run: %w", err)
	}

	return GetPrefixStats(ctx, db, indexSetID, latestRunID)
}
