package indexcompare

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
)

type sqliteIterator struct {
	rows *sql.Rows
}

func newSQLiteIterator(ctx context.Context, db *sql.DB, indexSetID string, observationRunID string) (*sqliteIterator, error) {
	query := `SELECT rel_key, size_bytes, last_modified, etag, storage_class
		FROM objects_current
		WHERE index_set_id = ? AND deleted_at IS NULL
`
	args := []any{indexSetID}
	if strings.TrimSpace(observationRunID) != "" {
		query += ` AND last_seen_run_id = ?`
		args = append(args, strings.TrimSpace(observationRunID))
	}
	query += ` ORDER BY rel_key`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query sqlite projection rows: %w", err)
	}
	return &sqliteIterator{rows: rows}, nil
}

func (i *sqliteIterator) Next() (projectionRow, bool, error) {
	if !i.rows.Next() {
		if err := i.rows.Err(); err != nil {
			return projectionRow{}, false, err
		}
		return projectionRow{}, false, nil
	}
	var row projectionRow
	var lastModified sql.NullString
	var etag sql.NullString
	var storageClass sql.NullString
	if err := i.rows.Scan(&row.RelKey, &row.SizeBytes, &lastModified, &etag, &storageClass); err != nil {
		return projectionRow{}, false, fmt.Errorf("scan sqlite projection row: %w", err)
	}
	normalized, err := normalizeTimeString(lastModified.String)
	if lastModified.Valid && err != nil {
		return projectionRow{}, false, fmt.Errorf("parse sqlite last_modified for %s: %w", row.RelKey, err)
	}
	row.LastModified = normalized
	row.ETag = nullStringValue(etag)
	row.StorageClass = nullStringValue(storageClass)
	return row, true, nil
}

func (i *sqliteIterator) Close() error {
	if i == nil || i.rows == nil {
		return nil
	}
	return i.rows.Close()
}

type durableIterator struct {
	rows <-chan durableRowResult
}

type durableRowResult struct {
	row projectionRow
	err error
}

// newDurableIterator yields the durable current-state projection for one crawl.
// When observationRunID is set it applies the same observation-run predicate the
// SQLite side uses (last_seen_run_id == observationRunID): a scope-reduced build
// retains out-of-scope active rows whose last-seen lineage is an older run, and
// those rows are not part of this run's LIST projection. Filtering them here
// keeps parity symmetric without erasing or re-stamping retained durable state.
func newDurableIterator(ctx context.Context, segmentDir string, manifest indexsubstrate.InternalManifest, observationRunID string) *durableIterator {
	observationRunID = strings.TrimSpace(observationRunID)
	out := make(chan durableRowResult, 1)
	go func() {
		defer close(out)
		err := indexsubstrate.WalkManifestRows(segmentDir, manifest, func(current indexsubstrate.CurrentObjectRow) error {
			if current.DeletedAt != nil {
				return nil
			}
			if observationRunID != "" && strings.TrimSpace(current.LastSeenRunID) != observationRunID {
				return nil
			}
			row, err := projectionFromDurable(current)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- durableRowResult{row: row}:
				return nil
			}
		})
		if err != nil {
			select {
			case <-ctx.Done():
			case out <- durableRowResult{err: err}:
			}
		}
	}()
	return &durableIterator{rows: out}
}

func (i *durableIterator) Next() (projectionRow, bool, error) {
	result, ok := <-i.rows
	if !ok {
		return projectionRow{}, false, nil
	}
	if result.err != nil {
		return projectionRow{}, false, result.err
	}
	return result.row, true, nil
}

func projectionFromDurable(row indexsubstrate.CurrentObjectRow) (projectionRow, error) {
	lastModified := ""
	if row.LastModified != nil {
		lastModified = row.LastModified.UTC().Format(time.RFC3339Nano)
	}
	return projectionRow{
		RelKey:       strings.TrimSpace(row.RelKey),
		SizeBytes:    row.SizeBytes,
		LastModified: lastModified,
		StorageClass: ptrValue(row.StorageClass),
		ETag:         strings.TrimSpace(row.ETag),
	}, nil
}

type durableDeltaIterator struct {
	rows <-chan durableDeltaRowResult
}

type durableDeltaRowResult struct {
	row durableDeltaRow
	err error
}

func newDurableDeltaIterator(ctx context.Context, segmentDir string, manifest indexsubstrate.InternalManifest) *durableDeltaIterator {
	out := make(chan durableDeltaRowResult, 1)
	go func() {
		defer close(out)
		err := indexsubstrate.WalkManifestRows(segmentDir, manifest, func(current indexsubstrate.CurrentObjectRow) error {
			projected, err := projectionFromDurable(current)
			if err != nil {
				return err
			}
			row := durableDeltaRow{projectionRow: projected, DeletedAt: current.DeletedAt}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- durableDeltaRowResult{row: row}:
				return nil
			}
		})
		if err != nil {
			select {
			case <-ctx.Done():
			case out <- durableDeltaRowResult{err: err}:
			}
		}
	}()
	return &durableDeltaIterator{rows: out}
}

func (i *durableDeltaIterator) Next() (durableDeltaRow, bool, error) {
	result, ok := <-i.rows
	if !ok {
		return durableDeltaRow{}, false, nil
	}
	if result.err != nil {
		return durableDeltaRow{}, false, result.err
	}
	return result.row, true, nil
}

func normalizeTimeString(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	layouts := []string{
		"2006-01-02T15:04:05.000000000-0700",
		time.RFC3339Nano,
		time.RFC3339,
	}
	var lastErr error
	for _, layout := range layouts {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed.UTC().Format(time.RFC3339Nano), nil
		}
		lastErr = err
	}
	return "", lastErr
}

func nullStringValue(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return strings.TrimSpace(value.String)
}

func ptrValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}
