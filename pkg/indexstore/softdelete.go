package indexstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// MarkObjectsDeletedNotSeenInRun marks objects as deleted if they were not seen in the given run.
//
// This implements soft-delete semantics: objects that existed in previous runs but
// were not seen in the current run are marked with deleted_at = run.started_at.
//
// IMPORTANT: This should only be called for SUCCESSFUL runs. For partial runs,
// missing objects may be due to incomplete traversal (throttling, access denied),
// not actual deletions. Calling this on partial runs will incorrectly mark
// objects as deleted.
//
// For partial runs, consider:
// - Skipping soft-delete entirely (recommended default)
// - Using an explicit "reconcile deletes" mode with user confirmation
// - Only soft-deleting objects in prefixes that were fully traversed
func MarkObjectsDeletedNotSeenInRun(ctx context.Context, db *sql.DB, indexSetID, runID string, runStartedAt time.Time) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	result, err := db.ExecContext(ctx,
		`UPDATE objects_current
		 SET deleted_at = ?
		 WHERE index_set_id = ?
		   AND deleted_at IS NULL
		   AND last_seen_run_id != ?`,
		runStartedAt, indexSetID, runID)

	if err != nil {
		return 0, fmt.Errorf("mark deleted: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}

	return affected, nil
}

// PurgeDeletedObjects permanently removes objects marked as deleted.
//
// This can be used for garbage collection of soft-deleted objects.
// The olderThan parameter specifies the minimum age of deleted_at for purging.
func PurgeDeletedObjects(ctx context.Context, db *sql.DB, indexSetID string, olderThan time.Time) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	result, err := db.ExecContext(ctx,
		`DELETE FROM objects_current
		 WHERE index_set_id = ?
		   AND deleted_at IS NOT NULL
		   AND deleted_at < ?`,
		indexSetID, olderThan)

	if err != nil {
		return 0, fmt.Errorf("purge deleted: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}

	return affected, nil
}

// RestoreDeletedObjects restores soft-deleted objects by clearing deleted_at.
//
// This can be used to undo soft-deletes, for example if a run was marked
// as partial and we want to restore the previous state.
func RestoreDeletedObjects(ctx context.Context, db *sql.DB, indexSetID string, deletedAfter time.Time) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	result, err := db.ExecContext(ctx,
		`UPDATE objects_current
		 SET deleted_at = NULL
		 WHERE index_set_id = ?
		   AND deleted_at IS NOT NULL
		   AND deleted_at >= ?`,
		indexSetID, deletedAfter)

	if err != nil {
		return 0, fmt.Errorf("restore deleted: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}

	return affected, nil
}

// DeletedObjectStats returns statistics about deleted objects in an index set.
type DeletedObjectStats struct {
	TotalDeleted  int64
	OldestDeleted *time.Time
	NewestDeleted *time.Time
}

// GetDeletedObjectStats retrieves statistics about soft-deleted objects.
func GetDeletedObjectStats(ctx context.Context, db *sql.DB, indexSetID string) (*DeletedObjectStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var stats DeletedObjectStats
	var oldestRaw any
	var newestRaw any

	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*), MIN(deleted_at), MAX(deleted_at)
		 FROM objects_current
		 WHERE index_set_id = ? AND deleted_at IS NOT NULL`,
		indexSetID).Scan(&stats.TotalDeleted, &oldestRaw, &newestRaw)

	if err != nil {
		return nil, fmt.Errorf("get deleted stats: %w", err)
	}

	oldest, err := parseOptionalDBTime(oldestRaw)
	if err != nil {
		return nil, fmt.Errorf("parse oldest deleted: %w", err)
	}
	stats.OldestDeleted = oldest

	newest, err := parseOptionalDBTime(newestRaw)
	if err != nil {
		return nil, fmt.Errorf("parse newest deleted: %w", err)
	}
	stats.NewestDeleted = newest

	return &stats, nil
}
