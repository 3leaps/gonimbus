package indexstore

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"
)

// IndexSetSummary provides aggregate statistics for an IndexSet.
type IndexSetSummary struct {
	IndexSetID string
	BaseURI    string
	Provider   string
	CreatedAt  time.Time

	// Object statistics (from objects_current)
	TotalObjects   int64
	ActiveObjects  int64
	DeletedObjects int64
	TotalSizeBytes int64

	// Run statistics
	TotalRuns      int
	SuccessfulRuns int
	PartialRuns    int
	FailedRuns     int
	LatestRun      *IndexRun
}

// GetIndexSetSummary retrieves aggregate statistics for an IndexSet.
func GetIndexSetSummary(ctx context.Context, db *sql.DB, indexSetID string) (*IndexSetSummary, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Get the IndexSet first
	indexSet, err := GetIndexSet(ctx, db, indexSetID)
	if err != nil {
		return nil, err
	}

	summary := &IndexSetSummary{
		IndexSetID: indexSet.IndexSetID,
		BaseURI:    indexSet.BaseURI,
		Provider:   indexSet.Provider,
		CreatedAt:  indexSet.CreatedAt,
	}

	// Get object counts and size using SQLite-compatible aggregates
	err = db.QueryRowContext(ctx,
		`SELECT
			COUNT(*) as total,
			SUM(CASE WHEN deleted_at IS NULL THEN 1 ELSE 0 END) as active,
			SUM(CASE WHEN deleted_at IS NOT NULL THEN 1 ELSE 0 END) as deleted,
			COALESCE(SUM(CASE WHEN deleted_at IS NULL THEN size_bytes ELSE 0 END), 0) as total_size
		 FROM objects_current
		 WHERE index_set_id = ?`,
		indexSetID).Scan(
		&summary.TotalObjects, &summary.ActiveObjects,
		&summary.DeletedObjects, &summary.TotalSizeBytes)

	if err != nil {
		return nil, fmt.Errorf("get object stats: %w", err)
	}

	// Get run statistics using SQLite-compatible aggregates
	err = db.QueryRowContext(ctx,
		`SELECT
			COUNT(*) as total,
			SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success,
			SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) as partial,
			SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
		 FROM index_runs
		 WHERE index_set_id = ?`,
		indexSetID).Scan(
		&summary.TotalRuns, &summary.SuccessfulRuns,
		&summary.PartialRuns, &summary.FailedRuns)

	if err != nil {
		return nil, fmt.Errorf("get run stats: %w", err)
	}

	// Get latest run
	runs, err := ListIndexRuns(ctx, db, indexSetID)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	if len(runs) > 0 {
		summary.LatestRun = &runs[0]
	}

	return summary, nil
}

// IndexListEntry provides summary info for the index list command.
type IndexListEntry struct {
	IndexSetID     string
	BaseURI        string
	Provider       string
	CreatedAt      time.Time
	ObjectCount    int64
	TotalSizeBytes int64
	RunCount       int
	LatestRunAt    *time.Time
	LatestStatus   string
}

// ListIndexSetsWithStats returns all IndexSets with summary statistics.
func ListIndexSetsWithStats(ctx context.Context, db *sql.DB) ([]IndexListEntry, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Get all index sets
	sets, err := ListIndexSets(ctx, db, "")
	if err != nil {
		return nil, err
	}

	entries := make([]IndexListEntry, 0, len(sets))
	for _, is := range sets {
		entry := IndexListEntry{
			IndexSetID: is.IndexSetID,
			BaseURI:    is.BaseURI,
			Provider:   is.Provider,
			CreatedAt:  is.CreatedAt,
		}

		// Get object count and size
		var count int64
		var totalSize sql.NullInt64
		err := db.QueryRowContext(ctx,
			`SELECT COUNT(*), SUM(size_bytes)
			 FROM objects_current
			 WHERE index_set_id = ? AND deleted_at IS NULL`,
			is.IndexSetID).Scan(&count, &totalSize)
		if err == nil {
			entry.ObjectCount = count
			if totalSize.Valid {
				entry.TotalSizeBytes = totalSize.Int64
			}
		}

		// Get run info
		var runCount int
		err = db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM index_runs WHERE index_set_id = ?`,
			is.IndexSetID).Scan(&runCount)
		if err == nil {
			entry.RunCount = runCount
		}

		// Get latest run
		var latestRunAt sql.NullTime
		var latestStatus sql.NullString
		err = db.QueryRowContext(ctx,
			`SELECT started_at, status FROM index_runs
			 WHERE index_set_id = ?
			 ORDER BY started_at DESC LIMIT 1`,
			is.IndexSetID).Scan(&latestRunAt, &latestStatus)
		if err == nil {
			if latestRunAt.Valid {
				entry.LatestRunAt = &latestRunAt.Time
			}
			if latestStatus.Valid {
				entry.LatestStatus = latestStatus.String
			}
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// GCParams specifies parameters for garbage collection.
type GCParams struct {
	// MaxAge removes index sets older than this duration.
	// Zero means no age-based cleanup.
	MaxAge time.Duration

	// KeepLast keeps at least this many index sets per base URI.
	// Zero means no minimum.
	KeepLast int

	// DryRun if true, reports what would be deleted without deleting.
	DryRun bool
}

// GCResult contains the results of a garbage collection run.
type GCResult struct {
	// IndexSetsRemoved is the count of index sets deleted.
	IndexSetsRemoved int

	// ObjectsRemoved is the count of objects deleted.
	ObjectsRemoved int64

	// BytesFreed is an estimate of bytes freed based on active object sizes.
	// Note: this reflects indexed object sizes, not actual DB file shrinkage.
	BytesFreed int64

	// Candidates lists index sets that would be/were removed.
	Candidates []IndexListEntry
}

// GarbageCollect removes old index sets based on the given parameters.
//
// The cleanup strategy:
// 1. If MaxAge > 0: remove index sets created more than MaxAge ago
// 2. If KeepLast > 0: keep at least KeepLast index sets per base URI
//
// KeepLast takes precedence: even if an index set is older than MaxAge,
// it won't be removed if it's within the KeepLast threshold for its base URI.
func GarbageCollect(ctx context.Context, db *sql.DB, params GCParams) (*GCResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	result := &GCResult{}

	// Get all index sets with stats
	entries, err := ListIndexSetsWithStats(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("list index sets: %w", err)
	}

	if len(entries) == 0 {
		return result, nil
	}

	// Group by base URI to respect KeepLast
	byBaseURI := make(map[string][]IndexListEntry)
	for _, e := range entries {
		byBaseURI[e.BaseURI] = append(byBaseURI[e.BaseURI], e)
	}

	// Determine candidates for removal
	now := time.Now().UTC()
	cutoff := time.Time{}
	if params.MaxAge > 0 {
		cutoff = now.Add(-params.MaxAge)
	}

	for _, grouped := range byBaseURI {
		// Sort by CreatedAt DESC to ensure KeepLast keeps the most recent entries
		sort.Slice(grouped, func(i, j int) bool {
			return grouped[i].CreatedAt.After(grouped[j].CreatedAt)
		})

		// Skip KeepLast entries (now sorted by created_at DESC)
		toCheck := grouped
		if params.KeepLast > 0 && len(grouped) > params.KeepLast {
			toCheck = grouped[params.KeepLast:]
		} else if params.KeepLast > 0 {
			// Not enough to remove any
			continue
		}

		for _, entry := range toCheck {
			// Check age if MaxAge is specified
			if params.MaxAge > 0 && entry.CreatedAt.After(cutoff) {
				continue
			}

			// This entry is a candidate for removal
			result.Candidates = append(result.Candidates, entry)
			result.BytesFreed += entry.TotalSizeBytes
			result.ObjectsRemoved += entry.ObjectCount
		}
	}

	result.IndexSetsRemoved = len(result.Candidates)

	// Actually delete if not dry run
	if !params.DryRun {
		for _, candidate := range result.Candidates {
			if err := DeleteIndexSet(ctx, db, candidate.IndexSetID); err != nil {
				return result, fmt.Errorf("delete index set %s: %w", candidate.IndexSetID, err)
			}
		}
	}

	return result, nil
}
