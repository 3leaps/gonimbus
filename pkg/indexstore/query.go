package indexstore

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/bmatcuk/doublestar/v4"

	"github.com/3leaps/gonimbus/pkg/match"
)

// QueryParams specifies filters for querying indexed objects.
type QueryParams struct {
	// IndexSetID limits query to a specific index set.
	// Required.
	IndexSetID string

	// Pattern is a doublestar glob pattern to match against rel_key.
	// Optional. If empty, matches all keys.
	Pattern string

	// KeyRegex is a regex pattern to match against rel_key.
	// Applied after glob pattern (if both specified).
	// Optional.
	KeyRegex string

	// MinSize filters objects >= this size in bytes.
	// Optional. Zero means no minimum.
	MinSize int64

	// MaxSize filters objects <= this size in bytes.
	// Optional. Zero means no maximum.
	MaxSize int64

	// ModifiedAfter filters objects modified after this time.
	// Optional. Zero time means no lower bound.
	ModifiedAfter time.Time

	// ModifiedBefore filters objects modified before this time.
	// Optional. Zero time means no upper bound.
	ModifiedBefore time.Time

	// IncludeDeleted includes soft-deleted objects in results.
	// Default: false (only non-deleted objects).
	IncludeDeleted bool

	// Limit caps the number of results returned.
	// Optional. Zero means no limit.
	Limit int
}

// QueryResult holds a single object from the query.
type QueryResult struct {
	RelKey       string
	SizeBytes    int64
	LastModified *time.Time
	ETag         string
	DeletedAt    *time.Time
}

// QueryStats holds statistics about the query execution.
type QueryStats struct {
	// TimestampParseErrors is the count of rows with unparseable timestamps.
	// These rows are included in results but with nil timestamp fields.
	TimestampParseErrors int64
}

// QueryObjects queries the objects_current table with the given filters.
//
// Pattern matching uses doublestar semantics (same as crawl).
// Results are returned in rel_key order for deterministic output.
//
// Optimization: when Pattern has a derivable prefix (e.g., "data/2025/**"),
// prefix pushdown is applied via SQL LIKE to use the primary key index.
//
// Timestamp parse failures are handled gracefully: the row is included with
// nil timestamp fields, and the count is tracked in QueryStats. Callers should
// check stats.TimestampParseErrors and warn if non-zero.
func QueryObjects(ctx context.Context, db *sql.DB, params QueryParams) ([]QueryResult, QueryStats, error) {
	var stats QueryStats

	if params.IndexSetID == "" {
		return nil, stats, fmt.Errorf("index_set_id is required")
	}

	// Compile regex if provided
	var keyRe *regexp.Regexp
	if params.KeyRegex != "" {
		var err error
		keyRe, err = regexp.Compile(params.KeyRegex)
		if err != nil {
			return nil, stats, fmt.Errorf("invalid key regex: %w", err)
		}
	}

	// Validate pattern if provided
	if params.Pattern != "" && !doublestar.ValidatePattern(params.Pattern) {
		return nil, stats, fmt.Errorf("invalid glob pattern: %s", params.Pattern)
	}

	// Determine if we need client-side filtering
	needsClientFilter := params.Pattern != "" || keyRe != nil

	// Build SQL query with filters that can be pushed to DB
	query := `SELECT rel_key, size_bytes, last_modified, etag, deleted_at
		FROM objects_current
		WHERE index_set_id = ?`
	args := []interface{}{params.IndexSetID}

	// Deleted filter
	if !params.IncludeDeleted {
		query += ` AND deleted_at IS NULL`
	}

	// OPTIMIZATION A: Prefix pushdown for glob patterns
	// Derive literal prefix from pattern and push to SQL via LIKE
	if params.Pattern != "" {
		prefix := match.DerivePrefix(params.Pattern)
		if prefix != "" {
			// Use LIKE for prefix matching - SQLite can use indexes with LIKE 'prefix%'
			// Escape SQL LIKE wildcards in the prefix to ensure exact matching
			query += ` AND rel_key LIKE ? ESCAPE '\'`
			args = append(args, escapeLikePrefix(prefix)+"%")
		}
	}

	// Size filters (can be pushed to DB)
	if params.MinSize > 0 {
		query += ` AND size_bytes >= ?`
		args = append(args, params.MinSize)
	}
	if params.MaxSize > 0 {
		query += ` AND size_bytes <= ?`
		args = append(args, params.MaxSize)
	}

	// Date filters (can be pushed to DB)
	if !params.ModifiedAfter.IsZero() {
		query += ` AND last_modified >= ?`
		args = append(args, params.ModifiedAfter.Format(time.RFC3339))
	}
	if !params.ModifiedBefore.IsZero() {
		query += ` AND last_modified <= ?`
		args = append(args, params.ModifiedBefore.Format(time.RFC3339))
	}

	query += ` ORDER BY rel_key`

	// OPTIMIZATION D: SQL LIMIT when no client-side filtering needed
	if !needsClientFilter && params.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, params.Limit)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, stats, fmt.Errorf("query objects: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var results []QueryResult
	for rows.Next() {
		var (
			relKey       string
			sizeBytes    int64
			lastModified sql.NullString
			etag         sql.NullString
			deletedAt    sql.NullString
		)

		if err := rows.Scan(&relKey, &sizeBytes, &lastModified, &etag, &deletedAt); err != nil {
			return nil, stats, fmt.Errorf("scan row: %w", err)
		}

		// Apply glob pattern filter (client-side, after prefix pushdown)
		if params.Pattern != "" {
			matched, err := doublestar.Match(params.Pattern, relKey)
			if err != nil {
				return nil, stats, fmt.Errorf("match pattern: %w", err)
			}
			if !matched {
				continue
			}
		}

		// Apply regex filter (client-side)
		if keyRe != nil && !keyRe.MatchString(relKey) {
			continue
		}

		result := QueryResult{
			RelKey:    relKey,
			SizeBytes: sizeBytes,
		}

		// Parse timestamps - warn+skip on failure (don't fail the query)
		// This handles varied timestamp formats from different providers gracefully.
		if lastModified.Valid {
			if t, err := parseTimestamp(lastModified.String); err == nil {
				result.LastModified = &t
			} else {
				stats.TimestampParseErrors++
			}
		}

		if etag.Valid {
			result.ETag = etag.String
		}

		if deletedAt.Valid {
			if t, err := parseTimestamp(deletedAt.String); err == nil {
				result.DeletedAt = &t
			} else {
				stats.TimestampParseErrors++
			}
		}

		results = append(results, result)

		// Apply limit after all client-side filters
		if params.Limit > 0 && len(results) >= params.Limit {
			break
		}
	}

	if err := rows.Err(); err != nil {
		return nil, stats, fmt.Errorf("iterate rows: %w", err)
	}

	return results, stats, nil
}

// QueryObjectCount counts objects matching the query without materializing results.
//
// This is optimized for --count scenarios:
// - When no Pattern/KeyRegex: uses COUNT(*) at DB level (fast)
// - Otherwise: streams rows and counts matches (constant memory)
//
// Optimization: prefix pushdown is applied when Pattern has a derivable prefix.
func QueryObjectCount(ctx context.Context, db *sql.DB, params QueryParams) (int64, error) {
	if params.IndexSetID == "" {
		return 0, fmt.Errorf("index_set_id is required")
	}

	// Compile regex if provided
	var keyRe *regexp.Regexp
	if params.KeyRegex != "" {
		var err error
		keyRe, err = regexp.Compile(params.KeyRegex)
		if err != nil {
			return 0, fmt.Errorf("invalid key regex: %w", err)
		}
	}

	// Validate pattern if provided
	if params.Pattern != "" && !doublestar.ValidatePattern(params.Pattern) {
		return 0, fmt.Errorf("invalid glob pattern: %s", params.Pattern)
	}

	// Fast path: no client-side filtering needed, use COUNT(*)
	if params.Pattern == "" && keyRe == nil {
		return queryCountFast(ctx, db, params)
	}

	// Slow path: stream rows and count matches
	return queryCountStreaming(ctx, db, params, keyRe)
}

// queryCountFast uses COUNT(*) when no client-side filtering is needed.
// Note: Limit is ignored for count queries (count returns total matches).
func queryCountFast(ctx context.Context, db *sql.DB, params QueryParams) (int64, error) {
	query := `SELECT COUNT(*) FROM objects_current WHERE index_set_id = ?`
	args := []interface{}{params.IndexSetID}

	if !params.IncludeDeleted {
		query += ` AND deleted_at IS NULL`
	}
	if params.MinSize > 0 {
		query += ` AND size_bytes >= ?`
		args = append(args, params.MinSize)
	}
	if params.MaxSize > 0 {
		query += ` AND size_bytes <= ?`
		args = append(args, params.MaxSize)
	}
	if !params.ModifiedAfter.IsZero() {
		query += ` AND last_modified >= ?`
		args = append(args, params.ModifiedAfter.Format(time.RFC3339))
	}
	if !params.ModifiedBefore.IsZero() {
		query += ` AND last_modified <= ?`
		args = append(args, params.ModifiedBefore.Format(time.RFC3339))
	}

	var count int64
	if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count objects: %w", err)
	}

	return count, nil
}

// queryCountStreaming counts matches by streaming rows (constant memory).
func queryCountStreaming(ctx context.Context, db *sql.DB, params QueryParams, keyRe *regexp.Regexp) (int64, error) {
	// Only select rel_key - we don't need other columns for counting
	query := `SELECT rel_key FROM objects_current WHERE index_set_id = ?`
	args := []interface{}{params.IndexSetID}

	if !params.IncludeDeleted {
		query += ` AND deleted_at IS NULL`
	}

	// Prefix pushdown for glob patterns
	if params.Pattern != "" {
		prefix := match.DerivePrefix(params.Pattern)
		if prefix != "" {
			// Escape SQL LIKE wildcards in the prefix
			query += ` AND rel_key LIKE ? ESCAPE '\'`
			args = append(args, escapeLikePrefix(prefix)+"%")
		}
	}

	if params.MinSize > 0 {
		query += ` AND size_bytes >= ?`
		args = append(args, params.MinSize)
	}
	if params.MaxSize > 0 {
		query += ` AND size_bytes <= ?`
		args = append(args, params.MaxSize)
	}
	if !params.ModifiedAfter.IsZero() {
		query += ` AND last_modified >= ?`
		args = append(args, params.ModifiedAfter.Format(time.RFC3339))
	}
	if !params.ModifiedBefore.IsZero() {
		query += ` AND last_modified <= ?`
		args = append(args, params.ModifiedBefore.Format(time.RFC3339))
	}

	// No ORDER BY for counting - unnecessary overhead at scale

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("query objects: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var count int64
	for rows.Next() {
		var relKey string
		if err := rows.Scan(&relKey); err != nil {
			return 0, fmt.Errorf("scan row: %w", err)
		}

		// Apply glob pattern filter
		if params.Pattern != "" {
			matched, err := doublestar.Match(params.Pattern, relKey)
			if err != nil {
				return 0, fmt.Errorf("match pattern: %w", err)
			}
			if !matched {
				continue
			}
		}

		// Apply regex filter
		if keyRe != nil && !keyRe.MatchString(relKey) {
			continue
		}

		count++
		// Note: Limit is ignored for count queries (count returns total matches)
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate rows: %w", err)
	}

	return count, nil
}

// escapeLikePrefix escapes SQL LIKE wildcard characters in a prefix string.
// This ensures that literal %, _, and \ characters in object keys are matched exactly.
// Uses backslash as the escape character (requires ESCAPE '\' in the LIKE clause).
func escapeLikePrefix(s string) string {
	// Order matters: escape backslash first, then the wildcards
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}

// parseTimestamp parses a timestamp string with RFC3339Nano fallback.
// Returns error on parse failure to surface data quality issues.
func parseTimestamp(s string) (time.Time, error) {
	parsed, err := parseDBTimeString(s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timestamp format: %q", s)
	}
	return parsed, nil
}

// GetIndexSetByBaseURI finds an IndexSet by its base_uri.
//
// If multiple index sets exist for the same base_uri (different build params),
// returns the most recently created one.
// Returns nil if no matching index set is found.
func GetIndexSetByBaseURI(ctx context.Context, db *sql.DB, baseURI string) (*IndexSet, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var is IndexSet
	var (
		storageProvider sql.NullString
		cloudProvider   sql.NullString
		regionKind      sql.NullString
		region          sql.NullString
		endpoint        sql.NullString
		endpointHost    sql.NullString
	)

	var createdAtRaw any
	err := db.QueryRowContext(ctx, `
		SELECT index_set_id, base_uri, provider, storage_provider,
		       cloud_provider, region_kind, region, endpoint, endpoint_host,
		       index_build_hash, created_at
		FROM index_sets
		WHERE base_uri = ?
		ORDER BY created_at DESC
		LIMIT 1
	`, baseURI).Scan(
		&is.IndexSetID, &is.BaseURI, &is.Provider,
		&storageProvider, &cloudProvider, &regionKind,
		&region, &endpoint, &endpointHost,
		&is.IndexBuildHash, &createdAtRaw,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query index set by base_uri: %w", err)
	}

	createdAt, err := parseDBTimeValue(createdAtRaw)
	if err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}
	is.CreatedAt = createdAt

	// Transfer nullable values
	if storageProvider.Valid {
		is.StorageProvider = storageProvider.String
	}
	if cloudProvider.Valid {
		is.CloudProvider = cloudProvider.String
	}
	if regionKind.Valid {
		is.RegionKind = regionKind.String
	}
	if region.Valid {
		is.Region = region.String
	}
	if endpoint.Valid {
		is.Endpoint = endpoint.String
	}
	if endpointHost.Valid {
		is.EndpointHost = endpointHost.String
	}

	return &is, nil
}
