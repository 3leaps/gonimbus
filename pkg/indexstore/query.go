package indexstore

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"time"

	"github.com/bmatcuk/doublestar/v4"
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

// QueryObjects queries the objects_current table with the given filters.
//
// Pattern matching uses doublestar semantics (same as crawl).
// Results are returned in rel_key order for deterministic output.
func QueryObjects(ctx context.Context, db *sql.DB, params QueryParams) ([]QueryResult, error) {
	if params.IndexSetID == "" {
		return nil, fmt.Errorf("index_set_id is required")
	}

	// Compile regex if provided
	var keyRe *regexp.Regexp
	if params.KeyRegex != "" {
		var err error
		keyRe, err = regexp.Compile(params.KeyRegex)
		if err != nil {
			return nil, fmt.Errorf("invalid key regex: %w", err)
		}
	}

	// Validate pattern if provided
	if params.Pattern != "" && !doublestar.ValidatePattern(params.Pattern) {
		return nil, fmt.Errorf("invalid glob pattern: %s", params.Pattern)
	}

	// Build SQL query with filters that can be pushed to DB
	query := `SELECT rel_key, size_bytes, last_modified, etag, deleted_at
		FROM objects_current
		WHERE index_set_id = ?`
	args := []interface{}{params.IndexSetID}

	// Deleted filter
	if !params.IncludeDeleted {
		query += ` AND deleted_at IS NULL`
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

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query objects: %w", err)
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
			return nil, fmt.Errorf("scan row: %w", err)
		}

		// Apply glob pattern filter (client-side)
		if params.Pattern != "" {
			matched, err := doublestar.Match(params.Pattern, relKey)
			if err != nil {
				return nil, fmt.Errorf("match pattern: %w", err)
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

		if lastModified.Valid {
			t, err := time.Parse(time.RFC3339, lastModified.String)
			if err == nil {
				result.LastModified = &t
			}
		}

		if etag.Valid {
			result.ETag = etag.String
		}

		if deletedAt.Valid {
			t, err := time.Parse(time.RFC3339, deletedAt.String)
			if err == nil {
				result.DeletedAt = &t
			}
		}

		results = append(results, result)

		// Apply limit after all filters
		if params.Limit > 0 && len(results) >= params.Limit {
			break
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return results, nil
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

	err := db.QueryRowContext(ctx, `
		SELECT index_set_id, base_uri, provider, storage_provider,
		       cloud_provider, region_kind, region, endpoint, endpoint_host,
		       build_params_hash, created_at
		FROM index_sets
		WHERE base_uri = ?
		ORDER BY created_at DESC
		LIMIT 1
	`, baseURI).Scan(
		&is.IndexSetID, &is.BaseURI, &is.Provider,
		&storageProvider, &cloudProvider, &regionKind,
		&region, &endpoint, &endpointHost,
		&is.BuildParamsHash, &is.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query index set by base_uri: %w", err)
	}

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
