package indexstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ObjectRow represents a row in the objects_current table.
type ObjectRow struct {
	IndexSetID    string
	RelKey        string
	SizeBytes     int64
	LastModified  *time.Time
	ETag          string
	LastSeenRunID string
	LastSeenAt    time.Time
	DeletedAt     *time.Time
}

// UpsertObject inserts or updates an object in objects_current.
//
// If the object exists, updates last_seen_run_id, last_seen_at, and clears deleted_at.
// If the object doesn't exist, inserts a new row.
func UpsertObject(ctx context.Context, db *sql.DB, obj ObjectRow) error {
	if ctx == nil {
		ctx = context.Background()
	}

	_, err := db.ExecContext(ctx,
		`INSERT INTO objects_current
		 (index_set_id, rel_key, size_bytes, last_modified, etag,
		  last_seen_run_id, last_seen_at, deleted_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
		 ON CONFLICT(index_set_id, rel_key) DO UPDATE SET
		   size_bytes = excluded.size_bytes,
		   last_modified = excluded.last_modified,
		   etag = excluded.etag,
		   last_seen_run_id = excluded.last_seen_run_id,
		   last_seen_at = excluded.last_seen_at,
		   deleted_at = NULL`,
		obj.IndexSetID, obj.RelKey, obj.SizeBytes, obj.LastModified,
		obj.ETag, obj.LastSeenRunID, obj.LastSeenAt)

	if err != nil {
		return fmt.Errorf("upsert object: %w", err)
	}

	return nil
}

// BatchUpsertObjects inserts or updates multiple objects in a single transaction.
//
// This is more efficient than individual UpsertObject calls for bulk ingestion.
func BatchUpsertObjects(ctx context.Context, db *sql.DB, objects []ObjectRow) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(objects) == 0 {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO objects_current
		 (index_set_id, rel_key, size_bytes, last_modified, etag,
		  last_seen_run_id, last_seen_at, deleted_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
		 ON CONFLICT(index_set_id, rel_key) DO UPDATE SET
		   size_bytes = excluded.size_bytes,
		   last_modified = excluded.last_modified,
		   etag = excluded.etag,
		   last_seen_run_id = excluded.last_seen_run_id,
		   last_seen_at = excluded.last_seen_at,
		   deleted_at = NULL`)
	if err != nil {
		return fmt.Errorf("prepare stmt: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, obj := range objects {
		_, err := stmt.ExecContext(ctx,
			obj.IndexSetID, obj.RelKey, obj.SizeBytes, obj.LastModified,
			obj.ETag, obj.LastSeenRunID, obj.LastSeenAt)
		if err != nil {
			return fmt.Errorf("exec upsert for %s: %w", obj.RelKey, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

// GetObject retrieves a single object by index set and relative key.
func GetObject(ctx context.Context, db *sql.DB, indexSetID, relKey string) (*ObjectRow, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var obj ObjectRow
	var lastModifiedRaw any
	var lastSeenAtRaw any
	var deletedAtRaw any

	err := db.QueryRowContext(ctx,
		`SELECT index_set_id, rel_key, size_bytes, last_modified, etag,
		        last_seen_run_id, last_seen_at, deleted_at
		 FROM objects_current
		 WHERE index_set_id = ? AND rel_key = ?`,
		indexSetID, relKey).Scan(
		&obj.IndexSetID, &obj.RelKey, &obj.SizeBytes, &lastModifiedRaw,
		&obj.ETag, &obj.LastSeenRunID, &lastSeenAtRaw, &deletedAtRaw)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get object: %w", err)
	}

	lastModified, err := parseOptionalDBTime(lastModifiedRaw)
	if err != nil {
		return nil, fmt.Errorf("parse last_modified: %w", err)
	}
	obj.LastModified = lastModified

	lastSeenAt, err := parseDBTimeValue(lastSeenAtRaw)
	if err != nil {
		return nil, fmt.Errorf("parse last_seen_at: %w", err)
	}
	obj.LastSeenAt = lastSeenAt

	deletedAt, err := parseOptionalDBTime(deletedAtRaw)
	if err != nil {
		return nil, fmt.Errorf("parse deleted_at: %w", err)
	}
	obj.DeletedAt = deletedAt

	return &obj, nil
}

// CountObjects returns the count of objects in an index set.
// If includeDeleted is false, only counts objects where deleted_at IS NULL.
func CountObjects(ctx context.Context, db *sql.DB, indexSetID string, includeDeleted bool) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var count int64
	var err error

	if includeDeleted {
		err = db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM objects_current WHERE index_set_id = ?`,
			indexSetID).Scan(&count)
	} else {
		err = db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM objects_current WHERE index_set_id = ? AND deleted_at IS NULL`,
			indexSetID).Scan(&count)
	}

	if err != nil {
		return 0, fmt.Errorf("count objects: %w", err)
	}

	return count, nil
}

// DeriveRelKey derives the relative key from a full object key and base URI.
//
// The base URI should be in the form "s3://bucket/prefix/" and the key
// should be the full object key. Returns the key relative to the base prefix,
// normalized to never start with "/".
//
// Examples:
//
//	DeriveRelKey("s3://bucket/prefix/", "prefix/file.txt") => "file.txt"
//	DeriveRelKey("s3://bucket/a/b/", "a/b/c/d.txt") => "c/d.txt"
//	DeriveRelKey("s3://bucket/", "file.txt") => "file.txt"
func DeriveRelKey(baseURI, fullKey string) string {
	// Extract prefix from base URI (strip s3://bucket/ prefix)
	// base_uri: s3://bucket/prefix/
	// fullKey: prefix/path/to/object.txt
	// relKey: path/to/object.txt

	// Find the path portion of the base URI
	parts := strings.SplitN(baseURI, "://", 2)
	if len(parts) != 2 {
		return normalizeRelKey(fullKey)
	}

	// Split bucket and prefix
	pathParts := strings.SplitN(parts[1], "/", 2)
	if len(pathParts) < 2 {
		return normalizeRelKey(fullKey)
	}

	basePrefix := pathParts[1]
	if basePrefix == "" {
		return normalizeRelKey(fullKey)
	}

	// Normalize basePrefix: remove trailing slash for consistent matching
	basePrefix = strings.TrimSuffix(basePrefix, "/")

	// Strip the base prefix from the full key
	if strings.HasPrefix(fullKey, basePrefix) {
		rel := strings.TrimPrefix(fullKey, basePrefix)
		return normalizeRelKey(rel)
	}

	return normalizeRelKey(fullKey)
}

// normalizeRelKey ensures rel_key never starts with "/".
func normalizeRelKey(key string) string {
	return strings.TrimPrefix(key, "/")
}
