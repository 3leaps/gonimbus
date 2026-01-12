package indexstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"time"
)

// IndexSet represents a unique index identity.
//
// An IndexSet is identified by (base_uri, provider_identity, build_params_hash).
// Different build params (filters, path-date extraction, etc.) produce different IndexSets.
type IndexSet struct {
	IndexSetID      string
	BaseURI         string
	Provider        string
	StorageProvider string
	CloudProvider   string
	RegionKind      string
	Region          string
	Endpoint        string
	EndpointHost    string
	BuildParamsHash string
	CreatedAt       time.Time
}

// IndexSetParams contains parameters for creating or finding an IndexSet.
type IndexSetParams struct {
	BaseURI         string
	Provider        string
	StorageProvider string
	CloudProvider   string
	RegionKind      string
	Region          string
	Endpoint        string
	EndpointHost    string // Explicit endpoint host; if empty and Endpoint is set, derived from Endpoint
	BuildParams     BuildParams
}

// BuildParams captures parameters that affect index contents.
//
// Any change in build_params_hash requires a new IndexSet to ensure
// index consistency and proper query semantics.
type BuildParams struct {
	SourceType         string
	PathDateExtraction *PathDateExtraction
	SchemaVersion      int
	GonimbusVersion    string

	// Match parameters that affect index contents.
	Includes      []string
	Excludes      []string
	IncludeHidden bool
	FiltersHash   string // Hash of filters (size, modified, key_regex)
}

// PathDateExtraction configures extracting dates from object key paths.
type PathDateExtraction struct {
	Method       string // "regex" or "segment"
	Regex        string // For regex method
	SegmentIndex int    // For segment method (0-indexed)
}

// ComputeBuildParamsHash computes a hash for build parameters.
func ComputeBuildParamsHash(bp BuildParams) (string, error) {
	h := sha256.New()

	h.Write([]byte(bp.SourceType))
	_, _ = fmt.Fprintf(h, "schema=%d", bp.SchemaVersion)
	_, _ = fmt.Fprintf(h, "version=%s", bp.GonimbusVersion)

	if bp.PathDateExtraction != nil {
		h.Write([]byte("path_date=1"))
		h.Write([]byte(bp.PathDateExtraction.Method))
		h.Write([]byte(bp.PathDateExtraction.Regex))
		_, _ = fmt.Fprintf(h, "segment=%d", bp.PathDateExtraction.SegmentIndex)
	}

	// Include match/filtering parameters in hash for uniqueness.
	if bp.Includes != nil {
		for _, inc := range bp.Includes {
			h.Write([]byte("include="))
			h.Write([]byte(inc))
		}
	}
	if bp.Excludes != nil {
		for _, exc := range bp.Excludes {
			h.Write([]byte("exclude="))
			h.Write([]byte(exc))
		}
	}
	if bp.IncludeHidden {
		h.Write([]byte("include_hidden=1"))
	}
	if bp.FiltersHash != "" {
		h.Write([]byte("filters="))
		h.Write([]byte(bp.FiltersHash))
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// FindOrCreateIndexSet finds an existing IndexSet or creates a new one.
//
// Returns the IndexSet and whether it was created (true) or found existing (false).
func FindOrCreateIndexSet(ctx context.Context, db *sql.DB, params IndexSetParams) (*IndexSet, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	hash, err := ComputeBuildParamsHash(params.BuildParams)
	if err != nil {
		return nil, false, fmt.Errorf("compute build params hash: %w", err)
	}

	// Use explicit EndpointHost if provided; otherwise derive from Endpoint.
	endpointHost := params.EndpointHost
	if endpointHost == "" && params.Endpoint != "" {
		endpointHost = deriveEndpointHost(params.Endpoint)
	}

	// Try to find existing by full explicit identity tuple.
	// ENTARCH: Match on all identity fields, don't treat NULL as wildcard.
	var existingID string
	findErr := db.QueryRowContext(ctx,
		`SELECT index_set_id FROM index_sets
		 WHERE base_uri = ? AND provider = ?
		 AND storage_provider = ? AND cloud_provider = ?
		 AND region_kind = ? AND region = ?
		 AND endpoint_host = ?
		 AND build_params_hash = ?`,
		params.BaseURI, params.Provider,
		params.StorageProvider, params.CloudProvider,
		params.RegionKind, params.Region,
		endpointHost, hash).Scan(&existingID)

	if findErr == nil {
		// Found existing
		is, err := getFullIndexSet(ctx, db, existingID)
		return is, false, err
	}

	if findErr != sql.ErrNoRows {
		return nil, false, fmt.Errorf("query index_set: %w", findErr)
	}

	// Create new
	now := time.Now().UTC()
	newID := generateIndexSetID(params.BaseURI, hash)

	_, err = db.ExecContext(ctx,
		`INSERT INTO index_sets
		 (index_set_id, base_uri, provider, storage_provider, cloud_provider,
		  region_kind, region, endpoint, endpoint_host, build_params_hash, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		newID, params.BaseURI, params.Provider, params.StorageProvider,
		params.CloudProvider, params.RegionKind, params.Region,
		params.Endpoint, endpointHost, hash, now)

	if err != nil {
		return nil, false, fmt.Errorf("create index_set: %w", err)
	}

	is, err := getFullIndexSet(ctx, db, newID)
	return is, true, err
}

// GetIndexSet retrieves an IndexSet by ID.
func GetIndexSet(ctx context.Context, db *sql.DB, indexSetID string) (*IndexSet, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return getFullIndexSet(ctx, db, indexSetID)
}

// getFullIndexSet retrieves IndexSet with all fields populated.
func getFullIndexSet(ctx context.Context, db *sql.DB, indexSetID string) (*IndexSet, error) {
	var is IndexSet

	err := db.QueryRowContext(ctx,
		`SELECT index_set_id, base_uri, provider, storage_provider,
		        cloud_provider, region_kind, region, endpoint, endpoint_host,
		        build_params_hash, created_at
		 FROM index_sets WHERE index_set_id = ?`,
		indexSetID).Scan(
		&is.IndexSetID, &is.BaseURI, &is.Provider, &is.StorageProvider,
		&is.CloudProvider, &is.RegionKind, &is.Region, &is.Endpoint,
		&is.EndpointHost, &is.BuildParamsHash, &is.CreatedAt)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("index_set not found: %s", indexSetID)
	}
	if err != nil {
		return nil, fmt.Errorf("get index_set: %w", err)
	}

	return &is, nil
}

// ListIndexSets lists all IndexSets, optionally filtered by base URI.
func ListIndexSets(ctx context.Context, db *sql.DB, baseURI string) ([]IndexSet, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var rows *sql.Rows
	var err error

	if baseURI == "" {
		rows, err = db.QueryContext(ctx,
			`SELECT index_set_id, base_uri, provider, storage_provider,
			        cloud_provider, region_kind, region, endpoint, endpoint_host,
			        build_params_hash, created_at
			 FROM index_sets ORDER BY created_at DESC`)
	} else {
		rows, err = db.QueryContext(ctx,
			`SELECT index_set_id, base_uri, provider, storage_provider,
			        cloud_provider, region_kind, region, endpoint, endpoint_host,
			        build_params_hash, created_at
			 FROM index_sets WHERE base_uri = ? ORDER BY created_at DESC`,
			baseURI)
	}

	if err != nil {
		return nil, fmt.Errorf("list index_sets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var sets []IndexSet
	for rows.Next() {
		var is IndexSet
		err := rows.Scan(
			&is.IndexSetID, &is.BaseURI, &is.Provider, &is.StorageProvider,
			&is.CloudProvider, &is.RegionKind, &is.Region, &is.Endpoint,
			&is.EndpointHost, &is.BuildParamsHash, &is.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("scan index_set: %w", err)
		}
		sets = append(sets, is)
	}

	return sets, nil
}

// DeleteIndexSet deletes an IndexSet and all associated data.
func DeleteIndexSet(ctx context.Context, db *sql.DB, indexSetID string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Delete in order respecting FKs: objects_current -> prefix_stats/events -> runs -> index_set
	_, err = tx.ExecContext(ctx, `DELETE FROM objects_current WHERE index_set_id = ?`, indexSetID)
	if err != nil {
		return fmt.Errorf("delete objects_current: %w", err)
	}

	_, err = tx.ExecContext(ctx, `DELETE FROM prefix_stats WHERE index_set_id = ?`, indexSetID)
	if err != nil {
		return fmt.Errorf("delete prefix_stats: %w", err)
	}

	_, err = tx.ExecContext(ctx, `DELETE FROM index_run_events WHERE run_id IN (SELECT run_id FROM index_runs WHERE index_set_id = ?)`, indexSetID)
	if err != nil {
		return fmt.Errorf("delete index_run_events: %w", err)
	}

	_, err = tx.ExecContext(ctx, `DELETE FROM index_runs WHERE index_set_id = ?`, indexSetID)
	if err != nil {
		return fmt.Errorf("delete index_runs: %w", err)
	}

	_, err = tx.ExecContext(ctx, `DELETE FROM index_sets WHERE index_set_id = ?`, indexSetID)
	if err != nil {
		return fmt.Errorf("delete index_set: %w", err)
	}

	return tx.Commit()
}

// generateIndexSetID generates a stable ID for an IndexSet.
func generateIndexSetID(baseURI, hash string) string {
	h := sha256.New()
	h.Write([]byte(baseURI))
	h.Write([]byte(hash))
	return hex.EncodeToString(h.Sum(nil))
}

// deriveEndpointHost extracts host from endpoint URL.
// Handles credentials (user:pass@) and strips port.
func deriveEndpointHost(endpoint string) string {
	if endpoint == "" {
		return ""
	}

	parsed, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}

	host := parsed.Host
	if host == "" {
		return ""
	}

	host, _, err = net.SplitHostPort(host)
	if err != nil {
		return host
	}

	return host
}
