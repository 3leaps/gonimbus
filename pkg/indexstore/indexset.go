package indexstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/match"
)

// IndexSet represents a unique index identity.
//
// An IndexSet is identified by (base_uri, provider_identity, index_build_hash).
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
	IndexBuildHash  string
	CreatedAt       time.Time
}

// IndexSetIdentityPayload captures the canonical identity for hashing.
type IndexSetIdentityPayload struct {
	BaseURI         string                    `json:"base_uri"`
	Provider        string                    `json:"provider"`
	StorageProvider string                    `json:"storage_provider,omitempty"`
	CloudProvider   string                    `json:"cloud_provider,omitempty"`
	RegionKind      string                    `json:"region_kind,omitempty"`
	Region          string                    `json:"region,omitempty"`
	EndpointHost    string                    `json:"endpoint_host,omitempty"`
	Build           IndexSetIdentityBuild     `json:"build"`
	PathDate        *IndexSetIdentityPathDate `json:"path_date,omitempty"`
}

// IndexSetIdentityBuild captures build parameters that affect index identity.
type IndexSetIdentityBuild struct {
	SourceType      string   `json:"source_type"`
	SchemaVersion   int      `json:"schema_version"`
	GonimbusVersion string   `json:"gonimbus_version,omitempty"`
	Includes        []string `json:"includes"`
	Excludes        []string `json:"excludes,omitempty"`
	IncludeHidden   bool     `json:"include_hidden"`
	FiltersHash     string   `json:"filters_hash,omitempty"`
}

// IndexSetIdentityPathDate captures path date extraction identity fields.
type IndexSetIdentityPathDate struct {
	Method       string `json:"method"`
	Regex        string `json:"regex,omitempty"`
	SegmentIndex int    `json:"segment_index,omitempty"`
}

// IndexSetIdentityResult contains derived identity fields.
type IndexSetIdentityResult struct {
	IndexSetID      string
	DirName         string
	CanonicalJSON   string
	CanonicalSHA256 string
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
// Any change in index_build_hash requires a new IndexSet to ensure
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

// ComputeIndexSetID computes canonical identity details for an IndexSet.
func ComputeIndexSetID(params IndexSetParams) (*IndexSetIdentityResult, error) {
	payload := buildIndexSetIdentityPayload(params)
	canonicalJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal identity payload: %w", err)
	}

	sha := sha256.Sum256(canonicalJSON)
	shaHex := hex.EncodeToString(sha[:])
	if len(shaHex) < 16 {
		return nil, fmt.Errorf("identity hash too short")
	}

	return &IndexSetIdentityResult{
		IndexSetID:      "idx_" + shaHex,
		DirName:         "idx_" + shaHex[:16],
		CanonicalJSON:   string(canonicalJSON),
		CanonicalSHA256: shaHex,
	}, nil
}

// FindOrCreateIndexSet finds an existing IndexSet or creates a new one.
//
// Returns the IndexSet and whether it was created (true) or found existing (false).
func FindOrCreateIndexSet(ctx context.Context, db *sql.DB, params IndexSetParams) (*IndexSet, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	identity, err := ComputeIndexSetID(params)
	if err != nil {
		return nil, false, fmt.Errorf("compute index set identity: %w", err)
	}

	endpointHost := normalizeEndpointHost(params.EndpointHost)

	// Try to find existing by full explicit identity tuple.
	// ENTARCH: Match on all identity fields, don't treat NULL as wildcard.
	var existingID string
	findErr := db.QueryRowContext(ctx,
		`SELECT index_set_id FROM index_sets
		 WHERE base_uri = ? AND provider = ?
		 AND storage_provider = ? AND cloud_provider = ?
		 AND region_kind = ? AND region = ?
		 AND endpoint_host = ?
		 AND index_build_hash = ?`,
		normalizeBaseURI(params.BaseURI), params.Provider,
		params.StorageProvider, params.CloudProvider,
		params.RegionKind, params.Region,
		endpointHost, identity.CanonicalSHA256).Scan(&existingID)

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
	newID := identity.IndexSetID

	_, err = db.ExecContext(ctx,
		`INSERT INTO index_sets
		 (index_set_id, base_uri, provider, storage_provider, cloud_provider,
		  region_kind, region, endpoint, endpoint_host, index_build_hash, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		newID, normalizeBaseURI(params.BaseURI), params.Provider, params.StorageProvider,
		params.CloudProvider, params.RegionKind, params.Region,
		params.Endpoint, endpointHost, identity.CanonicalSHA256, now)

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
		        index_build_hash, created_at
		 FROM index_sets WHERE index_set_id = ?`,
		indexSetID).Scan(
		&is.IndexSetID, &is.BaseURI, &is.Provider, &is.StorageProvider,
		&is.CloudProvider, &is.RegionKind, &is.Region, &is.Endpoint,
		&is.EndpointHost, &is.IndexBuildHash, &is.CreatedAt)

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
		        index_build_hash, created_at
		 FROM index_sets ORDER BY created_at DESC`)

	} else {
		rows, err = db.QueryContext(ctx,
			`SELECT index_set_id, base_uri, provider, storage_provider,
		        cloud_provider, region_kind, region, endpoint, endpoint_host,
		        index_build_hash, created_at
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
			&is.EndpointHost, &is.IndexBuildHash, &is.CreatedAt)

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

func buildIndexSetIdentityPayload(params IndexSetParams) IndexSetIdentityPayload {
	endpointHost := normalizeEndpointHost(params.EndpointHost)
	baseURI := normalizeBaseURI(params.BaseURI)

	includes := normalizePatternList(params.BuildParams.Includes)
	if includes == nil {
		includes = []string{}
	}

	build := IndexSetIdentityBuild{
		SourceType:      strings.TrimSpace(params.BuildParams.SourceType),
		SchemaVersion:   params.BuildParams.SchemaVersion,
		GonimbusVersion: strings.TrimSpace(params.BuildParams.GonimbusVersion),
		Includes:        includes,
		Excludes:        normalizePatternList(params.BuildParams.Excludes),
		IncludeHidden:   params.BuildParams.IncludeHidden,
		FiltersHash:     strings.TrimSpace(params.BuildParams.FiltersHash),
	}

	payload := IndexSetIdentityPayload{
		BaseURI:         baseURI,
		Provider:        strings.TrimSpace(params.Provider),
		StorageProvider: strings.TrimSpace(params.StorageProvider),
		CloudProvider:   strings.TrimSpace(params.CloudProvider),
		RegionKind:      strings.TrimSpace(params.RegionKind),
		Region:          strings.TrimSpace(params.Region),
		EndpointHost:    endpointHost,
		Build:           build,
	}

	if params.BuildParams.PathDateExtraction != nil {
		payload.PathDate = &IndexSetIdentityPathDate{
			Method:       strings.TrimSpace(params.BuildParams.PathDateExtraction.Method),
			Regex:        strings.TrimSpace(params.BuildParams.PathDateExtraction.Regex),
			SegmentIndex: params.BuildParams.PathDateExtraction.SegmentIndex,
		}
	}

	return payload
}

func normalizeBaseURI(baseURI string) string {
	value := strings.TrimSpace(baseURI)
	if value == "" {
		return value
	}
	if !strings.HasSuffix(value, "/") {
		return value + "/"
	}
	return value
}

func normalizeEndpointHost(host string) string {
	value := strings.TrimSpace(host)
	if value == "" {
		return ""
	}
	return strings.ToLower(value)
}

func normalizePatternList(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	unique := make(map[string]struct{})
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		normalized := match.NormalizePattern(trimmed)
		normalized = strings.TrimPrefix(normalized, "/")
		if normalized == "" {
			continue
		}
		unique[normalized] = struct{}{}
	}
	if len(unique) == 0 {
		return nil
	}

	out := make([]string, 0, len(unique))
	for value := range unique {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
