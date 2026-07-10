package indexreader

import (
	"context"
	"fmt"
	"strings"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

// Format identifies the on-disk index substrate.
type Format string

const (
	// FormatSQLiteV1 is the legacy SQLite local index (index.db).
	FormatSQLiteV1 Format = "sqlite-v1"
	// FormatDurableV2 is the segment-backed durable snapshot format.
	FormatDurableV2 Format = "durable-v2"
)

// Meta describes the resolved index identity.
type Meta struct {
	Format     Format
	IndexSetID string
	BaseURI    string
	Provider   string
	// IdentityDir is the indexes/idx_* directory when known.
	IdentityDir string
	// SourcePath is the primary artifact path (index.db or latest.json).
	SourcePath string
	// RunID is the durable snapshot run id when Format is durable-v2.
	RunID string
}

// VisitObject is invoked for each matching row in rel_key order.
// Returning a non-nil error stops the walk and is returned to the caller.
type VisitObject func(result indexstore.QueryResult) error

// Reader is the narrow format-aware index read surface used by index query
// (and later stats/doctor/list consumers of the same seam).
type Reader interface {
	Meta() Meta
	// WalkObjects streams matching object rows in rel_key order without
	// accumulating them. On durable-v2, each segment digest is verified before
	// that segment's rows are visited. A later-segment failure aborts with
	// error after any already-visited rows (callers may have emitted a prefix).
	WalkObjects(ctx context.Context, params indexstore.QueryParams, visit VisitObject) (indexstore.QueryStats, error)
	// QueryObjects is a convenience collector over WalkObjects. Prefer WalkObjects
	// for large durable snapshots — this materialises the full match set.
	QueryObjects(ctx context.Context, params indexstore.QueryParams) ([]indexstore.QueryResult, indexstore.QueryStats, error)
	// QueryObjectCount counts matches without requiring full materialisation
	// for plain (non-canonical) queries on durable; SQLite uses COUNT when possible.
	QueryObjectCount(ctx context.Context, params indexstore.QueryParams) (int64, error)
	// QueryCanonicalObjects applies the same filters as WalkObjects, then
	// groups by ETag. This path is intentionally non-constant-memory: it
	// materialises the filtered row set before grouping. Envelope is
	// O(matched rows) with selection/output bounded by O(distinct non-empty
	// ETags) plus alternates storage for non-canonical group members.
	QueryCanonicalObjects(ctx context.Context, params indexstore.QueryParams) ([]indexstore.CanonicalOutputRecord, indexstore.CanonicalQueryStats, error)
	// ResolveSinceRunFilter validates a --since-run boundary for this index.
	// Durable-v2 currently fails closed: use sqlite-v1 or build with --format both.
	ResolveSinceRunFilter(ctx context.Context, runID string) (*indexstore.SinceRunFilter, error)
	Close() error
}

// ErrDurableSinceRunUnsupported is returned when --since-run is requested
// against a durable-v2 index. Use --format sqlite or both for forward deltas.
var ErrDurableSinceRunUnsupported = fmt.Errorf("--since-run is not supported on durable-v2 indexes; use --format sqlite or both")

// ResolveOptions configures local index discovery roots.
type ResolveOptions struct {
	// IndexesRoot is the indexes/ directory (contains idx_* identity dirs).
	IndexesRoot string
	// SegmentCacheRoot is the cache/segments/ directory (contains full index_set_id dirs).
	SegmentCacheRoot string
	// MaxMarkerBytes bounds latest/complete JSON reads (default: 1 MiB).
	MaxMarkerBytes int64
	// MaxManifestBytes bounds manifest JSON reads (default: 64 MiB).
	MaxManifestBytes int64
}

// ResolveTarget selects which index to open.
type ResolveTarget struct {
	// BaseURI selects by identity/base_uri when IndexSetID is empty.
	BaseURI string
	// IndexSetID is a full idx_<64hex> or a unique hex prefix / dir name.
	IndexSetID string
}

// ListedIndex is one discovered local index for ListIndexReaders.
type ListedIndex struct {
	Meta Meta
}

func normalizeResolveOptions(opts ResolveOptions) ResolveOptions {
	if opts.MaxMarkerBytes <= 0 {
		opts.MaxMarkerBytes = 1 << 20
	}
	if opts.MaxManifestBytes <= 0 {
		opts.MaxManifestBytes = 64 << 20
	}
	opts.IndexesRoot = strings.TrimSpace(opts.IndexesRoot)
	opts.SegmentCacheRoot = strings.TrimSpace(opts.SegmentCacheRoot)
	return opts
}

func validateQueryParams(params *indexstore.QueryParams) error {
	if params == nil {
		return fmt.Errorf("query params are required")
	}
	if strings.TrimSpace(params.IndexSetID) == "" {
		return fmt.Errorf("index_set_id is required")
	}
	if params.SinceRun != nil && params.IncludeDeleted {
		return fmt.Errorf("--include-deleted is not supported with --since-run; deletion deltas are not tracked in this index format")
	}
	return nil
}
