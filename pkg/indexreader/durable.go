package indexreader

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/bmatcuk/doublestar/v4"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/match"
)

type durableReader struct {
	meta             Meta
	opts             ResolveOptions
	snap             indexsubstrate.PublishedSnapshot
	segmentCacheRoot string
}

func openDurableReader(opts ResolveOptions, c candidate) (*durableReader, error) {
	snap, err := indexsubstrate.OpenLatestPublishedSnapshotBounded(c.latest, opts.MaxMarkerBytes, opts.MaxManifestBytes)
	if err != nil {
		return nil, fmt.Errorf("open durable snapshot: %w", err)
	}
	meta := c.meta
	meta.Format = FormatDurableV2
	meta.IndexSetID = snap.Manifest.IndexSetID
	meta.RunID = snap.Manifest.RunID
	meta.SourcePath = c.latest
	return &durableReader{
		meta:             meta,
		opts:             opts,
		snap:             snap,
		segmentCacheRoot: filepath.Dir(c.latest),
	}, nil
}

// openPinnedDurableRun opens a durable-v2 snapshot by exact set/run complete
// marker. It never reads latest.json, so a later latest advance cannot switch
// the opened run.
func openPinnedDurableRun(opts ResolveOptions, target ResolveTarget) (*durableReader, error) {
	if opts.SegmentCacheRoot == "" {
		return nil, fmt.Errorf("segment cache root is required for pinned durable run open")
	}
	if err := validatePinnedRunID(target.RunID); err != nil {
		return nil, err
	}
	wantID := strings.TrimPrefix(target.IndexSetID, "idx_")
	if !validHexPattern.MatchString(wantID) {
		return nil, fmt.Errorf("invalid index set ID: %s (must be hex characters, max 64)", target.IndexSetID)
	}
	fullID, err := matchSegmentCacheID(opts.SegmentCacheRoot, wantID)
	if err != nil {
		// Fall back: full 64-hex id may exist with runs even if latest is missing.
		if len(wantID) == 64 {
			fullID = "idx_" + wantID
		} else {
			return nil, fmt.Errorf("no durable snapshot matching index set %s: %w", target.IndexSetID, err)
		}
	}
	completePath := filepath.Join(opts.SegmentCacheRoot, fullID, "runs", target.RunID, "complete.json")
	snap, err := indexsubstrate.OpenPublishedRunSnapshotBounded(
		completePath,
		fullID,
		target.RunID,
		opts.MaxMarkerBytes,
		opts.MaxManifestBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("open pinned durable run %s/%s: %w", fullID, target.RunID, err)
	}
	identityDir := ""
	baseURI := ""
	provider := ""
	if opts.IndexesRoot != "" {
		// Identity metadata is optional; only attach when recomputed full
		// IndexSetID exactly matches the pinned set (no prefix-only guesses).
		entries, readErr := os.ReadDir(opts.IndexesRoot)
		if readErr == nil {
			setHex := strings.TrimPrefix(fullID, "idx_")
			for _, entry := range entries {
				if !entry.IsDir() {
					continue
				}
				dirHex := strings.TrimPrefix(entry.Name(), "idx_")
				if !indexSetHexMatches(dirHex, setHex) && !indexSetHexMatches(setHex, dirHex) {
					continue
				}
				dirPath := filepath.Join(opts.IndexesRoot, entry.Name())
				identity, idErr := readIdentityMeta(dirPath, opts.MaxMarkerBytes)
				if idErr != nil {
					continue
				}
				if identity.IndexSetID == "" || identity.IndexSetID != fullID {
					continue
				}
				identityDir = dirPath
				baseURI = identity.BaseURI
				provider = identity.Provider
				break
			}
		}
	}
	return &durableReader{
		meta: Meta{
			Format:      FormatDurableV2,
			IndexSetID:  snap.Manifest.IndexSetID,
			BaseURI:     baseURI,
			Provider:    provider,
			IdentityDir: identityDir,
			SourcePath:  completePath,
			RunID:       snap.Manifest.RunID,
		},
		opts:             opts,
		snap:             snap,
		segmentCacheRoot: filepath.Join(opts.SegmentCacheRoot, fullID),
	}, nil
}

// validatePinnedRunID requires a single safe path component at the package seam
// so ResolveTarget.RunID cannot traverse out of the runs/ directory. CLI
// validateRunID is stricter (schema form) but is not the only call path.
func validatePinnedRunID(runID string) error {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	if strings.Contains(runID, "/") || strings.Contains(runID, "\\") {
		return fmt.Errorf("invalid run_id: must be a single path component")
	}
	if runID == "." || runID == ".." {
		return fmt.Errorf("invalid run_id: must be a single path component")
	}
	if filepath.Base(runID) != runID || filepath.Clean(runID) != runID {
		return fmt.Errorf("invalid run_id: must be a single path component")
	}
	if strings.Contains(runID, string(filepath.Separator)) {
		return fmt.Errorf("invalid run_id: must be a single path component")
	}
	return nil
}

func (r *durableReader) Meta() Meta { return r.meta }

func (r *durableReader) SQLiteDB() *sql.DB { return nil }

func (r *durableReader) Close() error { return nil }

func (r *durableReader) ResolveSinceRunFilter(ctx context.Context, runID string) (*indexstore.SinceRunFilter, error) {
	_ = ctx
	if strings.TrimSpace(runID) == "" {
		return nil, fmt.Errorf("run_id is required")
	}
	// Fail closed: durable markers do not persist RunStartedAt or digest-bound
	// parent-chain lineage required for SQLite-parity --since-run. Approximating
	// via CreatedAt/CompletedAt or unlinked runs/<id>/complete.json would
	// silently widen/narrow deltas.
	return nil, fmt.Errorf("%w: run %s", ErrDurableSinceRunUnsupported, strings.TrimSpace(runID))
}

func (r *durableReader) WalkObjects(ctx context.Context, params indexstore.QueryParams, visit VisitObject) (indexstore.QueryStats, error) {
	if visit == nil {
		return indexstore.QueryStats{}, fmt.Errorf("visit callback is required")
	}
	params.IndexSetID = r.meta.IndexSetID
	if err := validateQueryParams(&params); err != nil {
		return indexstore.QueryStats{}, err
	}
	if params.SinceRun != nil {
		return indexstore.QueryStats{}, ErrDurableSinceRunUnsupported
	}
	filter, err := compileRowFilter(params)
	if err != nil {
		return indexstore.QueryStats{}, err
	}
	var stats indexstore.QueryStats
	var emitted int
	err = r.walkFiltered(ctx, filter, params, func(result indexstore.QueryResult) error {
		if err := visit(result); err != nil {
			return err
		}
		emitted++
		if params.Limit > 0 && emitted >= params.Limit {
			return errStopWalk
		}
		return nil
	})
	if err != nil && err != errStopWalk {
		return stats, err
	}
	return stats, nil
}

func (r *durableReader) QueryObjects(ctx context.Context, params indexstore.QueryParams) ([]indexstore.QueryResult, indexstore.QueryStats, error) {
	var results []indexstore.QueryResult
	stats, err := r.WalkObjects(ctx, params, func(result indexstore.QueryResult) error {
		results = append(results, result)
		return nil
	})
	return results, stats, err
}

func (r *durableReader) QueryObjectCount(ctx context.Context, params indexstore.QueryParams) (int64, error) {
	params.IndexSetID = r.meta.IndexSetID
	params.Limit = 0
	if err := validateQueryParams(&params); err != nil {
		return 0, err
	}
	if params.SinceRun != nil {
		return 0, ErrDurableSinceRunUnsupported
	}
	filter, err := compileRowFilter(params)
	if err != nil {
		return 0, err
	}
	var count int64
	err = r.walkFiltered(ctx, filter, params, func(result indexstore.QueryResult) error {
		_ = result
		count++
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (r *durableReader) QueryCanonicalObjects(ctx context.Context, params indexstore.QueryParams) ([]indexstore.CanonicalOutputRecord, indexstore.CanonicalQueryStats, error) {
	params.IndexSetID = r.meta.IndexSetID
	if err := validateQueryParams(&params); err != nil {
		return nil, indexstore.CanonicalQueryStats{}, err
	}
	if params.SinceRun != nil {
		return nil, indexstore.CanonicalQueryStats{}, ErrDurableSinceRunUnsupported
	}
	rule := params.CanonicalTieBreak
	if rule == "" {
		rule = indexstore.CanonicalTieBreakMinKey
	}
	switch rule {
	case indexstore.CanonicalTieBreakMinKey, indexstore.CanonicalTieBreakMinModified, indexstore.CanonicalTieBreakMaxModified:
	default:
		return nil, indexstore.CanonicalQueryStats{}, fmt.Errorf("canonical_tie_break %q is not supported; available values: %s, %s, %s", rule, indexstore.CanonicalTieBreakMinKey, indexstore.CanonicalTieBreakMinModified, indexstore.CanonicalTieBreakMaxModified)
	}
	// Non-constant-memory path: materialise filtered matches, then group.
	// Envelope is O(matched rows) before selection; output groups are
	// O(distinct non-empty ETags) plus alternates for multi-member groups.
	filterParams := params
	filterParams.Limit = 0
	results, queryStats, err := r.QueryObjects(ctx, filterParams)
	if err != nil {
		return nil, indexstore.CanonicalQueryStats{}, err
	}
	outputs, stats := groupCanonical(results, rule, params.Limit, queryStats)
	return outputs, stats, nil
}

var errStopWalk = fmt.Errorf("stop walk")

func (r *durableReader) walkFiltered(ctx context.Context, filter *rowFilter, params indexstore.QueryParams, visit func(indexstore.QueryResult) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for _, segment := range r.snap.Manifest.Segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if filter != nil && !segmentMayMatch(segment, filter) {
			// Integrity: still verify segment digest before trusting skip decision
			// based only on descriptor metadata. Descriptors come from a verified
			// manifest, so min/max key skips are safe without re-hashing.
			continue
		}
		if err := indexsubstrate.WalkSegmentFileVerified(r.snap.SegmentDir, segment, func(row indexsubstrate.CurrentObjectRow) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			result, ok, err := filterCurrentRow(row, filter, params)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			return visit(result)
		}); err != nil {
			return err
		}
	}
	return nil
}

type rowFilter struct {
	pattern        string
	keyRe          *regexp.Regexp
	prefix         string
	minSize        int64
	maxSize        int64
	modifiedAfter  time.Time
	modifiedBefore time.Time
	storageClasses map[string]struct{}
	enrichedAfter  time.Time
	includeDeleted bool
	sinceRun       *indexstore.SinceRunFilter
}

func compileRowFilter(params indexstore.QueryParams) (*rowFilter, error) {
	f := &rowFilter{
		pattern:        params.Pattern,
		minSize:        params.MinSize,
		maxSize:        params.MaxSize,
		modifiedAfter:  params.ModifiedAfter,
		modifiedBefore: params.ModifiedBefore,
		enrichedAfter:  params.EnrichedAfter,
		includeDeleted: params.IncludeDeleted,
		sinceRun:       params.SinceRun,
	}
	if params.Pattern != "" {
		if !doublestar.ValidatePattern(params.Pattern) {
			return nil, fmt.Errorf("invalid glob pattern: %s", params.Pattern)
		}
		f.prefix = match.DerivePrefix(params.Pattern)
	}
	if params.KeyRegex != "" {
		re, err := regexp.Compile(params.KeyRegex)
		if err != nil {
			return nil, fmt.Errorf("invalid key regex: %w", err)
		}
		f.keyRe = re
	}
	if len(params.StorageClasses) > 0 {
		f.storageClasses = make(map[string]struct{}, len(params.StorageClasses))
		for _, sc := range params.StorageClasses {
			if sc == "" {
				continue
			}
			f.storageClasses[sc] = struct{}{}
		}
	}
	return f, nil
}

func segmentMayMatch(segment indexsubstrate.SegmentDescriptor, filter *rowFilter) bool {
	if filter == nil || filter.prefix == "" {
		return true
	}
	// Safe skip: every key in the segment is lexicographically before the prefix,
	// so none can start with it. (MinRelKey upper-bound skips are not safe without
	// a delimiter-aware successor of the prefix.)
	if segment.MaxRelKey != "" && segment.MaxRelKey < filter.prefix {
		return false
	}
	return true
}

func filterCurrentRow(row indexsubstrate.CurrentObjectRow, filter *rowFilter, params indexstore.QueryParams) (indexstore.QueryResult, bool, error) {
	if filter == nil {
		filter = &rowFilter{}
	}
	if !filter.includeDeleted && row.DeletedAt != nil {
		return indexstore.QueryResult{}, false, nil
	}
	if filter.prefix != "" && !strings.HasPrefix(row.RelKey, filter.prefix) {
		return indexstore.QueryResult{}, false, nil
	}
	if filter.pattern != "" {
		matched, err := doublestar.Match(filter.pattern, row.RelKey)
		if err != nil {
			return indexstore.QueryResult{}, false, fmt.Errorf("match pattern: %w", err)
		}
		if !matched {
			return indexstore.QueryResult{}, false, nil
		}
	}
	if filter.keyRe != nil && !filter.keyRe.MatchString(row.RelKey) {
		return indexstore.QueryResult{}, false, nil
	}
	if filter.minSize > 0 && row.SizeBytes < filter.minSize {
		return indexstore.QueryResult{}, false, nil
	}
	if filter.maxSize > 0 && row.SizeBytes > filter.maxSize {
		return indexstore.QueryResult{}, false, nil
	}
	if !filter.modifiedAfter.IsZero() {
		if row.LastModified == nil || row.LastModified.Before(filter.modifiedAfter) {
			return indexstore.QueryResult{}, false, nil
		}
	}
	if !filter.modifiedBefore.IsZero() {
		if row.LastModified == nil || row.LastModified.After(filter.modifiedBefore) {
			return indexstore.QueryResult{}, false, nil
		}
	}
	if len(filter.storageClasses) > 0 {
		if row.StorageClass == nil {
			return indexstore.QueryResult{}, false, nil
		}
		if _, ok := filter.storageClasses[*row.StorageClass]; !ok {
			return indexstore.QueryResult{}, false, nil
		}
	}
	if !filter.enrichedAfter.IsZero() {
		if row.HeadEnrichedAt == nil || row.HeadEnrichedAt.Before(filter.enrichedAfter) {
			return indexstore.QueryResult{}, false, nil
		}
	}
	if filter.sinceRun != nil {
		// Match SQLite semantics using row observation timestamps against the
		// validated boundary started_at (durable has no index_runs table).
		firstOK := !row.FirstSeenAt.IsZero() && row.FirstSeenAt.After(filter.sinceRun.StartedAt)
		changedOK := !row.LastChangedAt.IsZero() && row.LastChangedAt.After(filter.sinceRun.StartedAt)
		if !firstOK && !changedOK {
			return indexstore.QueryResult{}, false, nil
		}
	}

	result := indexstore.QueryResult{
		RelKey:           row.RelKey,
		SizeBytes:        row.SizeBytes,
		LastModified:     cloneTimePtr(row.LastModified),
		ETag:             row.ETag,
		StorageClass:     cloneStringPtr(row.StorageClass),
		ArchiveStatus:    cloneStringPtr(row.ArchiveStatus),
		RestoreState:     cloneStringPtr(row.RestoreState),
		RestoreExpiry:    cloneTimePtr(row.RestoreExpiry),
		ContentType:      cloneStringPtr(row.ContentType),
		HeadEnrichedAt:   cloneTimePtr(row.HeadEnrichedAt),
		FirstSeenRunID:   row.FirstSeenRunID,
		FirstSeenAt:      timePtrOrNil(row.FirstSeenAt),
		LastChangedRunID: row.LastChangedRunID,
		LastChangedAt:    timePtrOrNil(row.LastChangedAt),
		DeletedAt:        cloneTimePtr(row.DeletedAt),
	}
	result.ChangeKind = changeKind(result, params.SinceRun)
	return result, true, nil
}

func changeKind(result indexstore.QueryResult, filter *indexstore.SinceRunFilter) string {
	if filter == nil {
		return ""
	}
	if result.FirstSeenAt != nil && result.FirstSeenAt.After(filter.StartedAt) {
		return indexstore.QueryChangeKindAdded
	}
	if result.LastChangedAt != nil && result.LastChangedAt.After(filter.StartedAt) {
		return indexstore.QueryChangeKindChanged
	}
	return ""
}

func groupCanonical(results []indexstore.QueryResult, rule indexstore.CanonicalTieBreak, limit int, queryStats indexstore.QueryStats) ([]indexstore.CanonicalOutputRecord, indexstore.CanonicalQueryStats) {
	groups := map[string][]indexstore.QueryResult{}
	outputs := make([]indexstore.CanonicalOutputRecord, 0, len(results))
	for _, result := range results {
		if strings.TrimSpace(result.ETag) == "" {
			r := result
			outputs = append(outputs, indexstore.CanonicalOutputRecord{Passthrough: &r})
			continue
		}
		groups[result.ETag] = append(groups[result.ETag], result)
	}
	for etag, members := range groups {
		group := makeCanonicalGroup(etag, members, rule)
		outputs = append(outputs, indexstore.CanonicalOutputRecord{Group: &group})
	}
	sort.SliceStable(outputs, func(i, j int) bool {
		return canonicalRelKey(outputs[i]) < canonicalRelKey(outputs[j])
	})
	if limit > 0 && len(outputs) > limit {
		outputs = outputs[:limit]
	}
	stats := indexstore.CanonicalQueryStats{
		QueryStats:   queryStats,
		TotalRecords: len(outputs),
	}
	for _, output := range outputs {
		if output.Group != nil {
			stats.CanonicalGroups++
		}
		if output.Passthrough != nil {
			stats.PassthroughRows++
		}
	}
	return outputs, stats
}

func makeCanonicalGroup(etag string, members []indexstore.QueryResult, rule indexstore.CanonicalTieBreak) indexstore.CanonicalObjectGroup {
	sorted := append([]indexstore.QueryResult(nil), members...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return compareCanonical(sorted[i], sorted[j], rule) < 0
	})
	group := indexstore.CanonicalObjectGroup{
		ETag:      etag,
		Canonical: sorted[0],
	}
	if len(sorted) > 1 {
		group.Alternates = append(group.Alternates, sorted[1:]...)
		sort.SliceStable(group.Alternates, func(i, j int) bool {
			return group.Alternates[i].RelKey < group.Alternates[j].RelKey
		})
	}
	return group
}

func compareCanonical(a, b indexstore.QueryResult, rule indexstore.CanonicalTieBreak) int {
	switch rule {
	case indexstore.CanonicalTieBreakMinModified:
		if cmp := compareOptTimeAsc(a.LastModified, b.LastModified); cmp != 0 {
			return cmp
		}
	case indexstore.CanonicalTieBreakMaxModified:
		if cmp := compareOptTimeDesc(a.LastModified, b.LastModified); cmp != 0 {
			return cmp
		}
	}
	return strings.Compare(a.RelKey, b.RelKey)
}

func compareOptTimeAsc(a, b *time.Time) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return 1
	case b == nil:
		return -1
	case a.Before(*b):
		return -1
	case a.After(*b):
		return 1
	default:
		return 0
	}
}

func compareOptTimeDesc(a, b *time.Time) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	case a.After(*b):
		return -1
	case a.Before(*b):
		return 1
	default:
		return 0
	}
}

func canonicalRelKey(output indexstore.CanonicalOutputRecord) string {
	if output.Group != nil {
		return output.Group.Canonical.RelKey
	}
	if output.Passthrough != nil {
		return output.Passthrough.RelKey
	}
	return ""
}

func cloneStringPtr(in *string) *string {
	if in == nil {
		return nil
	}
	v := *in
	return &v
}

func cloneTimePtr(in *time.Time) *time.Time {
	if in == nil {
		return nil
	}
	v := in.UTC()
	return &v
}

func timePtrOrNil(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	v := t.UTC()
	return &v
}
