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
	"sync"
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
	sourceIdentity   verifiedLocalIdentityFile
	segmentCacheRoot string
	pinned           bool
	sinceMu          sync.RWMutex
	sinceFilters     map[string]indexstore.SinceRunFilter
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
	var sourceIdentity verifiedLocalIdentityFile
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
	if identityDir != "" {
		verifiedIdentity, verifyErr := readVerifiedLocalIdentityFile(
			filepath.Join(identityDir, "identity.json"),
			opts.MaxMarkerBytes,
			fullID,
		)
		if verifyErr == nil {
			sourceIdentity = verifiedIdentity
			baseURI = verifiedIdentity.Payload.BaseURI
			provider = verifiedIdentity.Payload.Provider
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
		sourceIdentity:   sourceIdentity,
		segmentCacheRoot: filepath.Join(opts.SegmentCacheRoot, fullID),
		pinned:           true,
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

func (r *durableReader) VerifiedSnapshotMetadata() (VerifiedSnapshotMetadata, error) {
	if r == nil {
		return VerifiedSnapshotMetadata{}, fmt.Errorf("%w: nil durable reader", ErrVerifiedSnapshotMetadataUnavailable)
	}
	manifest := r.snap.Manifest
	complete := r.snap.Complete
	if manifest.RunStartedAt == nil || manifest.RunStartedAt.IsZero() {
		return VerifiedSnapshotMetadata{}, fmt.Errorf("%w: durable manifest run_started_at is required", ErrVerifiedSnapshotMetadataUnavailable)
	}
	completedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(complete.CompletedAt))
	if err != nil || completedAt.IsZero() {
		return VerifiedSnapshotMetadata{}, fmt.Errorf("%w: durable complete marker completed_at is invalid", ErrVerifiedSnapshotMetadataUnavailable)
	}
	if completedAt.Before(*manifest.RunStartedAt) {
		return VerifiedSnapshotMetadata{}, fmt.Errorf("%w: durable complete marker completed_at precedes run_started_at", ErrVerifiedSnapshotMetadataUnavailable)
	}
	if r.sourceIdentity.IndexSetID != manifest.IndexSetID ||
		len(r.sourceIdentity.CompleteFileSHA256) != 64 {
		return VerifiedSnapshotMetadata{}, fmt.Errorf("%w: exact canonical source identity is required", ErrVerifiedSnapshotMetadataUnavailable)
	}
	coverageSHA256, err := indexsubstrate.CoverageSHA256(manifest.Coverage)
	if err != nil {
		return VerifiedSnapshotMetadata{}, fmt.Errorf("%w: hash durable coverage: %v", ErrVerifiedSnapshotMetadataUnavailable, err)
	}
	coverage := CoverageSummary{Entries: len(manifest.Coverage)}
	for _, entry := range manifest.Coverage {
		if entry.Complete {
			coverage.CompleteEntries++
		}
		switch entry.Basis {
		case indexsubstrate.CoverageBasisConfirmed:
			coverage.ConfirmedEntries++
		case indexsubstrate.CoverageBasisInferred:
			coverage.InferredEntries++
		default:
			return VerifiedSnapshotMetadata{}, fmt.Errorf("%w: durable coverage basis %q is not supported", ErrVerifiedSnapshotMetadataUnavailable, entry.Basis)
		}
		coverage.GapCount += len(entry.Gaps)
	}
	return VerifiedSnapshotMetadata{
		SourceKind:            SnapshotSourceLocalPublished,
		IndexSetID:            manifest.IndexSetID,
		RunID:                 manifest.RunID,
		RunStartedAt:          manifest.RunStartedAt.UTC(),
		SnapshotCompletedAt:   completedAt.UTC(),
		SourceIdentitySHA256:  r.sourceIdentity.CompleteFileSHA256,
		SourceIdentitySchema:  SourceIdentitySchemaV1,
		SourceIdentityProfile: SourceIdentityProfileV1,
		ManifestSHA256:        complete.ManifestSHA256,
		CoverageSHA256:        coverageSHA256,
		Coverage:              coverage,
		Declared: DeclaredSnapshotCounts{
			Rows:          manifest.Counts.Rows,
			ActiveRows:    manifest.Counts.ActiveRows,
			Tombstones:    manifest.Counts.Tombstones,
			DistinctETags: manifest.Counts.DistinctETags,
			Segments:      len(manifest.Segments),
		},
	}, nil
}

func (r *durableReader) SQLiteDB() *sql.DB { return nil }

func (r *durableReader) Close() error { return nil }

func (r *durableReader) ResolveSinceRunFilter(ctx context.Context, runID string) (*indexstore.SinceRunFilter, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runID = strings.TrimSpace(runID)
	if err := validatePinnedRunID(runID); err != nil {
		return nil, err
	}
	if !r.pinned {
		return nil, fmt.Errorf("%w: exact --run-id current selection is required", ErrDurableSinceRunUnsupported)
	}
	budget := indexsubstrate.DefaultAncestryBudget()
	budget.MaxMarkerBytes = r.opts.MaxMarkerBytes
	budget.MaxManifestBytes = r.opts.MaxManifestBytes
	ancestry, err := indexsubstrate.ResolveAncestry(r.snap, indexsubstrate.AncestryResolveConfig{
		ThroughRunID: runID,
		Lookup: func(indexSetID, parentRunID string) (string, error) {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			if indexSetID != r.meta.IndexSetID {
				return "", fmt.Errorf("lineage parent crosses index set boundary")
			}
			if err := validatePinnedRunID(parentRunID); err != nil {
				return "", err
			}
			return filepath.Join(r.segmentCacheRoot, "runs", parentRunID, "complete.json"), nil
		},
		Budget:            budget,
		RequireContinuous: true,
	})
	if err != nil {
		return nil, err
	}
	if ancestry.RequestedBoundary == nil || len(ancestry.Chain) == 0 {
		return nil, &indexsubstrate.LineageError{
			Code:    indexsubstrate.LineageCodeBaselineNotAncestor,
			Message: "requested baseline was not verified",
		}
	}
	for i := 0; i+1 < len(ancestry.Chain); i++ {
		child := ancestry.Chain[i]
		parent := ancestry.Chain[i+1]
		if !child.RunStartedAt.After(parent.RunStartedAt) {
			return nil, &indexsubstrate.LineageError{
				Code:    indexsubstrate.LineageCodeInvalidTime,
				Message: "continuous descendant run_started_at must be strictly after its parent",
			}
		}
	}
	descendants := make([]string, 0, len(ancestry.Chain)-1)
	for _, node := range ancestry.Chain[:len(ancestry.Chain)-1] {
		descendants = append(descendants, node.RunID)
	}
	resolved := indexstore.SinceRunFilter{
		RunID:                    ancestry.RequestedBoundary.RunID,
		StartedAt:                ancestry.RequestedBoundary.RunStartedAt,
		VerifiedDescendantRunIDs: descendants,
		SameRun:                  len(ancestry.Chain) == 1,
	}
	r.sinceMu.Lock()
	if r.sinceFilters == nil {
		r.sinceFilters = make(map[string]indexstore.SinceRunFilter)
	}
	r.sinceFilters[resolved.RunID] = cloneSinceRunFilter(resolved)
	r.sinceMu.Unlock()
	out := cloneSinceRunFilter(resolved)
	return &out, nil
}

func (r *durableReader) WalkObjects(ctx context.Context, params indexstore.QueryParams, visit VisitObject) (indexstore.QueryStats, error) {
	if visit == nil {
		return indexstore.QueryStats{}, fmt.Errorf("visit callback is required")
	}
	params.IndexSetID = r.meta.IndexSetID
	if err := validateQueryParams(&params); err != nil {
		return indexstore.QueryStats{}, err
	}
	if err := r.validateResolvedSinceRunFilter(params.SinceRun); err != nil {
		return indexstore.QueryStats{}, err
	}
	filter, err := compileRowFilter(params)
	if err != nil {
		return indexstore.QueryStats{}, err
	}
	var stats indexstore.QueryStats
	var emitted int
	stats, err = r.walkFiltered(ctx, filter, params, func(result indexstore.QueryResult) error {
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
	if err := r.validateResolvedSinceRunFilter(params.SinceRun); err != nil {
		return 0, err
	}
	filter, err := compileRowFilter(params)
	if err != nil {
		return 0, err
	}
	var count int64
	_, err = r.walkFiltered(ctx, filter, params, func(result indexstore.QueryResult) error {
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
	if err := r.validateResolvedSinceRunFilter(params.SinceRun); err != nil {
		return nil, indexstore.CanonicalQueryStats{}, err
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

func cloneSinceRunFilter(in indexstore.SinceRunFilter) indexstore.SinceRunFilter {
	out := in
	out.VerifiedDescendantRunIDs = append([]string(nil), in.VerifiedDescendantRunIDs...)
	return out
}

func (r *durableReader) validateResolvedSinceRunFilter(filter *indexstore.SinceRunFilter) error {
	if filter == nil {
		return nil
	}
	r.sinceMu.RLock()
	expected, ok := r.sinceFilters[filter.RunID]
	r.sinceMu.RUnlock()
	if !ok || !expected.StartedAt.Equal(filter.StartedAt) ||
		expected.SameRun != filter.SameRun ||
		len(expected.VerifiedDescendantRunIDs) != len(filter.VerifiedDescendantRunIDs) {
		return fmt.Errorf("%w: filter is not bound to this verified reader", ErrDurableSinceRunUnsupported)
	}
	for i := range expected.VerifiedDescendantRunIDs {
		if expected.VerifiedDescendantRunIDs[i] != filter.VerifiedDescendantRunIDs[i] {
			return fmt.Errorf("%w: filter is not bound to this verified reader", ErrDurableSinceRunUnsupported)
		}
	}
	return nil
}

var errStopWalk = fmt.Errorf("stop walk")

func (r *durableReader) walkFiltered(ctx context.Context, filter *rowFilter, params indexstore.QueryParams, visit func(indexstore.QueryResult) error) (indexstore.QueryStats, error) {
	var stats indexstore.QueryStats
	if ctx == nil {
		ctx = context.Background()
	}
	for _, segment := range r.snap.Manifest.Segments {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if filter != nil && !segmentMayMatch(segment, filter) {
			// Descriptor bounds come from the digest-verified manifest. The
			// segment bytes are intentionally not walked or claimed verified.
			stats.SegmentsManifestPruned++
			continue
		}
		entered := false
		if err := indexsubstrate.WalkSegmentFileVerified(r.snap.SegmentDir, segment, func(row indexsubstrate.CurrentObjectRow) error {
			if !entered {
				// WalkSegmentFileVerified hashes the same file descriptor before
				// invoking the first row callback.
				stats.SegmentsWalked++
				stats.SegmentsVerified++
				entered = true
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			stats.Examined++
			result, ok, err := filterCurrentRow(row, filter, params)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			stats.Matched++
			return visit(result)
		}); err != nil {
			return stats, err
		}
		if !entered {
			// Empty segments still completed same-open digest verification and
			// a successful row walk.
			stats.SegmentsWalked++
			stats.SegmentsVerified++
		}
	}
	return stats, nil
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
		if filter.sinceRun.SameRun {
			return indexstore.QueryResult{}, false, nil
		}
		// A baseline-run observation normally occurs after run_started_at, but
		// the baseline is the boundary rather than its own descendant. Combine
		// time and verified run identity to distinguish it from a delta trigger.
		firstOK, err := durableDeltaFactAfterBoundary(filter.sinceRun, row.FirstSeenRunID, row.FirstSeenAt, "first_seen_run_id")
		if err != nil {
			return indexstore.QueryResult{}, false, err
		}
		changedOK, err := durableDeltaFactAfterBoundary(filter.sinceRun, row.LastChangedRunID, row.LastChangedAt, "last_changed_run_id")
		if err != nil {
			return indexstore.QueryResult{}, false, err
		}
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

func verifiedDescendantRun(filter *indexstore.SinceRunFilter, runID string) bool {
	if filter == nil {
		return false
	}
	for _, verified := range filter.VerifiedDescendantRunIDs {
		if runID == verified {
			return true
		}
	}
	return false
}

func durableDeltaFactAfterBoundary(filter *indexstore.SinceRunFilter, runID string, at time.Time, field string) (bool, error) {
	if filter == nil || at.IsZero() || !at.After(filter.StartedAt) {
		return false, nil
	}
	if runID == filter.RunID {
		return false, nil
	}
	if verifiedDescendantRun(filter, runID) {
		return true, nil
	}
	return false, fmt.Errorf("delta row %s is not the verified baseline or a verified descendant", field)
}

func changeKind(result indexstore.QueryResult, filter *indexstore.SinceRunFilter) string {
	if filter == nil {
		return ""
	}
	if result.FirstSeenAt != nil &&
		result.FirstSeenAt.After(filter.StartedAt) &&
		verifiedDescendantRun(filter, result.FirstSeenRunID) {
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
	availableRecords := len(outputs)
	availablePassthroughRows := countCanonicalPassthroughs(outputs)
	if limit > 0 && len(outputs) > limit {
		outputs = outputs[:limit]
	}
	stats := indexstore.CanonicalQueryStats{
		QueryStats:               queryStats,
		TotalRecords:             len(outputs),
		AvailableRecords:         availableRecords,
		AvailablePassthroughRows: availablePassthroughRows,
		Truncated:                len(outputs) < availableRecords,
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

func countCanonicalPassthroughs(outputs []indexstore.CanonicalOutputRecord) int {
	count := 0
	for _, output := range outputs {
		if output.Passthrough != nil {
			count++
		}
	}
	return count
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
