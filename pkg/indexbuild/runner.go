package indexbuild

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/crawler"
	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/uri"
)

// Runner executes durable index build workflows.
type Runner struct {
	config Config
}

// NewRunner returns a Runner for config.
func NewRunner(config Config) *Runner {
	return &Runner{config: config}
}

// Build crawls the injected provider, seals journals, and publishes a snapshot
// through the durable publication gate.
//
// The index-set write lease is acquired before any ObservationSink mutation
// (including SQLite dual-format sinks) and held through durable latest advance.
func (r *Runner) Build(ctx context.Context) (Summary, error) {
	cfg, err := normalizeConfig(r.config)
	if err != nil {
		return Summary{}, err
	}
	if err := emitEvent(ctx, cfg.Events, Event{
		Type:  EventTypeRunStart,
		RunID: cfg.RunID,
		Details: map[string]any{
			"index_set_id": cfg.IndexSetID,
			"base_uri":     cfg.BaseURI,
		},
	}); err != nil {
		return Summary{}, err
	}

	// Acquire the shared durable write transaction before crawl sinks mutate
	// any substrate (SQLite observation fanout in --format both).
	segmentRoot := filepath.Dir(cfg.Paths.LatestPath)
	authority, authorityOwned, err := acquireIndexSetAuthority(ctx, cfg.Authority, segmentRoot, cfg.IndexSetID, "build-"+cfg.RunID)
	if err != nil {
		return Summary{}, fmt.Errorf("acquire index set authority: %w", err)
	}
	if authorityOwned {
		defer func() { _ = authority.Release() }()
	}
	lease, err := indexsubstrate.AcquireWriteLease(segmentRoot, cfg.IndexSetID, "build-"+cfg.RunID, 0)
	if err != nil {
		return Summary{}, fmt.Errorf("acquire durable write lease: %w", err)
	}
	defer func() { _ = lease.Release() }()
	// Capture/validate parent under the lease before any mutable sinks run.
	parent, parentErr := resolveExpectedParent(cfg.Paths.LatestPath, cfg.ExpectedParent)
	if parentErr != nil {
		return Summary{}, parentErr
	}
	cfg.ExpectedParent = parent

	basePrefix, err := basePrefixFromURI(cfg.BaseURI)
	if err != nil {
		return Summary{}, err
	}
	matcher, err := buildMatcher(basePrefix, cfg.Match)
	if err != nil {
		return Summary{}, err
	}
	journalPath := filepath.Join(cfg.Paths.JournalDir, "shard-0001.jsonl")
	writer, err := newJournalWriter(journalWriterConfig{
		Path:       journalPath,
		IndexSetID: cfg.IndexSetID,
		RunID:      cfg.RunID,
		StartedAt:  cfg.RunStartedAt,
		BaseURI:    cfg.BaseURI,
		BasePrefix: basePrefix,
		Now:        cfg.Clock,
		Events:     cfg.Events,
	})
	if err != nil {
		return Summary{}, err
	}
	observed := newObservationFanoutWriter(writer, cfg.ObservationSinks)

	crawlCfg := cfg.Crawl
	if crawlCfg.Concurrency <= 0 {
		crawlCfg.Concurrency = crawler.DefaultConfig().Concurrency
	}
	if crawlCfg.ChannelBuffer <= 0 {
		crawlCfg.ChannelBuffer = crawler.DefaultConfig().ChannelBuffer
	}
	if crawlCfg.ProgressEvery <= 0 {
		crawlCfg.ProgressEvery = crawler.DefaultConfig().ProgressEvery
	}
	c := crawler.New(cfg.Source.Provider, matcher, observed, cfg.RunID, crawlCfg)
	if cfg.Filter != nil {
		c = c.WithFilter(cfg.Filter)
	}
	prefixes := append([]string(nil), cfg.CrawlPrefixes...)
	if len(prefixes) == 0 {
		prefixes = matcher.Prefixes()
	}
	if len(prefixes) == 0 {
		prefixes = []string{""}
	}
	c = c.WithPrefixes(prefixes)

	crawlSummary, crawlErr := c.Run(ctx)
	if crawlErr != nil {
		closeErr := observed.Close()
		_ = emitEvent(context.Background(), cfg.Events, Event{
			Type:    EventTypeCrawlError,
			RunID:   cfg.RunID,
			Message: crawlErr.Error(),
		})
		if closeErr != nil {
			return Summary{}, fmt.Errorf("crawl failed: %w; close journal: %v", crawlErr, closeErr)
		}
		return Summary{}, fmt.Errorf("crawl failed: %w", crawlErr)
	}
	if writer.ErrorCount() > 0 {
		if closeErr := observed.Close(); closeErr != nil {
			return Summary{}, closeErr
		}
		return Summary{}, fmt.Errorf("crawl completed with %d errors; snapshot not published", writer.ErrorCount())
	}
	if err := writer.Seal(); err != nil {
		_ = observed.Close()
		return Summary{}, err
	}
	if err := observed.Close(); err != nil {
		return Summary{}, err
	}
	if crawlSummary != nil && len(crawlSummary.Prefixes) > 0 {
		prefixes = crawlSummary.Prefixes
	}

	retryCfg := RetryConfig{
		IndexSetID:           cfg.IndexSetID,
		RunID:                cfg.RunID,
		BaseURI:              cfg.BaseURI,
		Paths:                cfg.Paths,
		JournalPaths:         []string{journalPath},
		Coverage:             cfg.Coverage,
		PriorRows:            cfg.PriorRows,
		ExpectedParent:       cfg.ExpectedParent,
		RunStartedAt:         cfg.RunStartedAt,
		CreatedAt:            cfg.CreatedAt,
		Clock:                cfg.Clock,
		TargetRowsPerSegment: cfg.TargetRowsPerSegment,
		Events:               cfg.Events,
		OnSegmentProgress:    cfg.OnSegmentProgress,
		Authority:            cfg.Authority,
	}
	// Private path: reuse the held Build lease (never a public forgeable guard).
	result, err := retryWithLease(ctx, retryCfg, authority, lease)
	if err != nil {
		return Summary{}, err
	}
	result.PrefixesCrawled = append([]string(nil), prefixes...)
	result.ObjectsObserved = writer.ObjectCount()
	return result, nil
}

// Retry publishes a snapshot from already sealed journals. It is the library
// recovery entry point for compaction or publication interruptions after a crawl
// completed.
//
// Public Retry always acquires the real OS write lease (callers cannot inject
// a no-op guard). Build reuses its held lease via unexported retryWithLease.
func Retry(ctx context.Context, cfg RetryConfig) (Summary, error) {
	cfg, err := normalizeRetryConfig(cfg)
	if err != nil {
		return Summary{}, err
	}
	segmentRoot := filepath.Dir(cfg.Paths.LatestPath)
	authority, authorityOwned, err := acquireIndexSetAuthority(ctx, cfg.Authority, segmentRoot, cfg.IndexSetID, "build-publish-"+cfg.RunID)
	if err != nil {
		return Summary{}, fmt.Errorf("acquire index set authority: %w", err)
	}
	if authorityOwned {
		defer func() { _ = authority.Release() }()
	}
	lease, err := indexsubstrate.AcquireWriteLease(segmentRoot, cfg.IndexSetID, "build-publish-"+cfg.RunID, 0)
	if err != nil {
		return Summary{}, fmt.Errorf("acquire durable write lease: %w", err)
	}
	defer func() { _ = lease.Release() }()
	return retryWithLease(ctx, cfg, authority, lease)
}

func acquireIndexSetAuthority(ctx context.Context, held *indexcoord.Lease, segmentRoot, indexSetID, holder string) (*indexcoord.Lease, bool, error) {
	if held != nil {
		if err := held.AssertHeldFor(indexSetID, segmentRoot); err != nil {
			return nil, false, err
		}
		return held, false, nil
	}
	authority, err := indexcoord.Acquire(ctx, segmentRoot, indexSetID, holder)
	if err != nil {
		return nil, false, err
	}
	return authority, true, nil
}

// retryWithLease publishes under an already-held package-owned lease.
func retryWithLease(ctx context.Context, cfg RetryConfig, authority *indexcoord.Lease, lease *indexsubstrate.WriteLease) (Summary, error) {
	if authority == nil {
		return Summary{}, fmt.Errorf("index set authority is required")
	}
	segmentRoot := filepath.Dir(cfg.Paths.LatestPath)
	if err := authority.AssertHeldFor(cfg.IndexSetID, segmentRoot); err != nil {
		return Summary{}, fmt.Errorf("index set authority: %w", err)
	}
	if lease == nil {
		return Summary{}, fmt.Errorf("write lease is required")
	}
	if err := lease.AssertHeld(); err != nil {
		return Summary{}, fmt.Errorf("write lease: %w", err)
	}
	coverage, err := normalizeCoverageForBaseURI(cfg.BaseURI, cfg.Coverage)
	if err != nil {
		return Summary{}, err
	}

	expectedParent, err := resolveExpectedParent(cfg.Paths.LatestPath, cfg.ExpectedParent)
	if err != nil {
		return Summary{}, err
	}
	var substrateParent *indexsubstrate.ExpectedParentToken
	if expectedParent != nil {
		substrateParent = &indexsubstrate.ExpectedParentToken{
			IndexSetID:     expectedParent.IndexSetID,
			RunID:          expectedParent.RunID,
			ManifestSHA256: expectedParent.ManifestSHA256,
			CoverageSHA256: expectedParent.CoverageSHA256,
		}
	}
	// Revalidate the stable authority at the commit boundary. This is separate
	// from the inner publish lease because GC can rename the whole segment root.
	if err := authority.AssertHeldFor(cfg.IndexSetID, segmentRoot); err != nil {
		return Summary{}, fmt.Errorf("index set authority: %w", err)
	}

	result, err := indexsubstrate.PublishSnapshot(indexsubstrate.PublishConfig{
		IndexSetID:           cfg.IndexSetID,
		RunID:                cfg.RunID,
		RunStartedAt:         cfg.RunStartedAt,
		CreatedAt:            cfg.CreatedAt,
		PriorRows:            toSubstrateRows(cfg.PriorRows),
		JournalPaths:         append([]string(nil), cfg.JournalPaths...),
		Coverage:             toSubstrateCoverage(coverage),
		SegmentDir:           cfg.Paths.SegmentDir,
		ManifestPath:         cfg.Paths.ManifestPath,
		CompletePath:         cfg.Paths.CompletePath,
		LatestPath:           cfg.Paths.LatestPath,
		ExpectedParent:       substrateParent,
		WriteLease:           lease,
		TargetRowsPerSegment: cfg.TargetRowsPerSegment,
		OnSegmentProgress:    toSubstrateSegmentProgress(cfg.OnSegmentProgress),
	})
	if err != nil {
		return Summary{}, err
	}
	summary := Summary{
		IndexSetID:     cfg.IndexSetID,
		RunID:          cfg.RunID,
		JournalPaths:   append([]string(nil), cfg.JournalPaths...),
		ManifestSHA256: result.ManifestSHA256,
		Manifest:       manifestSummary(result.Manifest),
	}
	if err := emitEvent(ctx, cfg.Events, Event{
		Type:  EventTypeSnapshotPublished,
		RunID: cfg.RunID,
		Details: map[string]any{
			"index_set_id": cfg.IndexSetID,
			"segments":     len(result.Manifest.Segments),
			"rows":         result.Manifest.Counts.Rows,
		},
	}); err != nil {
		return summary, err
	}
	return summary, nil
}

// ReadLatest reads the latest published internal snapshot through the same
// digest and traversal guards used by the substrate.
func ReadLatest(latestPath string) (ManifestSummary, []ObjectState, error) {
	manifest, rows, err := indexsubstrate.ReadLatestPublishedRows(latestPath)
	if err != nil {
		return ManifestSummary{}, nil, err
	}
	return manifestSummary(manifest), fromSubstrateRows(rows), nil
}

// resolveExpectedParent returns the CAS token derived from the live verified
// latest under the caller's write lease. Only a true "not published" state
// means first publish (nil token). Any other open/trust error fails closed.
//
// When provided is non-nil it must equal the live parent token (including
// coverage digest); otherwise the token is rejected as stale before sinks run.
func resolveExpectedParent(latestPath string, provided *ParentToken) (*ParentToken, error) {
	snap, err := indexsubstrate.OpenLatestPublishedSnapshotBounded(
		latestPath,
		indexsubstrate.DefaultMaxPublishedMarkerBytes,
		indexsubstrate.DefaultMaxPublishedManifestBytes,
	)
	if err != nil {
		if errors.Is(err, indexsubstrate.ErrSnapshotNotPublished) {
			if provided != nil {
				return nil, fmt.Errorf("%w: ExpectedParent provided but latest is not published", indexsubstrate.ErrStaleParent)
			}
			return nil, nil
		}
		// Malformed/unreadable/digest-invalid latest must not be treated as
		// first publication (would silently overwrite a damaged pointer).
		return nil, fmt.Errorf("open durable latest for parent CAS: %w", err)
	}
	covDigest, err := indexsubstrate.CoverageSHA256(snap.Manifest.Coverage)
	if err != nil {
		return nil, fmt.Errorf("hash parent coverage: %w", err)
	}
	live := &ParentToken{
		IndexSetID:     snap.Complete.IndexSetID,
		RunID:          snap.Complete.RunID,
		ManifestSHA256: snap.Complete.ManifestSHA256,
		CoverageSHA256: covDigest,
	}
	if provided != nil {
		if strings.TrimSpace(provided.IndexSetID) != live.IndexSetID ||
			strings.TrimSpace(provided.RunID) != live.RunID ||
			strings.TrimSpace(provided.ManifestSHA256) != live.ManifestSHA256 ||
			(strings.TrimSpace(provided.CoverageSHA256) != "" && strings.TrimSpace(provided.CoverageSHA256) != live.CoverageSHA256) {
			return nil, fmt.Errorf("%w: provided ExpectedParent does not match live latest", indexsubstrate.ErrStaleParent)
		}
	}
	return live, nil
}

func normalizeConfig(cfg Config) (Config, error) {
	if strings.TrimSpace(cfg.IndexSetID) == "" {
		return Config{}, fmt.Errorf("index_set_id is required")
	}
	if strings.TrimSpace(cfg.RunID) == "" {
		cfg.RunID = "run_" + uuid.NewString()
	}
	if strings.TrimSpace(cfg.BaseURI) == "" {
		return Config{}, fmt.Errorf("base_uri is required")
	}
	if cfg.Source.Provider == nil {
		return Config{}, fmt.Errorf("source provider is required")
	}
	if err := validatePaths(cfg.Paths); err != nil {
		return Config{}, err
	}
	cfg.IndexSetID = strings.TrimSpace(cfg.IndexSetID)
	cfg.RunID = strings.TrimSpace(cfg.RunID)
	cfg.BaseURI = strings.TrimSpace(cfg.BaseURI)
	cfg.Clock = normalizeClock(cfg.Clock)
	if cfg.RunStartedAt.IsZero() {
		cfg.RunStartedAt = cfg.Clock()
	}
	cfg.RunStartedAt = cfg.RunStartedAt.UTC()
	if cfg.CreatedAt.IsZero() {
		cfg.CreatedAt = cfg.RunStartedAt
	}
	cfg.CreatedAt = cfg.CreatedAt.UTC()
	if cfg.TargetRowsPerSegment <= 0 {
		cfg.TargetRowsPerSegment = indexsubstrate.DefaultTargetRowsPerSegment
	}
	return cfg, nil
}

func normalizeRetryConfig(cfg RetryConfig) (RetryConfig, error) {
	if strings.TrimSpace(cfg.IndexSetID) == "" {
		return RetryConfig{}, fmt.Errorf("index_set_id is required")
	}
	if strings.TrimSpace(cfg.RunID) == "" {
		return RetryConfig{}, fmt.Errorf("run_id is required")
	}
	if len(cfg.JournalPaths) == 0 {
		return RetryConfig{}, fmt.Errorf("journal paths are required")
	}
	if err := validatePaths(cfg.Paths); err != nil {
		return RetryConfig{}, err
	}
	cfg.IndexSetID = strings.TrimSpace(cfg.IndexSetID)
	cfg.RunID = strings.TrimSpace(cfg.RunID)
	cfg.BaseURI = strings.TrimSpace(cfg.BaseURI)
	cfg.Clock = normalizeClock(cfg.Clock)
	if cfg.RunStartedAt.IsZero() {
		cfg.RunStartedAt = cfg.Clock()
	}
	cfg.RunStartedAt = cfg.RunStartedAt.UTC()
	if cfg.CreatedAt.IsZero() {
		cfg.CreatedAt = cfg.RunStartedAt
	}
	cfg.CreatedAt = cfg.CreatedAt.UTC()
	if cfg.TargetRowsPerSegment <= 0 {
		cfg.TargetRowsPerSegment = indexsubstrate.DefaultTargetRowsPerSegment
	}
	return cfg, nil
}

func normalizeClock(clock Clock) Clock {
	if clock != nil {
		return clock
	}
	return func() time.Time { return time.Now().UTC() }
}

func basePrefixFromURI(baseURI string) (string, error) {
	parsed, err := uri.ParseURI(baseURI)
	if err != nil {
		return "", err
	}
	if !parsed.IsPrefix() {
		return "", fmt.Errorf("base_uri must end with '/': %s", sanitizeURI(baseURI))
	}
	return parsed.Key, nil
}

func buildMatcher(basePrefix string, cfg MatchConfig) (*match.Matcher, error) {
	matchCfg := match.Config{
		Includes:      prefixPatterns(basePrefix, cfg.Includes),
		Excludes:      prefixPatterns(basePrefix, cfg.Excludes),
		IncludeHidden: cfg.IncludeHidden,
	}
	if len(matchCfg.Includes) == 0 {
		matchCfg.Includes = []string{basePrefix + "**"}
	}
	return match.New(matchCfg)
}

func prefixPatterns(basePrefix string, patterns []string) []string {
	if basePrefix == "" {
		return append([]string(nil), patterns...)
	}
	if !strings.HasSuffix(basePrefix, "/") {
		basePrefix += "/"
	}
	out := make([]string, 0, len(patterns))
	for _, raw := range patterns {
		p := strings.TrimSpace(raw)
		if p == "" {
			continue
		}
		p = strings.TrimPrefix(p, "/")
		if strings.HasPrefix(p, basePrefix) {
			out = append(out, p)
			continue
		}
		out = append(out, basePrefix+p)
	}
	return out
}

func deriveRelKey(baseURI, fullKey string) string {
	parsed, err := uri.ParseURI(baseURI)
	if err != nil {
		return strings.TrimPrefix(fullKey, "/")
	}
	basePrefix := strings.TrimSuffix(parsed.Key, "/")
	if basePrefix == "" {
		return strings.TrimPrefix(fullKey, "/")
	}
	if strings.HasPrefix(fullKey, basePrefix) {
		return strings.TrimPrefix(strings.TrimPrefix(fullKey, basePrefix), "/")
	}
	return strings.TrimPrefix(fullKey, "/")
}

func manifestSummary(manifest indexsubstrate.InternalManifest) ManifestSummary {
	out := ManifestSummary{
		Rows:          manifest.Counts.Rows,
		ActiveRows:    manifest.Counts.ActiveRows,
		Tombstones:    manifest.Counts.Tombstones,
		DistinctETags: manifest.Counts.DistinctETags,
		Segments:      make([]SegmentSummary, 0, len(manifest.Segments)),
	}
	for _, segment := range manifest.Segments {
		out.Segments = append(out.Segments, SegmentSummary{
			SegmentID:  segment.SegmentID,
			Path:       segment.Path,
			Rows:       segment.Rows,
			Tombstones: segment.Tombstones,
			Digest: Digest{
				Algorithm: segment.Digest.Algorithm,
				Hex:       segment.Digest.Hex,
			},
		})
	}
	return out
}

func toSubstrateCoverage(in []CoverageAttestation) []indexsubstrate.CoverageAttestation {
	out := make([]indexsubstrate.CoverageAttestation, 0, len(in))
	for _, entry := range in {
		out = append(out, indexsubstrate.CoverageAttestation{
			Scope:    toSubstrateScope(entry.Scope),
			Basis:    indexsubstrate.CoverageBasis(entry.Basis),
			Complete: entry.Complete,
			Gaps:     toSubstrateScopes(entry.Gaps),
		})
	}
	return out
}

func toSubstrateSegmentProgress(fn OnSegmentProgressFunc) indexsubstrate.OnSegmentProgressFunc {
	if fn == nil {
		return nil
	}
	return func(progress indexsubstrate.SegmentProgress) {
		fn(SegmentProgress{
			Segment:  progress.Segment,
			Total:    progress.Total,
			Rows:     progress.Rows,
			RowsDone: progress.RowsDone,
		})
	}
}

func toSubstrateScope(scope *Scope) *indexsubstrate.Scope {
	if scope == nil {
		return nil
	}
	return &indexsubstrate.Scope{Prefix: scope.Prefix, Window: toSubstrateWindow(scope.Window)}
}

func toSubstrateWindow(window *Window) *indexsubstrate.Window {
	if window == nil {
		return nil
	}
	return &indexsubstrate.Window{From: window.From, To: window.To}
}

func toSubstrateScopes(in []Scope) []indexsubstrate.Scope {
	out := make([]indexsubstrate.Scope, 0, len(in))
	for i := range in {
		out = append(out, *toSubstrateScope(&in[i]))
	}
	return out
}

func toSubstrateRows(in []ObjectState) []indexsubstrate.CurrentObjectRow {
	out := make([]indexsubstrate.CurrentObjectRow, 0, len(in))
	for _, row := range in {
		out = append(out, indexsubstrate.CurrentObjectRow(row))
	}
	return out
}

func fromSubstrateRows(in []indexsubstrate.CurrentObjectRow) []ObjectState {
	out := make([]ObjectState, 0, len(in))
	for _, row := range in {
		out = append(out, ObjectState(row))
	}
	return out
}

func ensureDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}
	return nil
}
