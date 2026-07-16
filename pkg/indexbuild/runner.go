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
	// Capture/validate the verified parent under the held authority/lease before
	// any mutable sinks run. This single capture is the canonical parent authority
	// for the whole build; retryWithLease reuses it rather than reopening latest.
	plan, parentErr := captureVerifiedParent(cfg.Paths.LatestPath, cfg.IndexSetID, cfg.ExpectedParent)
	if parentErr != nil {
		return Summary{}, parentErr
	}
	cfg.ExpectedParent = plan.expectedToken()

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
		ExpectedParent:       cfg.ExpectedParent,
		RunStartedAt:         cfg.RunStartedAt,
		CreatedAt:            cfg.CreatedAt,
		Clock:                cfg.Clock,
		TargetRowsPerSegment: cfg.TargetRowsPerSegment,
		Events:               cfg.Events,
		OnSegmentProgress:    cfg.OnSegmentProgress,
		Authority:            cfg.Authority,
	}
	// Private path: reuse the held Build lease (never a public forgeable guard)
	// and the parent captured once above (no second latest reopen).
	result, err := retryWithLease(ctx, retryCfg, plan, authority, lease)
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
	// Capture the verified parent once under the held authority/lease, bound to
	// the requested set.
	plan, err := captureVerifiedParent(cfg.Paths.LatestPath, cfg.IndexSetID, cfg.ExpectedParent)
	if err != nil {
		return Summary{}, err
	}
	return retryWithLease(ctx, cfg, plan, authority, lease)
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

// retryWithLease publishes under an already-held package-owned lease, using the
// parent captured once by the caller (Build or public Retry). It does not reopen
// latest to re-derive the parent; the publish-time CAS revalidation is the sole
// later read of the authoritative latest pointer.
func retryWithLease(ctx context.Context, cfg RetryConfig, plan *verifiedParentPlan, authority *indexcoord.Lease, lease *indexsubstrate.WriteLease) (Summary, error) {
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
	// The verified-parent plan is mandatory: it is the sole canonical parent
	// authority. A nil plan (missing/mis-threaded) must fail closed here, never
	// be treated as a verified first publication.
	if plan == nil {
		return Summary{}, fmt.Errorf("verified parent plan is required")
	}
	coverage, err := normalizeCoverageForBaseURI(cfg.BaseURI, cfg.Coverage)
	if err != nil {
		return Summary{}, err
	}

	expectedParent := plan.expectedToken()
	var substrateParent *indexsubstrate.ExpectedParentToken
	if expectedParent != nil {
		substrateParent = &indexsubstrate.ExpectedParentToken{
			IndexSetID:     expectedParent.IndexSetID,
			RunID:          expectedParent.RunID,
			ManifestSHA256: expectedParent.ManifestSHA256,
			CoverageSHA256: expectedParent.CoverageSHA256,
		}
	}
	// Derive continuity inputs (parent rows, state parent, lineage, parent
	// manifests) from the single verified capture under the three-way rule; this
	// validates a continuous parent's ancestry before extension.
	continuity, err := plan.continuityInputs(cfg.RunID)
	if err != nil {
		return Summary{}, err
	}

	// Revalidate the stable authority at the commit boundary. This is separate
	// from the inner publish lease because GC can rename the whole segment root.
	if err := authority.AssertHeldFor(cfg.IndexSetID, segmentRoot); err != nil {
		return Summary{}, fmt.Errorf("index set authority: %w", err)
	}

	result, err := indexsubstrate.PublishSnapshotContext(ctx, indexsubstrate.PublishConfig{
		IndexSetID:           cfg.IndexSetID,
		RunID:                cfg.RunID,
		RunStartedAt:         cfg.RunStartedAt,
		CreatedAt:            cfg.CreatedAt,
		JournalPaths:         append([]string(nil), cfg.JournalPaths...),
		Coverage:             toSubstrateCoverage(coverage),
		SegmentDir:           cfg.Paths.SegmentDir,
		ManifestPath:         cfg.Paths.ManifestPath,
		CompletePath:         cfg.Paths.CompletePath,
		LatestPath:           cfg.Paths.LatestPath,
		ExpectedParent:       substrateParent,
		ParentSource:         continuity.parentSource,
		StateParent:          continuity.stateParent,
		Lineage:              continuity.lineage,
		ParentManifests:      continuity.parentManifests,
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

// verifiedParentPlan is the single lease-held capture of the canonical parent.
// One latest -> complete -> digest-checked manifest open supplies every
// downstream input derived from the same verified bytes: the CAS assertion
// token today, and (as continuity activates) the parent row source, StateParent,
// lineage decision, coverage, and ParentManifests. Caller-supplied metadata is
// never authority; a provided ExpectedParent is only asserted against this live
// capture. A nil snapshot with a nil token means a proven first publication.
type verifiedParentPlan struct {
	snapshot *indexsubstrate.PublishedSnapshot
	expected *ParentToken
}

// expectedToken returns the CAS assertion token from the capture (nil only for a
// proven first publication — a non-nil plan with no captured parent). It does
// not tolerate a nil receiver: a missing plan must fail closed at the call site,
// never silently degrade to a first-publication token.
func (p *verifiedParentPlan) expectedToken() *ParentToken {
	return p.expected
}

// captureVerifiedParent opens the canonical latest once under the caller's held
// authority/write lease and returns the verified-parent plan. The captured
// snapshot must belong to the requested index set (indexSetID); a validly
// digested foreign-set latest is refused before any sink/publish so a build for
// set B can never adopt or advance set A's latest. Only a true "not published"
// state is a first publish (empty plan). Any other open/trust error fails
// closed. Callers must not independently reopen latest to derive rows versus
// lineage; the sole later reopen is the publish-time CAS revalidation.
//
// When provided is non-nil it must equal the live parent token (including
// coverage digest); otherwise it is rejected as stale before sinks run. The
// caller assertion is checked, never trusted as authority.
func captureVerifiedParent(latestPath, indexSetID string, provided *ParentToken) (*verifiedParentPlan, error) {
	indexSetID = strings.TrimSpace(indexSetID)
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
			return &verifiedParentPlan{}, nil
		}
		// Malformed/unreadable/digest-invalid latest must not be treated as
		// first publication (would silently overwrite a damaged pointer).
		return nil, fmt.Errorf("open durable latest for parent capture: %w", err)
	}
	// Bind the capture to the requested set: a foreign latest (even validly
	// digested) is not a parent of this set. Same-set continuity is mandatory;
	// cross-set adoption is prohibited.
	if strings.TrimSpace(snap.Complete.IndexSetID) != indexSetID {
		return nil, fmt.Errorf("%w: latest belongs to a different index set", indexsubstrate.ErrStaleParent)
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
	captured := snap
	return &verifiedParentPlan{snapshot: &captured, expected: live}, nil
}

// hasParent reports whether a verified parent snapshot was captured.
func (p *verifiedParentPlan) hasParent() bool {
	return p != nil && p.snapshot != nil
}

// continuityPublication is the same-set continuity metadata plus the streaming
// parent row source derived from the single verified capture, ready to thread
// into publication.
type continuityPublication struct {
	parentSource    indexsubstrate.ParentRowSource
	stateParent     *indexsubstrate.StateParent
	lineage         *indexsubstrate.LineageRecord
	parentManifests []indexsubstrate.ManifestReference
}

// runCompleteLookup returns a PublishedRunLookup over the canonical
// runs/<run_id>/complete.json layout, derived from a known complete-marker path
// in that layout.
func runCompleteLookup(knownCompletePath string) indexsubstrate.PublishedRunLookup {
	runsDir := filepath.Dir(filepath.Dir(knownCompletePath))
	return func(_ string, runID string) (string, error) {
		runID = strings.TrimSpace(runID)
		if runID == "" {
			return "", fmt.Errorf("ancestry lookup requires a run id")
		}
		return filepath.Join(runsDir, runID, "complete.json"), nil
	}
}

// continuityInputs derives the publication continuity inputs from the captured
// verified parent under the three-way baseline/generation rule. Every input
// comes only from the single retained same-set capture (plus, for an idempotent
// re-publish, that run's own recorded parent). For a continuous parent it
// validates bounded ancestry before extending and fails closed on any defect.
//
//  1. no verified parent      -> baseline gen 1, no state_parent, no parent rows;
//  2. pre-continuity parent   -> baseline gen 1 + digest-bound state_parent;
//  3. continuous parent        -> gen = parent.gen + 1, non-baseline, digest-bound.
//
// When latest already names the run being published (idempotent recovery
// re-publish), the run's own recorded lineage/state_parent/parent_manifests are
// reproduced verbatim — the run never extends itself into a self-cycle.
func (p *verifiedParentPlan) continuityInputs(runID string) (continuityPublication, error) {
	runID = strings.TrimSpace(runID)
	if !p.hasParent() {
		return continuityPublication{
			lineage: &indexsubstrate.LineageRecord{
				Version:    indexsubstrate.LineageVersionV1,
				Generation: indexsubstrate.LineageBaselineGeneration,
				Baseline:   true,
			},
		}, nil
	}
	snap := p.snapshot
	if strings.TrimSpace(snap.Complete.RunID) == runID {
		return reproduceRunInputs(snap)
	}

	stateParent := &indexsubstrate.StateParent{
		IndexSetID:     strings.TrimSpace(snap.Complete.IndexSetID),
		RunID:          strings.TrimSpace(snap.Complete.RunID),
		ManifestSHA256: strings.TrimSpace(snap.Complete.ManifestSHA256),
	}
	parentManifests := []indexsubstrate.ManifestReference{{
		IndexSetID:     stateParent.IndexSetID,
		RunID:          stateParent.RunID,
		ManifestSHA256: stateParent.ManifestSHA256,
	}}
	// Validate the parent's ancestry before extending. A legacy (no-lineage)
	// parent is an accepted verified state source (ResolveAncestry returns legacy
	// mode without walking); a continuous parent's chain is walked to its baseline
	// under a run lookup derived from the canonical runs/<run_id>/complete.json
	// layout of the parent's own complete marker.
	if _, err := indexsubstrate.ResolveAncestry(*snap, indexsubstrate.AncestryResolveConfig{
		Lookup: runCompleteLookup(snap.CompletePath),
	}); err != nil {
		return continuityPublication{}, fmt.Errorf("verify parent ancestry before extend: %w", err)
	}
	var lineage *indexsubstrate.LineageRecord
	if indexsubstrate.HasContinuousLineage(snap.Manifest) {
		lineage = &indexsubstrate.LineageRecord{
			Version:    indexsubstrate.LineageVersionV1,
			Generation: snap.Manifest.Lineage.Generation + 1,
			Baseline:   false,
		}
	} else {
		lineage = &indexsubstrate.LineageRecord{
			Version:    indexsubstrate.LineageVersionV1,
			Generation: indexsubstrate.LineageBaselineGeneration,
			Baseline:   true,
		}
	}
	return continuityPublication{
		parentSource:    indexsubstrate.NewPublishedParentRowSource(*snap),
		stateParent:     stateParent,
		lineage:         lineage,
		parentManifests: parentManifests,
	}, nil
}

// reproduceRunInputs reproduces a run's own recorded continuity metadata for an
// idempotent re-publish. Parent rows come from the run's recorded state parent
// (the grandparent), opened digest-bound; a baseline run with no state parent
// re-publishes with no parent rows.
func reproduceRunInputs(snap *indexsubstrate.PublishedSnapshot) (continuityPublication, error) {
	out := continuityPublication{
		stateParent:     snap.Manifest.StateParent,
		lineage:         snap.Manifest.Lineage,
		parentManifests: snap.Manifest.ParentManifests,
	}
	sp := snap.Manifest.StateParent
	if sp == nil {
		return out, nil
	}
	lookup := runCompleteLookup(snap.CompletePath)
	gpComplete, err := lookup(strings.TrimSpace(sp.IndexSetID), strings.TrimSpace(sp.RunID))
	if err != nil {
		return continuityPublication{}, fmt.Errorf("resolve re-publish parent: %w", err)
	}
	gp, err := indexsubstrate.OpenPublishedRunSnapshot(gpComplete, strings.TrimSpace(sp.IndexSetID), strings.TrimSpace(sp.RunID))
	if err != nil {
		return continuityPublication{}, fmt.Errorf("open re-publish parent: %w", err)
	}
	if strings.TrimSpace(gp.Complete.ManifestSHA256) != strings.TrimSpace(sp.ManifestSHA256) {
		return continuityPublication{}, fmt.Errorf("%w: re-publish parent digest mismatch", indexsubstrate.ErrStaleParent)
	}
	out.parentSource = indexsubstrate.NewPublishedParentRowSource(gp)
	return out, nil
}

// rejectCallerPriorRows fails closed when a public Build/Retry caller supplies
// prior rows. Durable prior state is canonical only when loaded from the verified
// parent under the held lease; caller-supplied rows plus independently supplied
// parent metadata cannot authorize continuity. The PriorRows fields remain for
// API compatibility but are not an accepted canonical-state channel. The check
// is strict presence (any non-nil slice, including an empty non-nil slice), so
// the field's rejected status is unambiguous.
func rejectCallerPriorRows(rows []ObjectState) error {
	if rows != nil {
		return fmt.Errorf("PriorRows is not an accepted input: durable prior state is loaded from the verified parent, not caller-supplied rows")
	}
	return nil
}

func normalizeConfig(cfg Config) (Config, error) {
	if strings.TrimSpace(cfg.IndexSetID) == "" {
		return Config{}, fmt.Errorf("index_set_id is required")
	}
	if err := rejectCallerPriorRows(cfg.PriorRows); err != nil {
		return Config{}, err
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
	runStartedAt, err := resolveRunStartedAtUTC(cfg.RunStartedAt, cfg.Clock)
	if err != nil {
		return Config{}, err
	}
	cfg.RunStartedAt = runStartedAt
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
	if err := rejectCallerPriorRows(cfg.PriorRows); err != nil {
		return RetryConfig{}, err
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
	runStartedAt, err := resolveRunStartedAtUTC(cfg.RunStartedAt, cfg.Clock)
	if err != nil {
		return RetryConfig{}, err
	}
	cfg.RunStartedAt = runStartedAt
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

// resolveRunStartedAtUTC defaults a zero run start to the clock, then refuses a
// non-UTC value (caller-supplied or clock-produced) on the raw input before any
// .UTC() laundering, so direct-library Build/Retry callers receive the
// authoritative-time contract before crawl or observation-sink mutation. The
// returned value is UTC-normalized.
func resolveRunStartedAtUTC(runStartedAt time.Time, clock Clock) (time.Time, error) {
	if runStartedAt.IsZero() {
		runStartedAt = clock()
	}
	if err := indexsubstrate.ValidateAuthoritativeRunStartedAt(runStartedAt); err != nil {
		return time.Time{}, err
	}
	return runStartedAt.UTC(), nil
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
