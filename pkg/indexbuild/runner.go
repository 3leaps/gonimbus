package indexbuild

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
	// Enforce the continuity layout contract now — before the provider crawl,
	// journal creation, or any observation-sink mutation: same-run recovery at
	// its exact captured locus; an edge-emitting build only over a canonically
	// resolvable parent and into a canonical target.
	if err := plan.preflightContinuityLayout(cfg.RunID, cfg.Paths); err != nil {
		return Summary{}, err
	}

	basePrefix, err := basePrefixFromURI(cfg.BaseURI)
	if err != nil {
		return Summary{}, err
	}
	matcher, err := buildMatcher(basePrefix, cfg.Match)
	if err != nil {
		return Summary{}, err
	}
	// The sealed journal records the canonical observation plan so a later
	// recovery re-publish can bind its coverage authority to what was actually
	// crawled (scoped plan, or the full base prefix when unscoped).
	journalPlan, err := journalCrawlPlan(basePrefix, cfg.CrawlPrefixes)
	if err != nil {
		return Summary{}, err
	}
	journalPath := filepath.Join(cfg.Paths.JournalDir, "shard-0001.jsonl")
	writer, err := newJournalWriter(journalWriterConfig{
		Path:          journalPath,
		IndexSetID:    cfg.IndexSetID,
		RunID:         cfg.RunID,
		StartedAt:     cfg.RunStartedAt,
		BaseURI:       cfg.BaseURI,
		BasePrefix:    basePrefix,
		CrawlPrefixes: journalPlan,
		Now:           cfg.Clock,
		Events:        cfg.Events,
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
	// Enforce the continuity layout contract before any publication work (same
	// boundary contract as Build's pre-crawl check).
	if err := plan.preflightContinuityLayout(cfg.RunID, cfg.Paths); err != nil {
		return Summary{}, err
	}
	// Bind coverage authority to sealed-journal provenance before any publish
	// side effect. Coverage authorizes tombstones over the verified-parent rows,
	// so the plan it must match comes from the journals (what was actually
	// observed), never from a caller field that could widen both plan and
	// coverage together. Legacy journals without a recorded plan fail closed.
	// Runs after capture/authority so wrong-set and stale-parent refusals keep
	// priority; no publish artifact exists yet, so latest stays byte-identical.
	boundPlan, err := boundCrawlPlanFromJournals(cfg.JournalPaths)
	if err != nil {
		return Summary{}, err
	}
	if err := validateCoverageMatchesCrawlPlan(cfg.BaseURI, boundPlan, cfg.Coverage); err != nil {
		return Summary{}, fmt.Errorf("retry coverage authority: %w", err)
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
	// The verified-parent plan is the sole canonical parent authority. Validate it
	// as one invariant (both-absent first publication, or both-present and mutually
	// consistent) before any token or continuity use, so an inconsistent plan can
	// never degrade into a baseline or a token/continuity disagreement.
	if err := plan.validate(); err != nil {
		return Summary{}, err
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
	continuity, err := plan.continuityInputs(cfg.RunID, cfg.Paths)
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

// validate enforces the typed-plan invariant before any token or continuity use.
// The only legal shapes are a proven first publication (snapshot and token both
// absent) or a fully consistent captured parent (both present and mutually
// identical on index set, run, manifest digest, and coverage digest derived from
// the captured snapshot). Every mixed or mismatched shape — and a nil plan —
// fails closed, so an inconsistent plan can never degrade into a baseline
// first-publication or a token/continuity disagreement.
func (p *verifiedParentPlan) validate() error {
	if p == nil {
		return fmt.Errorf("verified parent plan is required")
	}
	hasSnapshot := p.snapshot != nil
	hasToken := p.expected != nil
	if hasSnapshot != hasToken {
		return fmt.Errorf("%w: inconsistent verified parent plan (snapshot and token must both be present or both absent)", indexsubstrate.ErrStaleParent)
	}
	if !hasSnapshot {
		return nil
	}
	snap := p.snapshot
	tok := p.expected
	if strings.TrimSpace(snap.Complete.IndexSetID) != strings.TrimSpace(tok.IndexSetID) ||
		strings.TrimSpace(snap.Complete.RunID) != strings.TrimSpace(tok.RunID) ||
		strings.TrimSpace(snap.Complete.ManifestSHA256) != strings.TrimSpace(tok.ManifestSHA256) {
		return fmt.Errorf("%w: verified parent plan snapshot and token identity disagree", indexsubstrate.ErrStaleParent)
	}
	covDigest, err := indexsubstrate.CoverageSHA256(snap.Manifest.Coverage)
	if err != nil {
		return fmt.Errorf("hash captured parent coverage: %w", err)
	}
	if strings.TrimSpace(tok.CoverageSHA256) != covDigest {
		return fmt.Errorf("%w: verified parent plan coverage digest disagrees with the captured snapshot", indexsubstrate.ErrStaleParent)
	}
	return nil
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
	// If the captured parent carries a state parent, any continuation (extend or
	// continuous re-publish) walks ancestry through the canonical runs layout;
	// validate that layout now — before any sink — so an off-layout captured
	// pointer fails closed early rather than mid-publish. The runs root is bound
	// to the directory that owns the configured latest, and the captured run's
	// recorded manifest/segment loci must be contained in its run directory.
	if snap.Manifest.StateParent != nil {
		if _, err := canonicalRunsRoot(snap.CompletePath, snap.Complete.RunID, latestPath); err != nil {
			return nil, err
		}
		runDir := filepath.Dir(filepath.Clean(snap.CompletePath))
		if !pathWithinDir(snap.Complete.ManifestPath, runDir) || !pathWithinDir(snap.SegmentDir, runDir) {
			return nil, fmt.Errorf("%w: captured parent artifacts are not contained in its canonical run directory", indexsubstrate.ErrStaleParent)
		}
	}
	captured := snap
	return &verifiedParentPlan{snapshot: &captured, expected: live}, nil
}

// hasParent reports whether a verified parent snapshot was captured.
func (p *verifiedParentPlan) hasParent() bool {
	return p != nil && p.snapshot != nil
}

// preflightContinuityLayout enforces the continuity layout contract before any
// provider crawl, journal creation, or observation-sink mutation. Build and
// public Retry call it immediately after the verified capture; the publish
// seam re-checks it as defense.
//
//   - Same-run recovery: the requested paths must equal the captured run's
//     recorded immutable locus exactly (complete marker, manifest, segment dir).
//     A same set/run id at any other locus is not idempotent recovery — it
//     would write a second artifact identity for that run id.
//   - New run over a captured parent (an edge-emitting publication): the
//     captured parent — whatever its own lineage shape — must sit at the
//     latest-owned canonical runs/<run>/complete.json locus with its recorded
//     manifest/segment artifacts contained in that run directory, because the
//     child records it as a pathless state_parent that the production ancestry
//     lookup can only rediscover at that root. The new target must satisfy the
//     same canonical contract, so the advanced latest is never a
//     continuity-bearing root the next capture would reject.
//   - First publication (no captured parent): no layout constraint. It emits
//     no edge and resolves without any ancestry lookup; extending it later
//     requires the canonical layout.
func (p *verifiedParentPlan) preflightContinuityLayout(runID string, paths PathConfig) error {
	if !p.hasParent() {
		return nil
	}
	runID = strings.TrimSpace(runID)
	snap := p.snapshot
	if strings.TrimSpace(snap.Complete.RunID) == runID {
		if filepath.Clean(paths.CompletePath) != filepath.Clean(snap.CompletePath) ||
			filepath.Clean(paths.ManifestPath) != filepath.Clean(snap.Complete.ManifestPath) ||
			filepath.Clean(paths.SegmentDir) != filepath.Clean(snap.SegmentDir) {
			return fmt.Errorf("%w: same run id at a different artifact locus is not a recovery re-publish", indexsubstrate.ErrStaleParent)
		}
		return nil
	}
	// Edge-emitting publication: the captured parent becomes the child's
	// pathless state_parent, so it must be resolvable at the canonical root.
	if _, err := canonicalRunsRoot(snap.CompletePath, snap.Complete.RunID, paths.LatestPath); err != nil {
		return fmt.Errorf("captured parent for continuity edge: %w", err)
	}
	parentRunDir := filepath.Dir(filepath.Clean(snap.CompletePath))
	if !pathWithinDir(snap.Complete.ManifestPath, parentRunDir) || !pathWithinDir(snap.SegmentDir, parentRunDir) {
		return fmt.Errorf("%w: captured parent artifacts are not contained in its canonical run directory", indexsubstrate.ErrStaleParent)
	}
	// The new continuity-bearing target must itself be canonical.
	if _, err := canonicalRunsRoot(paths.CompletePath, runID, paths.LatestPath); err != nil {
		return fmt.Errorf("continuity target: %w", err)
	}
	targetRunDir := filepath.Dir(filepath.Clean(paths.CompletePath))
	if !pathWithinDir(paths.ManifestPath, targetRunDir) || !pathWithinDir(paths.SegmentDir, targetRunDir) {
		return fmt.Errorf("%w: continuity target manifest/segment paths are not contained in the canonical run directory", indexsubstrate.ErrStaleParent)
	}
	return nil
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

// safeRunComponent reports whether s is a single safe path component usable as a
// run directory name (no separators, traversal, or empty/dot names).
func safeRunComponent(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" || s == "." || s == ".." {
		return false
	}
	if strings.ContainsAny(s, `/\`) || strings.Contains(s, "..") {
		return false
	}
	return s == filepath.Base(s)
}

// canonicalRunsRoot validates that a captured complete-marker path is exactly
// <set-root>/runs/<capturedRunID>/complete.json, where <set-root> is the
// directory that owns the configured latest pointer. The root is derived from
// the trusted latest location — never from the pointer under validation — so a
// digest-valid pointer cannot redirect ancestry/re-publish lookups to a
// renamed marker, a sibling directory, or a foreign tree that merely imitates
// the runs/<run>/complete.json shape.
func canonicalRunsRoot(completePath, capturedRunID, latestPath string) (string, error) {
	completePath = filepath.Clean(completePath)
	if filepath.Base(completePath) != "complete.json" {
		return "", fmt.Errorf("%w: captured parent marker is not a canonical complete.json", indexsubstrate.ErrStaleParent)
	}
	runDir := filepath.Dir(completePath)
	if filepath.Base(runDir) != strings.TrimSpace(capturedRunID) {
		return "", fmt.Errorf("%w: captured complete marker is not under its canonical run directory", indexsubstrate.ErrStaleParent)
	}
	runsRoot := filepath.Dir(runDir)
	if filepath.Base(runsRoot) != "runs" {
		return "", fmt.Errorf("%w: captured complete marker is not under a canonical runs/ root", indexsubstrate.ErrStaleParent)
	}
	owned := filepath.Join(filepath.Dir(filepath.Clean(strings.TrimSpace(latestPath))), "runs")
	if runsRoot != owned {
		return "", fmt.Errorf("%w: captured runs/ root is not the set root that owns latest", indexsubstrate.ErrStaleParent)
	}
	return runsRoot, nil
}

// pathWithinDir reports whether p is contained in dir — the directory itself
// or below it. Equality is allowed because the enrich publisher records its
// segment directory as the run directory itself.
func pathWithinDir(p, dir string) bool {
	rel, err := filepath.Rel(filepath.Clean(dir), filepath.Clean(strings.TrimSpace(p)))
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// runCompleteLookup returns a PublishedRunLookup rooted at the captured snapshot.
// It validates the canonical runs/<run_id>/complete.json layout on every call —
// with the runs root bound to the directory that owns the captured latest
// pointer, so an off-layout or foreign-tree pointer fails closed rather than
// redirecting the walk — reasserts the captured index set, rejects unsafe run
// components, and keeps every resolved marker contained directly under the
// canonical runs root. The validation is per-call so a baseline parent that
// never triggers an ancestry walk does not require the canonical layout.
func runCompleteLookup(snap *indexsubstrate.PublishedSnapshot) indexsubstrate.PublishedRunLookup {
	setID := strings.TrimSpace(snap.Complete.IndexSetID)
	completePath := snap.CompletePath
	latestPath := snap.LatestPath
	capturedRun := strings.TrimSpace(snap.Complete.RunID)
	return func(reqSetID, runID string) (string, error) {
		if strings.TrimSpace(latestPath) == "" {
			return "", fmt.Errorf("%w: ancestry lookup requires a latest-owned capture", indexsubstrate.ErrStaleParent)
		}
		runsRoot, err := canonicalRunsRoot(completePath, capturedRun, latestPath)
		if err != nil {
			return "", err
		}
		if s := strings.TrimSpace(reqSetID); s != "" && s != setID {
			return "", fmt.Errorf("%w: ancestry lookup crossed index sets", indexsubstrate.ErrStaleParent)
		}
		runID = strings.TrimSpace(runID)
		if !safeRunComponent(runID) {
			return "", fmt.Errorf("ancestry lookup requires a safe run id")
		}
		resolved := filepath.Join(runsRoot, runID, "complete.json")
		if filepath.Dir(filepath.Dir(resolved)) != runsRoot {
			return "", fmt.Errorf("%w: ancestry lookup escaped the canonical runs root", indexsubstrate.ErrStaleParent)
		}
		return resolved, nil
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
func (p *verifiedParentPlan) continuityInputs(runID string, publishPaths PathConfig) (continuityPublication, error) {
	runID = strings.TrimSpace(runID)
	// Publish-seam defense re-check of the continuity layout contract that
	// Build/Retry already enforced before any sink ran: same-run recovery at the
	// exact captured locus; edge emission only over a canonically resolvable
	// parent into a canonical target.
	if err := p.preflightContinuityLayout(runID, publishPaths); err != nil {
		return continuityPublication{}, err
	}
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
		Lookup: runCompleteLookup(snap),
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
	lookup := runCompleteLookup(snap)
	// A continuous run must revalidate its own bounded ancestry before reproducing
	// — the same fail-closed contract as extension, so a re-publish cannot re-emit
	// a continuous claim over a broken deep ancestor.
	if indexsubstrate.HasContinuousLineage(snap.Manifest) {
		if _, err := indexsubstrate.ResolveAncestry(*snap, indexsubstrate.AncestryResolveConfig{
			Lookup: lookup,
		}); err != nil {
			return continuityPublication{}, fmt.Errorf("verify re-publish ancestry: %w", err)
		}
	}
	sp := snap.Manifest.StateParent
	if sp == nil {
		return out, nil
	}
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

// journalCrawlPlan returns the canonical observation plan to seal into the
// journal header. A scoped build stamps its exact crawl prefixes; an unscoped
// build stamps the full base prefix (or the relative-root sentinel for a
// bucket-root base), so every U4+ journal records a non-empty plan and a later
// recovery can distinguish it from a legacy (pre-provenance) journal.
func journalCrawlPlan(basePrefix string, crawlPrefixes []string) ([]string, error) {
	if len(crawlPrefixes) > 0 {
		return append([]string(nil), crawlPrefixes...), nil
	}
	basePrefix = strings.TrimSpace(basePrefix)
	if basePrefix == "" {
		return []string{indexsubstrate.RelativeRootScopePrefix}, nil
	}
	return []string{basePrefix}, nil
}

// boundCrawlPlanFromJournals reads the sealed journal headers and returns the
// single crawl-prefix plan they were built under. This is the observation
// provenance that bounds recovery coverage authority, so every journal must:
//   - be content-integrity verified (ValidateJournal recomputes the footer
//     ContentSHA256 over the header+records, so a post-seal header edit fails);
//   - carry that sealed digest (a journal without one is not tamper-evident and
//     fails closed — a plan added to an unauthenticated legacy journal is not
//     provenance);
//   - record a canonical, non-empty plan (leading slash / whitespace / empty /
//     duplicate entries are invalid provenance, never trimmed into validity);
//   - agree with every other journal on the exact plan (order-independent).
func boundCrawlPlanFromJournals(journalPaths []string) ([]string, error) {
	if len(journalPaths) == 0 {
		return nil, fmt.Errorf("journal paths are required")
	}
	var plan []string
	var planKey string
	for i, path := range journalPaths {
		summary, err := indexsubstrate.ValidateJournal(path)
		if err != nil {
			return nil, fmt.Errorf("read journal header for coverage provenance: %w", err)
		}
		if strings.TrimSpace(summary.ContentSHA256) == "" {
			return nil, fmt.Errorf("%w: journal %d is not content-integrity sealed; recovery cannot trust its crawl-plan provenance", indexsubstrate.ErrStaleParent, i)
		}
		if len(summary.Header.CrawlPrefixes) == 0 {
			return nil, fmt.Errorf("%w: journal %d predates crawl-plan provenance; recovery cannot validate coverage authority", indexsubstrate.ErrStaleParent, i)
		}
		if err := validateJournalPlanCanonical(summary.Header.CrawlPrefixes); err != nil {
			return nil, fmt.Errorf("%w: journal %d: %v", indexsubstrate.ErrStaleParent, i, err)
		}
		key := crawlPlanSetKey(summary.Header.CrawlPrefixes)
		if i == 0 {
			plan = append([]string(nil), summary.Header.CrawlPrefixes...)
			planKey = key
			continue
		}
		if key != planKey {
			return nil, fmt.Errorf("%w: sealed journals disagree on their crawl-prefix plan", indexsubstrate.ErrStaleParent)
		}
	}
	return plan, nil
}

// crawlPlanSetKey is an order-independent identity for a canonical plan so
// journals recorded in any order compare equal. It does not trim: canonicality
// is enforced separately by validateJournalPlanCanonical so a plan cannot be
// normalized into agreement.
func crawlPlanSetKey(prefixes []string) string {
	sorted := append([]string(nil), prefixes...)
	sort.Strings(sorted)
	return strings.Join(sorted, "\x00")
}

// validateJournalPlanCanonical enforces the same canonical-bytes and
// no-duplicate rules on a recovered journal plan that Build enforces on the
// caller plan, so a structurally valid but non-canonical or duplicated journal
// plan is rejected rather than trimmed/deduplicated into agreement.
func validateJournalPlanCanonical(prefixes []string) error {
	if err := validateCrawlPlanCanonical(prefixes); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(prefixes))
	for _, p := range prefixes {
		if _, dup := seen[p]; dup {
			return fmt.Errorf("crawl plan has duplicate prefix %q", p)
		}
		seen[p] = struct{}{}
	}
	return nil
}

// validateCrawlPlanCanonical refuses a crawl-prefix plan entry whose bytes are
// not the exact provider-key prefix that drives LIST. Surrounding whitespace or
// a leading slash would let the coverage equality gate normalize a different
// string than the crawler observes, so a scoped build could attest complete
// coverage of a prefix it never listed and tombstone live parent rows.
func validateCrawlPlanCanonical(crawlPrefixes []string) error {
	for i, raw := range crawlPrefixes {
		if raw != strings.TrimSpace(raw) {
			return fmt.Errorf("crawl prefix plan[%d] has surrounding whitespace; supply the exact provider-key prefix", i)
		}
		if raw == "" {
			return fmt.Errorf("crawl prefix plan[%d] is empty", i)
		}
		if strings.HasPrefix(raw, "/") {
			return fmt.Errorf("crawl prefix plan[%d] must not begin with '/'; supply the exact provider-key prefix", i)
		}
	}
	return nil
}

// validateDurableObservationSelector refuses any durable build whose match or
// filter would observe fewer objects than its recorded plan attests complete.
// Coverage is destructive authority over verified-parent rows, and the sealed
// crawl plan is the observation universe recovery re-publishes against — so an
// exclude, non-default include, hidden inclusion, or post-list filter would make
// the plan a false record of what was observed and authorize tombstones over
// rows the run intentionally skipped. The check is unconditional (scoped and
// unscoped): an unscoped build stamps its base prefix as the plan, so the base
// prefix must genuinely be the complete observation universe. This mirrors the
// CLI adapter's faithful-coverage match gate at the engine seam; it intentionally
// narrows the Experimental library API — restrictive match/filter selection is
// no longer a durable-build surface.
func validateDurableObservationSelector(cfg Config) error {
	const reason = "coverage and the sealed crawl plan attest complete observation"
	if len(cfg.Match.Excludes) > 0 {
		return fmt.Errorf("a durable build does not support match excludes: %s", reason)
	}
	if cfg.Match.IncludeHidden {
		return fmt.Errorf("a durable build does not support include_hidden: %s", reason)
	}
	if !isDefaultObservationIncludes(cfg.Match.Includes) {
		return fmt.Errorf("a durable build supports only the default include: %s", reason)
	}
	if cfg.Filter != nil {
		return fmt.Errorf("a durable build does not support a post-list filter: %s", reason)
	}
	return nil
}

// isDefaultObservationIncludes reports whether includes is the unrestricted
// default (empty or a single "**"), the only match include compatible with a
// complete-coverage attestation over the crawl plan.
func isDefaultObservationIncludes(includes []string) bool {
	if len(includes) == 0 {
		return true
	}
	if len(includes) != 1 {
		return false
	}
	switch strings.TrimSpace(includes[0]) {
	case "", "**":
		return true
	default:
		return false
	}
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
	// Coverage authorizes tombstones over verified-parent rows and the crawl
	// plan is the observation universe recovery re-publishes against, so both
	// must be faithful to what is actually observed — validated before any event,
	// sink, or crawl side effect. The effective plan is the explicit crawl
	// prefixes when scoped, or the full base prefix when unscoped; coverage must
	// equal it exactly (so an unscoped [base] stamp is truthful and Build/Retry
	// are symmetric), the plan bytes must be exactly what drives LIST, and the
	// observation selector must not reduce below the plan.
	basePrefix, err := basePrefixFromURI(cfg.BaseURI)
	if err != nil {
		return Config{}, err
	}
	if err := validateCrawlPlanCanonical(cfg.CrawlPrefixes); err != nil {
		return Config{}, err
	}
	if err := validateDurableObservationSelector(cfg); err != nil {
		return Config{}, err
	}
	effectivePlan, err := journalCrawlPlan(basePrefix, cfg.CrawlPrefixes)
	if err != nil {
		return Config{}, err
	}
	if err := validateCoverageMatchesCrawlPlan(cfg.BaseURI, effectivePlan, cfg.Coverage); err != nil {
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
