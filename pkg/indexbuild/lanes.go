package indexbuild

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/crawler"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
)

const (
	// DefaultMaxJournalLanes is the resolved lane ceiling when Config
	// .MaxJournalLanes is zero.
	DefaultMaxJournalLanes = 4
	// MaxJournalLanesCeiling is the highest lane count this engine accepts. It
	// stays well below the streaming merge's journal-source budget, so a run can
	// never produce more sealed journals than publication is able to stage.
	MaxJournalLanesCeiling = 32
)

// lane is one journal writer and the crawl-plan subset it attests.
type lane struct {
	// ordinal is the 1-based canonical position, derived from the assignment
	// order rather than from completion order, so journal identity — and with it
	// cross-journal conflict resolution — does not vary with scheduling.
	ordinal int
	// prefixes is both what this lane lists and what its journal header records
	// as the plan it attests. They are the same slice deliberately: the lane's
	// coverage claim cannot drift from the work it actually did.
	prefixes []string
}

// planLanes maps a crawl plan onto lanes.
//
// Lanes are only available for an explicit multi-entry crawl plan. The plan
// entries are the unit of coverage authority, so a lane can attest a subset only
// when there is more than one entry to divide; a build that derives its prefixes
// from the matcher records a single synthesized plan entry and stays single-lane,
// exactly as it does today.
//
// Assignment runs over a sorted copy, never the authoritative slice. Sorting the
// authoritative plan would change the journal artifact for a valid caller plan
// supplied in another order, and the caller's exact order is what the single-lane
// artifact and summary compatibility depend on. Round-robin over the sorted copy
// spreads lexicographically adjacent prefixes — which tend to correlate in
// population — across lanes rather than concentrating that skew in one.
func planLanes(crawlPrefixes []string, crawlConcurrency, maxLanes int) []lane {
	if len(crawlPrefixes) < 2 {
		return nil
	}
	effective := len(crawlPrefixes)
	if crawlConcurrency > 0 && crawlConcurrency < effective {
		effective = crawlConcurrency
	}
	if maxLanes < effective {
		effective = maxLanes
	}
	if effective < 2 {
		return nil
	}

	assignment := append([]string(nil), crawlPrefixes...)
	sort.Strings(assignment)

	lanes := make([]lane, effective)
	for i := range lanes {
		lanes[i].ordinal = i + 1
	}
	for i, prefix := range assignment {
		target := i % effective
		lanes[target].prefixes = append(lanes[target].prefixes, prefix)
	}
	return lanes
}

// resolveMaxJournalLanes turns the public control into an effective ceiling.
// Zero selects the resolved default and one is the compatibility setting; a
// value above the engine ceiling is refused rather than clamped, so an operator
// asking for more lanes than publication can stage is told so instead of
// silently getting fewer.
func resolveMaxJournalLanes(configured int) (int, error) {
	switch {
	case configured < 0:
		return 0, fmt.Errorf("max journal lanes must not be negative, got %d", configured)
	case configured == 0:
		return DefaultMaxJournalLanes, nil
	case configured > MaxJournalLanesCeiling:
		return 0, fmt.Errorf("max journal lanes must not exceed %d, got %d", MaxJournalLanesCeiling, configured)
	default:
		return configured, nil
	}
}

// sharedObservationSinks owns the observation sinks supplied by the caller for
// the whole run, not per lane.
//
// Ownership sits here because the sinks are one set shared by every lane: each
// must be closed exactly once no matter how many lanes wrote to it, and the
// progress and summary a caller receives must describe the run rather than
// arriving N times each describing a fraction of it.
//
// Object and error delivery is not serialized. output.Writer already requires
// every Write method to be safe for concurrent use, and those are the hot path.
type sharedObservationSinks struct {
	sinks []output.Writer
	// laneScoped marks a run that genuinely has several lanes. A single-lane run
	// keeps its crawler's own progress and summary records verbatim, so the
	// observable stream a caller has always received is unchanged; the run-scope
	// rewriting below exists only where there are several lanes to reconcile.
	laneScoped bool

	mu sync.Mutex
	// laneProgress holds each lane's most recent progress counts so a forwarded
	// record can carry the run total instead of one lane's share of it.
	laneProgress map[int]output.ProgressRecord
	// laneSummaries holds each lane's final summary until the run emits one.
	laneSummaries map[int]output.SummaryRecord
	closed        bool
}

func newSharedObservationSinks(sinks []output.Writer, laneScoped bool) *sharedObservationSinks {
	return &sharedObservationSinks{
		sinks:         append([]output.Writer(nil), sinks...),
		laneScoped:    laneScoped,
		laneProgress:  make(map[int]output.ProgressRecord),
		laneSummaries: make(map[int]output.SummaryRecord),
	}
}

func (s *sharedObservationSinks) writeObject(ctx context.Context, obj *output.ObjectRecord) error {
	for i, sink := range s.sinks {
		if sink == nil {
			continue
		}
		if err := sink.WriteObject(ctx, obj); err != nil {
			return fmt.Errorf("observation sink %d object: %w", i, err)
		}
	}
	return nil
}

func (s *sharedObservationSinks) writeError(ctx context.Context, rec *output.ErrorRecord) error {
	for i, sink := range s.sinks {
		if sink == nil {
			continue
		}
		if err := sink.WriteError(ctx, rec); err != nil {
			return fmt.Errorf("observation sink %d error: %w", i, err)
		}
	}
	return nil
}

// writeProgress folds one lane's progress into the run total and forwards a
// single record describing the run. Without this a caller watching a four-lane
// build sees four interleaved streams, each of which looks like the whole run
// and none of which is.
//
// The aggregate is computed and delivered under one lock. Releasing it before
// delivery would let a lane that computed a newer total overtake a lane still
// delivering an older one, so a caller would watch the run's counts move
// backwards. Progress is a cold path — object and error delivery stay
// concurrent, and only this ordering is serialized.
//
// The emitting lane's phase is deliberately not carried into the run-scope
// record. A lane reports `complete` when its own prefixes are done, which says
// nothing about its peers; forwarding it would announce a finished run while
// other lanes are still listing. The runner emits the run's single terminal
// record once every lane is actually terminal.
func (s *sharedObservationSinks) writeProgress(ctx context.Context, ordinal int, prog *output.ProgressRecord) error {
	if prog == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.laneProgress[ordinal] = *prog
	runScope := s.aggregateProgressLocked(*prog)
	if s.laneScoped {
		runScope.Phase = output.PhaseListing
	}
	return s.deliverProgressLocked(ctx, &runScope)
}

// aggregateProgressLocked sums every lane's latest counts into one run-scope
// record. The caller must hold s.mu.
func (s *sharedObservationSinks) aggregateProgressLocked(base output.ProgressRecord) output.ProgressRecord {
	runScope := base
	runScope.ObjectsFound = 0
	runScope.ObjectsMatched = 0
	runScope.BytesTotal = 0
	for _, lp := range s.laneProgress {
		runScope.ObjectsFound += lp.ObjectsFound
		runScope.ObjectsMatched += lp.ObjectsMatched
		runScope.BytesTotal += lp.BytesTotal
	}
	return runScope
}

// deliverProgressLocked writes one record to every sink. The caller must hold
// s.mu, which is what keeps the delivered sequence monotonic.
func (s *sharedObservationSinks) deliverProgressLocked(ctx context.Context, rec *output.ProgressRecord) error {
	for i, sink := range s.sinks {
		if sink == nil {
			continue
		}
		if err := sink.WriteProgress(ctx, rec); err != nil {
			return fmt.Errorf("observation sink %d progress: %w", i, err)
		}
	}
	return nil
}

// emitTerminalProgress delivers the run's single completed progress record,
// after every lane is terminal. Its counts are the run's final totals, so a
// caller can rely on the last progress record agreeing with the run summary.
func (s *sharedObservationSinks) emitTerminalProgress(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.laneScoped || len(s.laneProgress) == 0 {
		return nil
	}
	runScope := s.aggregateProgressLocked(output.ProgressRecord{})
	runScope.Phase = output.PhaseComplete
	return s.deliverProgressLocked(ctx, &runScope)
}

// recordLaneSummary retains a lane's summary until the run has one to emit.
func (s *sharedObservationSinks) recordLaneSummary(ordinal int, sum *output.SummaryRecord) {
	if sum == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.laneSummaries[ordinal] = *sum
}

// emitRunSummary writes exactly one summary describing the whole run.
//
// Counts sum across lanes. The duration is the run's measured wall span, not the
// longest lane's: lanes do not start together, so the slowest lane's own elapsed
// time understates a run whose lanes were staggered. With a single lane the
// crawler's own record is passed through untouched, which keeps the
// single-journal path's observable output exactly as it was.
func (s *sharedObservationSinks) emitRunSummary(ctx context.Context, runWall time.Duration) error {
	s.mu.Lock()
	if len(s.laneSummaries) == 0 {
		s.mu.Unlock()
		return nil
	}
	ordinals := make([]int, 0, len(s.laneSummaries))
	for ordinal := range s.laneSummaries {
		ordinals = append(ordinals, ordinal)
	}
	sort.Ints(ordinals)

	runScope := s.laneSummaries[ordinals[0]]
	if s.laneScoped {
		runScope = output.SummaryRecord{}
		prefixes := make([]string, 0)
		for _, ordinal := range ordinals {
			ls := s.laneSummaries[ordinal]
			runScope.ObjectsFound += ls.ObjectsFound
			runScope.ObjectsMatched += ls.ObjectsMatched
			runScope.BytesTotal += ls.BytesTotal
			runScope.Errors += ls.Errors
			prefixes = append(prefixes, ls.Prefixes...)
		}
		runScope.Prefixes = prefixes
		runScope.Duration = runWall
		runScope.DurationHuman = runWall.String()
	}
	sinks := s.sinks
	s.mu.Unlock()

	for i, sink := range sinks {
		if sink == nil {
			continue
		}
		if err := sink.WriteSummary(ctx, &runScope); err != nil {
			return fmt.Errorf("observation sink %d summary: %w", i, err)
		}
	}
	return nil
}

// close closes every shared sink exactly once, however many lanes used them.
func (s *sharedObservationSinks) close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	var firstErr error
	for i, sink := range s.sinks {
		if sink == nil {
			continue
		}
		if err := sink.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("observation sink %d close: %w", i, err)
		}
	}
	return firstErr
}

// laneObservationWriter is the output.Writer one lane's crawler writes into. It
// owns its journal and closes it, and it borrows the run's shared sinks without
// owning them.
type laneObservationWriter struct {
	ordinal int
	journal *journalWriter
	shared  *sharedObservationSinks
}

func newLaneObservationWriter(ordinal int, journal *journalWriter, shared *sharedObservationSinks) *laneObservationWriter {
	return &laneObservationWriter{ordinal: ordinal, journal: journal, shared: shared}
}

func (w *laneObservationWriter) WriteObject(ctx context.Context, obj *output.ObjectRecord) error {
	if err := w.journal.WriteObject(ctx, obj); err != nil {
		return err
	}
	return w.shared.writeObject(ctx, obj)
}

func (w *laneObservationWriter) WriteError(ctx context.Context, rec *output.ErrorRecord) error {
	if err := w.journal.WriteError(ctx, rec); err != nil {
		return err
	}
	return w.shared.writeError(ctx, rec)
}

func (w *laneObservationWriter) WriteProgress(ctx context.Context, prog *output.ProgressRecord) error {
	return w.shared.writeProgress(ctx, w.ordinal, prog)
}

// WriteSummary is retained rather than forwarded. A lane summary describes a
// fraction of the run, so the runner emits one run-scope summary from these once
// every lane is terminal.
func (w *laneObservationWriter) WriteSummary(_ context.Context, sum *output.SummaryRecord) error {
	w.shared.recordLaneSummary(w.ordinal, sum)
	return nil
}

func (w *laneObservationWriter) WritePrefix(context.Context, *output.PrefixRecord) error { return nil }
func (w *laneObservationWriter) WritePreflight(context.Context, *output.PreflightRecord) error {
	return nil
}
func (w *laneObservationWriter) WriteTransfer(context.Context, *output.TransferRecord) error {
	return nil
}
func (w *laneObservationWriter) WriteSkip(context.Context, *output.SkipRecord) error { return nil }

// Close closes only this lane's journal. The shared sinks outlive the lane and
// are closed once by the runner.
func (w *laneObservationWriter) Close() error { return w.journal.Close() }

var _ output.Writer = (*laneObservationWriter)(nil)

// serializedEventSink delivers events one at a time.
//
// Lane crawlers report errors through the same caller-supplied sink, which was
// only ever called from a single goroutine before lanes existed and promises no
// concurrent-safety. Crawl errors are a cold path, so serializing here costs
// nothing measurable and asks nothing new of existing implementations.
type serializedEventSink struct {
	mu    sync.Mutex
	inner EventSink
}

func newSerializedEventSink(inner EventSink) EventSink {
	if inner == nil {
		return nil
	}
	return &serializedEventSink{inner: inner}
}

func (s *serializedEventSink) OnEvent(ctx context.Context, event Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.OnEvent(ctx, event)
}

// crawlLanesConfig is the resolved input for one run's crawl.
type crawlLanesConfig struct {
	build       Config
	basePrefix  string
	matcher     *match.Matcher
	crawl       crawler.Config
	maxLanes    int
	journalPlan []string
	prefixes    []string
}

// crawlLanesResult is what publication needs from a completed crawl.
type crawlLanesResult struct {
	// journalPaths is in lane-ordinal order, so the sealed set is handed to
	// publication in the same deterministic order that journal identity resolves
	// cross-journal conflicts in.
	journalPaths    []string
	objectsObserved int64
	prefixesCrawled []string
}

// runCrawlLanes crawls the plan across one or more journal-writing lanes and
// seals them.
//
// Publication is all-or-nothing across lanes: a run publishes only when every
// admitted lane completed, observed no errors, and sealed. A partially sealed
// set would be a set of individually valid journals that together attest less
// than the run's coverage claims, which is exactly the authority gap lane-local
// provenance exists to close.
func runCrawlLanes(ctx context.Context, cfg crawlLanesConfig) (crawlLanesResult, error) {
	lanes := planLanes(cfg.build.CrawlPrefixes, cfg.crawl.Concurrency, cfg.maxLanes)
	if len(lanes) == 0 {
		// One lane attesting the whole run plan: the pre-lane form, and the shape
		// an unscoped or matcher-derived build always takes.
		lanes = []lane{{ordinal: 1, prefixes: cfg.prefixes}}
	}
	mode := laneCrawlPlanMode(len(lanes))
	shared := newSharedObservationSinks(cfg.build.ObservationSinks, len(lanes) > 1)
	// Lane crawlers report errors through one caller-supplied sink concurrently;
	// serialize that delivery rather than widening the sink's contract.
	events := newSerializedEventSink(cfg.build.Events)

	budget := crawler.NewRequestBudget(cfg.crawl.Concurrency, cfg.crawl.RateLimit)

	writers := make([]*laneObservationWriter, 0, len(lanes))
	journalPaths := make([]string, 0, len(lanes))
	crawlers := make([]*crawler.Crawler, 0, len(lanes))

	// Construct every lane before starting any of them: each reserves its share of
	// the shared budget at construction, and a lane that reserved only after its
	// peers began listing could find nothing left to reserve.
	for _, ln := range lanes {
		journalPlan := ln.prefixes
		if mode == "" {
			// The single-lane journal records the run's canonical plan, which for an
			// unscoped build is the synthesized base-prefix stamp rather than the
			// listing prefixes.
			journalPlan = cfg.journalPlan
		}
		path := filepath.Join(cfg.build.Paths.JournalDir, laneJournalFileName(ln.ordinal))
		w, err := newJournalWriter(journalWriterConfig{
			Path:          path,
			IndexSetID:    cfg.build.IndexSetID,
			RunID:         cfg.build.RunID,
			StartedAt:     cfg.build.RunStartedAt,
			BaseURI:       cfg.build.BaseURI,
			BasePrefix:    cfg.basePrefix,
			CrawlPrefixes: journalPlan,
			CrawlPlanMode: mode,
			LaneOrdinal:   ln.ordinal,
			Now:           cfg.build.Clock,
			Events:        events,
		})
		if err != nil {
			_ = closeLaneWriters(writers)
			return crawlLanesResult{}, err
		}
		laneWriter := newLaneObservationWriter(ln.ordinal, w, shared)
		writers = append(writers, laneWriter)
		journalPaths = append(journalPaths, path)

		c := crawler.New(cfg.build.Source.Provider, cfg.matcher, laneWriter, cfg.build.RunID, cfg.crawl).
			WithRequestBudget(budget).
			WithPrefixes(ln.prefixes)
		if cfg.build.Filter != nil {
			c = c.WithFilter(cfg.build.Filter)
		}
		crawlers = append(crawlers, c)
	}

	summaries := make([]*crawler.Summary, len(crawlers))
	errs := make([]error, len(crawlers))
	// The run's wall span, measured across every lane rather than taken from the
	// slowest lane's own elapsed time, which understates a staggered run.
	runStart := time.Now()
	var wg sync.WaitGroup
	for i := range crawlers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			summaries[i], errs[i] = crawlers[i].Run(ctx)
		}(i)
	}
	wg.Wait()
	runWall := time.Since(runStart)

	// A failure in any lane blocks publication for the whole run, not just its own
	// journal.
	var crawlErr error
	for i, err := range errs {
		if err != nil {
			crawlErr = fmt.Errorf("lane %d: %w", lanes[i].ordinal, err)
			break
		}
	}
	if crawlErr != nil {
		closeErr := closeLaneWriters(writers)
		sharedCloseErr := shared.close()
		_ = emitEvent(context.Background(), cfg.build.Events, Event{
			Type:    EventTypeCrawlError,
			RunID:   cfg.build.RunID,
			Message: crawlErr.Error(),
		})
		if closeErr == nil {
			closeErr = sharedCloseErr
		}
		if closeErr != nil {
			return crawlLanesResult{}, fmt.Errorf("crawl failed: %w; close journal: %v", crawlErr, closeErr)
		}
		return crawlLanesResult{}, fmt.Errorf("crawl failed: %w", crawlErr)
	}

	var observedErrors int64
	var objectsObserved int64
	for _, w := range writers {
		observedErrors += w.journal.ErrorCount()
		objectsObserved += w.journal.ObjectCount()
	}
	if observedErrors > 0 {
		closeErr := closeLaneWriters(writers)
		if sharedErr := shared.close(); closeErr == nil {
			closeErr = sharedErr
		}
		if closeErr != nil {
			return crawlLanesResult{}, closeErr
		}
		return crawlLanesResult{}, fmt.Errorf("crawl completed with %d errors; snapshot not published", observedErrors)
	}

	// Seal every lane, or none: a sealed journal is a completeness claim, so a
	// lane that could not seal must not leave its peers looking authoritative.
	for _, w := range writers {
		if err := w.journal.Seal(); err != nil {
			_ = closeLaneWriters(writers)
			_ = shared.close()
			return crawlLanesResult{}, err
		}
	}
	if err := closeLaneWriters(writers); err != nil {
		_ = shared.close()
		return crawlLanesResult{}, err
	}
	// The run's single terminal progress record, emitted only now that every lane
	// is genuinely terminal, so its counts are the run's finals.
	if err := shared.emitTerminalProgress(ctx); err != nil {
		_ = shared.close()
		return crawlLanesResult{}, err
	}
	if err := shared.emitRunSummary(ctx, runWall); err != nil {
		_ = shared.close()
		return crawlLanesResult{}, err
	}
	if err := shared.close(); err != nil {
		return crawlLanesResult{}, err
	}

	return crawlLanesResult{
		journalPaths:    journalPaths,
		objectsObserved: objectsObserved,
		prefixesCrawled: crawledPrefixes(lanes, summaries),
	}, nil
}

// crawledPrefixes reports what the run actually listed, in lane-ordinal order,
// preferring each lane's own reported prefixes over its assignment.
func crawledPrefixes(lanes []lane, summaries []*crawler.Summary) []string {
	out := make([]string, 0)
	for i, ln := range lanes {
		if i < len(summaries) && summaries[i] != nil && len(summaries[i].Prefixes) > 0 {
			out = append(out, summaries[i].Prefixes...)
			continue
		}
		out = append(out, ln.prefixes...)
	}
	return out
}

// closeLaneWriters closes each lane through its own writer, which closes that
// lane's journal and deliberately leaves the run's shared sinks alone.
func closeLaneWriters(writers []*laneObservationWriter) error {
	var firstErr error
	for _, w := range writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// laneJournalFileName is the journal file for a lane ordinal. Ordinal 1 yields
// the single-journal name a pre-lane build produces.
func laneJournalFileName(ordinal int) string {
	return fmt.Sprintf("shard-%04d.jsonl", ordinal)
}

// laneCrawlPlanMode is the provenance mode a lane seals.
//
// A single-lane run records no mode at all: its journal carries the whole run
// plan, which is the pre-lane contract, and the header stays byte-identical.
// Only a genuinely multi-lane run declares lane-local provenance.
func laneCrawlPlanMode(totalLanes int) string {
	if totalLanes < 2 {
		return ""
	}
	return indexsubstrate.CrawlPlanModeLaneLocal
}
