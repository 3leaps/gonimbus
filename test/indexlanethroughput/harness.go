// Package indexlanethroughput measures how a durable index build scales as its
// crawl is divided across journal-writing lanes.
//
// The measurement exists to answer one question with evidence rather than
// argument: does giving each lane its own journal actually raise ingest
// throughput, and does it leave a listing-bound crawl alone? Both halves matter.
// A writer-bound case that does not improve means the lanes are not worth their
// complexity; a listing-bound case that changes means lanes are taking credit for
// prefix fan-out that already existed.
//
// Figures produced here are machine-local and directional. They are not a
// product benchmark and must not be quoted as one.
package indexlanethroughput

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/crawler"
	"github.com/3leaps/gonimbus/pkg/indexbuild"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// Corpus describes the synthetic estate a run crawls.
type Corpus struct {
	Prefixes         int
	ObjectsPerPrefix int
	PageSize         int
	// PageLatency is the simulated per-LIST-page round trip. Zero produces the
	// writer-bound case; a large value produces the listing-bound control.
	PageLatency time.Duration
}

func (c Corpus) totalObjects() int { return c.Prefixes * c.ObjectsPerPrefix }

func (c Corpus) prefixList(basePrefix string) []string {
	out := make([]string, 0, c.Prefixes)
	for i := 0; i < c.Prefixes; i++ {
		out = append(out, fmt.Sprintf("%ssite%03d/", basePrefix, i))
	}
	return out
}

// Instrument records aggregate provider pressure and lane activity for one run.
//
// Every counter here is deliberately aggregate rather than per-lane: the
// ceilings a shared request budget promises are run-global, and a per-lane
// reading would look correct even when N lanes together breach them.
type Instrument struct {
	mu sync.Mutex

	activeLists int
	peakLists   int
	listStarts  int
	// runStart is stamped before any request is issued. The rate window is
	// measured from here rather than from the first observed listing, because a
	// listing is timestamped only after the limiter admits it: scheduler delay
	// between admission and provider entry would otherwise shorten the measured
	// interval and make a correct limiter look like a breach.
	runStart     time.Time
	firstStart   time.Time
	lastStart    time.Time
	lastDelivery time.Time

	// laneWindow is each lane's first and last observation delivery. Overlapping
	// windows are what distinguish lanes doing real concurrent work from lanes
	// that merely ran in sequence and happened to finish.
	laneWindow map[int][2]time.Time
}

func newInstrument() *Instrument {
	return &Instrument{runStart: time.Now(), laneWindow: make(map[int][2]time.Time)}
}

func (in *Instrument) enterList() {
	in.mu.Lock()
	defer in.mu.Unlock()
	now := time.Now()
	in.activeLists++
	in.listStarts++
	if in.activeLists > in.peakLists {
		in.peakLists = in.activeLists
	}
	if in.firstStart.IsZero() {
		in.firstStart = now
	}
	in.lastStart = now
}

func (in *Instrument) exitList() {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.activeLists--
}

func (in *Instrument) recordDelivery(lane int) {
	in.mu.Lock()
	defer in.mu.Unlock()
	now := time.Now()
	in.lastDelivery = now
	win, ok := in.laneWindow[lane]
	if !ok {
		in.laneWindow[lane] = [2]time.Time{now, now}
		return
	}
	win[1] = now
	in.laneWindow[lane] = win
}

// PeakConcurrentLanes reports the largest number of lanes whose delivery windows
// overlapped at one instant.
//
// Headline throughput alone cannot distinguish a genuinely parallel run from a
// scheduler that happened to interleave lanes; this can. A value of 1 means the
// lanes ran one after another whatever the wall clock says.
func (in *Instrument) PeakConcurrentLanes() int {
	in.mu.Lock()
	defer in.mu.Unlock()
	type edge struct {
		at    time.Time
		delta int
	}
	edges := make([]edge, 0, len(in.laneWindow)*2)
	for _, win := range in.laneWindow {
		edges = append(edges, edge{win[0], 1}, edge{win[1], -1})
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].at.Equal(edges[j].at) {
			// Close before open at the same instant, so touching windows are not
			// counted as overlapping.
			return edges[i].delta < edges[j].delta
		}
		return edges[i].at.Before(edges[j].at)
	})
	active, peak := 0, 0
	for _, e := range edges {
		active += e.delta
		if active > peak {
			peak = active
		}
	}
	return peak
}

// PeakListConcurrency is the highest number of simultaneous provider listings
// observed across every lane.
func (in *Instrument) PeakListConcurrency() int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.peakLists
}

// ListStarts is the total number of provider listing requests issued.
func (in *Instrument) ListStarts() int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.listStarts
}

// RequestWindow is the interval a sustained-rate ceiling applies to: from before
// the first request could have been admitted, to the last one observed.
//
// It starts at the run rather than at the first observed listing on purpose. A
// listing is timestamped on provider entry, which is after the limiter admitted
// it, so scheduler delay after that first admission would shrink the window and
// report a breach the limiter did not commit.
func (in *Instrument) RequestWindow() time.Duration {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.lastStart.IsZero() {
		return 0
	}
	return in.lastStart.Sub(in.runStart)
}

// CrawlWall is the crawl phase measured directly by this harness: from the first
// provider listing to the last observation delivered. It does not depend on the
// engine reporting its own duration.
func (in *Instrument) CrawlWall() time.Duration {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.firstStart.IsZero() || in.lastDelivery.IsZero() {
		return 0
	}
	return in.lastDelivery.Sub(in.firstStart)
}

// syntheticProvider serves a deterministic paginated estate with a configurable
// per-page latency, so the same corpus can be made writer-bound or listing-bound.
type syntheticProvider struct {
	corpus     Corpus
	instrument *Instrument
}

func (p *syntheticProvider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	p.instrument.enterList()
	defer p.instrument.exitList()

	if p.corpus.PageLatency > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(p.corpus.PageLatency):
		}
	}

	start := 0
	if opts.ContinuationToken != "" {
		if _, err := fmt.Sscanf(opts.ContinuationToken, "%d", &start); err != nil {
			return nil, fmt.Errorf("bad continuation token %q", opts.ContinuationToken)
		}
	}
	end := start + p.corpus.PageSize
	if end > p.corpus.ObjectsPerPrefix {
		end = p.corpus.ObjectsPerPrefix
	}
	objs := make([]provider.ObjectSummary, 0, end-start)
	modified := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	for i := start; i < end; i++ {
		objs = append(objs, provider.ObjectSummary{
			Key:          fmt.Sprintf("%sobj%06d.xml", opts.Prefix, i),
			Size:         int64(1024 + i),
			ETag:         fmt.Sprintf(`"%x"`, i),
			LastModified: modified,
			StorageClass: "STANDARD",
		})
	}
	truncated := end < p.corpus.ObjectsPerPrefix
	token := ""
	if truncated {
		token = fmt.Sprintf("%d", end)
	}
	return &provider.ListResult{Objects: objs, IsTruncated: truncated, ContinuationToken: token}, nil
}

func (p *syntheticProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}
func (p *syntheticProvider) Close() error { return nil }

// laneAttributingSink records which lane delivered each observation.
//
// It infers the lane from the object's own prefix rather than from engine
// internals: lanes own disjoint plan entries, so a key's prefix identifies its
// lane exactly. That keeps the harness on the public API and keeps the engine
// free of instrumentation that exists only for measurement.
type laneAttributingSink struct {
	instrument *Instrument
	laneOf     map[string]int
}

func (s *laneAttributingSink) WriteObject(_ context.Context, obj *output.ObjectRecord) error {
	if obj == nil {
		return nil
	}
	if idx := strings.LastIndex(obj.Key, "/"); idx >= 0 {
		if lane, ok := s.laneOf[obj.Key[:idx+1]]; ok {
			s.instrument.recordDelivery(lane)
		}
	}
	return nil
}

// WriteSummary is discarded. The crawl interval this harness reports is measured
// directly by the Instrument, so retaining the engine's own summary here would
// keep a second, unused source of the same figure.
func (s *laneAttributingSink) WriteSummary(context.Context, *output.SummaryRecord) error {
	return nil
}

func (s *laneAttributingSink) WriteError(context.Context, *output.ErrorRecord) error { return nil }
func (s *laneAttributingSink) WriteProgress(context.Context, *output.ProgressRecord) error {
	return nil
}
func (s *laneAttributingSink) WritePrefix(context.Context, *output.PrefixRecord) error { return nil }
func (s *laneAttributingSink) WritePreflight(context.Context, *output.PreflightRecord) error {
	return nil
}
func (s *laneAttributingSink) WriteTransfer(context.Context, *output.TransferRecord) error {
	return nil
}
func (s *laneAttributingSink) WriteSkip(context.Context, *output.SkipRecord) error { return nil }
func (s *laneAttributingSink) Close() error                                        { return nil }

// RunSpec is one cell of the sweep.
type RunSpec struct {
	Corpus      Corpus
	Lanes       int
	Concurrency int
	RateLimit   float64
	Root        string
}

// RunResult is what one cell reports.
type RunResult struct {
	Spec                RunSpec
	JournalsSealed      int
	ObjectsObserved     int64
	CrawlDuration       time.Duration
	TotalDuration       time.Duration
	PeakListConcurrency int
	PeakConcurrentLanes int
	ListStarts          int
	RequestWindow       time.Duration
}

// CrawlObjectsPerSec is the throughput the lane feature is meant to move. It is
// deliberately the crawl phase alone: publication runs after every lane is
// terminal and is unaffected by how many lanes produced the journals.
//
// The duration is this harness's own measurement of the crawl interval, not a
// figure the engine reported about itself.
func (r RunResult) CrawlObjectsPerSec() float64 {
	if r.CrawlDuration <= 0 {
		return 0
	}
	return float64(r.ObjectsObserved) / r.CrawlDuration.Seconds()
}

// Run executes one build and returns its measurements.
func Run(ctx context.Context, spec RunSpec) (RunResult, error) {
	const baseURI = "s3://bench/data/"
	const basePrefix = "data/"

	instrument := newInstrument()
	prefixes := spec.Corpus.prefixList(basePrefix)

	// Mirror the engine's own assignment so delivered objects can be attributed
	// to a lane: sorted plan, round-robin across the effective lane count.
	effective := spec.Lanes
	if effective > len(prefixes) {
		effective = len(prefixes)
	}
	if spec.Concurrency > 0 && effective > spec.Concurrency {
		effective = spec.Concurrency
	}
	if effective < 1 {
		effective = 1
	}
	assignment := append([]string(nil), prefixes...)
	sort.Strings(assignment)
	laneOf := make(map[string]int, len(assignment))
	for i, p := range assignment {
		laneOf[p] = i%effective + 1
	}

	sink := &laneAttributingSink{instrument: instrument, laneOf: laneOf}
	coverage := make([]indexbuild.CoverageAttestation, 0, len(prefixes))
	for _, p := range prefixes {
		coverage = append(coverage, indexbuild.CoverageAttestation{
			Scope:    &indexbuild.Scope{Prefix: p},
			Basis:    indexbuild.CoverageBasisConfirmed,
			Complete: true,
		})
	}

	runID := fmt.Sprintf("run_lanes%02d", spec.Lanes)
	root := filepath.Join(spec.Root, fmt.Sprintf("lanes%02d", spec.Lanes))
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)

	cfg := indexbuild.Config{
		IndexSetID: "idx_lane_bench",
		RunID:      runID,
		BaseURI:    baseURI,
		Source: indexbuild.Source{
			Provider:     &syntheticProvider{corpus: spec.Corpus, instrument: instrument},
			ProviderName: "s3",
		},
		Match: indexbuild.MatchConfig{Includes: []string{"**"}},
		Crawl: crawler.Config{
			Concurrency:   spec.Concurrency,
			RateLimit:     spec.RateLimit,
			ChannelBuffer: 1000,
			ProgressEvery: 100000,
		},
		MaxJournalLanes:  spec.Lanes,
		CrawlPrefixes:    prefixes,
		ObservationSinks: []output.Writer{sink},
		Paths: indexbuild.PathConfig{
			JournalDir:   filepath.Join(root, "journals"),
			SegmentDir:   filepath.Join(root, "segments"),
			ManifestPath: filepath.Join(root, "manifest.json"),
			CompletePath: filepath.Join(root, "complete.json"),
			LatestPath:   filepath.Join(root, "latest.json"),
		},
		Coverage:             coverage,
		RunStartedAt:         now,
		CreatedAt:            now,
		Clock:                func() time.Time { return now },
		TargetRowsPerSegment: 100000,
	}

	start := time.Now()
	summary, err := indexbuild.NewRunner(cfg).Build(ctx)
	total := time.Since(start)
	if err != nil {
		return RunResult{}, fmt.Errorf("lanes=%d: %w", spec.Lanes, err)
	}

	sealed, err := countSealedJournals(cfg.Paths.JournalDir)
	if err != nil {
		return RunResult{}, fmt.Errorf("lanes=%d: %w", spec.Lanes, err)
	}

	return RunResult{
		Spec:                spec,
		JournalsSealed:      sealed,
		ObjectsObserved:     summary.ObjectsObserved,
		CrawlDuration:       instrument.CrawlWall(),
		TotalDuration:       total,
		PeakListConcurrency: instrument.PeakListConcurrency(),
		PeakConcurrentLanes: instrument.PeakConcurrentLanes(),
		ListStarts:          instrument.ListStarts(),
		RequestWindow:       instrument.RequestWindow(),
	}, nil
}

// countSealedJournals reports how many sealed, integrity-valid journals a run
// actually produced.
//
// It enumerates and validates artifacts rather than restating the lane count the
// harness itself computed. Reporting the intended topology back would make the
// journal claim unfalsifiable — the number would be right even if the run
// produced nothing.
func countSealedJournals(journalDir string) (int, error) {
	entries, err := os.ReadDir(journalDir)
	if err != nil {
		return 0, fmt.Errorf("read journal dir: %w", err)
	}
	sealed := 0
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".jsonl" {
			continue
		}
		if _, err := indexsubstrate.ValidateJournalBounded(
			filepath.Join(journalDir, e.Name()),
			indexsubstrate.DefaultSpillMergeBudget().MaxRecordBytes,
		); err != nil {
			return 0, fmt.Errorf("journal %s is not sealed and valid: %w", e.Name(), err)
		}
		sealed++
	}
	return sealed, nil
}
