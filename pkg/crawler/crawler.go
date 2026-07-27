// Package crawler implements a bounded streaming pipeline for crawling
// cloud object storage.
//
// The crawler coordinates three stages:
//   - Lister: Fetches object listings from provider (parallelized by prefix)
//   - Matcher: Filters objects by glob patterns
//   - Writer: Emits matched objects as JSONL records
//
// Bounded channels between stages provide backpressure to prevent memory
// exhaustion on large buckets.
package crawler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// Config configures crawler behavior.
type Config struct {
	// Concurrency is the number of parallel list operations.
	// Each prefix from Matcher.Prefixes() can be listed concurrently.
	// Default: 4
	Concurrency int

	// ChannelBuffer is the size of bounded channels between pipeline stages.
	// Larger buffers reduce blocking but increase memory usage.
	// Default: 1000
	ChannelBuffer int

	// RateLimit is the maximum requests per second to the provider.
	// Zero means unlimited (provider handles its own throttling).
	// Default: 0
	RateLimit float64

	// ProgressEvery controls how often progress records are emitted.
	// A progress record is written every N matched objects.
	// Default: 1000
	ProgressEvery int
}

// DefaultConfig returns the default crawler configuration.
func DefaultConfig() Config {
	return Config{
		Concurrency:   4,
		ChannelBuffer: 1000,
		RateLimit:     0,
		ProgressEvery: 1000,
	}
}

// Summary contains aggregate statistics from a completed crawl.
type Summary struct {
	// ObjectsListed is the total number of objects seen from the provider.
	ObjectsListed int64

	// ObjectsMatched is the number of objects that matched the patterns.
	ObjectsMatched int64

	// BytesTotal is the cumulative size of matched objects in bytes.
	BytesTotal int64

	// Duration is the total time spent crawling.
	Duration time.Duration

	// Errors is the count of non-fatal errors encountered.
	Errors int64

	// Prefixes lists the prefixes that were crawled.
	Prefixes []string
}

// Crawler executes a crawl job against a cloud storage provider.
//
// Crawler is safe for single use only. Create a new Crawler for each job.
type Crawler struct {
	provider provider.Provider
	matcher  *match.Matcher
	filter   *match.CompositeFilter // Optional metadata filter
	writer   output.Writer
	config   Config
	jobID    string

	prefixes []string

	// budget is this crawler's handle on the request budget bounding its listing
	// concurrency and request rate. It is leased from an injected RequestBudget
	// when one is supplied, and otherwise from a private budget built from Config
	// at Run, which keeps both ceilings per-crawler exactly as they are today.
	budget *budgetLease

	// Atomic counters for stats
	objectsListed   atomic.Int64
	objectsMatched  atomic.Int64
	objectsFiltered atomic.Int64 // Objects that passed glob but failed filter
	bytesTotal      atomic.Int64
	errorCount      atomic.Int64
}

// New creates a new crawler.
//
// Parameters:
//   - p: Provider for listing objects
//   - m: Matcher for filtering objects by pattern
//   - w: Writer for JSONL output
//   - jobID: Correlation ID for this crawl job
//   - cfg: Crawler configuration (use DefaultConfig() as base)
//
// Use WithFilter() to add metadata filters after creation.
func New(p provider.Provider, m *match.Matcher, w output.Writer, jobID string, cfg Config) *Crawler {
	// Apply defaults for zero values
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultConfig().Concurrency
	}
	if cfg.ChannelBuffer <= 0 {
		cfg.ChannelBuffer = DefaultConfig().ChannelBuffer
	}
	if cfg.ProgressEvery <= 0 {
		cfg.ProgressEvery = DefaultConfig().ProgressEvery
	}

	c := &Crawler{
		provider: p,
		matcher:  m,
		writer:   w,
		config:   cfg,
		jobID:    jobID,
	}

	return c
}

// WithRequestBudget shares a run-global request budget with this crawler,
// replacing its own Config.Concurrency and Config.RateLimit ceilings.
//
// Use it when several crawlers make up one logical run: without a shared budget
// each crawler enforces the configured ceilings independently, so N crawlers
// issue N times the requested listing concurrency and N times the requested
// request rate against the provider. Returns the crawler for method chaining.
//
// This crawler's share of the budget is reserved here, at construction, rather
// than at Run. A permit is held for a whole paginated listing, so a crawler that
// waited until Run to reserve could find its peers already holding every permit.
// Construct every crawler in the run before starting any of them.
//
// Calling this more than once is safe: the crawler's previous share is returned
// before a new one is taken, so rebinding cannot strand a permit that nothing
// can reach again.
func (c *Crawler) WithRequestBudget(b *RequestBudget) *Crawler {
	if b == nil {
		return c
	}
	// Retire before leasing, not after: taking the new reservation first would
	// have this crawler holding two permits at once and could block on a budget
	// its own outstanding lease is the reason for exhausting.
	if c.budget != nil {
		c.budget.retire()
	}
	c.budget = b.lease()
	return c
}

// WithFilter sets an optional metadata filter for the crawler.
// Filters are applied after glob pattern matching with AND semantics.
// Returns the crawler for method chaining.
func (c *Crawler) WithFilter(f *match.CompositeFilter) *Crawler {
	c.filter = f
	return c
}

// WithPrefixes overrides the prefixes to crawl.
//
// When set, the crawler uses these prefixes instead of matcher-derived prefixes.
func (c *Crawler) WithPrefixes(prefixes []string) *Crawler {
	c.prefixes = prefixes
	return c
}

// Run executes the crawl and returns summary statistics.
//
// Run blocks until the crawl completes, is cancelled via context, or
// encounters a fatal error. Non-fatal errors (e.g., permission denied
// on a single object) are written as error records and counted in the
// summary.
//
// The crawl can be cancelled by cancelling the context. Cancellation
// is graceful: in-flight operations complete, channels are drained,
// and a partial summary is returned.
func (c *Crawler) Run(ctx context.Context) (*Summary, error) {
	startTime := time.Now()

	// No injected budget means this crawler is the whole run, so it gets a
	// private one carrying its own configured ceilings — the pre-budget behavior.
	if c.budget == nil {
		c.budget = NewRequestBudget(c.config.Concurrency, c.config.RateLimit).lease()
	}
	// Retire on every return path, not only the one through the listing stage.
	// A refusal before listing begins — an initial progress write that fails, say
	// — would otherwise return while still holding a reservation, permanently
	// reducing the capacity of a budget its peers are still drawing on.
	// Retirement is idempotent, so this composes with the earlier retirement the
	// listing stage performs as soon as a crawler runs out of work.
	defer c.budget.retire()

	// Get prefixes to crawl
	prefixes := c.prefixes
	if prefixes == nil {
		prefixes = c.matcher.Prefixes()
	}
	if len(prefixes) == 0 {
		// No prefixes means match everything - use empty prefix
		prefixes = []string{""}
	}

	// Write initial progress
	if err := c.writeProgress(ctx, output.PhaseStarting, ""); err != nil {
		return nil, err
	}

	// Run the pipeline
	if err := c.runPipeline(ctx, prefixes); err != nil {
		// Check if it's a context error (cancellation/timeout)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// Return partial summary on cancellation
			return c.buildSummary(prefixes, time.Since(startTime)), err
		}
		return nil, err
	}

	summary := c.buildSummary(prefixes, time.Since(startTime))

	// Write final summary record
	if err := c.writeSummary(ctx, summary); err != nil {
		return summary, err
	}

	return summary, nil
}

// buildSummary creates a Summary from the atomic counters.
func (c *Crawler) buildSummary(prefixes []string, duration time.Duration) *Summary {
	return &Summary{
		ObjectsListed:  c.objectsListed.Load(),
		ObjectsMatched: c.objectsMatched.Load(),
		BytesTotal:     c.bytesTotal.Load(),
		Duration:       duration,
		Errors:         c.errorCount.Load(),
		Prefixes:       prefixes,
	}
}

// writeProgress emits a progress record.
func (c *Crawler) writeProgress(ctx context.Context, phase, prefix string) error {
	prog := &output.ProgressRecord{
		Phase:          phase,
		ObjectsFound:   c.objectsListed.Load(),
		ObjectsMatched: c.objectsMatched.Load(),
		BytesTotal:     c.bytesTotal.Load(),
		Prefix:         prefix,
	}
	return c.writer.WriteProgress(ctx, prog)
}

// writeSummary emits a summary record.
func (c *Crawler) writeSummary(ctx context.Context, summary *Summary) error {
	sum := &output.SummaryRecord{
		ObjectsFound:   summary.ObjectsListed,
		ObjectsMatched: summary.ObjectsMatched,
		BytesTotal:     summary.BytesTotal,
		Duration:       summary.Duration,
		DurationHuman:  summary.Duration.Round(time.Millisecond).String(),
		Errors:         summary.Errors,
		Prefixes:       summary.Prefixes,
	}
	return c.writer.WriteSummary(ctx, sum)
}

// writeError emits an error record and increments the error counter.
func (c *Crawler) writeError(ctx context.Context, code, message, prefix string) {
	c.errorCount.Add(1)

	errRec := &output.ErrorRecord{
		Code:    code,
		Message: message,
		Prefix:  prefix,
	}

	// Best effort - don't fail the crawl if we can't write the error
	_ = c.writer.WriteError(ctx, errRec)
}

// waitForRateLimit blocks until the request budget allows a request to start.
// Returns immediately if rate limiting is disabled.
func (c *Crawler) waitForRateLimit(ctx context.Context) error {
	return c.budget.waitForRequest(ctx)
}

// objectItem represents an object flowing through the pipeline.
type objectItem struct {
	summary provider.ObjectSummary
	prefix  string // The prefix this object was listed under
}

// runPipeline orchestrates the lister → matcher → writer pipeline.
func (c *Crawler) runPipeline(ctx context.Context, prefixes []string) error {
	// Create a cancellable context for the pipeline
	pipeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Channels between stages
	listCh := make(chan objectItem, c.config.ChannelBuffer)
	matchCh := make(chan objectItem, c.config.ChannelBuffer)

	// Error channel for fatal errors from any stage
	errCh := make(chan error, 1)

	var wg sync.WaitGroup

	// Start lister goroutines (one per prefix, limited by concurrency)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(listCh)
		if err := c.runListers(pipeCtx, prefixes, listCh); err != nil {
			select {
			case errCh <- err:
			default:
			}
			cancel()
		}
	}()

	// Start matcher goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(matchCh)
		c.runMatcher(pipeCtx, listCh, matchCh)
	}()

	// Start writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := c.runWriter(pipeCtx, matchCh); err != nil {
			select {
			case errCh <- err:
			default:
			}
			cancel()
		}
	}()

	// Wait for all goroutines to complete
	wg.Wait()

	// Check for fatal errors
	select {
	case err := <-errCh:
		return err
	default:
		return ctx.Err()
	}
}

// runListers runs listing operations for all prefixes, bounded by the request
// budget. The budget may be shared with other crawlers, in which case the
// concurrency ceiling is the whole run's rather than this crawler's.
//
// The budget lease is retired once every prefix here has been listed, handing
// this crawler's reserved permit back so crawlers still working can draw on the
// full budget instead of only the permits nobody reserved.
func (c *Crawler) runListers(ctx context.Context, prefixes []string, out chan<- objectItem) error {
	defer c.budget.retire()

	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once

	for _, prefix := range prefixes {
		release, err := c.budget.acquireListSlot(ctx)
		if err != nil {
			// Context ended before a slot was available; no slot was taken.
			break
		}

		wg.Add(1)
		go func(p string, release func()) {
			defer wg.Done()
			defer release()

			if err := c.listPrefix(ctx, p, out); err != nil {
				// Capture first error
				errOnce.Do(func() {
					firstErr = err
				})
			}
		}(prefix, release)
	}

	wg.Wait()
	return firstErr
}

// listPrefix lists all objects with the given prefix and sends them to the channel.
func (c *Crawler) listPrefix(ctx context.Context, prefix string, out chan<- objectItem) error {
	var continuationToken string

	for {
		// Check for cancellation
		if err := ctx.Err(); err != nil {
			return err
		}

		// Wait for rate limiter
		if err := c.waitForRateLimit(ctx); err != nil {
			return err
		}

		// List a page of objects
		result, err := c.provider.List(ctx, provider.ListOptions{
			Prefix:            prefix,
			ContinuationToken: continuationToken,
		})
		if err != nil {
			// Classify the error
			if provider.IsAccessDenied(err) {
				c.writeError(ctx, output.ErrCodeAccessDenied, err.Error(), prefix)
				return nil // Non-fatal: skip this prefix
			}
			if provider.IsNotFound(err) {
				c.writeError(ctx, output.ErrCodeNotFound, err.Error(), prefix)
				return nil // Non-fatal: skip this prefix
			}
			if provider.IsThrottled(err) {
				c.writeError(ctx, output.ErrCodeThrottled, err.Error(), prefix)
				// Could implement retry here, but for now treat as non-fatal
				return nil
			}
			if provider.IsProviderUnavailable(err) {
				c.writeError(ctx, output.ErrCodeProviderUnavailable, err.Error(), prefix)
				// Treat as non-fatal: mark run partial and continue other prefixes.
				return nil
			}
			// Fatal error
			return err
		}

		// Send objects to the matcher channel
		for _, obj := range result.Objects {
			c.objectsListed.Add(1)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- objectItem{summary: obj, prefix: prefix}:
			}
		}

		// Check for more pages
		if !result.IsTruncated || result.ContinuationToken == "" {
			break
		}
		continuationToken = result.ContinuationToken
	}

	return nil
}

// runMatcher filters objects by glob patterns and optional metadata filters,
// then forwards matches to the writer channel.
func (c *Crawler) runMatcher(ctx context.Context, in <-chan objectItem, out chan<- objectItem) {
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-in:
			if !ok {
				return // Input channel closed
			}

			// Apply glob pattern matching first
			if !c.matcher.Match(item.summary.Key) {
				continue
			}

			// Apply optional metadata filters (size, date, regex)
			if c.filter != nil && !c.filter.Match(&item.summary) {
				c.objectsFiltered.Add(1)
				continue
			}

			c.objectsMatched.Add(1)
			c.bytesTotal.Add(item.summary.Size)

			select {
			case <-ctx.Done():
				return
			case out <- item:
			}
		}
	}
}

// runWriter writes matched objects as JSONL records.
func (c *Crawler) runWriter(ctx context.Context, in <-chan objectItem) error {
	var matchCount int64
	var lastProgressPrefix string

	for {
		select {
		case <-ctx.Done():
			// Write final progress before exiting
			_ = c.writeProgress(ctx, output.PhaseComplete, lastProgressPrefix)
			return ctx.Err()
		case item, ok := <-in:
			if !ok {
				// Input channel closed - write final progress
				return c.writeProgress(ctx, output.PhaseComplete, lastProgressPrefix)
			}

			// Write object record
			obj := &output.ObjectRecord{
				Key:          item.summary.Key,
				Size:         item.summary.Size,
				ETag:         item.summary.ETag,
				LastModified: item.summary.LastModified,
				StorageClass: item.summary.StorageClass,
			}
			if err := c.writer.WriteObject(ctx, obj); err != nil {
				return err
			}

			matchCount++
			lastProgressPrefix = item.prefix

			// Emit progress periodically
			if c.config.ProgressEvery > 0 && matchCount%int64(c.config.ProgressEvery) == 0 {
				if err := c.writeProgress(ctx, output.PhaseListing, item.prefix); err != nil {
					return err
				}
			}
		}
	}
}
