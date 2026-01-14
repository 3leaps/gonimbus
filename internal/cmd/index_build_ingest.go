package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/output"
)

// Default batch sizes and timeouts for streaming ingestion.
const (
	// DefaultObjectBatchSize is the number of objects to batch before flushing.
	// Tuned for memory efficiency on large buckets (1M+ objects).
	DefaultObjectBatchSize = 5000

	// DefaultPrefixBatchSize is the number of prefixes to batch before flushing.
	// Prefixes are fewer than objects, so smaller batches are fine.
	DefaultPrefixBatchSize = 1000

	// DefaultFlushTimeout is the timeout for final flush operations in Close().
	// Prevents indefinite hangs if the database stalls.
	DefaultFlushTimeout = 2 * time.Minute
)

// indexIngestWriter implements output.Writer with streaming batched ingestion.
//
// Instead of capturing all records in memory (which would OOM on large buckets),
// this writer flushes batches to the database as records arrive. This keeps
// memory usage bounded regardless of bucket size.
type indexIngestWriter struct {
	db         *sql.DB
	indexSetID string
	run        *indexstore.IndexRun
	baseURI    string
	basePrefix string

	// Batch sizes
	objectBatchSize int
	prefixBatchSize int

	// Batches awaiting flush
	objectBatch []indexstore.ObjectRow
	prefixBatch []indexstore.PrefixStatRow

	// Counters for result
	objectsIngested  int64
	prefixesIngested int64
	errorCount       int64

	// Error tracking flags for partial run detection
	sawThrottle     bool
	sawAccessDenied bool
	sawTimeout      bool
	sawOtherError   bool

	// Guards for one-time event recording
	recordedTimeout        bool
	recordedScopeViolation bool

	// Scope violation tracking (guardrail)
	scopeViolationCount int64

	// Mutex for concurrent safety (crawler may call from multiple goroutines)
	mu sync.Mutex
}

// indexIngestWriterConfig configures the ingest writer.
type indexIngestWriterConfig struct {
	ObjectBatchSize int
	PrefixBatchSize int
}

// newIndexIngestWriter creates a streaming ingest writer.
func newIndexIngestWriter(
	db *sql.DB,
	indexSetID string,
	run *indexstore.IndexRun,
	baseURI string,
	basePrefix string,
	cfg indexIngestWriterConfig,
) *indexIngestWriter {
	objectBatchSize := cfg.ObjectBatchSize
	if objectBatchSize <= 0 {
		objectBatchSize = DefaultObjectBatchSize
	}
	prefixBatchSize := cfg.PrefixBatchSize
	if prefixBatchSize <= 0 {
		prefixBatchSize = DefaultPrefixBatchSize
	}

	// Ensure basePrefix ends with '/' when set.
	if basePrefix != "" && !strings.HasSuffix(basePrefix, "/") {
		basePrefix += "/"
	}

	return &indexIngestWriter{
		db:              db,
		indexSetID:      indexSetID,
		run:             run,
		baseURI:         baseURI,
		basePrefix:      basePrefix,
		objectBatchSize: objectBatchSize,
		prefixBatchSize: prefixBatchSize,
		objectBatch:     make([]indexstore.ObjectRow, 0, objectBatchSize),
		prefixBatch:     make([]indexstore.PrefixStatRow, 0, prefixBatchSize),
	}
}

// WriteObject converts and batches an object record, flushing when batch is full.
func (w *indexIngestWriter) WriteObject(ctx context.Context, obj *output.ObjectRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Guardrail: ensure we never ingest objects outside base_prefix.
	// This should never happen once matcher patterns are anchored correctly,
	// but a regression here would cause cross-prefix data isolation failures.
	if w.basePrefix != "" && !strings.HasPrefix(obj.Key, w.basePrefix) {
		w.scopeViolationCount++
		w.sawOtherError = true
		w.errorCount++
		if !w.recordedScopeViolation {
			w.recordedScopeViolation = true
			// Record only the first violation to avoid spamming the events table.
			if err := indexstore.RecordScopeViolation(ctx, w.db, w.run.RunID, obj.Key, w.basePrefix); err != nil {
				return err
			}
		}
		return nil
	}

	// Convert to ObjectRow immediately
	relKey := indexstore.DeriveRelKey(w.baseURI, obj.Key)
	row := indexstore.ObjectRow{
		IndexSetID:    w.indexSetID,
		RelKey:        relKey,
		SizeBytes:     obj.Size,
		LastModified:  &obj.LastModified,
		ETag:          obj.ETag,
		LastSeenRunID: w.run.RunID,
		LastSeenAt:    w.run.StartedAt,
	}

	w.objectBatch = append(w.objectBatch, row)

	// Flush if batch is full
	if len(w.objectBatch) >= w.objectBatchSize {
		return w.flushObjectsLocked(ctx)
	}
	return nil
}

// WritePrefix converts and batches a prefix record, flushing when batch is full.
func (w *indexIngestWriter) WritePrefix(ctx context.Context, prefix *output.PrefixRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	row := indexstore.PrefixStatRow{
		IndexSetID:      w.indexSetID,
		RunID:           w.run.RunID,
		Prefix:          prefix.Prefix,
		Depth:           prefix.Depth,
		ObjectsDirect:   prefix.ObjectsDirect,
		BytesDirect:     prefix.BytesDirect,
		CommonPrefixes:  prefix.CommonPrefixes,
		Truncated:       prefix.Truncated,
		TruncatedReason: prefix.TruncatedReason,
	}

	w.prefixBatch = append(w.prefixBatch, row)

	// Flush if batch is full
	if len(w.prefixBatch) >= w.prefixBatchSize {
		return w.flushPrefixesLocked(ctx)
	}
	return nil
}

// WriteError records error events and updates tracking flags.
//
// Error records are streamed to index_run_events immediately rather than batched,
// since they're typically few and need to be recorded even if the crawl fails.
func (w *indexIngestWriter) WriteError(ctx context.Context, errRec *output.ErrorRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.errorCount++

	// Record structured events based on error code
	switch errRec.Code {
	case output.ErrCodeThrottled:
		w.sawThrottle = true
		if err := indexstore.RecordThrottling(ctx, w.db, w.run.RunID, errRec.Prefix); err != nil {
			return err
		}

	case output.ErrCodeAccessDenied:
		w.sawAccessDenied = true
		if err := indexstore.RecordAccessDenied(ctx, w.db, w.run.RunID, errRec.Key, errRec.Prefix); err != nil {
			return err
		}

	case output.ErrCodeTimeout:
		w.sawTimeout = true
		// Record partial run event only once to avoid spamming
		if !w.recordedTimeout {
			w.recordedTimeout = true
			if err := indexstore.RecordPartialRun(ctx, w.db, w.run.RunID, "timeout"); err != nil {
				return err
			}
		}

	default:
		w.sawOtherError = true
	}

	return nil
}

// WriteProgress emits human-readable progress to stderr.
func (w *indexIngestWriter) WriteProgress(_ context.Context, prog *output.ProgressRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	prefix := prog.Prefix
	if prefix == "" {
		prefix = "(root)"
	}

	_, _ = fmt.Fprintf(os.Stderr, "progress: phase=%s prefix=%s objects_found=%d objects_matched=%d bytes=%d\n",
		prog.Phase,
		prefix,
		prog.ObjectsFound,
		prog.ObjectsMatched,
		prog.BytesTotal,
	)
	return nil
}

// WriteSummary is a no-op for index ingestion.
func (w *indexIngestWriter) WriteSummary(_ context.Context, _ *output.SummaryRecord) error {
	return nil
}

// WritePreflight is a no-op for index ingestion.
func (w *indexIngestWriter) WritePreflight(_ context.Context, _ *output.PreflightRecord) error {
	return nil
}

// WriteTransfer is a no-op for index ingestion.
func (w *indexIngestWriter) WriteTransfer(_ context.Context, _ *output.TransferRecord) error {
	return nil
}

// WriteSkip is a no-op for index ingestion.
func (w *indexIngestWriter) WriteSkip(_ context.Context, _ *output.SkipRecord) error {
	return nil
}

// Close flushes any remaining batches with a timeout.
//
// This must be called even if the crawl errors out to persist records
// that were already batched. Uses a timeout to prevent indefinite hangs
// if the database stalls.
func (w *indexIngestWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Use a bounded context for final flush - we want to persist even if
	// the crawl context was cancelled, but not hang indefinitely if DB stalls.
	ctx, cancel := context.WithTimeout(context.Background(), DefaultFlushTimeout)
	defer cancel()

	// Flush remaining objects first
	if err := w.flushObjectsLocked(ctx); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("final object flush timed out after %v: %w", DefaultFlushTimeout, err)
		}
		return err
	}

	// Then flush remaining prefixes
	if err := w.flushPrefixesLocked(ctx); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("final prefix flush timed out after %v: %w", DefaultFlushTimeout, err)
		}
		return err
	}

	return nil
}

// Result returns the ingestion result for status calculation.
//
// Call this after Close() to get final counts and determine run status.
func (w *indexIngestWriter) Result() *indexBuildResult {
	w.mu.Lock()
	defer w.mu.Unlock()

	result := &indexBuildResult{
		FinalStatus:      indexstore.RunStatusSuccess,
		ObjectsIngested:  w.objectsIngested,
		PrefixesIngested: w.prefixesIngested,
	}

	// If any errors were seen, mark as partial
	if w.sawThrottle || w.sawAccessDenied || w.sawTimeout || w.sawOtherError {
		result.FinalStatus = indexstore.RunStatusPartial
	}

	return result
}

// flushObjectsLocked flushes the object batch to the database.
// Caller must hold w.mu.
func (w *indexIngestWriter) flushObjectsLocked(ctx context.Context) error {
	if len(w.objectBatch) == 0 {
		return nil
	}

	if err := indexstore.BatchUpsertObjects(ctx, w.db, w.objectBatch); err != nil {
		return err
	}

	w.objectsIngested += int64(len(w.objectBatch))
	w.objectBatch = w.objectBatch[:0] // Clear batch, keep capacity
	return nil
}

// flushPrefixesLocked flushes the prefix batch to the database.
// Caller must hold w.mu.
func (w *indexIngestWriter) flushPrefixesLocked(ctx context.Context) error {
	if len(w.prefixBatch) == 0 {
		return nil
	}

	if err := indexstore.BatchInsertPrefixStats(ctx, w.db, w.prefixBatch); err != nil {
		return err
	}

	w.prefixesIngested += int64(len(w.prefixBatch))
	w.prefixBatch = w.prefixBatch[:0] // Clear batch, keep capacity
	return nil
}

// Compile-time check that indexIngestWriter implements output.Writer.
var _ output.Writer = (*indexIngestWriter)(nil)
