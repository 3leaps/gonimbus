package indexsubstrate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// OrderedRowSource is a pull stream of current-state rows already ordered by
// strict bytewise-increasing unique RelKey. Streaming segment write does not
// re-sort. Implementations include test adapters and CurrentStateSource.
//
// Ownership: WriteStreamingSegmentSet takes exclusive ownership of one terminal
// Close call on every exit path. Callers must not Close the same source after
// handing it to the writer.
type OrderedRowSource interface {
	Next(ctx context.Context) (CurrentObjectRow, error)
	Close() error
}

// StreamSegmentCategory classifies streaming segment-write failures.
type StreamSegmentCategory string

const (
	StreamSegmentInvalid  StreamSegmentCategory = "invalid"
	StreamSegmentOrder    StreamSegmentCategory = "order"
	StreamSegmentCanceled StreamSegmentCategory = "canceled"
	StreamSegmentWrite    StreamSegmentCategory = "write"
	StreamSegmentCleanup  StreamSegmentCategory = "cleanup"
	StreamSegmentSource   StreamSegmentCategory = "source"
)

// StreamSegmentError is the typed failure surface for streaming segment writes.
// Error() never renders RelKey values, configured Dir paths, or provider URIs.
type StreamSegmentError struct {
	Category StreamSegmentCategory
	Phase    string
	Message  string
	// RowIndex is 1-based for row-phase failures; zero when not applicable.
	RowIndex int
	Cause    error
}

func (e *StreamSegmentError) Error() string {
	if e == nil {
		return "streaming segment write error"
	}
	msg := fmt.Sprintf("streaming segment write %s phase=%s: %s", e.Category, e.Phase, e.Message)
	if e.RowIndex > 0 {
		msg += fmt.Sprintf(" row=%d", e.RowIndex)
	}
	if e.Cause != nil {
		msg += " cause_class=" + classifyStreamCause(e.Cause)
	}
	return msg
}

func (e *StreamSegmentError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func classifyStreamCause(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, os.ErrExist):
		return "exist"
	case errors.Is(err, os.ErrNotExist):
		return "not_exist"
	case errors.Is(err, os.ErrPermission):
		return "permission"
	case errors.Is(err, io.EOF):
		return "eof"
	default:
		// Avoid leaking cause text (may contain paths). Stable short class only.
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			return "path"
		}
		return "other"
	}
}

func streamSegErr(cat StreamSegmentCategory, phase, message string, rowIndex int, cause error) error {
	return &StreamSegmentError{
		Category: cat,
		Phase:    phase,
		Message:  message,
		RowIndex: rowIndex,
		Cause:    cause,
	}
}

// WriteStreamingSegmentSet drains an ordered row source into immutable parquet
// segments under config.Dir and returns the same in-memory InternalManifest
// contract as WriteSegmentSet.
//
// This writer is the production publish sink (drained by PublishSnapshot); it
// does not itself write manifest.json, complete, or latest — PublishSnapshot
// owns those. Continuous-parent loading and lineage emission remain a later
// activation.
//
// Contract (folded seat freezes):
//   - Writer owns one terminal src.Close on every exit path.
//   - Success requires full EOF and successful Close.
//   - Does not re-sort; refuses duplicate/out-of-order RelKey.
//   - Converts each row to segmentParquetRow at ingress (one open-segment buffer).
//   - On failure, removes only segments this call newly linked (created=true).
//   - Progress callbacks always report Total=0.
//   - Single-writer mutation authority over config.Dir is assumed for one call
//     (activation supplies write-lease exclusivity); concurrent writers are unsupported.
func WriteStreamingSegmentSet(ctx context.Context, config SegmentWriterConfig, src OrderedRowSource) (InternalManifest, error) {
	if src == nil {
		return InternalManifest{}, streamSegErr(StreamSegmentInvalid, "validate", "row source is required", 0, nil)
	}

	var (
		closeErr error
		closed   bool
	)
	closeSrc := func() {
		if closed {
			return
		}
		closed = true
		closeErr = src.Close()
	}

	config, runStartedAt, err := prepareSegmentWriter(config)
	if err != nil {
		closeSrc()
		return InternalManifest{}, joinStreamErrors(
			streamSegErr(StreamSegmentInvalid, "validate", "segment writer config or lineage refused", 0, err),
			wrapSourceCloseErr(closeErr),
		)
	}

	// Dir creation only after validation (lineage/config refuse without artifacts).
	if err := os.MkdirAll(config.Dir, 0o700); err != nil {
		closeSrc()
		return InternalManifest{}, joinStreamErrors(
			streamSegErr(StreamSegmentWrite, "mkdir", "create segment directory failed", 0, err),
			wrapSourceCloseErr(closeErr),
		)
	}

	manifest := newSegmentManifestSkeleton(config, runStartedAt)
	ops := resolveSegmentFileOps(config)
	var (
		ownedPaths  []string
		open        []segmentParquetRow
		counts      ManifestCounts
		etagSeen    = make(map[string]struct{})
		havePrevKey bool
		prevRelKey  string
		rowIndex    int
		rowsDone    int
	)

	fail := func(primary error) (InternalManifest, error) {
		closeSrc()
		cleanErr := removeOwnedSegmentPathsWith(ops, ownedPaths)
		return InternalManifest{}, joinStreamErrors(primary, wrapSourceCloseErr(closeErr), cleanErr)
	}

	sealOpen := func() error {
		if len(open) == 0 {
			return nil
		}
		desc, created, sealErr := writeParquetSegmentFileTracked(config, len(manifest.Segments), open)
		// Record ownership as soon as a new final is linked, even if post-link
		// cleanup/stat fails — outer fail path must roll back those dentries.
		if created && desc.Path != "" {
			ownedPaths = append(ownedPaths, filepath.Join(config.Dir, desc.Path))
		}
		if sealErr != nil {
			return streamSegErr(StreamSegmentWrite, "seal", "segment seal failed", rowIndex, sealErr)
		}
		manifest.Segments = append(manifest.Segments, desc)
		rowsDone += len(open)
		if config.OnSegmentProgress != nil {
			// Observational only. Streaming path always reports Total=0.
			// A later failure may roll back segments already reported.
			config.OnSegmentProgress(SegmentProgress{
				Segment:  len(manifest.Segments),
				Total:    0,
				Rows:     len(open),
				RowsDone: rowsDone,
			})
		}
		// Drop prior segment rows so pointer-backed fields are not retained.
		clear(open)
		open = open[:0]
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			cat := StreamSegmentCanceled
			if !errors.Is(err, context.Canceled) {
				cat = StreamSegmentCanceled
			}
			return fail(streamSegErr(cat, "drain", "context ended before source EOF", rowIndex, err))
		}

		row, nextErr := src.Next(ctx)
		if nextErr != nil {
			if errors.Is(nextErr, io.EOF) {
				break
			}
			if errors.Is(nextErr, context.Canceled) || errors.Is(nextErr, context.DeadlineExceeded) {
				return fail(streamSegErr(StreamSegmentCanceled, "next", "row source canceled", rowIndex, nextErr))
			}
			return fail(streamSegErr(StreamSegmentSource, "next", "row source failed", rowIndex, nextErr))
		}
		rowIndex++

		prepared, prepErr := prepareStreamingSegmentRow(row, config.IndexSetID)
		if prepErr != nil {
			return fail(streamSegErr(StreamSegmentInvalid, "row", prepErr.Error(), rowIndex, nil))
		}
		if havePrevKey {
			if prepared.RelKey == prevRelKey {
				return fail(streamSegErr(StreamSegmentOrder, "row", "duplicate rel_key", rowIndex, nil))
			}
			if prepared.RelKey < prevRelKey {
				return fail(streamSegErr(StreamSegmentOrder, "row", "rel_key order violation", rowIndex, nil))
			}
		}
		havePrevKey = true
		prevRelKey = prepared.RelKey

		pq := segmentParquetFromCurrentRow(prepared)
		open = append(open, pq)
		counts.Rows++
		if pq.DeletedAt != nil {
			counts.Tombstones++
		} else {
			counts.ActiveRows++
		}
		if pq.ETag != "" {
			if _, ok := etagSeen[pq.ETag]; !ok {
				etagSeen[pq.ETag] = struct{}{}
				counts.DistinctETags++
			}
		}

		if len(open) >= config.TargetRowsPerSegment {
			if err := sealOpen(); err != nil {
				return fail(err)
			}
		}
	}

	if err := sealOpen(); err != nil {
		return fail(err)
	}

	// Success requires source finalization (EOF alone is incomplete for 2.2 sources).
	closeSrc()
	if closeErr != nil {
		cleanErr := removeOwnedSegmentPathsWith(ops, ownedPaths)
		return InternalManifest{}, joinStreamErrors(
			wrapSourceCloseErr(closeErr),
			cleanErr,
		)
	}

	manifest.Counts = counts
	return manifest, nil
}

// wrapSourceCloseErr sanitizes raw source Close errors so joinStreamErrors never
// renders path-bearing or secret Close text via errors.Join.
func wrapSourceCloseErr(closeErr error) error {
	if closeErr == nil {
		return nil
	}
	return streamSegErr(StreamSegmentSource, "close", "row source close failed", 0, closeErr)
}

func prepareStreamingSegmentRow(row CurrentObjectRow, indexSetID string) (CurrentObjectRow, error) {
	row = normalizeCurrentObjectRow(row)
	if strings.TrimSpace(row.RelKey) == "" {
		return CurrentObjectRow{}, errors.New("rel_key is required")
	}
	if row.IndexSetID == "" {
		row.IndexSetID = indexSetID
	}
	if row.IndexSetID != indexSetID {
		return CurrentObjectRow{}, errors.New("index_set_id mismatch")
	}
	return row, nil
}

func removeOwnedSegmentPathsWith(ops segmentFileOps, paths []string) error {
	var first error
	for _, p := range paths {
		if p == "" {
			continue
		}
		// Never walk Dir or glob; only explicit owned finals from this call.
		if err := ops.remove(p); err != nil {
			if first == nil {
				first = streamSegErr(StreamSegmentCleanup, "cleanup", "remove owned segment failed", 0, err)
			}
		}
	}
	return first
}

func joinStreamErrors(errs ...error) error {
	var nonNil []error
	for _, err := range errs {
		if err != nil {
			nonNil = append(nonNil, err)
		}
	}
	switch len(nonNil) {
	case 0:
		return nil
	case 1:
		return nonNil[0]
	default:
		return errors.Join(nonNil...)
	}
}

// SliceOrderedRows adapts a pre-sorted unique slice as an OrderedRowSource.
// Order is not sorted here; the streaming writer refuses out-of-order keys.
type SliceOrderedRows struct {
	rows  []CurrentObjectRow
	i     int
	close func() error
}

// NewSliceOrderedRows returns a source over rows. Optional onClose is invoked
// once when Close is called (for spy/tests).
func NewSliceOrderedRows(rows []CurrentObjectRow) *SliceOrderedRows {
	cp := make([]CurrentObjectRow, len(rows))
	copy(cp, rows)
	return &SliceOrderedRows{rows: cp}
}

// WithCloseHook installs a Close side effect (tests).
func (s *SliceOrderedRows) WithCloseHook(fn func() error) *SliceOrderedRows {
	if s != nil {
		s.close = fn
	}
	return s
}

func (s *SliceOrderedRows) Next(ctx context.Context) (CurrentObjectRow, error) {
	if s == nil {
		return CurrentObjectRow{}, io.EOF
	}
	if err := ctx.Err(); err != nil {
		return CurrentObjectRow{}, err
	}
	if s.i >= len(s.rows) {
		return CurrentObjectRow{}, io.EOF
	}
	row := s.rows[s.i]
	s.i++
	return row, nil
}

func (s *SliceOrderedRows) Close() error {
	if s == nil {
		return nil
	}
	if s.close != nil {
		return s.close()
	}
	return nil
}
