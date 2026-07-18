package indexsubstrate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"
)

// ParentReaderCategory classifies a PublishedParentRowSource failure. Rendered
// errors carry only category/phase/message — never a segment path, directory,
// or object key.
type ParentReaderCategory string

const (
	ParentReaderInvalidConfig ParentReaderCategory = "invalid_config"
	ParentReaderOpen          ParentReaderCategory = "open"
	ParentReaderDigest        ParentReaderCategory = "digest"
	ParentReaderParse         ParentReaderCategory = "parse"
	ParentReaderOrder         ParentReaderCategory = "order"
	ParentReaderCount         ParentReaderCategory = "count"
	ParentReaderIO            ParentReaderCategory = "io"
	ParentReaderCanceled      ParentReaderCategory = "canceled"
	ParentReaderClosed        ParentReaderCategory = "closed"
)

// ParentReaderError is the sanitized failure surface of a parent row source. It
// never renders path or key material; Unwrap preserves cancellation
// classification for errors.Is.
type ParentReaderError struct {
	Category ParentReaderCategory
	Phase    string
	Message  string
	cause    error
}

func (e *ParentReaderError) Error() string {
	return fmt.Sprintf("parent row source %s (%s): %s", e.Category, e.Phase, e.Message)
}

// Unwrap exposes only classification-bearing causes (e.g. context.Canceled). A
// raw filesystem cause that could render a path is intentionally not chained.
func (e *ParentReaderError) Unwrap() error { return e.cause }

func parentErr(cat ParentReaderCategory, phase, message string, cause error) error {
	return &ParentReaderError{Category: cat, Phase: phase, Message: message, cause: cause}
}

// PublishedParentRowSource is a bounded pull ParentRowSource over a verified
// PublishedSnapshot's segments. It walks segments in manifest order, holding at
// most one open segment descriptor at a time with a small fixed row buffer, and
// verifies each segment's digest on the same open file descriptor before parsing
// it (never re-opening by path — no digest/parse ABA). It enforces strict
// bytewise-increasing unique RelKey across the whole stream plus declared
// per-segment and total row counts, and never materializes the full
// current-state row set.
//
// It satisfies ParentRowSource. Parent selection and digest binding remain the
// caller's responsibility; this source only streams an already-verified parent.
type PublishedParentRowSource struct {
	segmentDir    string
	segments      []SegmentDescriptor
	totalExpected int

	segIdx  int
	cur     *parentSegmentCursor
	lastKey string
	hasLast bool
	total   int

	atEOF   bool
	closed  bool
	termErr error
}

type parentSegmentCursor struct {
	file     *os.File
	root     *os.Root
	reader   *parquet.GenericReader[segmentParquetRow]
	buf      []segmentParquetRow
	bufN     int
	bufPos   int
	rowsRead int
	expected int
}

// NewPublishedParentRowSource builds a bounded pull source over the snapshot's
// segments in manifest order. The snapshot must already be trust-verified
// (latest -> complete -> digest-checked manifest); this source additionally
// verifies each segment file digest as it is streamed.
func NewPublishedParentRowSource(snap PublishedSnapshot) *PublishedParentRowSource {
	segs := make([]SegmentDescriptor, len(snap.Manifest.Segments))
	copy(segs, snap.Manifest.Segments)
	return &PublishedParentRowSource{
		segmentDir:    snap.SegmentDir,
		segments:      segs,
		totalExpected: snap.Manifest.Counts.Rows,
	}
}

// Next returns the next parent row in strict bytewise RelKey order, or io.EOF
// after the last row. Pointer field ownership transfers to the caller.
func (s *PublishedParentRowSource) Next(ctx context.Context) (CurrentObjectRow, error) {
	if s == nil {
		return CurrentObjectRow{}, parentErr(ParentReaderInvalidConfig, "next", "nil source", nil)
	}
	if s.closed {
		return CurrentObjectRow{}, parentErr(ParentReaderClosed, "next", "source closed", nil)
	}
	if s.termErr != nil {
		return CurrentObjectRow{}, s.termErr
	}
	if s.atEOF {
		return CurrentObjectRow{}, io.EOF
	}
	if err := ctx.Err(); err != nil {
		s.termErr = parentErr(ParentReaderCanceled, "next", "context canceled", err)
		return CurrentObjectRow{}, s.termErr
	}
	row, err := s.nextRow(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			s.atEOF = true
			return CurrentObjectRow{}, io.EOF
		}
		s.termErr = err
		return CurrentObjectRow{}, err
	}
	return row, nil
}

func (s *PublishedParentRowSource) nextRow(ctx context.Context) (CurrentObjectRow, error) {
	for {
		if s.cur == nil {
			if s.segIdx >= len(s.segments) {
				// All segments drained: total count must match manifest.
				if s.total != s.totalExpected {
					return CurrentObjectRow{}, parentErr(ParentReaderCount, "drain",
						fmt.Sprintf("total rows %d does not match declared %d", s.total, s.totalExpected), nil)
				}
				return CurrentObjectRow{}, io.EOF
			}
			if err := s.openSegment(s.segments[s.segIdx]); err != nil {
				return CurrentObjectRow{}, err
			}
		}
		row, ok, err := s.readCurrent(ctx)
		if err != nil {
			return CurrentObjectRow{}, err
		}
		if !ok {
			// Current segment exhausted: verify its declared row count and advance.
			if s.cur.rowsRead != s.cur.expected {
				got, want := s.cur.rowsRead, s.cur.expected
				_ = s.closeCurrent()
				return CurrentObjectRow{}, parentErr(ParentReaderCount, "segment",
					fmt.Sprintf("segment rows %d do not match declared %d", got, want), nil)
			}
			if err := s.closeCurrent(); err != nil {
				return CurrentObjectRow{}, err
			}
			s.segIdx++
			continue
		}
		return row, nil
	}
}

func (s *PublishedParentRowSource) openSegment(desc SegmentDescriptor) error {
	path, err := safeSegmentPath(s.segmentDir, desc.Path)
	if err != nil {
		return parentErr(ParentReaderOpen, "open", "invalid segment path", nil)
	}
	if desc.Digest.Algorithm != "sha256" {
		return parentErr(ParentReaderDigest, "open", "unsupported segment digest algorithm", nil)
	}
	if desc.Digest.Hex == "" {
		return parentErr(ParentReaderDigest, "open", "segment digest is required", nil)
	}
	// Same-open trust: open once, digest that FD, seek, parse the same FD. Never
	// hash then re-open the pathname (TOCTOU/ABA).
	file, root, err := openSegmentFile(path)
	if err != nil {
		return parentErr(ParentReaderOpen, "open", "open segment failed", nil)
	}
	got, err := sha256HexFileHandle(file)
	if err != nil {
		_ = file.Close()
		_ = root.Close()
		return parentErr(ParentReaderIO, "digest", "hash segment failed", nil)
	}
	if got != desc.Digest.Hex {
		_ = file.Close()
		_ = root.Close()
		return parentErr(ParentReaderDigest, "digest", "segment digest mismatch", nil)
	}
	if afterSegmentDigestVerifiedForTest != nil {
		afterSegmentDigestVerifiedForTest(path)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		_ = root.Close()
		return parentErr(ParentReaderIO, "seek", "seek segment after digest failed", nil)
	}
	s.cur = &parentSegmentCursor{
		file:     file,
		root:     root,
		reader:   parquet.NewGenericReader[segmentParquetRow](file),
		buf:      make([]segmentParquetRow, 64),
		expected: desc.Rows,
	}
	return nil
}

// readCurrent returns the next converted+validated row from the current segment,
// ok=false at segment EOF.
func (s *PublishedParentRowSource) readCurrent(ctx context.Context) (CurrentObjectRow, bool, error) {
	if err := ctx.Err(); err != nil {
		return CurrentObjectRow{}, false, parentErr(ParentReaderCanceled, "read", "context canceled", err)
	}
	c := s.cur
	if c.bufPos >= c.bufN {
		n, err := c.reader.Read(c.buf)
		if n == 0 {
			if err == io.EOF {
				return CurrentObjectRow{}, false, nil
			}
			if err != nil {
				return CurrentObjectRow{}, false, parentErr(ParentReaderParse, "read", "read segment failed", nil)
			}
			return CurrentObjectRow{}, false, nil
		}
		c.bufN = n
		c.bufPos = 0
		// A trailing io.EOF alongside n>0 is handled on the next refill.
		if err != nil && err != io.EOF {
			return CurrentObjectRow{}, false, parentErr(ParentReaderParse, "read", "read segment failed", nil)
		}
	}
	pr := c.buf[c.bufPos]
	c.bufPos++
	current, err := currentRowFromSegmentParquet(pr)
	if err != nil {
		return CurrentObjectRow{}, false, parentErr(ParentReaderParse, "convert", "malformed segment row", nil)
	}
	// Strict bytewise-increasing unique RelKey across the whole stream.
	if s.hasLast && current.RelKey <= s.lastKey {
		return CurrentObjectRow{}, false, parentErr(ParentReaderOrder, "order",
			"segment rows are not strictly increasing unique RelKey", nil)
	}
	s.lastKey = current.RelKey
	s.hasLast = true
	c.rowsRead++
	s.total++
	if c.rowsRead > c.expected {
		return CurrentObjectRow{}, false, parentErr(ParentReaderCount, "segment",
			"segment produced more rows than declared", nil)
	}
	if s.total > s.totalExpected {
		return CurrentObjectRow{}, false, parentErr(ParentReaderCount, "drain",
			"parent produced more rows than declared", nil)
	}
	return current, true, nil
}

func (s *PublishedParentRowSource) closeCurrent() error {
	if s.cur == nil {
		return nil
	}
	var first error
	if s.cur.reader != nil {
		if err := s.cur.reader.Close(); err != nil {
			first = parentErr(ParentReaderIO, "close", "close segment reader failed", nil)
		}
	}
	if s.cur.file != nil {
		if err := s.cur.file.Close(); err != nil && first == nil {
			first = parentErr(ParentReaderIO, "close", "close segment failed", nil)
		}
	}
	if s.cur.root != nil {
		if err := s.cur.root.Close(); err != nil && first == nil {
			first = parentErr(ParentReaderIO, "close", "close segment root failed", nil)
		}
	}
	s.cur = nil
	return first
}

// Close releases any open segment file. It is idempotent and safe after a
// terminal error; disclosure is limited to category/phase/message.
func (s *PublishedParentRowSource) Close() error {
	if s == nil {
		return nil
	}
	if s.closed {
		return nil
	}
	s.closed = true
	return s.closeCurrent()
}
