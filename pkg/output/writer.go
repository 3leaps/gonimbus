package output

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"
)

// Writer outputs JSONL records for crawl results.
//
// Implementations must be safe for concurrent use from multiple
// goroutines. Each Write* method emits a complete record as a
// single line of JSON followed by a newline.
type Writer interface {
	// WriteObject emits an object record.
	WriteObject(ctx context.Context, obj *ObjectRecord) error

	// WriteError emits an error record.
	WriteError(ctx context.Context, err *ErrorRecord) error

	// WriteProgress emits a progress record.
	WriteProgress(ctx context.Context, prog *ProgressRecord) error

	// WriteSummary emits a summary record.
	WriteSummary(ctx context.Context, sum *SummaryRecord) error

	// WritePreflight emits a preflight record.
	WritePreflight(ctx context.Context, preflight *PreflightRecord) error

	// WriteTransfer emits a transfer record.
	WriteTransfer(ctx context.Context, transfer *TransferRecord) error

	// WriteSkip emits a skip record.
	WriteSkip(ctx context.Context, skip *SkipRecord) error

	// Close flushes any buffered output and releases resources.
	Close() error
}

// JSONLWriter writes records as newline-delimited JSON to an io.Writer.
//
// JSONLWriter is safe for concurrent use. Writes are serialized using
// a mutex to ensure atomic line writes (no interleaved output).
type JSONLWriter struct {
	w        io.Writer
	jobID    string
	provider string
	mu       sync.Mutex

	// closed indicates the writer has been closed.
	closed bool
}

// NewJSONLWriter creates a new JSONL writer.
//
// Parameters:
//   - w: The underlying writer (stdout, file, etc.)
//   - jobID: Correlation ID for this crawl job
//   - provider: Storage provider identifier (e.g., "s3")
func NewJSONLWriter(w io.Writer, jobID, provider string) *JSONLWriter {
	return &JSONLWriter{
		w:        w,
		jobID:    jobID,
		provider: provider,
	}
}

// WriteObject emits an object record.
func (jw *JSONLWriter) WriteObject(ctx context.Context, obj *ObjectRecord) error {
	return jw.writeRecord(ctx, TypeObject, obj)
}

// WriteError emits an error record.
func (jw *JSONLWriter) WriteError(ctx context.Context, err *ErrorRecord) error {
	return jw.writeRecord(ctx, TypeError, err)
}

// WriteProgress emits a progress record.
func (jw *JSONLWriter) WriteProgress(ctx context.Context, prog *ProgressRecord) error {
	return jw.writeRecord(ctx, TypeProgress, prog)
}

// WriteSummary emits a summary record.
func (jw *JSONLWriter) WriteSummary(ctx context.Context, sum *SummaryRecord) error {
	return jw.writeRecord(ctx, TypeSummary, sum)
}

// WritePreflight emits a preflight record.
func (jw *JSONLWriter) WritePreflight(ctx context.Context, preflight *PreflightRecord) error {
	return jw.writeRecord(ctx, TypePreflight, preflight)
}

func (jw *JSONLWriter) WriteTransfer(ctx context.Context, transfer *TransferRecord) error {
	return jw.writeRecord(ctx, TypeTransfer, transfer)
}

func (jw *JSONLWriter) WriteSkip(ctx context.Context, skip *SkipRecord) error {
	return jw.writeRecord(ctx, TypeSkip, skip)
}

// Close marks the writer as closed.
//
// If the underlying writer implements io.Closer, it is NOT closed.
// The caller is responsible for closing the underlying writer.
func (jw *JSONLWriter) Close() error {
	jw.mu.Lock()
	defer jw.mu.Unlock()

	jw.closed = true
	return nil
}

// writeRecord marshals data and writes a complete record line.
//
// This method holds the mutex for the entire operation to ensure
// atomic line writes. The record is written as a single line of
// JSON followed by a newline character.
func (jw *JSONLWriter) writeRecord(ctx context.Context, recordType string, data any) error {
	// Check context cancellation before acquiring lock
	if err := ctx.Err(); err != nil {
		return err
	}

	// Marshal the data payload first (outside the lock for better concurrency)
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return &WriteError{Op: "marshal_data", Err: err}
	}

	jw.mu.Lock()
	defer jw.mu.Unlock()

	// Check if writer is closed
	if jw.closed {
		return ErrWriterClosed
	}

	// Check context again after acquiring lock
	if err := ctx.Err(); err != nil {
		return err
	}

	// Create the envelope record
	record := Record{
		Type:     recordType,
		TS:       time.Now().UTC(),
		JobID:    jw.jobID,
		Provider: jw.provider,
		Data:     dataBytes,
	}

	// Marshal the complete record
	recordBytes, err := json.Marshal(record)
	if err != nil {
		return &WriteError{Op: "marshal_record", Err: err}
	}

	// Write the record followed by newline.
	// We must handle short writes: io.Writer is allowed to return n < len(p)
	// with nil error, which would silently truncate JSONL lines and corrupt output.
	recordBytes = append(recordBytes, '\n')
	if err := writeAll(jw.w, recordBytes); err != nil {
		return &WriteError{Op: "write", Err: err}
	}

	return nil
}

// writeAll writes all bytes to w, handling short writes.
//
// io.Writer.Write may return n < len(p) with a nil error (short write).
// This function loops until all bytes are written or an error occurs,
// ensuring complete JSONL lines are emitted.
func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			// No progress made - avoid infinite loop
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

// Compile-time check that JSONLWriter implements Writer.
var _ Writer = (*JSONLWriter)(nil)
