package stream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/3leaps/gonimbus/pkg/output"
)

// Writer emits a mixed-framing stream:
// - JSONL control-plane records (output.Record) terminated by '\n'
// - for chunk records, exactly N raw bytes follow immediately
//
// Writer is safe for concurrent use.
type Writer struct {
	w        io.Writer
	jobID    string
	provider string

	mu     sync.Mutex
	closed bool
}

func NewWriter(w io.Writer, jobID, provider string) *Writer {
	return &Writer{w: w, jobID: jobID, provider: provider}
}

func (sw *Writer) Close() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.closed = true
	return nil
}

func (sw *Writer) WriteOpen(ctx context.Context, open *Open) error {
	return sw.writeJSON(ctx, TypeStreamOpen, open)
}

func (sw *Writer) WriteClose(ctx context.Context, closeRec *Close) error {
	return sw.writeJSON(ctx, TypeStreamClose, closeRec)
}

// WriteChunk writes a chunk header record and then copies exactly hdr.NBytes
// bytes from body.
//
// Caller may pass an io.Reader that yields fewer bytes; this will be surfaced as
// io.ErrUnexpectedEOF.
func (sw *Writer) WriteChunk(ctx context.Context, hdr *Chunk, body io.Reader) error {
	if hdr == nil {
		return errors.New("stream: chunk header is nil")
	}
	if hdr.NBytes < 0 {
		return errors.New("stream: chunk nbytes must be >= 0")
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	dataBytes, err := json.Marshal(hdr)
	if err != nil {
		return err
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	if sw.closed {
		return output.ErrWriterClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	rec := output.Record{
		Type:     TypeStreamChunk,
		TS:       time.Now().UTC(),
		JobID:    sw.jobID,
		Provider: sw.provider,
		Data:     dataBytes,
	}

	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if err := writeAll(sw.w, line); err != nil {
		return err
	}

	if hdr.NBytes == 0 {
		return nil
	}
	if _, err := io.CopyN(sw.w, body, hdr.NBytes); err != nil {
		if errors.Is(err, io.EOF) {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}

func (sw *Writer) writeJSON(ctx context.Context, recordType string, data any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	if sw.closed {
		return output.ErrWriterClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	rec := output.Record{
		Type:     recordType,
		TS:       time.Now().UTC(),
		JobID:    sw.jobID,
		Provider: sw.provider,
		Data:     dataBytes,
	}

	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return writeAll(sw.w, b)
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}
