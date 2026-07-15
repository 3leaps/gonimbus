package reflowthroughput

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/3leaps/gonimbus/pkg/reflow"
)

// DefaultMaxRecordBytes bounds a single JSONL line retained for inspection.
const DefaultMaxRecordBytes = 4 << 20 // 4 MiB

// TapStats records transparent pass-through metrics for the probe→reflow pipe.
type TapStats struct {
	ValidReflowInputRows int64
	UnexpectedRows       int64
	BytesForwarded       int64
	FirstForward         time.Time
	LastForward          time.Time
	// CopyDuration is wall time of the tap Copy loop (includes producer pacing
	// and downstream backpressure). Not calibrated isolated overhead.
	CopyDuration time.Duration
}

// Tap is a bounded raw-byte pass-through: forwards fragments immediately with
// backpressure, frames on newline for counting, and hard-fails on
// oversize/malformed/unexpected records via strict JSON decode.
type Tap struct {
	MaxRecordBytes        int
	AcceptReflowInputOnly bool

	mu    sync.Mutex
	stats TapStats
	err   error
}

// Stats returns a copy of tap statistics.
func (t *Tap) Stats() TapStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stats
}

// Err returns the first hard-fail error, if any.
func (t *Tap) Err() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

// Copy runs the transparent tap from r to w until EOF or error.
func (t *Tap) Copy(w io.Writer, r io.Reader) error {
	if t.MaxRecordBytes <= 0 {
		t.MaxRecordBytes = DefaultMaxRecordBytes
	}
	start := monoNow()
	buf := make([]byte, 32*1024)
	var partial []byte
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if _, err := w.Write(chunk); err != nil {
				t.setErr(fmt.Errorf("tap forward write: %w", err))
				return t.Err()
			}
			t.mu.Lock()
			t.stats.BytesForwarded += int64(n)
			now := monoNow()
			if t.stats.FirstForward.IsZero() {
				t.stats.FirstForward = now
			}
			t.stats.LastForward = now
			t.mu.Unlock()

			partial = append(partial, chunk...)
			for {
				idx := bytes.IndexByte(partial, '\n')
				if idx < 0 {
					if len(partial) > t.MaxRecordBytes {
						t.setErr(fmt.Errorf("tap record exceeds max size %d", t.MaxRecordBytes))
						return t.Err()
					}
					break
				}
				line := partial[:idx]
				partial = partial[idx+1:]
				if len(line) == 0 {
					continue
				}
				if len(line) > t.MaxRecordBytes {
					t.setErr(fmt.Errorf("tap record exceeds max size %d", t.MaxRecordBytes))
					return t.Err()
				}
				if err := t.inspectLine(line); err != nil {
					t.setErr(err)
					return t.Err()
				}
			}
		}
		if readErr == io.EOF {
			if len(partial) > 0 {
				t.setErr(fmt.Errorf("tap: incomplete final record (missing newline)"))
				return t.Err()
			}
			t.mu.Lock()
			t.stats.CopyDuration = monoNow().Sub(start)
			t.mu.Unlock()
			return nil
		}
		if readErr != nil {
			t.setErr(fmt.Errorf("tap read: %w", readErr))
			return t.Err()
		}
	}
}

func (t *Tap) inspectLine(line []byte) error {
	trim := bytes.TrimSpace(line)
	if len(trim) == 0 {
		return nil
	}
	// Strict JSON decode — substring checks are insufficient.
	var env struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(trim, &env); err != nil {
		t.mu.Lock()
		t.stats.UnexpectedRows++
		t.mu.Unlock()
		return fmt.Errorf("tap: malformed JSONL: %w", err)
	}
	if env.Type == "" {
		t.mu.Lock()
		t.stats.UnexpectedRows++
		t.mu.Unlock()
		return fmt.Errorf("tap: missing record type")
	}
	if t.AcceptReflowInputOnly {
		if env.Type != reflow.ReflowInputRecordType {
			t.mu.Lock()
			t.stats.UnexpectedRows++
			t.mu.Unlock()
			return fmt.Errorf("tap: unexpected record type %q (want %s)", env.Type, reflow.ReflowInputRecordType)
		}
		if len(env.Data) == 0 || string(env.Data) == "null" {
			t.mu.Lock()
			t.stats.UnexpectedRows++
			t.mu.Unlock()
			return fmt.Errorf("tap: reflow.input record missing data object")
		}
	}
	t.mu.Lock()
	t.stats.ValidReflowInputRows++
	t.mu.Unlock()
	return nil
}

func (t *Tap) setErr(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err == nil {
		t.err = err
	}
}

// ActiveInterval returns last-first duration for probe-delivered rate windows.
func (s TapStats) ActiveInterval() time.Duration {
	if s.FirstForward.IsZero() || s.LastForward.IsZero() {
		return 0
	}
	return s.LastForward.Sub(s.FirstForward)
}
