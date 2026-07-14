package indexsubstrate

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"
)

// spillEvent is a journal mutation ordered by (rel_key, phase, journal_id, sequence).
type spillEvent struct {
	RelKey    string         `json:"rel_key"`
	Phase     int            `json:"phase"` // 0=observe, 1=enrich
	JournalID string         `json:"journal_id"`
	Sequence  uint64         `json:"sequence"`
	Op        ObjectRecordOp `json:"op"`
	Record    ObjectRecord   `json:"record"`
}

func eventPhase(op ObjectRecordOp) int {
	if op == ObjectRecordOpEnrich {
		return 1
	}
	return 0
}

func eventLess(a, b spillEvent) bool {
	if a.RelKey != b.RelKey {
		return a.RelKey < b.RelKey
	}
	if a.Phase != b.Phase {
		return a.Phase < b.Phase
	}
	if a.JournalID != b.JournalID {
		return a.JournalID < b.JournalID
	}
	return a.Sequence < b.Sequence
}

type spillRunHeader struct {
	Magic     string `json:"magic"`
	Version   int    `json:"version"`
	Kind      string `json:"kind"`
	AttemptID string `json:"attempt_id"`
}

type spillRunFooter struct {
	Type      string `json:"type"`
	Count     int    `json:"count"`
	SHA256    string `json:"sha256"`
	Bytes     int64  `json:"bytes"`
	Monotonic bool   `json:"monotonic"`
}

type spillRunWriter struct {
	path      string
	kind      string
	attemptID string
	src       *CurrentStateSource
	file      *os.File
	bw        *bufio.Writer
	hash      hash.Hash
	count     int
	bytes     int64
	sealed    bool
	aborted   bool
	prevKey   string
	prevPhase int
	prevJID   string
	prevSeq   uint64
	hasPrev   bool
}

func newSpillRunWriter(path, kind, attemptID string, src *CurrentStateSource) (*spillRunWriter, error) {
	if err := src.noteRunCreate(); err != nil {
		return nil, err
	}
	if err := src.reserveFD(1); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) // #nosec G304 -- workspace path
	if err != nil {
		src.releaseFD(1)
		return nil, src.fail(SpillMergeIO, "spill", "create run file", err)
	}
	w := &spillRunWriter{
		path:      path,
		kind:      kind,
		attemptID: attemptID,
		src:       src,
		file:      f,
		bw:        bufio.NewWriter(f),
		hash:      sha256.New(),
	}
	hdr := spillRunHeader{
		Magic:     spillMagic,
		Version:   spillMergeFormatVersion,
		Kind:      kind,
		AttemptID: attemptID,
	}
	if err := w.writeJSONLine(hdr); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	return w, nil
}

func (w *spillRunWriter) writeJSONLine(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return w.src.fail(SpillMergeIO, "spill", "encode run record", err)
	}
	if int64(len(data)) > w.src.budget.MaxRecordBytes {
		return w.src.fail(SpillMergeBudgetExhausted, "spill", "MaxRecordBytes exceeded", nil)
	}
	// Prospective charge: payload + newline.
	n := int64(len(data) + 1)
	if err := w.src.chargeWorkspace(n); err != nil {
		return err
	}
	if _, err := w.bw.Write(data); err != nil {
		return w.src.fail(SpillMergeIO, "spill", "write run record", err)
	}
	if err := w.bw.WriteByte('\n'); err != nil {
		return w.src.fail(SpillMergeIO, "spill", "write run newline", err)
	}
	_, _ = w.hash.Write(data)
	_, _ = w.hash.Write([]byte{'\n'})
	w.bytes += n
	return nil
}

func (w *spillRunWriter) WriteParent(row CurrentObjectRow) error {
	if w.sealed || w.aborted {
		return w.src.fail(SpillMergeIO, "spill", "write after seal", nil)
	}
	if w.kind != spillRunKindParent {
		return w.src.fail(SpillMergeIO, "spill", "not a parent run", nil)
	}
	if w.hasPrev && row.RelKey <= w.prevKey {
		return w.src.fail(SpillMergeSpillIntegrity, "spill", "parent run not strictly increasing", nil)
	}
	if err := w.writeJSONLine(row); err != nil {
		return err
	}
	w.prevKey = row.RelKey
	w.hasPrev = true
	w.count++
	return nil
}

func (w *spillRunWriter) WriteEvent(ev spillEvent) error {
	if w.sealed || w.aborted {
		return w.src.fail(SpillMergeIO, "spill", "write after seal", nil)
	}
	if w.kind != spillRunKindEvents {
		return w.src.fail(SpillMergeIO, "spill", "not an events run", nil)
	}
	if w.hasPrev {
		prev := spillEvent{RelKey: w.prevKey, Phase: w.prevPhase, JournalID: w.prevJID, Sequence: w.prevSeq}
		if !eventLess(prev, ev) {
			return w.src.fail(SpillMergeSpillIntegrity, "spill", "event run not strictly ordered", nil)
		}
	}
	if err := w.writeJSONLine(ev); err != nil {
		return err
	}
	w.prevKey = ev.RelKey
	w.prevPhase = ev.Phase
	w.prevJID = ev.JournalID
	w.prevSeq = ev.Sequence
	w.hasPrev = true
	w.count++
	return nil
}

func (w *spillRunWriter) Seal() error {
	if w.aborted {
		return w.src.fail(SpillMergeIO, "spill", "seal after abort", nil)
	}
	if w.sealed {
		return nil
	}
	footer := spillRunFooter{
		Type:      "gonimbus.index.spill_run_footer.v1",
		Count:     w.count,
		SHA256:    hex.EncodeToString(w.hash.Sum(nil)),
		Bytes:     w.bytes,
		Monotonic: true,
	}
	// Footer is not included in the payload digest (digest covers header+records).
	data, err := json.Marshal(footer)
	if err != nil {
		return w.src.fail(SpillMergeIO, "spill", "encode footer", err)
	}
	if err := w.src.chargeWorkspace(int64(len(data) + 1)); err != nil {
		return err
	}
	if _, err := w.bw.Write(data); err != nil {
		return w.src.fail(SpillMergeIO, "spill", "write footer", err)
	}
	if err := w.bw.WriteByte('\n'); err != nil {
		return w.src.fail(SpillMergeIO, "spill", "write footer newline", err)
	}
	if err := w.bw.Flush(); err != nil {
		return w.src.fail(SpillMergeIO, "spill", "flush run", err)
	}
	if err := w.file.Sync(); err != nil {
		return w.src.fail(SpillMergeIO, "spill", "sync run", err)
	}
	if err := w.file.Close(); err != nil {
		return w.src.fail(SpillMergeIO, "spill", "close run", err)
	}
	w.file = nil
	w.src.releaseFD(1)
	w.sealed = true
	// Live size = header+records+footer (footer already charged in Seal).
	footerLen := int64(len(data) + 1)
	w.src.trackSealedRun(w.path, w.bytes+footerLen)
	return nil
}

func (w *spillRunWriter) Abort() error {
	if w == nil || w.sealed || w.aborted {
		return nil
	}
	w.aborted = true
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
		w.src.releaseFD(1)
	}
	_ = os.Remove(w.path)
	return nil
}

type spillRunReader struct {
	file           *os.File
	sc             *bufio.Scanner
	kind           string
	header         spillRunHeader
	count          int
	expected       int
	digest         hash.Hash
	bytes          int64
	footerBytes    int64
	maxRecordBytes int64
	expectAttempt  string
	sawFoot        bool
	finished       bool // footer validated and physical EOF observed
	// event peek for multi-reader merge
	hasPeek bool
	peekEv  spillEvent
	done    bool
}

// heldSpillRun is a fully attested sealed run whose open file descriptor is
// retained so drain cannot observe a substituted pathname.
type heldSpillRun struct {
	path  string
	kind  string
	file  *os.File
	bound os.FileInfo
	count int
	bytes int64
}

func (h *heldSpillRun) Close() error {
	if h == nil || h.file == nil {
		return nil
	}
	err := h.file.Close()
	h.file = nil
	return err
}

func openSpillRunReader(path, wantKind, attemptID string, maxRecordBytes int64) (*spillRunReader, error) {
	f, err := os.Open(path) // #nosec G304 -- workspace path under attempt root
	if err != nil {
		return nil, err
	}
	r, err := newSpillRunReaderFromFile(f, wantKind, attemptID, maxRecordBytes)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return r, nil
}

func newSpillRunReaderFromFile(f *os.File, wantKind, attemptID string, maxRecordBytes int64) (*spillRunReader, error) {
	if maxRecordBytes < 64 {
		maxRecordBytes = 64
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	r := &spillRunReader{
		file:           f,
		sc:             bufio.NewScanner(f),
		kind:           wantKind,
		digest:         sha256.New(),
		maxRecordBytes: maxRecordBytes,
		expectAttempt:  attemptID,
	}
	maxTok := int(maxRecordBytes)
	if maxTok < 64 {
		maxTok = 64
	}
	r.sc.Buffer(make([]byte, 0, min(maxTok, 64*1024)), maxTok)
	if !r.sc.Scan() {
		if err := r.sc.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("empty spill run")
	}
	line := append([]byte(nil), r.sc.Bytes()...)
	if int64(len(line)) > maxRecordBytes {
		return nil, fmt.Errorf("spill header exceeds MaxRecordBytes")
	}
	r.digest.Write(line)
	r.digest.Write([]byte{'\n'})
	r.bytes += int64(len(line) + 1)
	var hdr spillRunHeader
	if err := json.Unmarshal(line, &hdr); err != nil {
		return nil, fmt.Errorf("decode spill header: %w", err)
	}
	if hdr.Magic != spillMagic || hdr.Version != spillMergeFormatVersion {
		return nil, fmt.Errorf("unsupported spill run version")
	}
	if hdr.Kind != wantKind {
		return nil, fmt.Errorf("spill run kind mismatch")
	}
	if attemptID != "" && hdr.AttemptID != attemptID {
		return nil, fmt.Errorf("spill run attempt_id mismatch")
	}
	r.header = hdr
	return r, nil
}

// holdAttestedRun opens, fully validates, and retains the FD for drain.
// No row is returned to the caller. Context is checked during the walk.
func holdAttestedRun(ctx context.Context, path, wantKind, attemptID string, maxRecordBytes int64) (*heldSpillRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(path) // #nosec G304 -- workspace sealed run
	if err != nil {
		return nil, err
	}
	bound, err := f.Stat()
	if err != nil || !bound.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("spill run must be a regular file")
	}
	count, bytes, err := attestFile(ctx, f, wantKind, attemptID, maxRecordBytes)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	// Re-bind after full read (detect replace during attestation).
	cur, err := f.Stat()
	if err != nil || !os.SameFile(bound, cur) {
		_ = f.Close()
		return nil, fmt.Errorf("spill run binding changed during attestation")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &heldSpillRun{path: path, kind: wantKind, file: f, bound: bound, count: count, bytes: bytes}, nil
}

// revalidateHeld re-attests the held FD (still no caller-visible rows) and
// rewinds for drain. Detects late corruption / identity change.
func revalidateHeld(ctx context.Context, h *heldSpillRun, attemptID string, maxRecordBytes int64) error {
	if h == nil || h.file == nil {
		return fmt.Errorf("nil held run")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	cur, err := h.file.Stat()
	if err != nil || !os.SameFile(h.bound, cur) {
		return fmt.Errorf("held spill run identity mismatch")
	}
	if _, err := h.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	count, bytes, err := attestFile(ctx, h.file, h.kind, attemptID, maxRecordBytes)
	if err != nil {
		return err
	}
	if count != h.count || bytes != h.bytes {
		return fmt.Errorf("held spill run digest/count changed after READY")
	}
	if _, err := h.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func attestFile(ctx context.Context, f *os.File, wantKind, attemptID string, maxRecordBytes int64) (count int, payloadBytes int64, err error) {
	r, err := newSpillRunReaderFromFile(f, wantKind, attemptID, maxRecordBytes)
	if err != nil {
		return 0, 0, err
	}
	// Do not close f — caller owns it. Detach scanner ownership carefully:
	// Read through r without closing underlying file.
	var prevKey string
	var prevPhase int
	var prevJID string
	var prevSeq uint64
	hasPrev := false
	for {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		if wantKind == spillRunKindParent {
			row, err := r.ReadParent()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return 0, 0, err
			}
			if hasPrev && row.RelKey <= prevKey {
				return 0, 0, fmt.Errorf("parent run not strictly increasing")
			}
			prevKey = row.RelKey
			hasPrev = true
			continue
		}
		ev, err := r.ReadEvent()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, 0, err
		}
		if hasPrev {
			prev := spillEvent{RelKey: prevKey, Phase: prevPhase, JournalID: prevJID, Sequence: prevSeq}
			if !eventLess(prev, ev) {
				return 0, 0, fmt.Errorf("event run not strictly ordered")
			}
		}
		prevKey, prevPhase, prevJID, prevSeq = ev.RelKey, ev.Phase, ev.JournalID, ev.Sequence
		hasPrev = true
	}
	if !r.sawFoot || !r.finished {
		return 0, 0, fmt.Errorf("spill run missing validated footer or physical EOF")
	}
	// Prevent Close of held file via reader.
	r.file = nil
	return r.expected, r.footerBytes, nil
}

// attestSpillRun is a path-based full validation helper (tests / merge prep).
func attestSpillRun(path, wantKind, attemptID string, maxRecordBytes int64) (count int, payloadBytes int64, err error) {
	h, err := holdAttestedRun(context.Background(), path, wantKind, attemptID, maxRecordBytes)
	if err != nil {
		return 0, 0, err
	}
	count, payloadBytes = h.count, h.bytes
	_ = h.Close()
	return count, payloadBytes, nil
}

func (r *spillRunReader) readLine() ([]byte, error) {
	if r.sawFoot {
		return nil, io.EOF
	}
	if !r.sc.Scan() {
		if err := r.sc.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("spill run missing footer")
	}
	line := append([]byte(nil), r.sc.Bytes()...)
	// Detect footer
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &probe); err == nil && probe.Type == "gonimbus.index.spill_run_footer.v1" {
		var foot spillRunFooter
		if err := json.Unmarshal(line, &foot); err != nil {
			return nil, fmt.Errorf("decode spill footer: %w", err)
		}
		got := hex.EncodeToString(r.digest.Sum(nil))
		if !strings.EqualFold(got, foot.SHA256) {
			return nil, fmt.Errorf("spill run checksum mismatch")
		}
		if foot.Count != r.count {
			return nil, fmt.Errorf("spill run count mismatch")
		}
		if foot.Bytes != r.bytes {
			return nil, fmt.Errorf("spill run bytes mismatch")
		}
		if !foot.Monotonic {
			return nil, fmt.Errorf("spill run monotonic flag false")
		}
		r.sawFoot = true
		r.expected = foot.Count
		r.footerBytes = foot.Bytes
		// Physical EOF required: trailing bytes after a valid footer are corruption.
		if r.sc.Scan() {
			return nil, fmt.Errorf("spill run has trailing data after footer")
		}
		if err := r.sc.Err(); err != nil {
			return nil, err
		}
		r.finished = true
		return nil, io.EOF
	}
	if r.maxRecordBytes > 0 && int64(len(line)) > r.maxRecordBytes {
		return nil, fmt.Errorf("spill record exceeds MaxRecordBytes")
	}
	r.digest.Write(line)
	r.digest.Write([]byte{'\n'})
	r.bytes += int64(len(line) + 1)
	r.count++
	return line, nil
}

// Finish asserts the reader has consumed a validated footer and physical EOF.
// Call after the last Read* returns io.EOF so merge paths cannot launder trailing data.
func (r *spillRunReader) Finish() error {
	if r == nil {
		return fmt.Errorf("nil spill run reader")
	}
	if r.finished {
		return nil
	}
	if !r.sawFoot {
		// Force read until footer/EOF.
		for {
			_, err := r.readLine()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
		}
	}
	if !r.finished {
		return fmt.Errorf("spill run incomplete: footer/EOF not validated")
	}
	return nil
}

func (r *spillRunReader) ReadParent() (CurrentObjectRow, error) {
	line, err := r.readLine()
	if err != nil {
		return CurrentObjectRow{}, err
	}
	var row CurrentObjectRow
	if err := json.Unmarshal(line, &row); err != nil {
		return CurrentObjectRow{}, fmt.Errorf("decode parent row: %w", err)
	}
	return normalizeCurrentObjectRow(row), nil
}

func (r *spillRunReader) ReadEvent() (spillEvent, error) {
	line, err := r.readLine()
	if err != nil {
		return spillEvent{}, err
	}
	var ev spillEvent
	if err := json.Unmarshal(line, &ev); err != nil {
		return spillEvent{}, fmt.Errorf("decode event: %w", err)
	}
	return ev, nil
}

func (r *spillRunReader) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}
