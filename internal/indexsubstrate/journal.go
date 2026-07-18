package indexsubstrate

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	JournalHeaderType = "gonimbus.index.journal_header.v1"
	ObjectRecordType  = "gonimbus.index.object_record.v1"
	JournalFooterType = "gonimbus.index.journal_footer.v1"

	ObjectRecordOpObserve ObjectRecordOp = "observe"
	ObjectRecordOpEnrich  ObjectRecordOp = "enrich"

	IndexSchemaVersion = 8

	truncateScanChunkSize = 64 * 1024
)

var (
	ErrIncompleteJournal = errors.New("incomplete journal")
	ErrInvalidJournal    = errors.New("invalid journal")
)

type ObjectRecordOp string

type Scope struct {
	Prefix string  `json:"prefix,omitempty"`
	Window *Window `json:"window,omitempty"`
}

type Window struct {
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

type JournalHeader struct {
	Type       string `json:"type"`
	JournalID  string `json:"journal_id"`
	IndexSetID string `json:"index_set_id"`
	RunID      string `json:"run_id"`
	Shard      string `json:"shard"`
	Scope      *Scope `json:"scope,omitempty"`
	// CrawlPrefixes records the canonical provider-key prefix plan whose complete
	// observation this journal attests. On a recovery re-publish, coverage
	// authorizes tombstones over verified-parent rows, so public Retry validates
	// its caller-supplied coverage against this recorded plan and re-publishes
	// only within the observed plan. Absent on legacy journals; recovery over
	// such a journal fails closed rather than trusting caller coverage.
	CrawlPrefixes      []string  `json:"crawl_prefixes,omitempty"`
	IndexSchemaVersion int       `json:"index_schema_version"`
	StartedAt          time.Time `json:"started_at"`
}

type ObjectRecord struct {
	Type      string         `json:"type"`
	JournalID string         `json:"journal_id"`
	Sequence  uint64         `json:"sequence"`
	Op        ObjectRecordOp `json:"op"`

	RelKey       string     `json:"rel_key"`
	ObservedAt   time.Time  `json:"observed_at"`
	SizeBytes    *int64     `json:"size_bytes,omitempty"`
	ETag         string     `json:"etag,omitempty"`
	LastModified *time.Time `json:"last_modified,omitempty"`
	StorageClass *string    `json:"storage_class,omitempty"`

	ContentType    *string    `json:"content_type,omitempty"`
	ArchiveStatus  *string    `json:"archive_status,omitempty"`
	RestoreState   *string    `json:"restore_state,omitempty"`
	RestoreExpiry  *time.Time `json:"restore_expiry,omitempty"`
	HeadEnrichedAt *time.Time `json:"head_enriched_at,omitempty"`
}

type JournalFooter struct {
	Type      string `json:"type"`
	JournalID string `json:"journal_id"`
	Records   uint64 `json:"records"`
	// ContentSHA256 is the writer-generated lowercase-hex SHA-256 over the exact
	// header line and every ordered record line (the footer line itself
	// excluded). Any post-seal mutation of the header — including its
	// crawl_prefixes provenance — or of any record, or truncation, changes the
	// recomputed digest and fails validation. Absent on legacy journals sealed
	// before content integrity; those are not tamper-evident and must not be
	// trusted as observation provenance.
	ContentSHA256 string    `json:"content_sha256,omitempty"`
	CompletedAt   time.Time `json:"completed_at"`
}

type JournalSummary struct {
	Header        JournalHeader
	Footer        JournalFooter
	Records       uint64
	ContentSHA256 string
}

type Journal struct {
	Header  JournalHeader
	Records []ObjectRecord
	Footer  JournalFooter
}

type JournalWriter struct {
	mu       sync.Mutex
	path     string
	file     *os.File
	writer   *bufio.Writer
	content  hash.Hash
	header   JournalHeader
	records  uint64
	nextSeq  uint64
	sealed   bool
	isClosed bool
}

func CreateJournal(path string, header JournalHeader) (*JournalWriter, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("journal path is required")
	}
	dir, name, err := splitJournalPath(path)
	if err != nil {
		return nil, err
	}
	header = normalizeHeader(header)
	if err := validateHeader(header); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create journal directory: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open journal directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	file, err := root.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create journal: %w", err)
	}
	jw := &JournalWriter{
		path:    path,
		file:    file,
		writer:  bufio.NewWriter(file),
		content: sha256.New(),
		header:  header,
		nextSeq: 1,
	}
	if err := writeHashedJSONLine(jw.writer, jw.content, header); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("write journal header: %w", err)
	}
	return jw, nil
}

func (w *JournalWriter) Path() string {
	if w == nil {
		return ""
	}
	return w.path
}

func (w *JournalWriter) Append(record ObjectRecord) (ObjectRecord, error) {
	if w == nil {
		return ObjectRecord{}, fmt.Errorf("journal writer is nil")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isClosed {
		return ObjectRecord{}, fmt.Errorf("journal is closed")
	}
	if w.sealed {
		return ObjectRecord{}, fmt.Errorf("journal is sealed")
	}
	record = normalizeObjectRecord(record, w.header.JournalID, w.nextSeq)
	if err := validateObjectRecord(record); err != nil {
		return ObjectRecord{}, err
	}
	if err := writeHashedJSONLine(w.writer, w.content, record); err != nil {
		return ObjectRecord{}, fmt.Errorf("write journal record: %w", err)
	}
	w.records++
	w.nextSeq++
	return record, nil
}

func (w *JournalWriter) Seal(completedAt time.Time) error {
	if w == nil {
		return fmt.Errorf("journal writer is nil")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isClosed {
		return fmt.Errorf("journal is closed")
	}
	if w.sealed {
		return nil
	}
	if completedAt.IsZero() {
		completedAt = time.Now().UTC()
	}
	footer := JournalFooter{
		Type:          JournalFooterType,
		JournalID:     w.header.JournalID,
		Records:       w.records,
		ContentSHA256: hex.EncodeToString(w.content.Sum(nil)),
		CompletedAt:   completedAt.UTC(),
	}
	if err := writeJSONLine(w.writer, footer); err != nil {
		return fmt.Errorf("write journal footer: %w", err)
	}
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("flush journal: %w", err)
	}
	w.sealed = true
	return nil
}

func (w *JournalWriter) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isClosed {
		return nil
	}
	var err error
	if w.writer != nil {
		if flushErr := w.writer.Flush(); flushErr != nil {
			err = flushErr
		}
	}
	if closeErr := w.file.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	w.isClosed = true
	if err != nil {
		return fmt.Errorf("close journal: %w", err)
	}
	return nil
}

func ValidateJournal(path string) (JournalSummary, error) {
	return validateJournalFile(path, 0)
}

// validateJournalFile validates a sealed journal, bounding each line read by
// maxRecordBytes (0 = unbounded). The publish path passes the resolved
// MaxRecordBytes so an oversized record is refused before the journal is declared
// validated; errJournalRecordTooLong propagates for the caller to classify.
func validateJournalFile(path string, maxRecordBytes int64) (JournalSummary, error) {
	dir, name, err := splitJournalPath(path)
	if err != nil {
		return JournalSummary{}, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return JournalSummary{}, fmt.Errorf("open journal directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	file, err := root.Open(name)
	if err != nil {
		return JournalSummary{}, fmt.Errorf("open journal: %w", err)
	}
	defer func() { _ = file.Close() }()
	journal, records, err := readJournalReader(file, false, maxRecordBytes)
	if err != nil {
		return JournalSummary{}, err
	}
	return JournalSummary{
		Header:        journal.Header,
		Footer:        journal.Footer,
		Records:       records,
		ContentSHA256: journal.Footer.ContentSHA256,
	}, nil
}

func ReadJournal(path string) (Journal, error) {
	dir, name, err := splitJournalPath(path)
	if err != nil {
		return Journal{}, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return Journal{}, fmt.Errorf("open journal directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	file, err := root.Open(name)
	if err != nil {
		return Journal{}, fmt.Errorf("open journal: %w", err)
	}
	defer func() { _ = file.Close() }()
	journal, _, err := readJournalReader(file, true, 0)
	return journal, err
}

func TruncateToLastFullLine(path string) (int64, error) {
	dir, name, err := splitJournalPath(path)
	if err != nil {
		return 0, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return 0, fmt.Errorf("open journal directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	file, err := root.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return 0, fmt.Errorf("open journal: %w", err)
	}
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat journal: %w", err)
	}
	size := info.Size()
	if size == 0 {
		return 0, nil
	}

	buf := make([]byte, truncateScanChunkSize)
	for end := size; end > 0; {
		readSize := int64(len(buf))
		if end < readSize {
			readSize = end
		}
		start := end - readSize
		chunk := buf[:readSize]
		if _, err := file.ReadAt(chunk, start); err != nil {
			return 0, fmt.Errorf("read journal chunk: %w", err)
		}
		for i := len(chunk) - 1; i >= 0; i-- {
			if chunk[i] == '\n' {
				keep := start + int64(i) + 1
				if keep == size {
					return keep, nil
				}
				if err := file.Truncate(keep); err != nil {
					return 0, fmt.Errorf("truncate journal: %w", err)
				}
				return keep, nil
			}
		}
		end = start
	}

	if err := file.Truncate(0); err != nil {
		return 0, fmt.Errorf("truncate journal: %w", err)
	}
	return 0, nil
}

func splitJournalPath(path string) (string, string, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" || path == "." {
		return "", "", fmt.Errorf("journal path is required")
	}
	dir := filepath.Dir(path)
	name := filepath.Base(path)
	if name == "." || name == string(filepath.Separator) {
		return "", "", fmt.Errorf("journal filename is required")
	}
	return dir, name, nil
}

// errJournalRecordTooLong is returned by the bounded read when a single journal
// line exceeds the configured MaxRecordBytes. The publish validation path maps it
// to a typed SpillMergeBudgetExhausted; unbounded callers (maxRecordBytes <= 0)
// never see it.
var errJournalRecordTooLong = errors.New("journal record exceeds max record bytes")

// readJournalLineBounded reads one '\n'-terminated line. When max > 0 it refuses
// a line longer than max bytes instead of accumulating unbounded, so an oversized
// header/record cannot allocate past the budget before enforcement. The returned
// line includes the trailing '\n' (matching bufio.Reader.ReadString) so the
// caller's newline trimming and partial-trailing-line detection are unchanged.
func readJournalLineBounded(reader *bufio.Reader, max int64) (string, error) {
	if max <= 0 {
		return reader.ReadString('\n')
	}
	var b []byte
	for {
		chunk, err := reader.ReadSlice('\n')
		if int64(len(b))+int64(len(chunk)) > max {
			return "", errJournalRecordTooLong
		}
		b = append(b, chunk...)
		if err == nil {
			return string(b), nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return string(b), err
	}
}

func readJournalReader(r io.Reader, collectRecords bool, maxRecordBytes int64) (Journal, uint64, error) {
	reader := bufio.NewReader(r)
	var journal Journal
	var records uint64
	var sawHeader bool
	var sawFooter bool
	// content accumulates the digest over the header line and every record line
	// (footer excluded), recomputed from the exact bytes on disk so a post-seal
	// mutation of the header (including crawl_prefixes) or any record fails the
	// footer's ContentSHA256 check.
	content := sha256.New()
	lineNo := 0
	for {
		line, err := readJournalLineBounded(reader, maxRecordBytes)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if line != "" {
					return Journal{}, 0, fmt.Errorf("%w: partial trailing line", ErrIncompleteJournal)
				}
				break
			}
			if errors.Is(err, errJournalRecordTooLong) {
				return Journal{}, 0, errJournalRecordTooLong
			}
			return Journal{}, 0, fmt.Errorf("read journal line: %w", err)
		}
		lineNo++
		line = strings.TrimSuffix(line, "\n")
		line = strings.TrimSuffix(line, "\r")
		if strings.TrimSpace(line) == "" {
			return Journal{}, 0, fmt.Errorf("%w: empty line %d", ErrInvalidJournal, lineNo)
		}
		var env struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			return Journal{}, 0, fmt.Errorf("%w: decode line %d: %v", ErrInvalidJournal, lineNo, err)
		}
		switch env.Type {
		case JournalHeaderType:
			if sawHeader {
				return Journal{}, 0, fmt.Errorf("%w: duplicate header", ErrInvalidJournal)
			}
			if records != 0 || sawFooter {
				return Journal{}, 0, fmt.Errorf("%w: header after records", ErrInvalidJournal)
			}
			var header JournalHeader
			if err := json.Unmarshal([]byte(line), &header); err != nil {
				return Journal{}, 0, fmt.Errorf("%w: decode header: %v", ErrInvalidJournal, err)
			}
			header = normalizeHeader(header)
			if err := validateHeader(header); err != nil {
				return Journal{}, 0, err
			}
			_, _ = content.Write([]byte(line))
			journal.Header = header
			sawHeader = true
		case ObjectRecordType:
			if !sawHeader {
				return Journal{}, 0, fmt.Errorf("%w: record before header", ErrInvalidJournal)
			}
			if sawFooter {
				return Journal{}, 0, fmt.Errorf("%w: record after footer", ErrInvalidJournal)
			}
			var rec ObjectRecord
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				return Journal{}, 0, fmt.Errorf("%w: decode record: %v", ErrInvalidJournal, err)
			}
			if err := validateObjectRecord(rec); err != nil {
				return Journal{}, 0, err
			}
			if rec.JournalID != journal.Header.JournalID {
				return Journal{}, 0, fmt.Errorf("%w: record journal_id mismatch", ErrInvalidJournal)
			}
			expected := records + 1
			if rec.Sequence != expected {
				return Journal{}, 0, fmt.Errorf("%w: non-monotonic record sequence %d, expected %d", ErrInvalidJournal, rec.Sequence, expected)
			}
			records++
			_, _ = content.Write([]byte(line))
			if collectRecords {
				journal.Records = append(journal.Records, normalizeObjectRecord(rec, rec.JournalID, rec.Sequence))
			}
		case JournalFooterType:
			if !sawHeader {
				return Journal{}, 0, fmt.Errorf("%w: footer before header", ErrInvalidJournal)
			}
			if sawFooter {
				return Journal{}, 0, fmt.Errorf("%w: duplicate footer", ErrInvalidJournal)
			}
			var footer JournalFooter
			if err := json.Unmarshal([]byte(line), &footer); err != nil {
				return Journal{}, 0, fmt.Errorf("%w: decode footer: %v", ErrInvalidJournal, err)
			}
			footer = normalizeFooter(footer)
			if err := validateFooter(journal.Header, footer, records); err != nil {
				return Journal{}, 0, err
			}
			if err := verifyJournalContentDigest(footer, content); err != nil {
				return Journal{}, 0, err
			}
			journal.Footer = footer
			sawFooter = true
		default:
			return Journal{}, 0, fmt.Errorf("%w: unknown record type %q", ErrInvalidJournal, env.Type)
		}
	}
	if !sawHeader {
		return Journal{}, 0, fmt.Errorf("%w: missing header", ErrIncompleteJournal)
	}
	if !sawFooter {
		return Journal{}, 0, fmt.Errorf("%w: missing footer", ErrIncompleteJournal)
	}
	return journal, records, nil
}

// verifyJournalContentDigest checks the recomputed header+records digest against
// the footer's sealed ContentSHA256. A legacy footer without ContentSHA256 is
// not tamper-evident and is left unverified here (callers that require
// provenance must reject an absent digest); a present digest that disagrees is a
// tampered or truncated journal and fails closed.
func verifyJournalContentDigest(footer JournalFooter, content hash.Hash) error {
	sealed := strings.TrimSpace(footer.ContentSHA256)
	if sealed == "" {
		return nil
	}
	got := hex.EncodeToString(content.Sum(nil))
	if got != sealed {
		return fmt.Errorf("%w: journal content digest mismatch (tampered or truncated)", ErrInvalidJournal)
	}
	return nil
}

// writeHashedJSONLine writes v as a JSON line and feeds the exact marshaled
// bytes (without the newline) into h, so the writer's running content digest
// covers the same bytes a reader recomputes from disk.
func writeHashedJSONLine(w io.Writer, h hash.Hash, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := w.Write(raw); err != nil {
		return err
	}
	if _, err := h.Write(raw); err != nil {
		return err
	}
	_, err = w.Write([]byte{'\n'})
	return err
}

func writeJSONLine(w io.Writer, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := w.Write(raw); err != nil {
		return err
	}
	_, err = w.Write([]byte{'\n'})
	return err
}

func normalizeHeader(header JournalHeader) JournalHeader {
	header.Type = JournalHeaderType
	if header.IndexSchemaVersion == 0 {
		header.IndexSchemaVersion = IndexSchemaVersion
	}
	if !header.StartedAt.IsZero() {
		header.StartedAt = header.StartedAt.UTC()
	}
	return header
}

func normalizeObjectRecord(record ObjectRecord, journalID string, sequence uint64) ObjectRecord {
	record.Type = ObjectRecordType
	record.JournalID = journalID
	record.Sequence = sequence
	if !record.ObservedAt.IsZero() {
		record.ObservedAt = record.ObservedAt.UTC()
	}
	if record.HeadEnrichedAt != nil {
		t := record.HeadEnrichedAt.UTC()
		record.HeadEnrichedAt = &t
	}
	return record
}

func normalizeFooter(footer JournalFooter) JournalFooter {
	footer.Type = JournalFooterType
	if !footer.CompletedAt.IsZero() {
		footer.CompletedAt = footer.CompletedAt.UTC()
	}
	return footer
}

func validateHeader(header JournalHeader) error {
	switch {
	case header.Type != JournalHeaderType:
		return fmt.Errorf("%w: header type must be %q", ErrInvalidJournal, JournalHeaderType)
	case strings.TrimSpace(header.JournalID) == "":
		return fmt.Errorf("%w: journal_id is required", ErrInvalidJournal)
	case strings.TrimSpace(header.IndexSetID) == "":
		return fmt.Errorf("%w: index_set_id is required", ErrInvalidJournal)
	case strings.TrimSpace(header.RunID) == "":
		return fmt.Errorf("%w: run_id is required", ErrInvalidJournal)
	case strings.TrimSpace(header.Shard) == "":
		return fmt.Errorf("%w: shard is required", ErrInvalidJournal)
	case header.IndexSchemaVersion != IndexSchemaVersion:
		return fmt.Errorf("%w: index_schema_version must be %d", ErrInvalidJournal, IndexSchemaVersion)
	case header.StartedAt.IsZero():
		return fmt.Errorf("%w: started_at is required", ErrInvalidJournal)
	default:
		return nil
	}
}

func validateObjectRecord(record ObjectRecord) error {
	if record.Type != ObjectRecordType {
		return fmt.Errorf("%w: object record type must be %q", ErrInvalidJournal, ObjectRecordType)
	}
	if strings.TrimSpace(record.JournalID) == "" {
		return fmt.Errorf("%w: record journal_id is required", ErrInvalidJournal)
	}
	if record.Sequence == 0 {
		return fmt.Errorf("%w: record sequence is required", ErrInvalidJournal)
	}
	switch record.Op {
	case ObjectRecordOpObserve, ObjectRecordOpEnrich:
	default:
		return fmt.Errorf("%w: unsupported journal op %q", ErrInvalidJournal, record.Op)
	}
	if strings.TrimSpace(record.RelKey) == "" {
		return fmt.Errorf("%w: rel_key is required", ErrInvalidJournal)
	}
	if record.ObservedAt.IsZero() {
		return fmt.Errorf("%w: observed_at is required", ErrInvalidJournal)
	}
	return nil
}

func validateFooter(header JournalHeader, footer JournalFooter, records uint64) error {
	switch {
	case footer.Type != JournalFooterType:
		return fmt.Errorf("%w: footer type must be %q", ErrInvalidJournal, JournalFooterType)
	case footer.JournalID != header.JournalID:
		return fmt.Errorf("%w: footer journal_id mismatch", ErrInvalidJournal)
	case footer.Records != records:
		return fmt.Errorf("%w: footer records=%d, saw %d", ErrInvalidJournal, footer.Records, records)
	case footer.CompletedAt.IsZero():
		return fmt.Errorf("%w: completed_at is required", ErrInvalidJournal)
	default:
		return nil
	}
}
