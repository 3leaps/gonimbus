package indexsubstrate

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"strings"
)

// scanJournalStreaming reads a sealed journal from a single open handle with
// an explicit max line/record bound. It does not call ReadJournal and does
// not retain the full journal in memory — each record is delivered via onEvent.
func scanJournalStreaming(
	ctx context.Context,
	r io.Reader,
	cfg SpillMergeConfig,
	budget SpillMergeBudget,
	onEvent func(spillEvent) error,
	onHeader func(JournalHeader),
) error {
	sc := bufio.NewScanner(r)
	maxLine := int(budget.MaxRecordBytes)
	if maxLine < 64 {
		maxLine = 64
	}
	// MaxRecordBytes bounds record payload bytes; the line terminator ("\n" or
	// "\r\n") is framing. The scanner buffer must hold payload plus terminator
	// before ScanLines can emit the token, so it gets a two-byte allowance —
	// the explicit post-trim length check below remains the payload refusal, so
	// exactly-max succeeds and max+1 refuses typed.
	sc.Buffer(make([]byte, 0, min(maxLine+2, 64*1024)), maxLine+2)

	var header JournalHeader
	var records uint64
	var sawHeader bool
	var sawFooter bool
	// content recomputes the header+records digest so the compaction reopen is
	// digest-bound: a journal whose header (including crawl_prefixes) or records
	// were mutated after sealing fails its footer's ContentSHA256 here too, not
	// only at ValidateJournal.
	content := sha256.New()
	lineNo := 0

	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return spillErr(SpillMergeCanceled, "journal", "context canceled", "", err)
		}
		lineNo++
		line := sc.Bytes()
		if len(line) == 0 {
			return spillErr(SpillMergeJournal, "journal", fmt.Sprintf("empty line %d", lineNo), "", ErrInvalidJournal)
		}
		if int64(len(line)) > budget.MaxRecordBytes {
			return spillErr(SpillMergeBudgetExhausted, "journal", "MaxRecordBytes exceeded", "", nil)
		}
		var env struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(line, &env); err != nil {
			return spillErr(SpillMergeJournal, "journal", fmt.Sprintf("decode line %d", lineNo), "", fmt.Errorf("%w: %v", ErrInvalidJournal, err))
		}
		switch env.Type {
		case JournalHeaderType:
			if sawHeader {
				return spillErr(SpillMergeJournal, "journal", "duplicate header", "", ErrInvalidJournal)
			}
			if records != 0 || sawFooter {
				return spillErr(SpillMergeJournal, "journal", "header after records", "", ErrInvalidJournal)
			}
			var h JournalHeader
			if err := json.Unmarshal(line, &h); err != nil {
				return spillErr(SpillMergeJournal, "journal", "decode header", "", fmt.Errorf("%w: %v", ErrInvalidJournal, err))
			}
			h = normalizeHeader(h)
			if err := validateHeader(h); err != nil {
				return spillErr(SpillMergeJournal, "journal", "invalid header", "", err)
			}
			if h.IndexSetID != cfg.IndexSetID {
				return spillErr(SpillMergeJournal, "journal", "journal index_set_id mismatch", "", ErrInvalidJournal)
			}
			if h.RunID != cfg.RunID {
				return spillErr(SpillMergeJournal, "journal", "journal run_id mismatch", "", ErrInvalidJournal)
			}
			hashJournalLine(content, line)
			header = h
			sawHeader = true
			if onHeader != nil {
				onHeader(header)
			}
		case ObjectRecordType:
			if !sawHeader {
				return spillErr(SpillMergeJournal, "journal", "record before header", "", ErrInvalidJournal)
			}
			if sawFooter {
				return spillErr(SpillMergeJournal, "journal", "record after footer", "", ErrInvalidJournal)
			}
			var rec ObjectRecord
			if err := json.Unmarshal(line, &rec); err != nil {
				return spillErr(SpillMergeJournal, "journal", "decode record", "", fmt.Errorf("%w: %v", ErrInvalidJournal, err))
			}
			if err := validateObjectRecord(rec); err != nil {
				return spillErr(SpillMergeJournal, "journal", "invalid record", "", err)
			}
			if rec.JournalID != header.JournalID {
				return spillErr(SpillMergeJournal, "journal", "record journal_id mismatch", "", ErrInvalidJournal)
			}
			expected := records + 1
			if rec.Sequence != expected {
				return spillErr(SpillMergeJournal, "journal", fmt.Sprintf("non-monotonic sequence %d", rec.Sequence), "", ErrInvalidJournal)
			}
			records++
			hashJournalLine(content, line)
			rec = normalizeObjectRecord(rec, rec.JournalID, rec.Sequence)
			if strings.TrimSpace(rec.RelKey) == "" {
				return spillErr(SpillMergeJournal, "journal", "record rel_key required", "", ErrInvalidJournal)
			}
			ev := spillEvent{
				RelKey:    rec.RelKey,
				Phase:     eventPhase(rec.Op),
				JournalID: rec.JournalID,
				Sequence:  rec.Sequence,
				Op:        rec.Op,
				Record:    rec,
			}
			if onEvent != nil {
				if err := onEvent(ev); err != nil {
					return err
				}
			}
		case JournalFooterType:
			if !sawHeader {
				return spillErr(SpillMergeJournal, "journal", "footer before header", "", ErrInvalidJournal)
			}
			if sawFooter {
				return spillErr(SpillMergeJournal, "journal", "duplicate footer", "", ErrInvalidJournal)
			}
			var footer JournalFooter
			if err := json.Unmarshal(line, &footer); err != nil {
				return spillErr(SpillMergeJournal, "journal", "decode footer", "", fmt.Errorf("%w: %v", ErrInvalidJournal, err))
			}
			footer = normalizeFooter(footer)
			if err := validateFooter(header, footer, records); err != nil {
				return spillErr(SpillMergeJournal, "journal", "invalid footer", "", err)
			}
			if err := verifyJournalContentDigest(footer, content); err != nil {
				return spillErr(SpillMergeJournal, "journal", "content digest mismatch", "", err)
			}
			sawFooter = true
		default:
			return spillErr(SpillMergeJournal, "journal", "unknown record type", "", ErrInvalidJournal)
		}
	}
	if err := sc.Err(); err != nil {
		if strings.Contains(err.Error(), "token too long") {
			return spillErr(SpillMergeBudgetExhausted, "journal", "MaxRecordBytes exceeded", "", err)
		}
		return spillErr(SpillMergeJournal, "journal", "scan journal", "", err)
	}
	if !sawHeader {
		return spillErr(SpillMergeJournal, "journal", "missing header", "", ErrIncompleteJournal)
	}
	if !sawFooter {
		return spillErr(SpillMergeJournal, "journal", "missing footer", "", ErrIncompleteJournal)
	}
	return nil
}

// hashJournalLine feeds one on-disk journal line (header or record, no newline)
// into the running content digest, matching the writer's per-line hashing.
func hashJournalLine(h hash.Hash, line []byte) {
	_, _ = h.Write(line)
}

// estimateEventBytes accounts for the encoded spill event JSON plus fixed
// residency overhead for scanner/decoder/sort-slice copies.
func estimateEventBytes(ev spillEvent) int64 {
	data, err := json.Marshal(ev)
	if err != nil {
		// Fail closed: treat as unfittable large.
		return 1 << 62
	}
	const residencyOverhead = 192 // token + decoded copies + slice growth amortised
	return int64(len(data)) + residencyOverhead
}
