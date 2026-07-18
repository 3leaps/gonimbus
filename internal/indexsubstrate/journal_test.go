package indexsubstrate

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJournalWriterSealsValidJournalWithDeterministicSequences(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard-0001.jsonl")
	writer, err := CreateJournal(path, testHeader())
	require.NoError(t, err)

	observed := time.Date(2026, 7, 6, 16, 0, 0, 0, time.UTC)
	first, err := writer.Append(ObjectRecord{
		Op:         ObjectRecordOpObserve,
		RelKey:     "prefix/object-a.xml",
		ObservedAt: observed,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), first.Sequence)
	require.Equal(t, "jrn_test", first.JournalID)

	second, err := writer.Append(ObjectRecord{
		Op:         ObjectRecordOpEnrich,
		RelKey:     "prefix/object-a.xml",
		ObservedAt: observed.Add(time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), second.Sequence)

	require.NoError(t, writer.Seal(observed.Add(2*time.Second)))
	require.NoError(t, writer.Close())

	summary, err := ValidateJournal(path)
	require.NoError(t, err)
	require.Equal(t, "jrn_test", summary.Header.JournalID)
	require.Equal(t, uint64(2), summary.Records)
	require.Equal(t, uint64(2), summary.Footer.Records)
}

func TestValidateJournalRejectsIncompleteJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incomplete.jsonl")
	writer, err := CreateJournal(path, testHeader())
	require.NoError(t, err)
	_, err = writer.Append(ObjectRecord{
		Op:         ObjectRecordOpObserve,
		RelKey:     "prefix/object-a.xml",
		ObservedAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	_, err = ValidateJournal(path)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIncompleteJournal), err)
}

func TestTruncateToLastFullLineRemovesPartialRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partial.jsonl")
	writer, err := CreateJournal(path, testHeader())
	require.NoError(t, err)
	_, err = writer.Append(ObjectRecord{
		Op:         ObjectRecordOpObserve,
		RelKey:     "prefix/object-a.xml",
		ObservedAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	require.NoError(t, appendFile(path, []byte(`{"type":"gonimbus.index.object_record.v1"`)))
	_, err = ValidateJournal(path)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIncompleteJournal), err)

	kept, err := TruncateToLastFullLine(path)
	require.NoError(t, err)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, kept, info.Size())

	_, err = ValidateJournal(path)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIncompleteJournal), err)

	require.NoError(t, appendFile(path, []byte(`{"type":"gonimbus.index.journal_footer.v1","journal_id":"jrn_test","records":1,"completed_at":"2026-07-06T16:01:00Z"}`+"\n")))
	summary, err := ValidateJournal(path)
	require.NoError(t, err)
	require.Equal(t, uint64(1), summary.Records)
}

func TestTruncateToLastFullLineScansBackwardAcrossChunks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large-partial.jsonl")
	fullLine := strings.Repeat("a", truncateScanChunkSize+512) + "\n"
	partialLine := strings.Repeat("b", truncateScanChunkSize+512)
	require.NoError(t, os.WriteFile(path, []byte(fullLine+partialLine), 0o600))

	kept, err := TruncateToLastFullLine(path)
	require.NoError(t, err)
	require.Equal(t, int64(len(fullLine)), kept)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, fullLine, string(data))
}

func TestJournalWriterRejectsTombstoneOperation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad-op.jsonl")
	writer, err := CreateJournal(path, testHeader())
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })

	_, err = writer.Append(ObjectRecord{
		Op:         ObjectRecordOp("tombstone"),
		RelKey:     "prefix/object-a.xml",
		ObservedAt: time.Now().UTC(),
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidJournal), err)
}

func TestValidateJournalRejectsRecordAfterFooter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "record-after-footer.jsonl")
	lines := []byte(
		`{"type":"gonimbus.index.journal_header.v1","journal_id":"jrn_test","index_set_id":"idx_test","run_id":"run_test","shard":"shard-0001","index_schema_version":8,"started_at":"2026-07-06T16:00:00Z"}` + "\n" +
			`{"type":"gonimbus.index.journal_footer.v1","journal_id":"jrn_test","records":0,"completed_at":"2026-07-06T16:01:00Z"}` + "\n" +
			`{"type":"gonimbus.index.object_record.v1","journal_id":"jrn_test","sequence":1,"op":"observe","rel_key":"prefix/object-a.xml","observed_at":"2026-07-06T16:00:01Z"}` + "\n",
	)
	require.NoError(t, os.WriteFile(path, lines, 0o600))

	_, err := ValidateJournal(path)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidJournal), err)
	require.Contains(t, err.Error(), "record after footer")
}

func TestValidateJournalRejectsFooterCountMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "footer-count-mismatch.jsonl")
	lines := []byte(
		`{"type":"gonimbus.index.journal_header.v1","journal_id":"jrn_test","index_set_id":"idx_test","run_id":"run_test","shard":"shard-0001","index_schema_version":8,"started_at":"2026-07-06T16:00:00Z"}` + "\n" +
			`{"type":"gonimbus.index.object_record.v1","journal_id":"jrn_test","sequence":1,"op":"observe","rel_key":"prefix/object-a.xml","observed_at":"2026-07-06T16:00:01Z"}` + "\n" +
			`{"type":"gonimbus.index.journal_footer.v1","journal_id":"jrn_test","records":2,"completed_at":"2026-07-06T16:01:00Z"}` + "\n",
	)
	require.NoError(t, os.WriteFile(path, lines, 0o600))

	_, err := ValidateJournal(path)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidJournal), err)
	require.Contains(t, err.Error(), "footer records")
}

func TestValidateJournalRejectsNonMonotonicSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad-sequence.jsonl")
	lines := []byte(
		`{"type":"gonimbus.index.journal_header.v1","journal_id":"jrn_test","index_set_id":"idx_test","run_id":"run_test","shard":"shard-0001","index_schema_version":8,"started_at":"2026-07-06T16:00:00Z"}` + "\n" +
			`{"type":"gonimbus.index.object_record.v1","journal_id":"jrn_test","sequence":2,"op":"observe","rel_key":"prefix/object-a.xml","observed_at":"2026-07-06T16:00:01Z"}` + "\n" +
			`{"type":"gonimbus.index.journal_footer.v1","journal_id":"jrn_test","records":1,"completed_at":"2026-07-06T16:01:00Z"}` + "\n",
	)
	require.NoError(t, os.WriteFile(path, lines, 0o600))

	_, err := ValidateJournal(path)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidJournal), err)
	require.Contains(t, err.Error(), "non-monotonic")
}

func testHeader() JournalHeader {
	return JournalHeader{
		JournalID:          "jrn_test",
		IndexSetID:         "idx_test",
		RunID:              "run_test",
		Shard:              "shard-0001",
		IndexSchemaVersion: IndexSchemaVersion,
		StartedAt:          time.Date(2026, 7, 6, 16, 0, 0, 0, time.UTC),
	}
}

func appendFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = f.Write(data)
	return err
}

// TestReadJournalLineBoundedPayloadLimitSemantics pins the record-budget
// contract at the read primitive: MaxRecordBytes bounds encoded record payload
// bytes, the line terminator ("\n" or "\r\n") is framing. A payload of exactly
// max succeeds, max+1 refuses, for both LF and CRLF framing.
func TestReadJournalLineBoundedPayloadLimitSemantics(t *testing.T) {
	const max = 8
	cases := []struct {
		name    string
		line    string
		tooLong bool
	}{
		{"exact payload LF", "12345678\n", false},
		{"exact payload CRLF", "12345678\r\n", false},
		{"payload plus one LF", "123456789\n", true},
		{"payload plus one CRLF", "123456789\r\n", true},
		{"payload under limit LF", "1234567\n", false},
		{"far over budget LF", strings.Repeat("x", 4*max) + "\n", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			line, err := readJournalLineBounded(bufio.NewReader(strings.NewReader(tc.line)), max)
			if tc.tooLong {
				require.ErrorIs(t, err, errJournalRecordTooLong)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.line, line, "bounded read must return the full framed line")
		})
	}
}

// TestReadJournalLineBoundedSpansSmallReaderBuffers proves the payload bound is
// enforced across chunked ReadSlice accumulation (line longer than the bufio
// buffer), not only within a single chunk.
func TestReadJournalLineBoundedSpansSmallReaderBuffers(t *testing.T) {
	payload := strings.Repeat("a", 100)
	reader := bufio.NewReaderSize(strings.NewReader(payload+"\n"), 16)

	line, err := readJournalLineBounded(reader, 100)
	require.NoError(t, err)
	require.Equal(t, payload+"\n", line)

	reader = bufio.NewReaderSize(strings.NewReader(payload+"\n"), 16)
	_, err = readJournalLineBounded(reader, 99)
	require.ErrorIs(t, err, errJournalRecordTooLong)
}

// TestValidateJournalBoundedRefusesTyped proves the exported capacity-aware
// validator maps an over-budget record to the same typed
// SpillMergeBudgetExhausted as the streaming scan, and passes a sufficient
// budget through unchanged.
func TestValidateJournalBoundedRefusesTyped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard-0001.jsonl")
	writer, err := CreateJournal(path, testHeader())
	require.NoError(t, err)
	observed := time.Date(2026, 7, 6, 16, 0, 0, 0, time.UTC)
	_, err = writer.Append(ObjectRecord{Op: ObjectRecordOpObserve, RelKey: "data/a.xml", ObservedAt: observed})
	require.NoError(t, err)
	require.NoError(t, writer.Seal(observed.Add(time.Minute)))
	require.NoError(t, writer.Close())

	_, err = ValidateJournalBounded(path, 1)
	var sme *SpillMergeError
	require.True(t, errors.As(err, &sme), "expected typed spill-merge error, got %v", err)
	require.Equal(t, SpillMergeBudgetExhausted, sme.Category)
	require.Contains(t, err.Error(), "MaxRecordBytes exceeded")

	summary, err := ValidateJournalBounded(path, DefaultSpillMergeBudget().MaxRecordBytes)
	require.NoError(t, err)
	require.Equal(t, uint64(1), summary.Records)
}
