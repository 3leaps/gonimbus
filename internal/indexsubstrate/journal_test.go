package indexsubstrate

import (
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
