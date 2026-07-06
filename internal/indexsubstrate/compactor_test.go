package indexsubstrate

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/stretchr/testify/require"
)

func TestCompactMatchesIndexstoreSchemaV8MutationSemantics(t *testing.T) {
	ctx := context.Background()
	db, err := indexstore.Open(ctx, indexstore.Config{Path: ":memory:"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, indexstore.Migrate(ctx, db))

	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, indexstore.IndexSetParams{
		BaseURI:  "s3://bucket/prefix/",
		Provider: "s3",
		BuildParams: indexstore.BuildParams{
			SourceType:    "crawl",
			SchemaVersion: indexstore.SchemaVersion,
			Includes:      []string{"**"},
		},
	})
	require.NoError(t, err)
	oldRun, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	currentRun, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	oldAt := time.Date(2026, 7, 5, 10, 0, 0, 0, time.UTC)
	oldChangedAt := oldAt.Add(time.Hour)
	runStartedAt := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	observedAt := runStartedAt.Add(10 * time.Minute)
	oldModified := time.Date(2026, 7, 1, 8, 0, 0, 0, time.FixedZone("offset", -4*60*60))
	newModified := oldModified.Add(time.Hour)
	deletedAt := oldAt.Add(2 * time.Hour)
	standard := "STANDARD"
	glacier := "GLACIER"
	oldType := "application/xml"
	newType := "application/json"
	restoreState := "available"
	restoreExpiry := time.Date(2026, 7, 8, 12, 0, 0, 123, time.FixedZone("offset", -4*60*60))

	seed := []indexstore.ObjectRow{
		{IndexSetID: indexSet.IndexSetID, RelKey: "data/unchanged.xml", SizeBytes: 10, LastModified: &oldModified, ETag: `"same"`, StorageClass: &standard, FirstSeenRunID: oldRun.RunID, FirstSeenAt: oldAt, LastChangedRunID: oldRun.RunID, LastChangedAt: oldChangedAt, LastSeenRunID: oldRun.RunID, LastSeenAt: oldAt},
		{IndexSetID: indexSet.IndexSetID, RelKey: "data/changed.xml", SizeBytes: 10, LastModified: &oldModified, ETag: `"old"`, StorageClass: &standard, FirstSeenRunID: oldRun.RunID, FirstSeenAt: oldAt, LastChangedRunID: oldRun.RunID, LastChangedAt: oldChangedAt, LastSeenRunID: oldRun.RunID, LastSeenAt: oldAt},
		{IndexSetID: indexSet.IndexSetID, RelKey: "data/reappeared.xml", SizeBytes: 5, LastModified: &oldModified, ETag: `"reappear"`, StorageClass: &glacier, FirstSeenRunID: oldRun.RunID, FirstSeenAt: oldAt, LastChangedRunID: oldRun.RunID, LastChangedAt: oldChangedAt, LastSeenRunID: oldRun.RunID, LastSeenAt: oldAt},
		{IndexSetID: indexSet.IndexSetID, RelKey: "data/missing.xml", SizeBytes: 7, LastModified: &oldModified, ETag: `"missing"`, StorageClass: &standard, FirstSeenRunID: oldRun.RunID, FirstSeenAt: oldAt, LastChangedRunID: oldRun.RunID, LastChangedAt: oldChangedAt, LastSeenRunID: oldRun.RunID, LastSeenAt: oldAt},
		{IndexSetID: indexSet.IndexSetID, RelKey: "data/enrich-only.xml", SizeBytes: 8, LastModified: &oldModified, ETag: `"enrich"`, StorageClass: &standard, ContentType: &oldType, FirstSeenRunID: oldRun.RunID, FirstSeenAt: oldAt, LastChangedRunID: oldRun.RunID, LastChangedAt: oldChangedAt, LastSeenRunID: oldRun.RunID, LastSeenAt: oldAt},
	}
	require.NoError(t, indexstore.BatchUpsertObjects(ctx, db, seed))
	require.NoError(t, setDeletedAtForTest(ctx, db, indexSet.IndexSetID, "data/reappeared.xml", deletedAt))

	priorRows := make([]CurrentObjectRow, 0, len(seed))
	for _, row := range seed {
		got, err := indexstore.GetObject(ctx, db, indexSet.IndexSetID, row.RelKey)
		require.NoError(t, err)
		require.NotNil(t, got)
		priorRows = append(priorRows, currentRowFromIndexstore(*got))
	}

	size10 := int64(10)
	size20 := int64(20)
	size5 := int64(5)
	size1 := int64(1)
	records := []ObjectRecord{
		observe("jrn_a", 1, "data/unchanged.xml", observedAt, size10, &oldModified, `"same"`, &standard),
		observe("jrn_a", 2, "data/changed.xml", observedAt.Add(time.Second), size20, &newModified, `"new"`, &standard),
		observe("jrn_a", 3, "data/reappeared.xml", observedAt.Add(2*time.Second), size5, &oldModified, `"reappear"`, &glacier),
		observe("jrn_a", 4, "data/new.xml", observedAt.Add(3*time.Second), size1, &newModified, `"created"`, &standard),
		enrich("jrn_a", 5, "data/new.xml", observedAt.Add(4*time.Second), &newType, &restoreState, &restoreExpiry),
		enrich("jrn_a", 6, "data/enrich-only.xml", observedAt.Add(5*time.Second), &newType, &restoreState, &restoreExpiry),
	}
	journal := journalWithRecords(indexSet.IndexSetID, currentRun.RunID, "jrn_a", records)

	result, err := Compact(CompactionInput{
		IndexSetID:   indexSet.IndexSetID,
		RunID:        currentRun.RunID,
		RunStartedAt: runStartedAt,
		PriorRows:    priorRows,
		Journals:     []Journal{journal},
		Coverage: []CoverageAttestation{{
			Scope:    &Scope{Prefix: "data/"},
			Basis:    CoverageBasisConfirmed,
			Complete: true,
		}},
	})
	require.NoError(t, err)

	for _, record := range records {
		switch record.Op {
		case ObjectRecordOpObserve:
			require.NoError(t, indexstore.UpsertObject(ctx, db, indexstoreRowFromRecord(indexSet.IndexSetID, currentRun.RunID, record)))
		case ObjectRecordOpEnrich:
			require.NoError(t, indexstore.BatchUpdateHeadEnrichment(ctx, db, []indexstore.HeadEnrichmentUpdate{{
				IndexSetID:     indexSet.IndexSetID,
				RelKey:         record.RelKey,
				ArchiveStatus:  record.ArchiveStatus,
				RestoreState:   record.RestoreState,
				RestoreExpiry:  record.RestoreExpiry,
				ContentType:    record.ContentType,
				HeadEnrichedAt: record.ObservedAt,
			}}))
		}
	}
	deleted, err := indexstore.MarkObjectsDeletedNotSeenInRun(ctx, db, indexSet.IndexSetID, currentRun.RunID, runStartedAt)
	require.NoError(t, err)
	require.Equal(t, int64(2), deleted)
	require.Len(t, result.Tombstones, 2)

	resultRows := rowsByRelKey(result.Rows)
	for _, key := range []string{
		"data/unchanged.xml",
		"data/changed.xml",
		"data/reappeared.xml",
		"data/missing.xml",
		"data/enrich-only.xml",
		"data/new.xml",
	} {
		dbRow, err := indexstore.GetObject(ctx, db, indexSet.IndexSetID, key)
		require.NoError(t, err)
		require.NotNil(t, dbRow)
		require.Equal(t, currentRowFromIndexstore(*dbRow), resultRows[key], "rel_key=%s", key)
	}
}

func TestCompactDoesNotTombstoneWithoutExactConfirmedCoverage(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	prior := []CurrentObjectRow{{
		IndexSetID:       "idx_test",
		RelKey:           "data/missing.xml",
		SizeBytes:        10,
		FirstSeenRunID:   "run_old",
		FirstSeenAt:      runStartedAt.Add(-24 * time.Hour),
		LastChangedRunID: "run_old",
		LastChangedAt:    runStartedAt.Add(-24 * time.Hour),
		LastSeenRunID:    "run_old",
		LastSeenAt:       runStartedAt.Add(-24 * time.Hour),
	}}

	tests := []struct {
		name     string
		coverage []CoverageAttestation
	}{
		{name: "none"},
		{name: "inferred", coverage: []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisInferred, Complete: true}}},
		{name: "partial", coverage: []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed}}},
		{name: "gapped", coverage: []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true, Gaps: []Scope{{Prefix: "data/"}}}}},
		{name: "different scope", coverage: []CoverageAttestation{{Scope: &Scope{Prefix: "other/"}, Basis: CoverageBasisConfirmed, Complete: true}}},
		{name: "nil scope", coverage: []CoverageAttestation{{Basis: CoverageBasisConfirmed, Complete: true}}},
		{name: "empty prefix", coverage: []CoverageAttestation{{Scope: &Scope{}, Basis: CoverageBasisConfirmed, Complete: true}}},
		{name: "root prefix", coverage: []CoverageAttestation{{Scope: &Scope{Prefix: "/"}, Basis: CoverageBasisConfirmed, Complete: true}}},
		{name: "windowed scope", coverage: []CoverageAttestation{{Scope: &Scope{Prefix: "data/", Window: &Window{From: "2026-07-01", To: "2026-07-02"}}, Basis: CoverageBasisConfirmed, Complete: true}}},
		{
			name: "gap from separate entry",
			coverage: []CoverageAttestation{
				{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true},
				{Scope: &Scope{Prefix: "other/"}, Basis: CoverageBasisInferred, Gaps: []Scope{{Prefix: "data/"}}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Compact(CompactionInput{
				IndexSetID:   "idx_test",
				RunID:        "run_current",
				RunStartedAt: runStartedAt,
				PriorRows:    prior,
				Coverage:     tt.coverage,
			})
			require.NoError(t, err)
			require.Empty(t, result.Tombstones)
			require.Nil(t, rowsByRelKey(result.Rows)["data/missing.xml"].DeletedAt)
		})
	}
}

func TestCompactJournalFilesRejectsUnsealedJournals(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unsealed.jsonl")
	writer, err := CreateJournal(path, testHeader())
	require.NoError(t, err)
	_, err = writer.Append(ObjectRecord{
		Op:         ObjectRecordOpObserve,
		RelKey:     "data/object.xml",
		ObservedAt: time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	_, err = CompactJournalFiles(CompactionInput{
		IndexSetID:   "idx_test",
		RunID:        "run_test",
		RunStartedAt: time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC),
	}, []string{path})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIncompleteJournal), err)
}

func TestCompactOrdersJournalsDeterministically(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	observedAt := runStartedAt.Add(time.Minute)
	size1 := int64(1)
	size2 := int64(2)
	journalB := journalWithRecords("idx_test", "run_test", "jrn_b", []ObjectRecord{
		observe("jrn_b", 1, "data/race.xml", observedAt, size2, nil, `"b"`, nil),
	})
	journalA := journalWithRecords("idx_test", "run_test", "jrn_a", []ObjectRecord{
		observe("jrn_a", 1, "data/race.xml", observedAt, size1, nil, `"a"`, nil),
	})

	result, err := Compact(CompactionInput{
		IndexSetID:   "idx_test",
		RunID:        "run_test",
		RunStartedAt: runStartedAt,
		Journals:     []Journal{journalB, journalA},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), rowsByRelKey(result.Rows)["data/race.xml"].SizeBytes)
	require.Equal(t, `"b"`, rowsByRelKey(result.Rows)["data/race.xml"].ETag)
}

func TestCompactMergesEnrichThatSortsBeforeCurrentRunObserve(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	observedAt := runStartedAt.Add(time.Minute)
	size := int64(10)
	contentType := "application/xml"
	restoreState := "available"
	restoreExpiry := observedAt.Add(24 * time.Hour)
	enrichJournal := journalWithRecords("idx_test", "run_test", "jrn_a", []ObjectRecord{
		enrich("jrn_a", 1, "data/new.xml", observedAt.Add(time.Second), &contentType, &restoreState, &restoreExpiry),
	})
	observeJournal := journalWithRecords("idx_test", "run_test", "jrn_b", []ObjectRecord{
		observe("jrn_b", 1, "data/new.xml", observedAt, size, nil, `"etag"`, nil),
	})

	result, err := Compact(CompactionInput{
		IndexSetID:   "idx_test",
		RunID:        "run_test",
		RunStartedAt: runStartedAt,
		Journals:     []Journal{observeJournal, enrichJournal},
	})
	require.NoError(t, err)
	row := rowsByRelKey(result.Rows)["data/new.xml"]
	require.NotNil(t, row.ContentType)
	require.Equal(t, contentType, *row.ContentType)
	require.NotNil(t, row.RestoreState)
	require.Equal(t, restoreState, *row.RestoreState)
	require.NotNil(t, row.RestoreExpiry)
	require.True(t, restoreExpiry.UTC().Equal(*row.RestoreExpiry))
	require.Equal(t, 1, result.EnrichmentRecords)
}

func observe(journalID string, sequence uint64, relKey string, observedAt time.Time, size int64, lastModified *time.Time, etag string, storageClass *string) ObjectRecord {
	return ObjectRecord{
		Type:           ObjectRecordType,
		JournalID:      journalID,
		Sequence:       sequence,
		Op:             ObjectRecordOpObserve,
		RelKey:         relKey,
		ObservedAt:     observedAt,
		SizeBytes:      &size,
		LastModified:   lastModified,
		ETag:           etag,
		StorageClass:   storageClass,
		HeadEnrichedAt: nil,
	}
}

func enrich(journalID string, sequence uint64, relKey string, observedAt time.Time, contentType *string, restoreState *string, restoreExpiry *time.Time) ObjectRecord {
	return ObjectRecord{
		Type:          ObjectRecordType,
		JournalID:     journalID,
		Sequence:      sequence,
		Op:            ObjectRecordOpEnrich,
		RelKey:        relKey,
		ObservedAt:    observedAt,
		ContentType:   contentType,
		RestoreState:  restoreState,
		RestoreExpiry: restoreExpiry,
	}
}

func journalWithRecords(indexSetID string, runID string, journalID string, records []ObjectRecord) Journal {
	startedAt := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	return Journal{
		Header: JournalHeader{
			Type:               JournalHeaderType,
			JournalID:          journalID,
			IndexSetID:         indexSetID,
			RunID:              runID,
			Shard:              journalID,
			IndexSchemaVersion: IndexSchemaVersion,
			StartedAt:          startedAt,
		},
		Records: records,
		Footer: JournalFooter{
			Type:        JournalFooterType,
			JournalID:   journalID,
			Records:     uint64(len(records)),
			CompletedAt: startedAt.Add(time.Hour),
		},
	}
}

func setDeletedAtForTest(ctx context.Context, db *sql.DB, indexSetID string, relKey string, deletedAt time.Time) error {
	_, err := db.ExecContext(ctx,
		`UPDATE objects_current SET deleted_at = ? WHERE index_set_id = ? AND rel_key = ?`,
		dbTimeStringForTest(deletedAt), indexSetID, relKey)
	return err
}

func dbTimeStringForTest(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000-0700")
}

func currentRowFromIndexstore(row indexstore.ObjectRow) CurrentObjectRow {
	return CurrentObjectRow{
		IndexSetID:       row.IndexSetID,
		RelKey:           row.RelKey,
		SizeBytes:        row.SizeBytes,
		LastModified:     canonicalTimePtr(row.LastModified),
		ETag:             row.ETag,
		StorageClass:     stringPtrCopy(row.StorageClass),
		ArchiveStatus:    stringPtrCopy(row.ArchiveStatus),
		RestoreState:     stringPtrCopy(row.RestoreState),
		RestoreExpiry:    canonicalTimePtr(row.RestoreExpiry),
		ContentType:      stringPtrCopy(row.ContentType),
		HeadEnrichedAt:   canonicalTimePtr(row.HeadEnrichedAt),
		FirstSeenRunID:   row.FirstSeenRunID,
		FirstSeenAt:      row.FirstSeenAt.UTC(),
		LastChangedRunID: row.LastChangedRunID,
		LastChangedAt:    row.LastChangedAt.UTC(),
		LastSeenRunID:    row.LastSeenRunID,
		LastSeenAt:       row.LastSeenAt.UTC(),
		DeletedAt:        canonicalTimePtr(row.DeletedAt),
	}
}

func indexstoreRowFromRecord(indexSetID string, runID string, record ObjectRecord) indexstore.ObjectRow {
	return indexstore.ObjectRow{
		IndexSetID:    indexSetID,
		RelKey:        record.RelKey,
		SizeBytes:     valueOrZero(record.SizeBytes),
		LastModified:  canonicalTimePtr(record.LastModified),
		ETag:          record.ETag,
		StorageClass:  stringPtrCopy(record.StorageClass),
		LastSeenRunID: runID,
		LastSeenAt:    record.ObservedAt.UTC(),
	}
}

func rowsByRelKey(rows []CurrentObjectRow) map[string]CurrentObjectRow {
	out := make(map[string]CurrentObjectRow, len(rows))
	for _, row := range rows {
		out[row.RelKey] = row
	}
	return out
}
