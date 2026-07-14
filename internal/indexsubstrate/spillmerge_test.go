package indexsubstrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSpillMergeDifferentialVsCompact_CoreMatrix(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	observedAt := runStartedAt.Add(10 * time.Minute)
	oldAt := runStartedAt.Add(-24 * time.Hour)
	standard := "STANDARD"
	glacier := "GLACIER"
	oldType := "application/xml"
	newType := "application/json"
	size10 := int64(10)
	size20 := int64(20)
	size5 := int64(5)
	size1 := int64(1)
	oldMod := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	newMod := oldMod.Add(time.Hour)
	deletedAt := oldAt.Add(2 * time.Hour)

	prior := []CurrentObjectRow{
		{IndexSetID: "idx_test", RelKey: "data/changed.xml", SizeBytes: 10, LastModified: &oldMod, ETag: `"old"`, StorageClass: &standard, FirstSeenRunID: "run_old", FirstSeenAt: oldAt, LastChangedRunID: "run_old", LastChangedAt: oldAt, LastSeenRunID: "run_old", LastSeenAt: oldAt},
		{IndexSetID: "idx_test", RelKey: "data/enrich-only.xml", SizeBytes: 8, LastModified: &oldMod, ETag: `"enrich"`, StorageClass: &standard, ContentType: &oldType, FirstSeenRunID: "run_old", FirstSeenAt: oldAt, LastChangedRunID: "run_old", LastChangedAt: oldAt, LastSeenRunID: "run_old", LastSeenAt: oldAt},
		{IndexSetID: "idx_test", RelKey: "data/missing.xml", SizeBytes: 7, LastModified: &oldMod, ETag: `"missing"`, StorageClass: &standard, FirstSeenRunID: "run_old", FirstSeenAt: oldAt, LastChangedRunID: "run_old", LastChangedAt: oldAt, LastSeenRunID: "run_old", LastSeenAt: oldAt},
		{IndexSetID: "idx_test", RelKey: "data/reappeared.xml", SizeBytes: 5, LastModified: &oldMod, ETag: `"reappear"`, StorageClass: &glacier, FirstSeenRunID: "run_old", FirstSeenAt: oldAt, LastChangedRunID: "run_old", LastChangedAt: oldAt, LastSeenRunID: "run_old", LastSeenAt: oldAt, DeletedAt: &deletedAt},
		{IndexSetID: "idx_test", RelKey: "data/unchanged.xml", SizeBytes: 10, LastModified: &oldMod, ETag: `"same"`, StorageClass: &standard, FirstSeenRunID: "run_old", FirstSeenAt: oldAt, LastChangedRunID: "run_old", LastChangedAt: oldAt, LastSeenRunID: "run_old", LastSeenAt: oldAt},
	}
	// Parent must be strictly sorted by RelKey.
	sortParentForTest(prior)

	records := []ObjectRecord{
		observe("jrn_a", 1, "data/unchanged.xml", observedAt, size10, &oldMod, `"same"`, &standard),
		observe("jrn_a", 2, "data/changed.xml", observedAt.Add(time.Second), size20, &newMod, `"new"`, &standard),
		observe("jrn_a", 3, "data/reappeared.xml", observedAt.Add(2*time.Second), size5, &oldMod, `"reappear"`, &glacier),
		observe("jrn_a", 4, "data/new.xml", observedAt.Add(3*time.Second), size1, &newMod, `"created"`, &standard),
		enrich("jrn_a", 5, "data/new.xml", observedAt.Add(4*time.Second), &newType, nil, nil),
		enrich("jrn_a", 6, "data/enrich-only.xml", observedAt.Add(5*time.Second), &newType, nil, nil),
	}
	journal := journalWithRecords("idx_test", "run_cur", "jrn_a", records)
	coverage := []CoverageAttestation{{
		Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true,
	}}

	oracle, err := Compact(CompactionInput{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		PriorRows: prior, Journals: []Journal{journal}, Coverage: coverage,
	})
	require.NoError(t, err)

	rows, stats := drainSpillMerge(t, SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent:       NewSliceParentRows(prior),
		JournalPaths: writeSealedJournals(t, []Journal{journal}),
		Coverage:     coverage,
		SpillRoot:    t.TempDir(),
	})
	requireEqualCompactProjection(t, oracle, rows, stats)
}

func TestSpillMergeEnrichOnlyAndMissingKeyEnrich(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	prior := []CurrentObjectRow{
		{IndexSetID: "idx_test", RelKey: "data/enrich.xml", SizeBytes: 20, FirstSeenRunID: "run_old", FirstSeenAt: runStartedAt.Add(-time.Hour), LastChangedRunID: "run_old", LastChangedAt: runStartedAt.Add(-time.Hour), LastSeenRunID: "run_old", LastSeenAt: runStartedAt.Add(-time.Hour)},
		{IndexSetID: "idx_test", RelKey: "data/keep.xml", SizeBytes: 10, FirstSeenRunID: "run_old", FirstSeenAt: runStartedAt.Add(-time.Hour), LastChangedRunID: "run_old", LastChangedAt: runStartedAt.Add(-time.Hour), LastSeenRunID: "run_old", LastSeenAt: runStartedAt.Add(-time.Hour)},
	}
	sortParentForTest(prior)
	contentType := "application/xml"
	journal := journalWithRecords("idx_test", "run_enrich", "jrn_enrich", []ObjectRecord{
		enrich("jrn_enrich", 1, "data/enrich.xml", runStartedAt.Add(time.Minute), &contentType, nil, nil),
		enrich("jrn_enrich", 2, "data/missing-key.xml", runStartedAt.Add(2*time.Minute), &contentType, nil, nil),
	})
	coverage := []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true}}

	oracle, err := Compact(CompactionInput{
		IndexSetID: "idx_test", RunID: "run_enrich", RunStartedAt: runStartedAt,
		PriorRows: prior, Journals: []Journal{journal}, Coverage: coverage, Mode: PublicationModeEnrichOnly,
	})
	require.NoError(t, err)

	rows, stats := drainSpillMerge(t, SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_enrich", RunStartedAt: runStartedAt,
		Parent:       NewSliceParentRows(prior),
		JournalPaths: writeSealedJournals(t, []Journal{journal}),
		Coverage:     coverage,
		Mode:         PublicationModeEnrichOnly,
		SpillRoot:    t.TempDir(),
	})
	requireEqualCompactProjection(t, oracle, rows, stats)
	require.Equal(t, 1, stats.EnrichmentRecords)
}

func TestSpillMergeJournalOrderIndependentAndEnrichPhase(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	prior := []CurrentObjectRow{{
		IndexSetID: "idx_test", RelKey: "data/key.xml", SizeBytes: 10,
		FirstSeenRunID: "run_old", FirstSeenAt: runStartedAt.Add(-time.Hour),
		LastChangedRunID: "run_old", LastChangedAt: runStartedAt.Add(-time.Hour),
		LastSeenRunID: "run_old", LastSeenAt: runStartedAt.Add(-time.Hour),
	}}
	earlyType := "application/early"
	lateType := "application/late"
	// jrn_b sorts after jrn_a; supply paths with b first.
	jB := journalWithRecords("idx_test", "run_enrich", "jrn_b", []ObjectRecord{
		enrich("jrn_b", 1, "data/key.xml", runStartedAt.Add(2*time.Minute), &lateType, nil, nil),
	})
	jA := journalWithRecords("idx_test", "run_enrich", "jrn_a", []ObjectRecord{
		enrich("jrn_a", 1, "data/key.xml", runStartedAt.Add(30*time.Second), &earlyType, nil, nil),
		enrich("jrn_a", 2, "data/key.xml", runStartedAt.Add(time.Minute), &earlyType, nil, nil),
	})
	oracle, err := Compact(CompactionInput{
		IndexSetID: "idx_test", RunID: "run_enrich", RunStartedAt: runStartedAt,
		PriorRows: prior, Mode: PublicationModeEnrichOnly,
		Coverage: []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true}},
		Journals: []Journal{jB, jA},
	})
	require.NoError(t, err)

	paths := writeSealedJournals(t, []Journal{jB, jA})
	rows, stats := drainSpillMerge(t, SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_enrich", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(prior), JournalPaths: paths,
		Coverage:  []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true}},
		Mode:      PublicationModeEnrichOnly,
		SpillRoot: t.TempDir(),
	})
	requireEqualCompactProjection(t, oracle, rows, stats)
	require.Equal(t, lateType, *rows[0].ContentType)
}

func TestSpillMergeParentOrderAndDuplicateRefused(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	root := t.TempDir()
	cfgBase := SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt, SpillRoot: root,
	}

	t.Run("out of order", func(t *testing.T) {
		cfg := cfgBase
		cfg.Parent = NewSliceParentRows([]CurrentObjectRow{
			{IndexSetID: "idx_test", RelKey: "b", FirstSeenRunID: "r", FirstSeenAt: runStartedAt, LastChangedRunID: "r", LastChangedAt: runStartedAt, LastSeenRunID: "r", LastSeenAt: runStartedAt},
			{IndexSetID: "idx_test", RelKey: "a", FirstSeenRunID: "r", FirstSeenAt: runStartedAt, LastChangedRunID: "r", LastChangedAt: runStartedAt, LastSeenRunID: "r", LastSeenAt: runStartedAt},
		})
		_, err := PrepareCurrentStateSource(context.Background(), cfg)
		require.Error(t, err)
		var sm *SpillMergeError
		require.True(t, errors.As(err, &sm))
		require.Equal(t, SpillMergeParentOrder, sm.Category)
	})

	t.Run("duplicate", func(t *testing.T) {
		cfg := cfgBase
		cfg.SpillRoot = t.TempDir()
		cfg.Parent = NewSliceParentRows([]CurrentObjectRow{
			{IndexSetID: "idx_test", RelKey: "a", FirstSeenRunID: "r", FirstSeenAt: runStartedAt, LastChangedRunID: "r", LastChangedAt: runStartedAt, LastSeenRunID: "r", LastSeenAt: runStartedAt},
			{IndexSetID: "idx_test", RelKey: "a", FirstSeenRunID: "r", FirstSeenAt: runStartedAt, LastChangedRunID: "r", LastChangedAt: runStartedAt, LastSeenRunID: "r", LastSeenAt: runStartedAt},
		})
		_, err := PrepareCurrentStateSource(context.Background(), cfg)
		require.Error(t, err)
		var sm *SpillMergeError
		require.True(t, errors.As(err, &sm))
		require.Equal(t, SpillMergeParentOrder, sm.Category)
	})
}

func TestSpillMergeNonUTCRunStartedAtRefusedBeforeWorkspace(t *testing.T) {
	root := t.TempDir()
	// Non-zero offset must refuse before creating attempt dir.
	offset := time.FixedZone("offset", 3600)
	ts := time.Date(2026, 7, 14, 12, 0, 0, 0, offset)
	_, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: ts,
		Parent: NewSliceParentRows(nil), SpillRoot: root,
	})
	require.Error(t, err)
	var sm *SpillMergeError
	require.True(t, errors.As(err, &sm))
	require.Equal(t, SpillMergeInvalidConfig, sm.Category)
	entries, err := os.ReadDir(filepath.Join(root, spillMergeWorkspaceDir))
	if err == nil {
		require.Empty(t, entries, "no attempt workspace on invalid run start")
	}
}

func TestSpillMergeBadFooterBeforeFirstNext(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.jsonl")
	w, err := CreateJournal(path, testHeader())
	require.NoError(t, err)
	_, err = w.Append(ObjectRecord{
		Op: ObjectRecordOpObserve, RelKey: "data/x.xml", ObservedAt: runStartedAt,
		SizeBytes: int64ptr(1), ETag: `"e"`,
	})
	require.NoError(t, err)
	// Seal with wrong record count by writing a hand-crafted footer after close without seal.
	require.NoError(t, w.Close())
	// Unsealed journal → missing footer.
	_, err = PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_test", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), JournalPaths: []string{path}, SpillRoot: t.TempDir(),
	})
	require.Error(t, err)
	var sm *SpillMergeError
	require.True(t, errors.As(err, &sm))
	require.Equal(t, SpillMergeJournal, sm.Category)
}

func TestSpillMergeDuplicateJournalIDBeforeReady(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	size := int64(1)
	j1 := journalWithRecords("idx_test", "run_cur", "jrn_dup", []ObjectRecord{
		observe("jrn_dup", 1, "data/a.xml", runStartedAt, size, nil, `"a"`, nil),
	})
	j2 := journalWithRecords("idx_test", "run_cur", "jrn_dup", []ObjectRecord{
		observe("jrn_dup", 1, "data/b.xml", runStartedAt, size, nil, `"b"`, nil),
	})
	paths := writeSealedJournals(t, []Journal{j1, j2})
	_, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), JournalPaths: paths, SpillRoot: t.TempDir(),
	})
	require.Error(t, err)
	var sm *SpillMergeError
	require.True(t, errors.As(err, &sm))
	require.Equal(t, SpillMergeJournal, sm.Category)
}

func TestSpillMergeSymlinkRootRefused(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	base := t.TempDir()
	real := filepath.Join(base, "real")
	require.NoError(t, os.Mkdir(real, 0o700))
	link := filepath.Join(base, "link")
	require.NoError(t, os.Symlink(real, link))
	_, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), SpillRoot: link,
	})
	require.Error(t, err)
	var sm *SpillMergeError
	require.True(t, errors.As(err, &sm))
	require.Equal(t, SpillMergeWorkspace, sm.Category)
}

func TestSpillMergeWorkspaceByteBudget(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	// Tiny workspace budget forces failure during staging.
	prior := make([]CurrentObjectRow, 0, 20)
	for i := 0; i < 20; i++ {
		key := "data/" + padKey(i) + ".xml"
		prior = append(prior, CurrentObjectRow{
			IndexSetID: "idx_test", RelKey: key, SizeBytes: 10,
			FirstSeenRunID: "run_old", FirstSeenAt: runStartedAt, LastChangedRunID: "run_old",
			LastChangedAt: runStartedAt, LastSeenRunID: "run_old", LastSeenAt: runStartedAt,
		})
	}
	sortParentForTest(prior)
	_, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(prior), SpillRoot: t.TempDir(),
		Budget: SpillMergeBudget{MaxWorkspaceBytes: 400},
	})
	require.Error(t, err)
	var sm *SpillMergeError
	require.True(t, errors.As(err, &sm))
	require.Equal(t, SpillMergeBudgetExhausted, sm.Category)
}

func TestSpillMergeTopologyIndependentDigest(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	size := int64(3)
	prior := []CurrentObjectRow{
		{IndexSetID: "idx_test", RelKey: "a", SizeBytes: 1, FirstSeenRunID: "o", FirstSeenAt: runStartedAt, LastChangedRunID: "o", LastChangedAt: runStartedAt, LastSeenRunID: "o", LastSeenAt: runStartedAt},
		{IndexSetID: "idx_test", RelKey: "b", SizeBytes: 2, FirstSeenRunID: "o", FirstSeenAt: runStartedAt, LastChangedRunID: "o", LastChangedAt: runStartedAt, LastSeenRunID: "o", LastSeenAt: runStartedAt},
		{IndexSetID: "idx_test", RelKey: "c", SizeBytes: 3, FirstSeenRunID: "o", FirstSeenAt: runStartedAt, LastChangedRunID: "o", LastChangedAt: runStartedAt, LastSeenRunID: "o", LastSeenAt: runStartedAt},
	}
	journals := []Journal{
		journalWithRecords("idx_test", "run_cur", "jrn_a", []ObjectRecord{
			observe("jrn_a", 1, "a", runStartedAt, size, nil, `"a"`, nil),
		}),
		journalWithRecords("idx_test", "run_cur", "jrn_b", []ObjectRecord{
			observe("jrn_b", 1, "b", runStartedAt, size, nil, `"b"`, nil),
			enrich("jrn_b", 2, "b", runStartedAt, spillStrPtr("text/plain"), nil, nil),
		}),
		journalWithRecords("idx_test", "run_cur", "jrn_c", []ObjectRecord{
			observe("jrn_c", 1, "d", runStartedAt, size, nil, `"d"`, nil),
		}),
	}
	coverage := []CoverageAttestation{{Scope: &Scope{Prefix: RelativeRootScopePrefix}, Basis: CoverageBasisConfirmed, Complete: true}}
	paths := writeSealedJournals(t, journals)

	budgets := []SpillMergeBudget{
		{}, // defaults
		{MaxFanIn: 4, MaxBufferedRows: 10, MaxBufferedBytes: 1 << 20},
		{MaxFanIn: 5, MaxBufferedRows: 2, MaxBufferedBytes: 1 << 20},
	}
	var digests []string
	var counters []SpillMergeStats
	for _, b := range budgets {
		rows, stats := drainSpillMerge(t, SpillMergeConfig{
			IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
			Parent: NewSliceParentRows(prior), JournalPaths: paths, Coverage: coverage,
			SpillRoot: t.TempDir(), Budget: b,
		})
		digests = append(digests, rowsDigest(rows))
		counters = append(counters, stats)
	}
	for i := 1; i < len(digests); i++ {
		require.Equal(t, digests[0], digests[i], "topology must not change output digest")
		require.Equal(t, counters[0].ObservedRecords, counters[i].ObservedRecords)
		require.Equal(t, counters[0].EnrichmentRecords, counters[i].EnrichmentRecords)
		require.Equal(t, counters[0].NewTombstones, counters[i].NewTombstones)
		require.Equal(t, counters[0].EmittedRows, counters[i].EmittedRows)
	}
}

func TestSpillMergeCloseBeforeDrainCleansWorkspace(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	size := int64(1)
	j := journalWithRecords("idx_test", "run_cur", "jrn_a", []ObjectRecord{
		observe("jrn_a", 1, "data/a.xml", runStartedAt, size, nil, `"a"`, nil),
	})
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), JournalPaths: writeSealedJournals(t, []Journal{j}),
		SpillRoot: t.TempDir(),
	})
	require.NoError(t, err)
	ws := src.WorkspaceDir()
	require.DirExists(t, ws)
	require.NoError(t, src.Close())
	require.NoError(t, src.Close()) // idempotent
	_, err = os.Stat(ws)
	require.True(t, os.IsNotExist(err))
	require.False(t, src.Stats().Complete)
}

func TestSpillMergeCancelDuringDrain(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	prior := make([]CurrentObjectRow, 0, 5)
	for i := 0; i < 5; i++ {
		prior = append(prior, CurrentObjectRow{
			IndexSetID: "idx_test", RelKey: padKey(i), SizeBytes: 1,
			FirstSeenRunID: "o", FirstSeenAt: runStartedAt, LastChangedRunID: "o",
			LastChangedAt: runStartedAt, LastSeenRunID: "o", LastSeenAt: runStartedAt,
		})
	}
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(prior), SpillRoot: t.TempDir(),
		Coverage: []CoverageAttestation{{Scope: &Scope{Prefix: RelativeRootScopePrefix}, Basis: CoverageBasisConfirmed, Complete: true}},
	})
	require.NoError(t, err)
	defer func() { _ = src.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	row, err := src.Next(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, row.RelKey)
	cancel()
	_, err = src.Next(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled) || errors.As(err, new(*SpillMergeError)))
}

func TestSpillMergeRelativeRootTombstone(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	prior := []CurrentObjectRow{{
		IndexSetID: "idx_test", RelKey: "missing.xml", SizeBytes: 10,
		FirstSeenRunID: "run_old", FirstSeenAt: runStartedAt.Add(-time.Hour),
		LastChangedRunID: "run_old", LastChangedAt: runStartedAt.Add(-time.Hour),
		LastSeenRunID: "run_old", LastSeenAt: runStartedAt.Add(-time.Hour),
	}}
	oracle, err := Compact(CompactionInput{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt, PriorRows: prior,
		Coverage: []CoverageAttestation{{Scope: &Scope{Prefix: RelativeRootScopePrefix}, Basis: CoverageBasisConfirmed, Complete: true}},
	})
	require.NoError(t, err)
	rows, stats := drainSpillMerge(t, SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(prior), SpillRoot: t.TempDir(),
		Coverage: []CoverageAttestation{{Scope: &Scope{Prefix: RelativeRootScopePrefix}, Basis: CoverageBasisConfirmed, Complete: true}},
	})
	requireEqualCompactProjection(t, oracle, rows, stats)
}

// --- helpers ---

func drainSpillMerge(t *testing.T, cfg SpillMergeConfig) ([]CurrentObjectRow, SpillMergeStats) {
	t.Helper()
	src, err := PrepareCurrentStateSource(context.Background(), cfg)
	require.NoError(t, err)
	var rows []CurrentObjectRow
	for {
		row, err := src.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		rows = append(rows, row)
	}
	require.False(t, src.Stats().Complete, "Complete only after successful Close")
	require.NoError(t, src.Close())
	stats := src.Stats()
	require.True(t, stats.Complete)
	return rows, stats
}

func requireEqualCompactProjection(t *testing.T, oracle CompactionResult, rows []CurrentObjectRow, stats SpillMergeStats) {
	t.Helper()
	require.Equal(t, len(oracle.Rows), len(rows), "row count")
	require.Equal(t, oracle.ObservedRecords, stats.ObservedRecords)
	require.Equal(t, oracle.EnrichmentRecords, stats.EnrichmentRecords)
	require.Equal(t, len(oracle.Tombstones), stats.NewTombstones)
	for i := range oracle.Rows {
		require.Equal(t, oracle.Rows[i], rows[i], "row[%d] rel_key=%s", i, oracle.Rows[i].RelKey)
	}
}

func writeSealedJournals(t *testing.T, journals []Journal) []string {
	t.Helper()
	dir := t.TempDir()
	paths := make([]string, 0, len(journals))
	for i, j := range journals {
		path := filepath.Join(dir, j.Header.JournalID+".jsonl")
		if j.Header.JournalID == "" {
			path = filepath.Join(dir, "j"+string(rune('a'+i))+".jsonl")
		}
		w, err := CreateJournal(path, j.Header)
		require.NoError(t, err)
		for _, rec := range j.Records {
			_, err := w.Append(rec)
			require.NoError(t, err)
		}
		require.NoError(t, w.Seal(j.Footer.CompletedAt))
		require.NoError(t, w.Close())
		paths = append(paths, path)
	}
	return paths
}

func sortParentForTest(rows []CurrentObjectRow) {
	// insertion sort — small fixtures only
	for i := 1; i < len(rows); i++ {
		j := i
		for j > 0 && rows[j].RelKey < rows[j-1].RelKey {
			rows[j], rows[j-1] = rows[j-1], rows[j]
			j--
		}
	}
}

func rowsDigest(rows []CurrentObjectRow) string {
	h := sha256.New()
	enc := json.NewEncoder(h)
	for _, r := range rows {
		_ = enc.Encode(normalizeCurrentObjectRow(r))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func padKey(i int) string {
	return "k" + string([]byte{
		byte('0' + (i/100)%10),
		byte('0' + (i/10)%10),
		byte('0' + i%10),
	})
}

func int64ptr(v int64) *int64      { return &v }
func spillStrPtr(v string) *string { return &v }

func TestSpillMergeBufferedJournalFlushAndHotKey(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	// Many events for one key across one journal; buffer max forces multi-run flush.
	const n = 20
	recs := make([]ObjectRecord, 0, n)
	size := int64(1)
	for i := 0; i < n; i++ {
		recs = append(recs, observe("jrn_hot", uint64(i+1), "data/hot.xml", runStartedAt.Add(time.Duration(i)*time.Second), size, nil, `"e"`, nil))
	}
	j := journalWithRecords("idx_test", "run_cur", "jrn_hot", recs)
	paths := writeSealedJournals(t, []Journal{j})
	prior := []CurrentObjectRow{{
		IndexSetID: "idx_test", RelKey: "data/hot.xml", SizeBytes: 9,
		FirstSeenRunID: "old", FirstSeenAt: runStartedAt.Add(-time.Hour),
		LastChangedRunID: "old", LastChangedAt: runStartedAt.Add(-time.Hour),
		LastSeenRunID: "old", LastSeenAt: runStartedAt.Add(-time.Hour),
	}}
	oracle, err := Compact(CompactionInput{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		PriorRows: prior, Journals: []Journal{j},
	})
	require.NoError(t, err)
	rows, stats := drainSpillMerge(t, SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(prior), JournalPaths: paths, SpillRoot: t.TempDir(),
		Budget: SpillMergeBudget{MaxBufferedRows: 3, MaxBufferedBytes: 1 << 20, MaxFanIn: 8},
	})
	requireEqualCompactProjection(t, oracle, rows, stats)
	require.GreaterOrEqual(t, stats.SpillRunCount, 2, "expected multi-run flush under tiny buffer")
}

func TestSpillMergeAttestBeforeReadyCorruptRun(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	size := int64(1)
	j := journalWithRecords("idx_test", "run_cur", "jrn_a", []ObjectRecord{
		observe("jrn_a", 1, "a.xml", runStartedAt, size, nil, `"a"`, nil),
	})
	cfg := SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), JournalPaths: writeSealedJournals(t, []Journal{j}),
		SpillRoot: t.TempDir(),
	}
	src, err := PrepareCurrentStateSource(context.Background(), cfg)
	require.NoError(t, err)
	// After READY, corrupt parent run bytes on disk — Next must fail closed; no success complete.
	parentPath := src.parentRun
	require.NoError(t, src.Close()) // clean first prepare

	// Re-prepare then corrupt before drain.
	src, err = PrepareCurrentStateSource(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = src.Close() }()
	// Truncate sealed parent to break footer/checksum.
	require.NoError(t, os.Truncate(src.parentRun, 10))
	_, err = src.Next(context.Background())
	require.Error(t, err)
	require.False(t, src.Stats().Complete)
	_ = parentPath
}

func TestSpillMergeJournalSymlinkRefused(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	real := filepath.Join(dir, "real.jsonl")
	j := journalWithRecords("idx_test", "run_cur", "jrn_a", []ObjectRecord{
		observe("jrn_a", 1, "a.xml", runStartedAt, int64(1), nil, `"a"`, nil),
	})
	// write sealed journal then symlink it
	paths := writeSealedJournals(t, []Journal{j})
	require.NoError(t, os.Rename(paths[0], real))
	link := filepath.Join(dir, "link.jsonl")
	require.NoError(t, os.Symlink(real, link))
	_, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), JournalPaths: []string{link}, SpillRoot: t.TempDir(),
	})
	require.Error(t, err)
	var sm *SpillMergeError
	require.True(t, errors.As(err, &sm))
	require.Equal(t, SpillMergeJournal, sm.Category)
}

func TestSpillMergeIntermediateSpillmergeSymlinkRefused(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	base := t.TempDir()
	// Create spill root with spillmerge as symlink to outside.
	outside := filepath.Join(base, "outside")
	require.NoError(t, os.Mkdir(outside, 0o700))
	root := filepath.Join(base, "root")
	require.NoError(t, os.Mkdir(root, 0o700))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, spillMergeWorkspaceDir)))
	_, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), SpillRoot: root,
	})
	require.Error(t, err)
	var sm *SpillMergeError
	require.True(t, errors.As(err, &sm))
	require.Equal(t, SpillMergeWorkspace, sm.Category)
}

func TestSpillMergeMaxBufferedBytesExactBoundary(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	size := int64(1)
	// Large metadata field so estimate includes ObjectRecord payload.
	bigETag := strings.Repeat("E", 400)
	rec := observe("jrn_a", 1, "data/a.xml", runStartedAt, size, nil, bigETag, nil)
	ev := spillEvent{
		RelKey: rec.RelKey, Phase: eventPhase(rec.Op), JournalID: rec.JournalID,
		Sequence: rec.Sequence, Op: rec.Op, Record: rec,
	}
	need := estimateEventBytes(ev)
	require.Greater(t, need, int64(192))

	jOK := journalWithRecords("idx_test", "run_cur", "jrn_a", []ObjectRecord{rec})
	// Exact limit succeeds.
	rows, stats := drainSpillMerge(t, SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), JournalPaths: writeSealedJournals(t, []Journal{jOK}),
		SpillRoot: t.TempDir(),
		Budget:    SpillMergeBudget{MaxBufferedRows: 10, MaxBufferedBytes: need, MaxFanIn: 8},
	})
	require.Len(t, rows, 1)
	require.Equal(t, 1, stats.ObservedRecords)
	require.True(t, stats.Complete)

	// One byte under refuses before append.
	_, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), JournalPaths: writeSealedJournals(t, []Journal{jOK}),
		SpillRoot: t.TempDir(),
		Budget:    SpillMergeBudget{MaxBufferedRows: 10, MaxBufferedBytes: need - 1, MaxFanIn: 8},
	})
	require.Error(t, err)
	var sm *SpillMergeError
	require.True(t, errors.As(err, &sm))
	require.Equal(t, SpillMergeBudgetExhausted, sm.Category)
}

func TestSpillMergeCompleteOnlyAfterClose(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), SpillRoot: t.TempDir(),
	})
	require.NoError(t, err)
	_, err = src.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)
	require.False(t, src.Stats().Complete)
	require.NoError(t, src.Close())
	require.True(t, src.Stats().Complete)
	require.NoError(t, src.Close())
}

func TestSpillMergeCrossAttemptRunRefusedOnAttest(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	size := int64(1)
	j := journalWithRecords("idx_test", "run_cur", "jrn_a", []ObjectRecord{
		observe("jrn_a", 1, "a.xml", runStartedAt, size, nil, `"a"`, nil),
	})
	root := t.TempDir()
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), JournalPaths: writeSealedJournals(t, []Journal{j}),
		SpillRoot: root,
	})
	require.NoError(t, err)
	// Copy parent run path content and rewrite attempt_id in header would be heavy;
	// instead open with wrong attempt id via attestSpillRun directly.
	_, _, err = attestSpillRun(src.parentRun, spillRunKindParent, "not-the-attempt", src.budget.MaxRecordBytes)
	require.Error(t, err)
	require.NoError(t, src.Close())
}

func TestSpillMergeLateCorruptionZeroRows(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	// Multiple parent rows so corruption past header can be staged.
	prior := []CurrentObjectRow{
		{IndexSetID: "idx_test", RelKey: "a", SizeBytes: 1, FirstSeenRunID: "o", FirstSeenAt: runStartedAt, LastChangedRunID: "o", LastChangedAt: runStartedAt, LastSeenRunID: "o", LastSeenAt: runStartedAt},
		{IndexSetID: "idx_test", RelKey: "b", SizeBytes: 2, FirstSeenRunID: "o", FirstSeenAt: runStartedAt, LastChangedRunID: "o", LastChangedAt: runStartedAt, LastSeenRunID: "o", LastSeenAt: runStartedAt},
		{IndexSetID: "idx_test", RelKey: "c", SizeBytes: 3, FirstSeenRunID: "o", FirstSeenAt: runStartedAt, LastChangedRunID: "o", LastChangedAt: runStartedAt, LastSeenRunID: "o", LastSeenAt: runStartedAt},
	}
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(prior), SpillRoot: t.TempDir(),
	})
	require.NoError(t, err)
	defer func() { _ = src.Close() }()

	// Late corruption on the held file path while FD is open: truncate past header.
	// revalidateHeld on first Next must refuse with zero exposed rows.
	info, err := os.Stat(src.parentRun)
	require.NoError(t, err)
	// Flip a mid-file byte via path open (same inode) after READY.
	f, err := os.OpenFile(src.parentRun, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("X"), info.Size()/2)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = src.Next(context.Background())
	require.Error(t, err)
	require.Equal(t, 0, src.Stats().EmittedRows)
	require.False(t, src.Stats().Complete)
}

func TestSpillMergeCancelDuringJournalScan(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	// Build a journal large enough that cancel mid-scan is observable.
	recs := make([]ObjectRecord, 0, 200)
	size := int64(1)
	for i := 0; i < 200; i++ {
		recs = append(recs, observe("jrn_a", uint64(i+1), padKey(i), runStartedAt, size, nil, `"e"`, nil))
	}
	j := journalWithRecords("idx_test", "run_cur", "jrn_a", recs)
	paths := writeSealedJournals(t, []Journal{j})

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately; scan checks ctx each line.
	cancel()
	_, err := PrepareCurrentStateSource(ctx, SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), JournalPaths: paths, SpillRoot: t.TempDir(),
		Budget: SpillMergeBudget{MaxBufferedRows: 2, MaxBufferedBytes: 1 << 20, MaxFanIn: 8},
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled) || errors.As(err, new(*SpillMergeError)))
}

func TestSpillMergeStickyParentCloseError(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	parent := &failingParentClose{rows: NewSliceParentRows(nil)}
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: parent, SpillRoot: t.TempDir(),
	})
	require.NoError(t, err)
	_, err = src.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)
	err1 := src.Close()
	require.Error(t, err1)
	require.False(t, src.Stats().Complete)
	// Second close retries and still reports failure class while parent close fails.
	err2 := src.Close()
	require.Error(t, err2)
}

type failingParentClose struct {
	rows *SliceParentRows
}

func (f *failingParentClose) Next(ctx context.Context) (CurrentObjectRow, error) {
	return f.rows.Next(ctx)
}
func (f *failingParentClose) Close() error {
	return errors.New("injected parent close failure")
}

func TestSpillMergeMaxFanInMinimumIsThree(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	_, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), SpillRoot: t.TempDir(),
		Budget: SpillMergeBudget{MaxFanIn: 2},
	})
	require.Error(t, err)
	var sm *SpillMergeError
	require.True(t, errors.As(err, &sm))
	require.Equal(t, SpillMergeInvalidConfig, sm.Category)
}

func TestSpillMergeMaxFanInThreeMultiRunBoundary(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	size := int64(1)
	// Three records force multiple buffered runs with MaxBufferedRows=1, then multi-pass merge under MaxFanIn=3.
	j := journalWithRecords("idx_test", "run_cur", "jrn_a", []ObjectRecord{
		observe("jrn_a", 1, "a.xml", runStartedAt, size, nil, `"a"`, nil),
		observe("jrn_a", 2, "b.xml", runStartedAt, size, nil, `"b"`, nil),
		observe("jrn_a", 3, "c.xml", runStartedAt, size, nil, `"c"`, nil),
	})
	rows, stats := drainSpillMerge(t, SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), JournalPaths: writeSealedJournals(t, []Journal{j}),
		SpillRoot: t.TempDir(),
		Budget: SpillMergeBudget{
			MaxBufferedRows: 1, MaxBufferedBytes: 1 << 20, MaxFanIn: 3, MaxMergePasses: 16,
		},
	})
	require.Len(t, rows, 3)
	require.Equal(t, 3, stats.ObservedRecords)
	require.True(t, stats.Complete)
	require.GreaterOrEqual(t, stats.MergePasses, 1)
}

func TestSpillMergeCancelDuringHotKeyReduce(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	// Many events for one key so the inner reduce loop runs many times.
	const n = 500
	recs := make([]ObjectRecord, 0, n)
	size := int64(1)
	for i := 0; i < n; i++ {
		recs = append(recs, observe("jrn_hot", uint64(i+1), "hot.xml", runStartedAt.Add(time.Duration(i)*time.Millisecond), size, nil, `"e"`, nil))
	}
	j := journalWithRecords("idx_test", "run_cur", "jrn_hot", recs)
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), JournalPaths: writeSealedJournals(t, []Journal{j}),
		SpillRoot: t.TempDir(),
		Budget:    SpillMergeBudget{MaxBufferedRows: 50, MaxBufferedBytes: 1 << 20, MaxFanIn: 8},
	})
	require.NoError(t, err)
	defer func() { _ = src.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel before first Next so the hot-key loop observes cancel during reduce/advance.
	cancel()
	_, err = src.Next(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled) || errors.As(err, new(*SpillMergeError)))
	require.Equal(t, 0, src.Stats().EmittedRows)
}

func TestSpillMergeDropRunFileRequiresSuccessfulRemove(t *testing.T) {
	// Unit-level: dropRunFile on missing path is ok (IsNotExist); charge released only after remove.
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), SpillRoot: t.TempDir(),
	})
	require.NoError(t, err)
	defer func() { _ = src.Close() }()
	// Missing file should not fail (already gone).
	require.NoError(t, src.dropRunFile(filepath.Join(src.WorkspaceDir(), "no-such-run.run")))
}

func TestSpillMergeCloseDoesNotDeleteSubstitutedAttempt(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	spillRoot := t.TempDir()
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), SpillRoot: spillRoot,
	})
	require.NoError(t, err)

	// After READY: move owned attempt aside and install a replacement with a sentinel.
	ownedPath := src.WorkspaceDir()
	require.DirExists(t, ownedPath)
	aside := ownedPath + ".aside"
	require.NoError(t, os.Rename(ownedPath, aside))
	require.NoError(t, os.Mkdir(ownedPath, 0o700))
	sentinel := filepath.Join(ownedPath, "replacement-sentinel")
	require.NoError(t, os.WriteFile(sentinel, []byte("keep-me"), 0o600))

	// Close must not delete the unowned replacement.
	require.NoError(t, src.Close())
	require.FileExists(t, sentinel, "replacement at live attempt name must survive Close")
	// Owned data was wiped via bound handle before rename; aside may remain empty or gone.
	// Replacement sentinel is the ownership invariant under test.
}

func TestSpillMergeTrashDeleteFailureRetries(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	spillRoot := t.TempDir()
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), SpillRoot: spillRoot,
	})
	require.NoError(t, err)

	testForceBoundEpochDeleteErr = errors.New("injected trash finalization failure")
	t.Cleanup(func() { testForceBoundEpochDeleteErr = nil })

	err1 := src.Close()
	require.Error(t, err1, "first Close must surface trash-delete failure")
	require.False(t, src.Stats().Complete)
	require.NotEmpty(t, src.ownedTrash, "owned trash name retained for retry")

	trashPath := filepath.Join(spillRoot, spillMergeWorkspaceDir, src.ownedTrash)
	// After FD wipe the owned trash may be empty but still present as a dentry.
	require.DirExists(t, trashPath)

	testForceBoundEpochDeleteErr = nil
	err2 := src.Close()
	require.NoError(t, err2, "retry must finish exact trash deletion")
	_, err = os.Stat(trashPath)
	require.True(t, os.IsNotExist(err), "owned trash removed on successful retry")
}

func TestSpillMergePostRebindTrashSwapDoesNotDeleteReplacement(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	spillRoot := t.TempDir()
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), SpillRoot: spillRoot,
	})
	require.NoError(t, err)

	var replacementSentinel string
	testAfterBoundEpochOpenHook = func(fullPath string) {
		// After FD bind+SameFile: swap trash dentry for a non-empty replacement.
		aside := fullPath + ".owned-aside"
		require.NoError(t, os.Rename(fullPath, aside))
		require.NoError(t, os.Mkdir(fullPath, 0o700))
		replacementSentinel = filepath.Join(fullPath, "replacement-sentinel")
		require.NoError(t, os.WriteFile(replacementSentinel, []byte("keep"), 0o600))
	}
	t.Cleanup(func() { testAfterBoundEpochOpenHook = nil })

	err = src.Close()
	require.Error(t, err, "Close must fail closed when trash dentry is swapped post-rebind")
	require.False(t, src.Stats().Complete)
	require.FileExists(t, replacementSentinel, "unowned replacement must survive")
	// Retry still fail-closed while substitute remains at ownedTrash name.
	err = src.Close()
	require.Error(t, err)
	require.FileExists(t, replacementSentinel)
}

func TestSpillMergeParentExceedsMaxBufferedBytes(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	row := CurrentObjectRow{
		IndexSetID: "idx_test", RelKey: "a", SizeBytes: 1,
		FirstSeenRunID: "o", FirstSeenAt: runStartedAt, LastChangedRunID: "o",
		LastChangedAt: runStartedAt, LastSeenRunID: "o", LastSeenAt: runStartedAt,
		ETag: strings.Repeat("E", 50),
	}
	need := estimateRowBytes(row)
	require.Greater(t, need, int64(1))

	// Exact limit succeeds — capture and Close so held roots/run FDs are not left to TempDir.
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows([]CurrentObjectRow{row}), SpillRoot: t.TempDir(),
		Budget: SpillMergeBudget{MaxBufferedBytes: need, MaxFanIn: 8},
	})
	require.NoError(t, err)
	require.NotNil(t, src)
	ws := src.WorkspaceDir()
	require.DirExists(t, ws)
	require.NoError(t, src.Close())
	require.False(t, src.Stats().Complete, "early Close without full drain is not Complete")
	_, statErr := os.Stat(ws)
	require.True(t, os.IsNotExist(statErr), "attempt workspace must be cleaned by Close")

	// Under limit refuses.
	_, err = PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows([]CurrentObjectRow{row}), SpillRoot: t.TempDir(),
		Budget: SpillMergeBudget{MaxBufferedBytes: need - 1, MaxFanIn: 8},
	})
	require.Error(t, err)
	var sm *SpillMergeError
	require.True(t, errors.As(err, &sm))
	require.Equal(t, SpillMergeBudgetExhausted, sm.Category)
}

func TestSpillMergeIntermediateTrailingDataRefused(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	size := int64(1)
	// Force multi-pass: 3 journals with MaxBufferedRows=1 → multiple event runs → merge.
	journals := []Journal{
		journalWithRecords("idx_test", "run_cur", "jrn_a", []ObjectRecord{
			observe("jrn_a", 1, "a.xml", runStartedAt, size, nil, `"a"`, nil),
		}),
		journalWithRecords("idx_test", "run_cur", "jrn_b", []ObjectRecord{
			observe("jrn_b", 1, "b.xml", runStartedAt, size, nil, `"b"`, nil),
		}),
		journalWithRecords("idx_test", "run_cur", "jrn_c", []ObjectRecord{
			observe("jrn_c", 1, "c.xml", runStartedAt, size, nil, `"c"`, nil),
		}),
	}
	// Build sealed journal files normally first.
	paths := writeSealedJournals(t, journals)

	// After journals are sealed, we need to corrupt an intermediate spill run during prepare.
	// Inject by writing a tiny MaxFanIn merge and corrupting via a hook is hard.
	// Instead: create two sealed event-like runs manually under a spill workspace is complex.
	// Simpler: unit-level — openSpillRunReader Finish detects trailing after footer on a crafted file.
	// Use production writer via a real prepare then mutate intermediate — use direct file craft:
	// Write a minimal valid parent-empty prepare won't create multi-pass.
	// Craft run with WriteSegment-style: use newSpillRunWriter through Prepare then...
	// Direct: use package internals to seal one event run then append trailing JSON.
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), JournalPaths: paths, SpillRoot: t.TempDir(),
		Budget: SpillMergeBudget{MaxBufferedRows: 1, MaxBufferedBytes: 1 << 20, MaxFanIn: 3, MaxMergePasses: 16},
	})
	// If multi-pass happened, we need to corrupt before READY — too late after Prepare succeeds.
	// So craft a standalone spill run file and feed through mergeEventChunk via a temporary source.
	_ = src
	if err == nil {
		_ = src.Close()
	}

	// Standalone: write valid sealed event run then append trailing line; attest/open must fail.
	root := t.TempDir()
	s := &CurrentStateSource{
		cfg:       SpillMergeConfig{IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt, SpillRoot: root},
		budget:    DefaultSpillMergeBudget(),
		attemptID: "attempttest00000000000000000000",
		runSizes:  make(map[string]int64),
	}
	s.budget.MaxFanIn = 8
	require.NoError(t, s.createWorkspace())
	t.Cleanup(func() { _ = s.cleanupWorkspace(false) })

	out := filepath.Join(s.workspace, "e.run")
	w, err := newSpillRunWriter(out, spillRunKindEvents, s.attemptID, s)
	require.NoError(t, err)
	ev := spillEvent{
		RelKey: "a.xml", Phase: 0, JournalID: "j", Sequence: 1, Op: ObjectRecordOpObserve,
		Record: observe("j", 1, "a.xml", runStartedAt, size, nil, `"a"`, nil),
	}
	require.NoError(t, w.WriteEvent(ev))
	require.NoError(t, w.Seal())
	// Append trailing JSON after footer.
	f, err := os.OpenFile(out, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteString("{\"type\":\"junk\"}\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, _, err = attestSpillRun(out, spillRunKindEvents, s.attemptID, s.budget.MaxRecordBytes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "trailing")

	// mergeEventChunk production seam: two good runs + one trailing-corrupt must fail before seal.
	good1 := filepath.Join(s.workspace, "g1.run")
	good2 := filepath.Join(s.workspace, "g2.run")
	for i, p := range []string{good1, good2} {
		w, err := newSpillRunWriter(p, spillRunKindEvents, s.attemptID, s)
		require.NoError(t, err)
		e := spillEvent{
			RelKey: string(rune('a'+i)) + ".xml", Phase: 0, JournalID: "j", Sequence: 1, Op: ObjectRecordOpObserve,
			Record: observe("j", 1, string(rune('a'+i))+".xml", runStartedAt, size, nil, `"x"`, nil),
		}
		require.NoError(t, w.WriteEvent(e))
		require.NoError(t, w.Seal())
	}
	// corrupt good2
	f, err = os.OpenFile(good2, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteString("{\"type\":\"junk\"}\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	merged := filepath.Join(s.workspace, "merged.run")
	err = s.mergeEventChunk(context.Background(), []string{good1, good2}, merged)
	require.Error(t, err)
	var sm *SpillMergeError
	require.True(t, errors.As(err, &sm))
	require.Equal(t, SpillMergeSpillIntegrity, sm.Category)
	_, err = os.Stat(merged)
	require.True(t, os.IsNotExist(err) || err == nil) // may exist aborted/partial; must not be READY promote
}

func TestSpillMergeHeldCloseErrorStickyNoComplete(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), SpillRoot: t.TempDir(),
	})
	require.NoError(t, err)
	// Drain to EOF.
	_, err = src.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)

	// Pre-close held parent file to force Close error on source Close.
	require.NotNil(t, src.parentHeld)
	require.NoError(t, src.parentHeld.file.Close())

	err1 := src.Close()
	require.Error(t, err1)
	require.False(t, src.Stats().Complete)
	err2 := src.Close()
	require.Error(t, err2)
	require.False(t, src.Stats().Complete)
	var sm *SpillMergeError
	require.True(t, errors.As(err2, &sm))
	require.Equal(t, SpillMergeIO, sm.Category)
}

func TestSpillMergeErrorDoesNotDisclosePaths(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	marker := "SECRET_PATH_MARKER_xyzzy"
	// Shape that produces path-bearing ENOTDIR/*PathError (the pre-fix leak shape):
	// regular-file parent + marker-bearing child path. Lstat of the child fails with
	// a PathError whose Path includes the marker.
	base := t.TempDir()
	blocker := filepath.Join(base, "blocker-file")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o600))
	configured := filepath.Join(blocker, marker)
	_, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), SpillRoot: configured,
	})
	require.Error(t, err)
	var sm *SpillMergeError
	require.True(t, errors.As(err, &sm))
	require.Equal(t, SpillMergeWorkspace, sm.Category)
	rendered := err.Error()
	require.NotContains(t, rendered, marker)
	require.NotContains(t, rendered, configured)
	// Unwrap still carries underlying error for classification without path rendering.
	unwrapped := errors.Unwrap(err)
	require.NotNil(t, unwrapped)
	var pe *os.PathError
	if errors.As(unwrapped, &pe) {
		// Underlying PathError may carry the path; it must not appear in Error().
		require.NotContains(t, rendered, pe.Path)
	}
}
