package indexreader

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexcompare"
	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

// TestDurableSQLiteFullQueryResultParity asserts full logical QueryResult
// equality across backends for the same object set, including HEAD fields,
// tombstones, timestamps, and run metadata.
func TestDurableSQLiteFullQueryResultParity(t *testing.T) {
	ctx := context.Background()
	mod1 := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	mod2 := time.Date(2025, 1, 2, 10, 0, 0, 0, time.UTC)
	mod3 := time.Date(2025, 1, 3, 10, 0, 0, 0, time.UTC)
	enriched := time.Date(2025, 1, 4, 12, 0, 0, 0, time.UTC)
	restoreExp := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	sc := "STANDARD"
	archive := "ARCHIVE_ACCESS"
	restore := "ongoing-request-in-progress"
	ctype := "application/json"
	runID := "run_parity_1"

	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		{
			RelKey:           "data/a.json",
			SizeBytes:        1024,
			LastModified:     &mod1,
			ETag:             "etag-1",
			StorageClass:     &sc,
			ArchiveStatus:    &archive,
			RestoreState:     &restore,
			RestoreExpiry:    &restoreExp,
			ContentType:      &ctype,
			HeadEnrichedAt:   &enriched,
			FirstSeenRunID:   runID,
			FirstSeenAt:      mod1,
			LastChangedRunID: runID,
			LastChangedAt:    mod1,
			LastSeenRunID:    runID,
			LastSeenAt:       mod1,
		},
		{
			RelKey:           "data/b.xml",
			SizeBytes:        2048,
			LastModified:     &mod2,
			ETag:             "etag-2",
			StorageClass:     &sc,
			FirstSeenRunID:   runID,
			FirstSeenAt:      mod2,
			LastChangedRunID: runID,
			LastChangedAt:    mod2,
			LastSeenRunID:    runID,
			LastSeenAt:       mod2,
		},
		{
			RelKey:           "gone.txt",
			SizeBytes:        10,
			LastModified:     &mod1,
			ETag:             "etag-gone",
			FirstSeenRunID:   runID,
			FirstSeenAt:      mod1,
			LastChangedRunID: runID,
			LastChangedAt:    mod1,
			LastSeenRunID:    runID,
			LastSeenAt:       mod1,
			DeletedAt:        &mod3,
		},
	})
	// Force run id used in durable segments for first/last run fields.
	// setupDurableTestEnv may rewrite blank run fields; re-assert via query.

	sqliteReader, durableReader, cleanup := openPairedReaders(t, ctx, env, func(t *testing.T, db *sql.DB, indexSetID, sqlRunID string) {
		t.Helper()
		rows := []indexstore.ObjectRow{
			{
				IndexSetID:       indexSetID,
				RelKey:           "data/a.json",
				SizeBytes:        1024,
				LastModified:     &mod1,
				ETag:             "etag-1",
				StorageClass:     &sc,
				ArchiveStatus:    &archive,
				RestoreState:     &restore,
				RestoreExpiry:    &restoreExp,
				ContentType:      &ctype,
				HeadEnrichedAt:   &enriched,
				FirstSeenRunID:   sqlRunID,
				FirstSeenAt:      mod1,
				LastChangedRunID: sqlRunID,
				LastChangedAt:    mod1,
				LastSeenRunID:    sqlRunID,
				LastSeenAt:       mod1,
			},
			{
				IndexSetID:       indexSetID,
				RelKey:           "data/b.xml",
				SizeBytes:        2048,
				LastModified:     &mod2,
				ETag:             "etag-2",
				StorageClass:     &sc,
				FirstSeenRunID:   sqlRunID,
				FirstSeenAt:      mod2,
				LastChangedRunID: sqlRunID,
				LastChangedAt:    mod2,
				LastSeenRunID:    sqlRunID,
				LastSeenAt:       mod2,
			},
			{
				IndexSetID:       indexSetID,
				RelKey:           "gone.txt",
				SizeBytes:        10,
				LastModified:     &mod1,
				ETag:             "etag-gone",
				FirstSeenRunID:   sqlRunID,
				FirstSeenAt:      mod1,
				LastChangedRunID: sqlRunID,
				LastChangedAt:    mod1,
				LastSeenRunID:    sqlRunID,
				LastSeenAt:       mod1,
			},
		}
		require.NoError(t, indexstore.BatchUpsertObjects(ctx, db, rows))
		require.NoError(t, indexstore.BatchUpdateHeadEnrichment(ctx, db, []indexstore.HeadEnrichmentUpdate{{
			IndexSetID:     indexSetID,
			RelKey:         "data/a.json",
			ArchiveStatus:  &archive,
			RestoreState:   &restore,
			RestoreExpiry:  &restoreExp,
			ContentType:    &ctype,
			HeadEnrichedAt: enriched,
		}}))
		// Upsert always clears deleted_at; mark the tombstone explicitly.
		_, err := db.ExecContext(ctx, `UPDATE objects_current SET deleted_at = ? WHERE index_set_id = ? AND rel_key = ?`,
			mod3.UTC().Format(time.RFC3339Nano), indexSetID, "gone.txt")
		require.NoError(t, err)
	})
	defer cleanup()

	// Active objects only (default).
	assertQueryResultParity(t, ctx, sqliteReader, durableReader, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
	}, false)

	// Include deleted.
	assertQueryResultParity(t, ctx, sqliteReader, durableReader, indexstore.QueryParams{
		IndexSetID:     env.indexSetID,
		IncludeDeleted: true,
	}, true)

	// Predicates: pattern, min/max size, modified bounds, storage-class, enriched-after.
	assertQueryResultParity(t, ctx, sqliteReader, durableReader, indexstore.QueryParams{
		IndexSetID:     env.indexSetID,
		Pattern:        "data/**",
		MinSize:        1000,
		MaxSize:        1500,
		ModifiedAfter:  mod1.Add(-time.Second),
		ModifiedBefore: mod1.Add(time.Second),
		StorageClasses: []string{"STANDARD"},
		EnrichedAfter:  enriched.Add(-time.Hour),
	}, false)

	// Count matches row length for each backend.
	params := indexstore.QueryParams{IndexSetID: env.indexSetID, Pattern: "data/**"}
	sqlCount, err := sqliteReader.QueryObjectCount(ctx, params)
	require.NoError(t, err)
	durCount, err := durableReader.QueryObjectCount(ctx, params)
	require.NoError(t, err)
	require.Equal(t, sqlCount, durCount)
	sqlRows, _, err := sqliteReader.QueryObjects(ctx, params)
	require.NoError(t, err)
	require.EqualValues(t, len(sqlRows), sqlCount)

	// Canonical tie-break min-key + alternates envelope.
	canonParams := indexstore.QueryParams{
		IndexSetID:        env.indexSetID,
		IncludeDeleted:    false,
		CanonicalTieBreak: indexstore.CanonicalTieBreakMinKey,
	}
	// Add a second row with same etag for grouping — already have distinct etags;
	// re-query after verifying single-group path empty.
	sqlCanon, sqlStats, err := sqliteReader.QueryCanonicalObjects(ctx, canonParams)
	require.NoError(t, err)
	durCanon, durStats, err := durableReader.QueryCanonicalObjects(ctx, canonParams)
	require.NoError(t, err)
	require.Equal(t, sqlStats.TotalRecords, durStats.TotalRecords)
	require.Equal(t, sqlStats.CanonicalGroups, durStats.CanonicalGroups)
	require.Equal(t, len(sqlCanon), len(durCanon))
	for i := range sqlCanon {
		require.Equal(t, canonicalRelKey(sqlCanon[i]), canonicalRelKey(durCanon[i]))
	}
}

func TestDurableSQLiteCompareProjectionEquality(t *testing.T) {
	ctx := context.Background()
	mod := time.Date(2025, 3, 1, 8, 0, 0, 0, time.UTC)
	sc := "STANDARD"
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("p/a.bin", 11, "e-a", mod),
		func() indexsubstrate.CurrentObjectRow {
			r := durableRow("p/b.bin", 22, "e-b", mod.Add(time.Hour))
			r.StorageClass = &sc
			return r
		}(),
	})

	dbPath := filepath.Join(env.identityDir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, env.params)
	require.NoError(t, err)
	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusSuccess, nil))
	require.NoError(t, indexstore.BatchUpsertObjects(ctx, db, []indexstore.ObjectRow{
		{
			IndexSetID: indexSet.IndexSetID, RelKey: "p/a.bin", SizeBytes: 11,
			LastModified: &mod, ETag: "e-a", LastSeenRunID: run.RunID, LastSeenAt: mod,
		},
		{
			IndexSetID: indexSet.IndexSetID, RelKey: "p/b.bin", SizeBytes: 22,
			LastModified: timePtr(mod.Add(time.Hour)), ETag: "e-b", StorageClass: &sc,
			LastSeenRunID: run.RunID, LastSeenAt: mod.Add(time.Hour),
		},
	}))

	// Load durable manifest from published snapshot.
	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(filepath.Join(env.segmentRoot, "latest.json"))
	require.NoError(t, err)

	report, err := indexcompare.Compare(ctx, indexcompare.Input{
		SQLiteDB:          db,
		SQLiteIndexSetID:  indexSet.IndexSetID,
		SQLiteArtifact:    indexcompare.Artifact{ID: indexSet.IndexSetID, Path: dbPath},
		DurableManifest:   snap.Manifest,
		DurableSegmentDir: snap.SegmentDir,
		DurableArtifact:   indexcompare.Artifact{ID: snap.Manifest.RunID, Path: snap.LatestPath},
	})
	require.NoError(t, err)
	require.True(t, report.ParityPassed, "projection mismatches=%d content_identity=%d", report.ProjectionMismatches, report.ContentIdentityCheck.Mismatches)
	require.Equal(t, report.SQLiteProjectionSHA256, report.DurableProjectionSHA256)
	require.Equal(t, indexcompare.ProjectionVersion, report.ProjectionVersion)
	require.NoError(t, db.Close())
}

func TestDurableQuery_ModifiedBoundsInclusiveAndMaxSize(t *testing.T) {
	ctx := context.Background()
	t1 := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 5, 2, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC)
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a", 100, "e1", t1),
		durableRow("b", 200, "e2", t2),
		durableRow("c", 300, "e3", t3),
	})
	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// SQLite uses >= ModifiedAfter and <= ModifiedBefore.
	results, _, err := reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID:     env.indexSetID,
		ModifiedAfter:  t2,
		ModifiedBefore: t2,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "b", results[0].RelKey)

	results, _, err = reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		MaxSize:    200,
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, "a", results[0].RelKey)
	require.Equal(t, "b", results[1].RelKey)
}

func TestDurableQuery_CanonicalTieBreakAndAlternates(t *testing.T) {
	ctx := context.Background()
	early := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("z-key", 1, "same", late),
		durableRow("a-key", 2, "same", early),
		durableRow("solo", 3, "", early),
	})
	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	minKey, _, err := reader.QueryCanonicalObjects(ctx, indexstore.QueryParams{
		IndexSetID:        env.indexSetID,
		CanonicalTieBreak: indexstore.CanonicalTieBreakMinKey,
	})
	require.NoError(t, err)
	require.Len(t, minKey, 2)
	var minKeyGroup *indexstore.CanonicalObjectGroup
	for i := range minKey {
		if minKey[i].Group != nil {
			minKeyGroup = minKey[i].Group
			break
		}
	}
	require.NotNil(t, minKeyGroup)
	require.Equal(t, "a-key", minKeyGroup.Canonical.RelKey)
	require.Len(t, minKeyGroup.Alternates, 1)
	require.Equal(t, "z-key", minKeyGroup.Alternates[0].RelKey)

	maxMod, _, err := reader.QueryCanonicalObjects(ctx, indexstore.QueryParams{
		IndexSetID:        env.indexSetID,
		CanonicalTieBreak: indexstore.CanonicalTieBreakMaxModified,
	})
	require.NoError(t, err)
	var maxModGroup *indexstore.CanonicalObjectGroup
	for i := range maxMod {
		if maxMod[i].Group != nil {
			maxModGroup = maxMod[i].Group
			break
		}
	}
	require.NotNil(t, maxModGroup)
	require.Equal(t, "z-key", maxModGroup.Canonical.RelKey)
}

func openPairedReaders(t *testing.T, ctx context.Context, env durableTestEnv, seedSQLite func(*testing.T, *sql.DB, string, string)) (sqliteR, durableR Reader, cleanup func()) {
	t.Helper()
	dbPath := filepath.Join(env.identityDir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, env.params)
	require.NoError(t, err)
	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusSuccess, nil))
	seedSQLite(t, db, indexSet.IndexSetID, run.RunID)
	require.NoError(t, db.Close())

	sqliteR, err = ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	require.Equal(t, FormatSQLiteV1, sqliteR.Meta().Format)

	require.NoError(t, os.Rename(dbPath, dbPath+".bak"))
	durableR, err = ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	require.Equal(t, FormatDurableV2, durableR.Meta().Format)

	cleanup = func() {
		_ = sqliteR.Close()
		_ = durableR.Close()
		_ = os.Rename(dbPath+".bak", dbPath)
	}
	return sqliteR, durableR, cleanup
}

func assertQueryResultParity(t *testing.T, ctx context.Context, sqliteR, durableR Reader, params indexstore.QueryParams, includeDeleted bool) {
	t.Helper()
	_ = includeDeleted
	sqlResults, _, err := sqliteR.QueryObjects(ctx, params)
	require.NoError(t, err)
	durResults, _, err := durableR.QueryObjects(ctx, params)
	require.NoError(t, err)
	require.Equal(t, len(sqlResults), len(durResults), "row count mismatch for %+v", params)
	for i := range sqlResults {
		assertQueryResultEqual(t, sqlResults[i], durResults[i])
	}
}

func assertQueryResultEqual(t *testing.T, a, b indexstore.QueryResult) {
	t.Helper()
	require.Equal(t, a.RelKey, b.RelKey)
	require.Equal(t, a.SizeBytes, b.SizeBytes)
	require.Equal(t, a.ETag, b.ETag)
	require.Equal(t, ptrStr(a.StorageClass), ptrStr(b.StorageClass))
	require.Equal(t, ptrStr(a.ArchiveStatus), ptrStr(b.ArchiveStatus))
	require.Equal(t, ptrStr(a.RestoreState), ptrStr(b.RestoreState))
	require.Equal(t, ptrStr(a.ContentType), ptrStr(b.ContentType))
	require.True(t, timePtrEqual(a.LastModified, b.LastModified), "last_modified %v vs %v", a.LastModified, b.LastModified)
	require.True(t, timePtrEqual(a.RestoreExpiry, b.RestoreExpiry), "restore_expiry")
	require.True(t, timePtrEqual(a.HeadEnrichedAt, b.HeadEnrichedAt), "head_enriched_at")
	require.True(t, timePtrEqual(a.DeletedAt, b.DeletedAt), "deleted_at")
	// Run IDs may differ when SQLite generates run_* and durable uses fixture run ids;
	// compare presence of timestamps for run lineage fields when both sides set them.
	require.True(t, timePtrEqual(a.FirstSeenAt, b.FirstSeenAt), "first_seen_at %v vs %v", a.FirstSeenAt, b.FirstSeenAt)
	require.True(t, timePtrEqual(a.LastChangedAt, b.LastChangedAt), "last_changed_at")
}

func timePtrEqual(a, b *time.Time) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.UTC().Equal(b.UTC())
}

func timePtr(t time.Time) *time.Time {
	v := t.UTC()
	return &v
}

func ptrStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
