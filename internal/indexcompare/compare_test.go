package indexcompare

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

func TestCompareProjectionParityIgnoresOrdering(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	db, indexSetID, runID := setupSQLiteRows(t, ctx, []indexstore.ObjectRow{
		compareObjectRow(indexSetIDForTest, "b.xml", 20, base.Add(time.Minute), "etag-b", "STANDARD"),
		compareObjectRow(indexSetIDForTest, "a.xml", 10, base, "etag-a", "STANDARD"),
	})
	manifest, segmentDir := setupDurableRows(t, []indexsubstrate.CurrentObjectRow{
		compareDurableRow(indexSetIDForTest, "a.xml", 10, base, "etag-a", "STANDARD"),
		compareDurableRow(indexSetIDForTest, "b.xml", 20, base.Add(time.Minute), "etag-b", "STANDARD"),
	})

	report, err := Compare(ctx, Input{
		SQLiteDB:             db,
		SQLiteIndexSetID:     indexSetID,
		DurableManifest:      manifest,
		DurableSegmentDir:    segmentDir,
		ObservationRunID:     runID,
		ObservationStartedAt: base,
	})
	require.NoError(t, err)
	require.True(t, report.ParityPassed)
	require.Equal(t, int64(2), report.SQLiteRows)
	require.Equal(t, int64(2), report.DurableRows)
	require.Equal(t, int64(0), report.ContentIdentityCheck.Mismatches)
	require.NotEmpty(t, report.SQLiteProjectionSHA256)
	require.Equal(t, report.SQLiteProjectionSHA256, report.DurableProjectionSHA256)
	require.Equal(t, DefaultProjectionSemantics(), report.ProjectionSemantics)
}

func TestCompareProjectionChecksContentIdentitySeparately(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	db, indexSetID, runID := setupSQLiteRows(t, ctx, []indexstore.ObjectRow{
		compareObjectRow(indexSetIDForTest, "multipart.bin", 10, base, "abc-2", "STANDARD"),
	})
	manifest, segmentDir := setupDurableRows(t, []indexsubstrate.CurrentObjectRow{
		compareDurableRow(indexSetIDForTest, "multipart.bin", 10, base, "different-2", "STANDARD"),
	})

	report, err := Compare(ctx, Input{SQLiteDB: db, SQLiteIndexSetID: indexSetID, DurableManifest: manifest, DurableSegmentDir: segmentDir, ObservationRunID: runID})
	require.NoError(t, err)
	require.False(t, report.ParityPassed)
	require.Equal(t, int64(0), report.ProjectionMismatches)
	require.Equal(t, int64(1), report.ContentIdentityCheck.Mismatches)
	require.Equal(t, "provider_etag_equivalence", report.ContentIdentityCheck.Semantics)
	require.Equal(t, "content_identity_mismatch", report.Mismatches[0].Kind)
}

func TestDefaultProjectionSemanticsDescribesResultContract(t *testing.T) {
	semantics := DefaultProjectionSemantics()
	require.Equal(t, "LIST-projection fidelity (sqlite vs durable row projection over one crawl)", semantics.Certifies)
	require.Equal(t, "reflow-input readiness (HEAD-enrichment parity)", semantics.DoesNotCertify)
	require.Equal(t, []string{"rel_key", "size_bytes", "last_modified", "storage_class"}, semantics.IncludedFields)
	require.Contains(t, semantics.ContentIdentity, "provider_etag_equivalence")
	require.Contains(t, semantics.ContentIdentity, "not a portable content hash")

	require.Len(t, semantics.ExcludedFields, 4)
	require.Equal(t, ExcludedFieldSemantics{
		FieldClass: "HEAD-derived enrichment metadata",
		Reason:     "not present in LIST; needs a separate enrich-with-HEAD pass",
		OwningGate: "projection v2 / enrichment-parity (over enriched-index runs; future)",
	}, semantics.ExcludedFields[0])
	require.Equal(t, "run-scoped temporal fields (first_seen, last_seen, last_changed)", semantics.ExcludedFields[1].FieldClass)
	require.Equal(t, "temporal-delta comparator (durable_delta.v1)", semantics.ExcludedFields[1].OwningGate)
	require.Equal(t, "coverage attestation", semantics.ExcludedFields[2].FieldClass)
	require.Equal(t, "temporal-delta comparator", semantics.ExcludedFields[2].OwningGate)
	require.Equal(t, "physical/format-internal metadata", semantics.ExcludedFields[3].FieldClass)
	require.Equal(t, "excluded by design (format-specific)", semantics.ExcludedFields[3].OwningGate)
}

func TestCompareProjectionRejectsUnknownVersion(t *testing.T) {
	_, err := Compare(context.Background(), Input{
		SQLiteIndexSetID: indexSetIDForTest,
		Options:          Options{ProjectionVersion: "gonimbus.index.compare_projection.v999"},
	})
	require.ErrorContains(t, err, "unsupported projection version")
}

const indexSetIDForTest = "idx_compare"

func setupSQLiteRows(t *testing.T, ctx context.Context, rows []indexstore.ObjectRow) (*sql.DB, string, string) {
	t.Helper()
	db, err := indexstore.Open(ctx, indexstore.Config{Path: ":memory:"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, indexstore.Migrate(ctx, db))
	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, indexstore.IndexSetParams{
		BaseURI:         "s3://bucket/data/",
		Provider:        "s3",
		StorageProvider: "aws_s3",
		BuildParams:     indexstore.BuildParams{SourceType: "crawl", SchemaVersion: indexstore.SchemaVersion, GonimbusVersion: "test"},
	})
	require.NoError(t, err)
	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	for i := range rows {
		rows[i].IndexSetID = indexSet.IndexSetID
		rows[i].LastSeenRunID = run.RunID
		rows[i].LastSeenAt = run.StartedAt
	}
	require.NoError(t, indexstore.BatchUpsertObjects(ctx, db, rows))
	return db, indexSet.IndexSetID, run.RunID
}

func setupDurableRows(t *testing.T, rows []indexsubstrate.CurrentObjectRow) (indexsubstrate.InternalManifest, string) {
	t.Helper()
	segmentDir := filepath.Join(t.TempDir(), "segments")
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir:                  segmentDir,
		IndexSetID:           indexSetIDForTest,
		RunID:                "run_test",
		CreatedAt:            time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC),
		TargetRowsPerSegment: 1,
	}, rows)
	require.NoError(t, err)
	return manifest, segmentDir
}

func compareObjectRow(indexSetID, relKey string, size int64, modified time.Time, etag, storageClass string) indexstore.ObjectRow {
	return indexstore.ObjectRow{
		IndexSetID:   indexSetID,
		RelKey:       relKey,
		SizeBytes:    size,
		LastModified: &modified,
		ETag:         etag,
		StorageClass: &storageClass,
	}
}

func compareDurableRow(indexSetID, relKey string, size int64, modified time.Time, etag, storageClass string) indexsubstrate.CurrentObjectRow {
	return indexsubstrate.CurrentObjectRow{
		IndexSetID:       indexSetID,
		RelKey:           relKey,
		SizeBytes:        size,
		LastModified:     &modified,
		ETag:             etag,
		StorageClass:     &storageClass,
		FirstSeenRunID:   "run_test",
		FirstSeenAt:      modified,
		LastChangedRunID: "run_test",
		LastChangedAt:    modified,
		LastSeenRunID:    "run_test",
		LastSeenAt:       modified,
	}
}
