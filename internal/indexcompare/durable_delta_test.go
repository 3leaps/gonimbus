package indexcompare

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
)

func TestCompareDurableDeltaClassifiesRowsWithCoverageAttribution(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	beforeManifest, beforeDir := setupDurableDeltaRows(t, "run_before", base, nil, []indexsubstrate.CurrentObjectRow{
		compareDurableRow(indexSetIDForTest, "data/changed.xml", 10, base, "etag-old", "STANDARD"),
		compareDurableRow(indexSetIDForTest, "data/gone.xml", 20, base, "etag-gone", "STANDARD"),
		compareDurableRow(indexSetIDForTest, "data/same.xml", 30, base, "etag-same", "STANDARD"),
	})
	deletedAt := base.Add(2 * time.Hour)
	afterGone := compareDurableRow(indexSetIDForTest, "data/gone.xml", 20, base, "etag-gone", "STANDARD")
	afterGone.DeletedAt = &deletedAt
	afterManifest, afterDir := setupDurableDeltaRows(t, "run_after", base.Add(time.Hour), []indexsubstrate.ManifestReference{{
		IndexSetID: indexSetIDForTest,
		RunID:      "run_before",
	}}, []indexsubstrate.CurrentObjectRow{
		compareDurableRow(indexSetIDForTest, "data/added.xml", 40, base.Add(time.Hour), "etag-added", "STANDARD"),
		compareDurableRow(indexSetIDForTest, "data/changed.xml", 11, base.Add(time.Hour), "etag-new", "STANDARD"),
		afterGone,
		compareDurableRow(indexSetIDForTest, "data/same.xml", 30, base, "etag-same", "STANDARD"),
	})

	report, err := CompareDurableDelta(ctx, DurableDeltaInput{
		Before: DurableSnapshotInput{Manifest: beforeManifest, SegmentDir: beforeDir},
		After:  DurableSnapshotInput{Manifest: afterManifest, SegmentDir: afterDir},
	})
	require.NoError(t, err)
	require.Equal(t, indexSetIDForTest, report.IndexSetID)
	require.Equal(t, int64(1), report.Added)
	require.Equal(t, int64(1), report.Changed)
	require.Equal(t, int64(1), report.Tombstoned)
	require.Equal(t, int64(1), report.Unchanged)
	require.Equal(t, int64(3), report.Coverage.AttributedRows)
	require.Len(t, report.Changes, 3)
	require.Equal(t, []string{"data/added.xml", "data/changed.xml", "data/gone.xml"}, []string{
		report.Changes[0].RelKey,
		report.Changes[1].RelKey,
		report.Changes[2].RelKey,
	})
	require.Equal(t, "added", report.Changes[0].Kind)
	require.Equal(t, "changed", report.Changes[1].Kind)
	require.Equal(t, "tombstoned", report.Changes[2].Kind)
	require.True(t, report.Changes[2].Coverage.Attributed)
	require.Equal(t, "data/", report.Changes[2].Coverage.Scope)
	require.Equal(t, 1, report.Coverage.BeforeScopes)
	require.Equal(t, 1, report.Coverage.AfterScopes)
	require.NotEmpty(t, report.BeforeSHA256)
	require.NotEmpty(t, report.AfterSHA256)
	require.NotEqual(t, report.BeforeSHA256, report.AfterSHA256)
}

func TestCompareDurableDeltaBoundsChangeDetails(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	beforeManifest, beforeDir := setupDurableDeltaRows(t, "run_before", base, nil, nil)
	afterManifest, afterDir := setupDurableDeltaRows(t, "run_after", base.Add(time.Hour), nil, []indexsubstrate.CurrentObjectRow{
		compareDurableRow(indexSetIDForTest, "data/a.xml", 1, base, "a", "STANDARD"),
		compareDurableRow(indexSetIDForTest, "data/b.xml", 1, base, "b", "STANDARD"),
		compareDurableRow(indexSetIDForTest, "data/c.xml", 1, base, "c", "STANDARD"),
	})

	report, err := CompareDurableDelta(context.Background(), DurableDeltaInput{
		Before:     DurableSnapshotInput{Manifest: beforeManifest, SegmentDir: beforeDir},
		After:      DurableSnapshotInput{Manifest: afterManifest, SegmentDir: afterDir},
		MaxChanges: 2,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), report.Added)
	require.Len(t, report.Changes, 2)
	require.Equal(t, "data/a.xml", report.Changes[0].RelKey)
	require.Equal(t, "data/b.xml", report.Changes[1].RelKey)
}

func TestCompareDurableDeltaRejectsAmbiguousCoverage(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	beforeManifest, beforeDir := setupDurableDeltaRows(t, "run_before", base, nil, []indexsubstrate.CurrentObjectRow{
		compareDurableRow(indexSetIDForTest, "data/gone.xml", 20, base, "etag-gone", "STANDARD"),
	})
	afterManifest, afterDir := setupDurableDeltaRows(t, "run_after", base.Add(time.Hour), nil, nil)

	tests := []struct {
		name     string
		coverage []indexsubstrate.CoverageAttestation
		want     string
	}{
		{name: "missing", coverage: nil, want: "coverage is required"},
		{name: "partial", coverage: []indexsubstrate.CoverageAttestation{{Scope: &indexsubstrate.Scope{Prefix: "data/"}, Basis: indexsubstrate.CoverageBasisConfirmed}}, want: "confirmed and complete"},
		{name: "inferred", coverage: []indexsubstrate.CoverageAttestation{{Scope: &indexsubstrate.Scope{Prefix: "data/"}, Basis: indexsubstrate.CoverageBasisInferred, Complete: true}}, want: "confirmed and complete"},
		{name: "gapped", coverage: []indexsubstrate.CoverageAttestation{{Scope: &indexsubstrate.Scope{Prefix: "data/"}, Basis: indexsubstrate.CoverageBasisConfirmed, Complete: true, Gaps: []indexsubstrate.Scope{{Prefix: "data/"}}}}, want: "ungapped"},
		{name: "wrong scope", coverage: []indexsubstrate.CoverageAttestation{{Scope: &indexsubstrate.Scope{Prefix: "other/"}, Basis: indexsubstrate.CoverageBasisConfirmed, Complete: true}}, want: "does not cover tombstone"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			afterManifest.Coverage = tt.coverage
			_, err := CompareDurableDelta(context.Background(), DurableDeltaInput{
				Before: DurableSnapshotInput{Manifest: beforeManifest, SegmentDir: beforeDir},
				After:  DurableSnapshotInput{Manifest: afterManifest, SegmentDir: afterDir},
			})
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestCompareDurableDeltaRejectsAfterTombstoneWithoutPriorRow(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	beforeManifest, beforeDir := setupDurableDeltaRows(t, "run_before", base, nil, nil)
	deletedAt := base.Add(time.Hour)
	deleted := compareDurableRow(indexSetIDForTest, "data/gone.xml", 10, base, "etag-gone", "STANDARD")
	deleted.DeletedAt = &deletedAt
	afterManifest, afterDir := setupDurableDeltaRows(t, "run_after", base.Add(time.Hour), nil, []indexsubstrate.CurrentObjectRow{deleted})

	_, err := CompareDurableDelta(context.Background(), DurableDeltaInput{
		Before: DurableSnapshotInput{Manifest: beforeManifest, SegmentDir: beforeDir},
		After:  DurableSnapshotInput{Manifest: afterManifest, SegmentDir: afterDir},
	})
	require.ErrorContains(t, err, "no prior row")
}

func TestCompareDurableDeltaRejectsAddedRowsWithoutBeforeCoverage(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	beforeManifest, beforeDir := setupDurableDeltaRows(t, "run_before", base, nil, nil)
	afterManifest, afterDir := setupDurableDeltaRows(t, "run_after", base.Add(time.Hour), nil, []indexsubstrate.CurrentObjectRow{
		compareDurableRow(indexSetIDForTest, "data/new.xml", 10, base.Add(time.Hour), "etag-new", "STANDARD"),
	})

	tests := []struct {
		name     string
		coverage []indexsubstrate.CoverageAttestation
		want     string
	}{
		{name: "missing", coverage: nil, want: "before manifest coverage is required"},
		{name: "wrong scope", coverage: []indexsubstrate.CoverageAttestation{{Scope: &indexsubstrate.Scope{Prefix: "other/"}, Basis: indexsubstrate.CoverageBasisConfirmed, Complete: true}}, want: "before coverage does not cover added row"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			beforeManifest.Coverage = tt.coverage
			_, err := CompareDurableDelta(context.Background(), DurableDeltaInput{
				Before: DurableSnapshotInput{Manifest: beforeManifest, SegmentDir: beforeDir},
				After:  DurableSnapshotInput{Manifest: afterManifest, SegmentDir: afterDir},
			})
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestCompareDurableDeltaRejectsUncoveredUnchangedRows(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	beforeManifest, beforeDir := setupDurableDeltaRows(t, "run_before", base, nil, []indexsubstrate.CurrentObjectRow{
		compareDurableRow(indexSetIDForTest, "other/same.xml", 10, base, "etag-same", "STANDARD"),
	})
	afterManifest, afterDir := setupDurableDeltaRows(t, "run_after", base.Add(time.Hour), nil, []indexsubstrate.CurrentObjectRow{
		compareDurableRow(indexSetIDForTest, "other/same.xml", 10, base, "etag-same", "STANDARD"),
	})

	_, err := CompareDurableDelta(context.Background(), DurableDeltaInput{
		Before: DurableSnapshotInput{Manifest: beforeManifest, SegmentDir: beforeDir},
		After:  DurableSnapshotInput{Manifest: afterManifest, SegmentDir: afterDir},
	})
	require.ErrorContains(t, err, "does not cover row")
}

func TestCompareDurableDeltaRejectsMismatchedAndUnsupportedSnapshots(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	beforeManifest, beforeDir := setupDurableDeltaRows(t, "run_before", base, nil, nil)
	afterManifest, afterDir := setupDurableDeltaRows(t, "run_after", base.Add(time.Hour), nil, nil)

	mismatched := afterManifest
	mismatched.IndexSetID = "idx_other"
	_, err := CompareDurableDelta(context.Background(), DurableDeltaInput{
		Before: DurableSnapshotInput{Manifest: beforeManifest, SegmentDir: beforeDir},
		After:  DurableSnapshotInput{Manifest: mismatched, SegmentDir: afterDir},
	})
	require.ErrorContains(t, err, "matching index_set_id")

	unsupported := afterManifest
	unsupported.Type = "gonimbus.index.manifest.v999"
	_, err = CompareDurableDelta(context.Background(), DurableDeltaInput{
		Before: DurableSnapshotInput{Manifest: beforeManifest, SegmentDir: beforeDir},
		After:  DurableSnapshotInput{Manifest: unsupported, SegmentDir: afterDir},
	})
	require.ErrorContains(t, err, "unsupported")

	unsupportedSchema := afterManifest
	unsupportedSchema.IndexSchemaVersion = 999
	_, err = CompareDurableDelta(context.Background(), DurableDeltaInput{
		Before: DurableSnapshotInput{Manifest: beforeManifest, SegmentDir: beforeDir},
		After:  DurableSnapshotInput{Manifest: unsupportedSchema, SegmentDir: afterDir},
	})
	require.ErrorContains(t, err, "schema version")
}

func TestCompareDurableDeltaRejectsMissingParentWhenRequired(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	beforeManifest, beforeDir := setupDurableDeltaRows(t, "run_before", base, nil, nil)
	afterManifest, afterDir := setupDurableDeltaRows(t, "run_after", base.Add(time.Hour), nil, nil)

	_, err := CompareDurableDelta(context.Background(), DurableDeltaInput{
		Before:        DurableSnapshotInput{Manifest: beforeManifest, SegmentDir: beforeDir},
		After:         DurableSnapshotInput{Manifest: afterManifest, SegmentDir: afterDir},
		RequireParent: true,
	})
	require.ErrorContains(t, err, "prior row source")
}

func TestCompareDurableDeltaReadsSegmentsThroughDigestVerification(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	beforeManifest, beforeDir := setupDurableDeltaRows(t, "run_before", base, nil, nil)
	afterManifest, afterDir := setupDurableDeltaRows(t, "run_after", base.Add(time.Hour), nil, []indexsubstrate.CurrentObjectRow{
		compareDurableRow(indexSetIDForTest, "data/a.xml", 1, base, "a", "STANDARD"),
	})
	require.NotEmpty(t, afterManifest.Segments)
	require.NoError(t, overwriteSegmentForDigestTest(filepath.Join(afterDir, afterManifest.Segments[0].Path)))

	_, err := CompareDurableDelta(context.Background(), DurableDeltaInput{
		Before: DurableSnapshotInput{Manifest: beforeManifest, SegmentDir: beforeDir},
		After:  DurableSnapshotInput{Manifest: afterManifest, SegmentDir: afterDir},
	})
	require.ErrorContains(t, err, "digest mismatch")
}

func setupDurableDeltaRows(t *testing.T, runID string, createdAt time.Time, parents []indexsubstrate.ManifestReference, rows []indexsubstrate.CurrentObjectRow) (indexsubstrate.InternalManifest, string) {
	t.Helper()
	segmentDir := filepath.Join(t.TempDir(), "segments")
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir:                  segmentDir,
		IndexSetID:           indexSetIDForTest,
		RunID:                runID,
		CreatedAt:            createdAt,
		TargetRowsPerSegment: 1,
		ParentManifests:      parents,
		Coverage: []indexsubstrate.CoverageAttestation{{
			Scope:    &indexsubstrate.Scope{Prefix: "data/"},
			Basis:    indexsubstrate.CoverageBasisConfirmed,
			Complete: true,
		}},
	}, rows)
	require.NoError(t, err)
	return manifest, segmentDir
}

func overwriteSegmentForDigestTest(path string) error {
	return os.WriteFile(path, []byte(strings.Repeat("x", 64)), 0o600)
}
