package indexsubstrate

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeParentSnapshotFixture writes a segment set from sorted rows and returns a
// minimal trust-shaped PublishedSnapshot plus the segment directory. Rows must
// already be in strict increasing RelKey order.
func writeParentSnapshotFixture(t *testing.T, rows []CurrentObjectRow, targetPerSeg int) (PublishedSnapshot, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "segments")
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:                  dir,
		IndexSetID:           "idx_parent",
		RunID:                "run_parent",
		CreatedAt:            base,
		TargetRowsPerSegment: targetPerSeg,
	}, rows)
	require.NoError(t, err)
	return PublishedSnapshot{SegmentDir: dir, Manifest: manifest}, dir
}

// drainParent reads to EOF without closing so callers can assert post-EOF
// behavior before Close.
func drainParent(t *testing.T, src *PublishedParentRowSource) []CurrentObjectRow {
	t.Helper()
	var out []CurrentObjectRow
	for {
		row, err := src.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		out = append(out, row)
	}
	return out
}

func TestPublishedParentRowSourceRoundTripsAcrossSegments(t *testing.T) {
	base := time.Date(2026, 7, 14, 9, 0, 0, 0, time.UTC)
	std := "STANDARD"
	xml := "application/xml"
	// Sorted, unique keys spanning multiple segments; one carries HEAD enrichment.
	rows := []CurrentObjectRow{
		segmentTestRow("idx_parent", "data/a.xml", 1, `"a"`, base, &std, nil, nil, nil),
		segmentTestRow("idx_parent", "data/b.xml", 2, `"b"`, base, &std, &xml, nil, nil),
		segmentTestRow("idx_parent", "data/c.xml", 3, `"c"`, base, &std, nil, nil, nil),
		segmentTestRow("idx_parent", "data/d.xml", 4, `"d"`, base, &std, nil, nil, nil),
		segmentTestRow("idx_parent", "data/e.xml", 5, `"e"`, base, &std, nil, nil, nil),
	}
	snap, _ := writeParentSnapshotFixture(t, rows, 2) // 3 segments (2+2+1)
	require.Greater(t, len(snap.Manifest.Segments), 1, "fixture must span multiple segments")

	src := NewPublishedParentRowSource(snap)
	got := drainParent(t, src)

	// Round-trip: same rows, same order, enrichment preserved.
	require.Equal(t, rows, got)
	// EOF is sticky before Close.
	_, err := src.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)
	// Close is idempotent.
	require.NoError(t, src.Close())
	require.NoError(t, src.Close())
}

func TestPublishedParentRowSourceEmptySnapshot(t *testing.T) {
	snap, _ := writeParentSnapshotFixture(t, nil, 2)
	src := NewPublishedParentRowSource(snap)
	_, err := src.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, src.Close())
}

func TestPublishedParentRowSourceRefusesDigestMismatch(t *testing.T) {
	base := time.Date(2026, 7, 14, 9, 0, 0, 0, time.UTC)
	std := "STANDARD"
	rows := []CurrentObjectRow{
		segmentTestRow("idx_parent", "data/a.xml", 1, `"a"`, base, &std, nil, nil, nil),
		segmentTestRow("idx_parent", "data/b.xml", 2, `"b"`, base, &std, nil, nil, nil),
	}
	snap, dir := writeParentSnapshotFixture(t, rows, 10) // single segment
	require.Len(t, snap.Manifest.Segments, 1)

	// Corrupt the segment bytes so the stored descriptor digest no longer matches.
	segPath := filepath.Join(dir, snap.Manifest.Segments[0].Path)
	orig, err := os.ReadFile(segPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(segPath, append(orig, 0x00), 0o600))

	src := NewPublishedParentRowSource(snap)
	_, err = src.Next(context.Background())
	require.Error(t, err)
	var pe *ParentReaderError
	require.ErrorAs(t, err, &pe)
	require.Equal(t, ParentReaderDigest, pe.Category)
	// Sanitized: no path/key material rendered.
	require.NotContains(t, err.Error(), segPath)
	require.NotContains(t, err.Error(), "data/")
	require.NoError(t, src.Close())
}

func TestPublishedParentRowSourceRefusesCountMismatch(t *testing.T) {
	base := time.Date(2026, 7, 14, 9, 0, 0, 0, time.UTC)
	std := "STANDARD"
	rows := []CurrentObjectRow{
		segmentTestRow("idx_parent", "data/a.xml", 1, `"a"`, base, &std, nil, nil, nil),
		segmentTestRow("idx_parent", "data/b.xml", 2, `"b"`, base, &std, nil, nil, nil),
	}
	snap, _ := writeParentSnapshotFixture(t, rows, 10)
	// Tamper the declared total so the stream over/under-runs the manifest count.
	snap.Manifest.Counts.Rows = 3

	src := NewPublishedParentRowSource(snap)
	_, err := src.Next(context.Background()) // a.xml
	require.NoError(t, err)
	_, err = src.Next(context.Background()) // b.xml
	require.NoError(t, err)
	_, err = src.Next(context.Background()) // drain -> count mismatch
	require.Error(t, err)
	var pe *ParentReaderError
	require.ErrorAs(t, err, &pe)
	require.Equal(t, ParentReaderCount, pe.Category)
	require.NoError(t, src.Close())
}

func TestPublishedParentRowSourceCanceledContext(t *testing.T) {
	base := time.Date(2026, 7, 14, 9, 0, 0, 0, time.UTC)
	std := "STANDARD"
	rows := []CurrentObjectRow{
		segmentTestRow("idx_parent", "data/a.xml", 1, `"a"`, base, &std, nil, nil, nil),
	}
	snap, _ := writeParentSnapshotFixture(t, rows, 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	src := NewPublishedParentRowSource(snap)
	_, err := src.Next(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	var pe *ParentReaderError
	require.ErrorAs(t, err, &pe)
	require.Equal(t, ParentReaderCanceled, pe.Category)
	require.NoError(t, src.Close())
}

// TestPublishedParentRowSourceDrainsThroughSpillMerge proves the reader is a
// drop-in ParentRowSource for the streaming publish merge.
func TestPublishedParentRowSourceDrainsThroughSpillMerge(t *testing.T) {
	base := time.Date(2026, 7, 14, 9, 0, 0, 0, time.UTC)
	std := "STANDARD"
	rows := []CurrentObjectRow{
		segmentTestRow("idx_parent", "data/a.xml", 1, `"a"`, base, &std, nil, nil, nil),
		segmentTestRow("idx_parent", "data/b.xml", 2, `"b"`, base, &std, nil, nil, nil),
		segmentTestRow("idx_parent", "data/c.xml", 3, `"c"`, base, &std, nil, nil, nil),
	}
	snap, _ := writeParentSnapshotFixture(t, rows, 2)

	var parent ParentRowSource = NewPublishedParentRowSource(snap)
	runStartedAt := base.Add(time.Hour)
	// No journals: current state must equal the parent rows verbatim.
	source, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID:   "idx_parent",
		RunID:        "run_child",
		RunStartedAt: runStartedAt,
		Parent:       parent,
		SpillRoot:    t.TempDir(),
	})
	require.NoError(t, err)
	var out []CurrentObjectRow
	for {
		row, nextErr := source.Next(context.Background())
		if errors.Is(nextErr, io.EOF) {
			break
		}
		require.NoError(t, nextErr)
		out = append(out, row)
	}
	require.NoError(t, source.Close())
	require.Equal(t, rows, out)
}
