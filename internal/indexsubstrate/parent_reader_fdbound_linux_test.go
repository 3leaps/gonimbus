//go:build linux

package indexsubstrate

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// countOpenRegularFDsUnder returns how many of this process's open descriptors
// resolve to regular files under dir.
func countOpenRegularFDsUnder(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	count := 0
	for _, entry := range entries {
		if _, convErr := strconv.Atoi(entry.Name()); convErr != nil {
			continue
		}
		target, linkErr := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if linkErr != nil {
			continue // descriptor closed between ReadDir and Readlink
		}
		if !filepath.IsAbs(target) {
			continue
		}
		rel, relErr := filepath.Rel(dir, target)
		if relErr != nil || rel == ".." || len(rel) >= 3 && rel[:3] == ".."+string(filepath.Separator) {
			continue
		}
		info, statErr := os.Lstat(target)
		if statErr != nil || !info.Mode().IsRegular() {
			continue
		}
		count++
	}
	return count
}

// TestPublishedParentRowSourceHoldsAtMostOneSegmentFD pins the one-descriptor
// bound: while streaming a multi-segment parent, at every segment open the
// previous segment's descriptor is already closed — exactly one regular-file
// descriptor under the segment directory is ever held.
func TestPublishedParentRowSourceHoldsAtMostOneSegmentFD(t *testing.T) {
	base := time.Date(2026, 7, 14, 9, 0, 0, 0, time.UTC)
	std := "STANDARD"
	rows := []CurrentObjectRow{
		segmentTestRow("idx_parent", "data/a.xml", 1, `"a"`, base, &std, nil, nil, nil),
		segmentTestRow("idx_parent", "data/b.xml", 2, `"b"`, base, &std, nil, nil, nil),
		segmentTestRow("idx_parent", "data/c.xml", 3, `"c"`, base, &std, nil, nil, nil),
		segmentTestRow("idx_parent", "data/d.xml", 4, `"d"`, base, &std, nil, nil, nil),
		segmentTestRow("idx_parent", "data/e.xml", 5, `"e"`, base, &std, nil, nil, nil),
	}
	snap, dir := writeParentSnapshotFixture(t, rows, 2) // 3 segments
	require.Greater(t, len(snap.Manifest.Segments), 2)

	var observed []int
	t.Cleanup(func() { afterSegmentDigestVerifiedForTest = nil })
	afterSegmentDigestVerifiedForTest = func(string) {
		observed = append(observed, countOpenRegularFDsUnder(t, dir))
	}

	src := NewPublishedParentRowSource(snap)
	got := drainParent(t, src)
	require.NoError(t, src.Close())
	require.Equal(t, rows, got)

	require.Len(t, observed, len(snap.Manifest.Segments), "hook must fire once per segment open")
	for i, n := range observed {
		require.Equal(t, 1, n, "segment open %d must hold exactly one segment-file descriptor (previous segment closed)", i)
	}
}
