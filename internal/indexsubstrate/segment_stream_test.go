package indexsubstrate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteStreamingSegmentSetDifferentialVsBatch(t *testing.T) {
	base := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	storageClass := "STANDARD"
	contentType := "application/xml"
	deletedAt := base.Add(2 * time.Hour)

	// Pre-sorted unique keys (streaming refuses OOO; batch would sort).
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"etag-a"`, base.Add(time.Minute), &storageClass, &contentType, nil, nil),
		segmentTestRow("idx_test", "data/b.xml", 20, `"etag-a"`, base.Add(2*time.Minute), &storageClass, nil, nil, nil),
		segmentTestRow("idx_test", "data/c.xml", 30, `"etag-c"`, base.Add(3*time.Minute), nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/deleted.xml", 40, `"etag-deleted"`, base.Add(4*time.Minute), &storageClass, nil, nil, nil),
	}
	rows[3].DeletedAt = &deletedAt

	parents := []ManifestReference{{IndexSetID: "idx_test", RunID: "run_parent", ManifestSHA256: strings.Repeat("ab", 32)}}
	cov := []CoverageAttestation{{
		Scope:    &Scope{Prefix: "data/"},
		Basis:    CoverageBasisConfirmed,
		Complete: true,
	}}
	runStart := base
	lineage := &LineageRecord{Version: 1, Generation: 1, Baseline: true}

	for _, target := range []int{1, 2, 3, 4, 5, 100} {
		t.Run("target_"+itoa(target), func(t *testing.T) {
			cfg := SegmentWriterConfig{
				IndexSetID:           "idx_test",
				RunID:                "run_test",
				CreatedAt:            base,
				TargetRowsPerSegment: target,
				ParentManifests:      parents,
				Coverage:             cov,
				RunStartedAt:         &runStart,
				Lineage:              lineage,
			}
			batchDir := t.TempDir()
			streamDir := t.TempDir()
			cfgBatch := cfg
			cfgBatch.Dir = batchDir
			cfgStream := cfg
			cfgStream.Dir = streamDir

			batchMan, err := WriteSegmentSet(cfgBatch, rows)
			require.NoError(t, err)

			streamMan, err := WriteStreamingSegmentSet(context.Background(), cfgStream, NewSliceOrderedRows(rows))
			require.NoError(t, err)

			requireEqualStreamingManifest(t, batchMan, streamMan)
			requireEqualSegmentFiles(t, batchDir, streamDir, batchMan, streamMan)

			// Reader compatibility: walk reconstructed rows match.
			var walked []CurrentObjectRow
			require.NoError(t, WalkManifestRows(streamDir, streamMan, func(row CurrentObjectRow) error {
				walked = append(walked, row)
				return nil
			}))
			expected, err := normalizeAndSortSegmentRows(rows, "idx_test")
			require.NoError(t, err)
			require.Equal(t, expected, walked)
		})
	}
}

func TestWriteStreamingSegmentSetBoundarySizes(t *testing.T) {
	base := time.Date(2026, 7, 15, 13, 0, 0, 0, time.UTC)
	// target=3: sizes 0, 1, 2, 3, 4, 6
	sizes := []int{0, 1, 2, 3, 4, 6}
	const target = 3
	for _, n := range sizes {
		t.Run("n_"+itoa(n), func(t *testing.T) {
			rows := make([]CurrentObjectRow, 0, n)
			for i := 0; i < n; i++ {
				key := "data/" + strings.Repeat("k", 1) + pad3(i) + ".xml"
				rows = append(rows, segmentTestRow("idx_test", key, int64(10+i), `"etag-`+pad3(i)+`"`, base.Add(time.Duration(i)*time.Minute), nil, nil, nil, nil))
			}
			// Ensure sorted (pad3 makes lexical order match numeric for 0-999).
			cfg := SegmentWriterConfig{
				IndexSetID:           "idx_test",
				RunID:                "run_bound",
				CreatedAt:            base,
				TargetRowsPerSegment: target,
			}
			assertStreamBatchEqual(t, cfg, rows)
		})
	}
}

func TestWriteStreamingSegmentSetProgressTotalAlwaysZero(t *testing.T) {
	base := time.Date(2026, 7, 15, 14, 0, 0, 0, time.UTC)
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e-a"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/b.xml", 20, `"e-b"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/c.xml", 30, `"e-c"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/d.xml", 40, `"e-d"`, base, nil, nil, nil, nil),
	}
	// target=2 → exactly 2 seals; target=3 → 2 seals (3+1); target=4 → 1 seal exact multiple
	for _, target := range []int{2, 3, 4} {
		t.Run("target_"+itoa(target), func(t *testing.T) {
			var progress []SegmentProgress
			cfg := SegmentWriterConfig{
				Dir:                  t.TempDir(),
				IndexSetID:           "idx_test",
				RunID:                "run_prog",
				CreatedAt:            base,
				TargetRowsPerSegment: target,
				OnSegmentProgress: func(p SegmentProgress) {
					progress = append(progress, p)
				},
			}
			man, err := WriteStreamingSegmentSet(context.Background(), cfg, NewSliceOrderedRows(rows))
			require.NoError(t, err)
			require.Len(t, progress, len(man.Segments))
			var lastSeg, lastDone int
			for i, p := range progress {
				require.Equal(t, 0, p.Total, "streaming Total must stay 0")
				require.Equal(t, i+1, p.Segment)
				require.Greater(t, p.Rows, 0)
				require.GreaterOrEqual(t, p.RowsDone, lastDone)
				require.GreaterOrEqual(t, p.Segment, lastSeg)
				lastSeg = p.Segment
				lastDone = p.RowsDone
			}
			// Hook does not change artifacts vs no-hook.
			cfgOff := cfg
			cfgOff.Dir = t.TempDir()
			cfgOff.OnSegmentProgress = nil
			manOff, err := WriteStreamingSegmentSet(context.Background(), cfgOff, NewSliceOrderedRows(rows))
			require.NoError(t, err)
			requireEqualStreamingManifest(t, manOff, man)
		})
	}
}

func TestWriteStreamingSegmentSetWriterClosesSource(t *testing.T) {
	base := time.Date(2026, 7, 15, 15, 0, 0, 0, time.UTC)
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e-a"`, base, nil, nil, nil, nil),
	}
	var closeCount int
	src := NewSliceOrderedRows(rows).WithCloseHook(func() error {
		closeCount++
		return nil
	})
	_, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:        t.TempDir(),
		IndexSetID: "idx_test",
		RunID:      "run_close",
		CreatedAt:  base,
	}, src)
	require.NoError(t, err)
	require.Equal(t, 1, closeCount)

	// Empty source still closed.
	closeCount = 0
	empty := NewSliceOrderedRows(nil).WithCloseHook(func() error {
		closeCount++
		return nil
	})
	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:        t.TempDir(),
		IndexSetID: "idx_test",
		RunID:      "run_empty",
		CreatedAt:  base,
	}, empty)
	require.NoError(t, err)
	require.Equal(t, 0, man.Counts.Rows)
	require.Empty(t, man.Segments)
	require.Equal(t, 1, closeCount)
}

func TestWriteStreamingSegmentSetCloseFailureAfterEOFCleansOwned(t *testing.T) {
	base := time.Date(2026, 7, 15, 16, 0, 0, 0, time.UTC)
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e-a"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/b.xml", 20, `"e-b"`, base, nil, nil, nil, nil),
	}
	dir := t.TempDir()
	src := NewSliceOrderedRows(rows).WithCloseHook(func() error {
		return errors.New("injected close failure")
	})
	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:                  dir,
		IndexSetID:           "idx_test",
		RunID:                "run_close_fail",
		CreatedAt:            base,
		TargetRowsPerSegment: 1,
	}, src)
	require.Error(t, err)
	require.Equal(t, InternalManifest{}, man)
	require.Contains(t, err.Error(), "close")
	// Owned segments removed.
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	for _, e := range entries {
		require.False(t, strings.HasSuffix(e.Name(), ".parquet"), "owned parquet must be cleaned: %s", e.Name())
		require.False(t, strings.Contains(e.Name(), ".tmp"), "temps must be cleaned: %s", e.Name())
	}
}

func TestWriteStreamingSegmentSetOwnedCleanupOnOrderViolation(t *testing.T) {
	base := time.Date(2026, 7, 15, 17, 0, 0, 0, time.UTC)
	// Seal two segments (target=1), then OOO.
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e-a"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/b.xml", 20, `"e-b"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/z.xml", 30, `"e-z"`, base, nil, nil, nil, nil), // will become OOO when we replace third
	}
	// Third key descends relative to second.
	rows[2] = segmentTestRow("idx_test", "data/aa.xml", 30, `"e-aa"`, base, nil, nil, nil, nil)

	dir := t.TempDir()
	// foreign file must survive
	foreign := filepath.Join(dir, "unrelated.bin")
	require.NoError(t, os.WriteFile(foreign, []byte("keep-me"), 0o600))

	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:                  dir,
		IndexSetID:           "idx_test",
		RunID:                "run_ooo",
		CreatedAt:            base,
		TargetRowsPerSegment: 1,
	}, NewSliceOrderedRows(rows))
	require.Error(t, err)
	require.Equal(t, InternalManifest{}, man)
	var ssErr *StreamSegmentError
	require.True(t, errors.As(err, &ssErr))
	require.Equal(t, StreamSegmentOrder, ssErr.Category)
	// No rel key leakage (third key would be data/aa.xml)
	require.NotContains(t, err.Error(), "data/aa.xml")
	require.NotContains(t, err.Error(), dir)

	// Owned seals removed; foreign survives.
	require.FileExists(t, foreign)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	for _, e := range entries {
		if e.Name() == "unrelated.bin" {
			continue
		}
		require.False(t, strings.HasSuffix(e.Name(), ".parquet"), "owned parquet left: %s", e.Name())
		require.False(t, strings.Contains(e.Name(), ".tmp"), "temp left: %s", e.Name())
	}
}

func TestWriteStreamingSegmentSetAllowExistingIdenticalNotDeletedOnAbort(t *testing.T) {
	base := time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC)
	// First segment content will match pre-seeded file when AllowExistingIdentical.
	seedRows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e-a"`, base, nil, nil, nil, nil),
	}
	dir := t.TempDir()
	seedMan, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:                  dir,
		IndexSetID:           "idx_test",
		RunID:                "run_seed",
		CreatedAt:            base,
		TargetRowsPerSegment: 1,
	}, seedRows)
	require.NoError(t, err)
	require.Len(t, seedMan.Segments, 1)
	seedPath := filepath.Join(dir, seedMan.Segments[0].Path)
	require.FileExists(t, seedPath)
	seedBytes, err := os.ReadFile(seedPath)
	require.NoError(t, err)

	// Stream: first segment identical to seed (reuse), second seals, then OOO.
	// Use same run identity so segment ordinal 0 content matches seed digest/path
	// when TargetRowsPerSegment=1 and first row equals seed row.
	// Note: segment path embeds digest — same content → same path.
	streamRows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e-a"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/b.xml", 20, `"e-b"`, base, nil, nil, nil, nil),
		// OOO after two seals
		segmentTestRow("idx_test", "data/a2.xml", 30, `"e-a2"`, base, nil, nil, nil, nil),
	}
	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:                    dir,
		IndexSetID:             "idx_test",
		RunID:                  "run_seed", // same as seed so metadata match in parquet? Actually run_id is in parquet metadata — affects digest!
		CreatedAt:              base,
		TargetRowsPerSegment:   1,
		AllowExistingIdentical: true,
	}, NewSliceOrderedRows(streamRows))
	// Wait - run_id in parquet metadata means seed with run_seed vs stream with different run changes digest.
	// We used same RunID run_seed. Good.
	// But OOO third key data/a2.xml is after data/b.xml — a2 < b? "data/a2.xml" < "data/b.xml" is true (a2 < b).
	// Order: a, b, a2 → a2 after b is OOO. Good.
	_ = man
	require.Error(t, err)
	require.Equal(t, InternalManifest{}, man)

	// Pre-existing identical segment must survive.
	require.FileExists(t, seedPath)
	after, err := os.ReadFile(seedPath)
	require.NoError(t, err)
	require.Equal(t, seedBytes, after)

	// Segment for b (owned) must be gone.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	parquetCount := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".parquet") {
			parquetCount++
			require.Equal(t, seedMan.Segments[0].Path, e.Name(), "only seed parquet should remain")
		}
	}
	require.Equal(t, 1, parquetCount)
}

func TestWriteStreamingSegmentSetCancelAfterSeal(t *testing.T) {
	base := time.Date(2026, 7, 15, 19, 0, 0, 0, time.UTC)
	rows := make([]CurrentObjectRow, 0, 5)
	for i := 0; i < 5; i++ {
		rows = append(rows, segmentTestRow("idx_test", "data/"+pad3(i)+".xml", int64(i), `"e-`+pad3(i)+`"`, base, nil, nil, nil, nil))
	}
	ctx, cancel := context.WithCancel(context.Background())
	src := &cancelAfterNSource{rows: rows, cancelAt: 2, cancel: cancel}
	dir := t.TempDir()
	man, err := WriteStreamingSegmentSet(ctx, SegmentWriterConfig{
		Dir:                  dir,
		IndexSetID:           "idx_test",
		RunID:                "run_cancel",
		CreatedAt:            base,
		TargetRowsPerSegment: 1,
	}, src)
	require.Error(t, err)
	require.Equal(t, InternalManifest{}, man)
	require.True(t, errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "canceled"))
	require.Equal(t, 1, src.closeCount)
	// No leftover parquet/temps
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		require.False(t, strings.HasSuffix(e.Name(), ".parquet") || strings.Contains(e.Name(), ".tmp"), e.Name())
	}
}

func TestWriteStreamingSegmentSetDuplicateAndDescendingAcrossBoundary(t *testing.T) {
	base := time.Date(2026, 7, 15, 20, 0, 0, 0, time.UTC)
	// Within segment (target large)
	t.Run("duplicate_within", func(t *testing.T) {
		rows := []CurrentObjectRow{
			segmentTestRow("idx_test", "data/a.xml", 10, `"e-a"`, base, nil, nil, nil, nil),
			segmentTestRow("idx_test", "data/a.xml", 11, `"e-a2"`, base, nil, nil, nil, nil),
		}
		man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
			Dir:                  t.TempDir(),
			IndexSetID:           "idx_test",
			RunID:                "run_dup",
			CreatedAt:            base,
			TargetRowsPerSegment: 10,
		}, NewSliceOrderedRows(rows))
		require.Error(t, err)
		require.Equal(t, InternalManifest{}, man)
		require.Contains(t, err.Error(), "duplicate")
	})
	// Across segment boundary
	t.Run("descending_across", func(t *testing.T) {
		rows := []CurrentObjectRow{
			segmentTestRow("idx_test", "data/a.xml", 10, `"e-a"`, base, nil, nil, nil, nil),
			// first seal happens after a; second row descends
			segmentTestRow("idx_test", "data/0.xml", 20, `"e-0"`, base, nil, nil, nil, nil),
		}
		dir := t.TempDir()
		man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
			Dir:                  dir,
			IndexSetID:           "idx_test",
			RunID:                "run_desc",
			CreatedAt:            base,
			TargetRowsPerSegment: 1,
		}, NewSliceOrderedRows(rows))
		require.Error(t, err)
		require.Equal(t, InternalManifest{}, man)
		require.Contains(t, err.Error(), "order")
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Empty(t, entries)
	})
}

func TestWriteStreamingSegmentSetRejectsNilSourceAndInvalidConfig(t *testing.T) {
	base := time.Date(2026, 7, 15, 21, 0, 0, 0, time.UTC)
	_, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:        t.TempDir(),
		IndexSetID: "idx_test",
		RunID:      "run",
		CreatedAt:  base,
	}, nil)
	require.Error(t, err)

	// Non-UTC lineage refuse before Dir creation; still closes source (E1c).
	dir := filepath.Join(t.TempDir(), "must-not-create")
	runStart := time.Date(2026, 7, 15, 12, 0, 0, 0, time.FixedZone("CET", 3600))
	var closed int
	src := NewSliceOrderedRows([]CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e"`, base, nil, nil, nil, nil),
	}).WithCloseHook(func() error {
		closed++
		return nil
	})
	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:          dir,
		IndexSetID:   "idx_test",
		RunID:        "run_lineage",
		CreatedAt:    base,
		RunStartedAt: &runStart,
		Lineage:      &LineageRecord{Version: 1, Generation: 1, Baseline: true},
	}, src)
	require.Error(t, err)
	require.Equal(t, InternalManifest{}, man)
	require.Equal(t, 1, closed)
	require.NoDirExists(t, dir)
}

func TestWriteStreamingSegmentSetIndexSetIDFillAndMismatch(t *testing.T) {
	base := time.Date(2026, 7, 15, 22, 0, 0, 0, time.UTC)
	// Empty IndexSetID filled from config — batch parity
	row := segmentTestRow("", "data/a.xml", 10, `"e"`, base, nil, nil, nil, nil)
	assertStreamBatchEqual(t, SegmentWriterConfig{
		IndexSetID: "idx_test",
		RunID:      "run_fill",
		CreatedAt:  base,
	}, []CurrentObjectRow{row})

	// Mismatch refuse
	bad := segmentTestRow("idx_other", "data/a.xml", 10, `"e"`, base, nil, nil, nil, nil)
	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:        t.TempDir(),
		IndexSetID: "idx_test",
		RunID:      "run_mismatch",
		CreatedAt:  base,
	}, NewSliceOrderedRows([]CurrentObjectRow{bad}))
	require.Error(t, err)
	require.Equal(t, InternalManifest{}, man)
	require.Contains(t, err.Error(), "index_set_id mismatch")
}

func TestWriteStreamingSegmentSetDisclosureNoMarker(t *testing.T) {
	base := time.Date(2026, 7, 15, 23, 0, 0, 0, time.UTC)
	marker := "SENSITIVE_MARKER_KEY_SHOULD_NOT_APPEAR"
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/z.xml", 10, `"e-z"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", marker, 20, `"e-m"`, base, nil, nil, nil, nil),
	}
	dir := t.TempDir()
	_, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:        dir,
		IndexSetID: "idx_test",
		RunID:      "run_disc",
		CreatedAt:  base,
	}, NewSliceOrderedRows(rows))
	require.Error(t, err)
	require.NotContains(t, err.Error(), marker)
	require.NotContains(t, err.Error(), dir)
}

// --- helpers ---

type cancelAfterNSource struct {
	rows       []CurrentObjectRow
	i          int
	cancelAt   int
	cancel     context.CancelFunc
	closeCount int
}

func (s *cancelAfterNSource) Next(ctx context.Context) (CurrentObjectRow, error) {
	if err := ctx.Err(); err != nil {
		return CurrentObjectRow{}, err
	}
	if s.i >= len(s.rows) {
		return CurrentObjectRow{}, errors.New("unexpected EOF path")
	}
	if s.i == s.cancelAt {
		s.cancel()
		return CurrentObjectRow{}, context.Canceled
	}
	row := s.rows[s.i]
	s.i++
	return row, nil
}

func (s *cancelAfterNSource) Close() error {
	s.closeCount++
	return nil
}

func assertStreamBatchEqual(t *testing.T, cfg SegmentWriterConfig, rows []CurrentObjectRow) {
	t.Helper()
	batchDir := t.TempDir()
	streamDir := t.TempDir()
	cfgBatch := cfg
	cfgBatch.Dir = batchDir
	cfgStream := cfg
	cfgStream.Dir = streamDir

	batchMan, err := WriteSegmentSet(cfgBatch, append([]CurrentObjectRow(nil), rows...))
	require.NoError(t, err)
	streamMan, err := WriteStreamingSegmentSet(context.Background(), cfgStream, NewSliceOrderedRows(rows))
	require.NoError(t, err)
	requireEqualStreamingManifest(t, batchMan, streamMan)
	requireEqualSegmentFiles(t, batchDir, streamDir, batchMan, streamMan)
}

func requireEqualStreamingManifest(t *testing.T, batch, stream InternalManifest) {
	t.Helper()
	// CreatedAt may differ if zero was normalized to Now — tests always set CreatedAt.
	require.Equal(t, batch.Type, stream.Type)
	require.Equal(t, batch.Render, stream.Render)
	require.Equal(t, batch.IndexSetID, stream.IndexSetID)
	require.Equal(t, batch.RunID, stream.RunID)
	require.Equal(t, batch.IndexSchemaVersion, stream.IndexSchemaVersion)
	require.Equal(t, batch.CreatedAt, stream.CreatedAt)
	require.Equal(t, batch.RunStartedAt, stream.RunStartedAt)
	require.Equal(t, batch.StateParent, stream.StateParent)
	require.Equal(t, batch.Lineage, stream.Lineage)
	require.Equal(t, batch.ParentManifests, stream.ParentManifests)
	require.Equal(t, batch.Reachability, stream.Reachability)
	require.Equal(t, batch.Coverage, stream.Coverage)
	require.Equal(t, batch.SegmentSizing, stream.SegmentSizing)
	require.Equal(t, batch.Counts, stream.Counts)
	require.Equal(t, batch.Segments, stream.Segments)
}

func requireEqualSegmentFiles(t *testing.T, batchDir, streamDir string, batch, stream InternalManifest) {
	t.Helper()
	require.Len(t, stream.Segments, len(batch.Segments))
	for i := range batch.Segments {
		bPath := filepath.Join(batchDir, batch.Segments[i].Path)
		sPath := filepath.Join(streamDir, stream.Segments[i].Path)
		bDigest, err := sha256HexFile(bPath)
		require.NoError(t, err)
		sDigest, err := sha256HexFile(sPath)
		require.NoError(t, err)
		require.Equal(t, bDigest, sDigest)
		require.Equal(t, batch.Segments[i].Digest.Hex, sDigest)
		require.Equal(t, stream.Segments[i].Digest.Hex, sDigest)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func pad3(n int) string {
	if n < 0 {
		n = 0
	}
	return string([]byte{
		byte('0' + (n/100)%10),
		byte('0' + (n/10)%10),
		byte('0' + n%10),
	})
}
