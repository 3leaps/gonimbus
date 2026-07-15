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

const closePathMarker = "DEVREV_SECRET_CLOSE_PATH_MARKER"

func TestWriteStreamingCombinedCloseDisclosure(t *testing.T) {
	base := time.Date(2026, 7, 15, 12, 30, 0, 0, time.UTC)
	closeSentinel := errors.New(closePathMarker)

	type tc struct {
		name     string
		setup    func(t *testing.T) (context.Context, SegmentWriterConfig, OrderedRowSource)
		wantCat  StreamSegmentCategory
		wantText string // substring of primary message surface
	}

	cases := []tc{
		{
			name: "invalid_config",
			setup: func(t *testing.T) (context.Context, SegmentWriterConfig, OrderedRowSource) {
				src := NewSliceOrderedRows(nil).WithCloseHook(func() error { return closeSentinel })
				return context.Background(), SegmentWriterConfig{
					Dir:        t.TempDir(),
					IndexSetID: "", // invalid
					RunID:      "run",
					CreatedAt:  base,
				}, src
			},
			wantCat:  StreamSegmentInvalid,
			wantText: "invalid",
		},
		{
			name: "mkdir_refusal",
			setup: func(t *testing.T) (context.Context, SegmentWriterConfig, OrderedRowSource) {
				blocker := filepath.Join(t.TempDir(), "not-a-dir")
				require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o600))
				src := NewSliceOrderedRows([]CurrentObjectRow{
					segmentTestRow("idx_test", "data/a.xml", 10, `"e"`, base, nil, nil, nil, nil),
				}).WithCloseHook(func() error { return closeSentinel })
				return context.Background(), SegmentWriterConfig{
					Dir:        blocker, // MkdirAll fails: path is a file
					IndexSetID: "idx_test",
					RunID:      "run",
					CreatedAt:  base,
				}, src
			},
			wantCat:  StreamSegmentWrite,
			wantText: "mkdir",
		},
		{
			name: "order_violation",
			setup: func(t *testing.T) (context.Context, SegmentWriterConfig, OrderedRowSource) {
				rows := []CurrentObjectRow{
					segmentTestRow("idx_test", "data/b.xml", 10, `"e-b"`, base, nil, nil, nil, nil),
					segmentTestRow("idx_test", "data/a.xml", 20, `"e-a"`, base, nil, nil, nil, nil),
				}
				src := NewSliceOrderedRows(rows).WithCloseHook(func() error { return closeSentinel })
				return context.Background(), SegmentWriterConfig{
					Dir:        t.TempDir(),
					IndexSetID: "idx_test",
					RunID:      "run",
					CreatedAt:  base,
				}, src
			},
			wantCat:  StreamSegmentOrder,
			wantText: "order",
		},
		{
			name: "source_next_error",
			setup: func(t *testing.T) (context.Context, SegmentWriterConfig, OrderedRowSource) {
				src := &failingNextSource{
					closeFn: func() error { return closeSentinel },
					err:     errors.New("source next internal"),
				}
				return context.Background(), SegmentWriterConfig{
					Dir:        t.TempDir(),
					IndexSetID: "idx_test",
					RunID:      "run",
					CreatedAt:  base,
				}, src
			},
			wantCat:  StreamSegmentSource,
			wantText: "source",
		},
		{
			name: "context_canceled",
			setup: func(t *testing.T) (context.Context, SegmentWriterConfig, OrderedRowSource) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				src := NewSliceOrderedRows([]CurrentObjectRow{
					segmentTestRow("idx_test", "data/a.xml", 10, `"e"`, base, nil, nil, nil, nil),
				}).WithCloseHook(func() error { return closeSentinel })
				return ctx, SegmentWriterConfig{
					Dir:        t.TempDir(),
					IndexSetID: "idx_test",
					RunID:      "run",
					CreatedAt:  base,
				}, src
			},
			wantCat:  StreamSegmentCanceled,
			wantText: "canceled",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cfg, src := tt.setup(t)
			closeCountBefore := 0
			// wrap to count closes if slice source
			if s, ok := src.(*SliceOrderedRows); ok {
				prev := s.close
				s.close = func() error {
					closeCountBefore++
					if prev != nil {
						return prev()
					}
					return nil
				}
			}
			if s, ok := src.(*failingNextSource); ok {
				prev := s.closeFn
				s.closeFn = func() error {
					closeCountBefore++
					if prev != nil {
						return prev()
					}
					return nil
				}
			}

			man, err := WriteStreamingSegmentSet(ctx, cfg, src)
			require.Error(t, err)
			require.Equal(t, InternalManifest{}, man)
			require.NotContains(t, err.Error(), closePathMarker)
			require.NotContains(t, err.Error(), cfg.Dir)
			require.Contains(t, strings.ToLower(err.Error()), tt.wantText)

			cats := streamSegmentCategories(err)
			require.Contains(t, cats, tt.wantCat)
			require.Contains(t, cats, StreamSegmentSource) // wrapped close

			require.True(t, errors.Is(err, closeSentinel), "Close cause must remain classifiable")
			if tt.wantCat == StreamSegmentCanceled {
				require.True(t, errors.Is(err, context.Canceled))
			}
			require.Equal(t, 1, closeCountBefore, "exactly one Close")
		})
	}
}

func TestWriteStreamingCloseOnlyAfterEOFDisclosure(t *testing.T) {
	base := time.Date(2026, 7, 15, 12, 31, 0, 0, time.UTC)
	closeSentinel := errors.New(closePathMarker)
	var closes int
	src := NewSliceOrderedRows([]CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e"`, base, nil, nil, nil, nil),
	}).WithCloseHook(func() error {
		closes++
		return closeSentinel
	})
	dir := t.TempDir()
	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:        dir,
		IndexSetID: "idx_test",
		RunID:      "run",
		CreatedAt:  base,
	}, src)
	require.Error(t, err)
	require.Equal(t, InternalManifest{}, man)
	require.NotContains(t, err.Error(), closePathMarker)
	require.NotContains(t, err.Error(), dir)
	require.True(t, errors.Is(err, closeSentinel))
	require.Equal(t, 1, closes)
	require.Contains(t, streamSegmentCategories(err), StreamSegmentSource)
}

func TestWriteStreamingPostLinkTempUnlinkFailure(t *testing.T) {
	base := time.Date(2026, 7, 15, 12, 32, 0, 0, time.UTC)
	unlinkSentinel := errors.New("injected temp unlink failure")
	ops := scopedSegmentOps(func(path string) error {
		if isSegmentTempPath(path) {
			return unlinkSentinel
		}
		return productionSegmentFileOps().remove(path)
	}, nil, nil)

	dir := t.TempDir()
	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:                  dir,
		IndexSetID:           "idx_test",
		RunID:                "run_tmp_unlink",
		CreatedAt:            base,
		TargetRowsPerSegment: 1,
		segmentOps:           ops,
	}, NewSliceOrderedRows([]CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e"`, base, nil, nil, nil, nil),
	}))
	require.Error(t, err)
	require.Equal(t, InternalManifest{}, man)
	require.True(t, errors.Is(err, unlinkSentinel), "temp unlink failure must remain classifiable")
	require.Contains(t, streamSegmentCategories(err), StreamSegmentWrite)
	// Final cleaned via ownership rollback; no silent success with leftover claim.
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	for _, e := range entries {
		require.False(t, strings.HasSuffix(e.Name(), ".parquet"), "owned final must be rolled back: %s", e.Name())
	}
}

func TestWriteStreamingPostLinkStatAndFinalRemoveFailure(t *testing.T) {
	base := time.Date(2026, 7, 15, 12, 33, 0, 0, time.UTC)
	statSentinel := errors.New("injected stat failure")
	removeSentinel := errors.New("injected final remove failure")
	ops := scopedSegmentOps(func(path string) error {
		if strings.HasSuffix(path, ".parquet") {
			return removeSentinel
		}
		return productionSegmentFileOps().remove(path)
	}, func(path string) (os.FileInfo, error) {
		return nil, statSentinel
	}, nil)

	dir := t.TempDir()
	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:                  dir,
		IndexSetID:           "idx_test",
		RunID:                "run_stat_fail",
		CreatedAt:            base,
		TargetRowsPerSegment: 1,
		segmentOps:           ops,
	}, NewSliceOrderedRows([]CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e"`, base, nil, nil, nil, nil),
	}))
	require.Error(t, err)
	require.Equal(t, InternalManifest{}, man)
	require.True(t, errors.Is(err, statSentinel))
	require.True(t, errors.Is(err, removeSentinel), "cleanup must surface final-remove failure")
	require.Contains(t, streamSegmentCategories(err), StreamSegmentCleanup)
}

func TestWriteStreamingReuseSuccessTempUnlinkFailure(t *testing.T) {
	// P1-R: AllowExistingIdentical reuse must not return success if temp unlink fails.
	base := time.Date(2026, 7, 15, 12, 40, 0, 0, time.UTC)

	seedDir := t.TempDir()
	seedRows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e-a"`, base, nil, nil, nil, nil),
	}
	seedMan, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:                  seedDir,
		IndexSetID:           "idx_test",
		RunID:                "run_reuse_tmp",
		CreatedAt:            base,
		TargetRowsPerSegment: 1,
	}, seedRows)
	require.NoError(t, err)
	seedPath := filepath.Join(seedDir, seedMan.Segments[0].Path)
	seedBytes, err := os.ReadFile(seedPath)
	require.NoError(t, err)
	seedInfo, err := os.Stat(seedPath)
	require.NoError(t, err)

	unlinkSentinel := errors.New("injected reuse temp unlink failure")
	var removeFinalCalls int
	ops := scopedSegmentOps(func(path string) error {
		if isSegmentTempPath(path) {
			return unlinkSentinel
		}
		if strings.HasSuffix(path, ".parquet") {
			removeFinalCalls++
			return os.Remove(path)
		}
		return os.Remove(path)
	}, nil, nil)

	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:                    seedDir,
		IndexSetID:             "idx_test",
		RunID:                  "run_reuse_tmp",
		CreatedAt:              base,
		TargetRowsPerSegment:   1,
		AllowExistingIdentical: true,
		segmentOps:             ops,
	}, NewSliceOrderedRows(seedRows))
	require.Error(t, err)
	require.Equal(t, InternalManifest{}, man)
	require.True(t, errors.Is(err, unlinkSentinel))
	require.Equal(t, 0, removeFinalCalls, "reuse path must not attempt to remove the pre-existing final")

	after, err := os.ReadFile(seedPath)
	require.NoError(t, err)
	require.Equal(t, seedBytes, after)
	afterInfo, err := os.Stat(seedPath)
	require.NoError(t, err)
	require.Equal(t, seedInfo.Mode(), afterInfo.Mode())
}

func TestWriteStreamingPreLinkConflictTempUnlinkFailure(t *testing.T) {
	// Entarch P1: pre-link non-identical EEXIST + injected temp unlink must join
	// cleanup failure; never own/remove the conflicting final.
	base := time.Date(2026, 7, 15, 12, 42, 0, 0, time.UTC)
	dir := t.TempDir()
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e-a"`, base, nil, nil, nil, nil),
	}
	seedMan, err := WriteSegmentSet(SegmentWriterConfig{
		Dir: dir, IndexSetID: "idx_test", RunID: "run_prelink", CreatedAt: base, TargetRowsPerSegment: 1,
	}, rows)
	require.NoError(t, err)
	conflictPath := filepath.Join(dir, seedMan.Segments[0].Path)
	// Corrupt final so AllowExistingIdentical sees EEXIST but digest mismatch.
	require.NoError(t, os.WriteFile(conflictPath, []byte("not-the-same-digest-bytes"), 0o600))
	conflictBytes, err := os.ReadFile(conflictPath)
	require.NoError(t, err)

	unlinkSentinel := errors.New("injected prelink temp unlink failure")
	var removeFinalCalls int
	ops := scopedSegmentOps(func(path string) error {
		if isSegmentTempPath(path) {
			return unlinkSentinel
		}
		if strings.HasSuffix(path, ".parquet") {
			removeFinalCalls++
		}
		return productionSegmentFileOps().remove(path)
	}, nil, nil)

	var closes int
	src := NewSliceOrderedRows(rows).WithCloseHook(func() error {
		closes++
		return nil
	})
	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:                    dir,
		IndexSetID:             "idx_test",
		RunID:                  "run_prelink",
		CreatedAt:              base,
		TargetRowsPerSegment:   1,
		AllowExistingIdentical: true,
		segmentOps:             ops,
	}, src)
	require.Error(t, err)
	require.Equal(t, InternalManifest{}, man)
	require.True(t, errors.Is(err, os.ErrExist), "primary pre-link EEXIST must remain classifiable")
	require.True(t, errors.Is(err, unlinkSentinel), "joined temp cleanup failure must remain classifiable")
	require.Equal(t, 1, closes)
	require.Equal(t, 0, removeFinalCalls, "pre-link path owns only temp, never the conflicting final")
	require.NotContains(t, err.Error(), dir)
	require.NotContains(t, err.Error(), "data/a.xml")
	after, err := os.ReadFile(conflictPath)
	require.NoError(t, err)
	require.Equal(t, conflictBytes, after)
}

func TestWriteStreamingPreLinkConflictLeavesNoTemp(t *testing.T) {
	base := time.Date(2026, 7, 15, 12, 43, 0, 0, time.UTC)
	dir := t.TempDir()
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e-a"`, base, nil, nil, nil, nil),
	}
	seedMan, err := WriteSegmentSet(SegmentWriterConfig{
		Dir: dir, IndexSetID: "idx_test", RunID: "run_prelink_ok", CreatedAt: base, TargetRowsPerSegment: 1,
	}, rows)
	require.NoError(t, err)
	conflictPath := filepath.Join(dir, seedMan.Segments[0].Path)
	require.NoError(t, os.WriteFile(conflictPath, []byte("conflicting-final-content"), 0o600))

	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:                    dir,
		IndexSetID:             "idx_test",
		RunID:                  "run_prelink_ok",
		CreatedAt:              base,
		TargetRowsPerSegment:   1,
		AllowExistingIdentical: true,
	}, NewSliceOrderedRows(rows))
	require.Error(t, err)
	require.Equal(t, InternalManifest{}, man)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		require.False(t, strings.Contains(e.Name(), ".tmp"), "pre-link conflict must leave no temp when unlink succeeds: %s", e.Name())
	}
	// Conflicting final still present (unchanged ownership).
	require.FileExists(t, conflictPath)
}

func TestWriteStreamingReuseSuccessLeavesNoTemp(t *testing.T) {
	base := time.Date(2026, 7, 15, 12, 41, 0, 0, time.UTC)
	seedDir := t.TempDir()
	seedRows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e-a"`, base, nil, nil, nil, nil),
	}
	seedMan, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:                  seedDir,
		IndexSetID:           "idx_test",
		RunID:                "run_reuse_ok",
		CreatedAt:            base,
		TargetRowsPerSegment: 1,
	}, seedRows)
	require.NoError(t, err)
	require.Len(t, seedMan.Segments, 1)

	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:                    seedDir,
		IndexSetID:             "idx_test",
		RunID:                  "run_reuse_ok",
		CreatedAt:              base,
		TargetRowsPerSegment:   1,
		AllowExistingIdentical: true,
	}, NewSliceOrderedRows(seedRows))
	require.NoError(t, err)
	require.Len(t, man.Segments, 1)
	require.Equal(t, seedMan.Segments[0].Path, man.Segments[0].Path)

	entries, err := os.ReadDir(seedDir)
	require.NoError(t, err)
	for _, e := range entries {
		require.False(t, strings.Contains(e.Name(), ".tmp"), "ordinary reuse success must leave no temp: %s", e.Name())
	}
}

func TestWriteStreamingMixedReuseAndNewCleanup(t *testing.T) {
	base := time.Date(2026, 7, 15, 12, 34, 0, 0, time.UTC)
	// Seed segment 0 identical content.
	seedDir := t.TempDir()
	seedRows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e-a"`, base, nil, nil, nil, nil),
	}
	seedMan, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:                  seedDir,
		IndexSetID:           "idx_test",
		RunID:                "run_mixed",
		CreatedAt:            base,
		TargetRowsPerSegment: 1,
	}, seedRows)
	require.NoError(t, err)
	seedPath := filepath.Join(seedDir, seedMan.Segments[0].Path)
	seedBytes, err := os.ReadFile(seedPath)
	require.NoError(t, err)
	seedInfo, err := os.Stat(seedPath)
	require.NoError(t, err)

	// Stream: reuse a, create b, then OOO.
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"e-a"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/b.xml", 20, `"e-b"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/a0.xml", 30, `"e-a0"`, base, nil, nil, nil, nil),
	}
	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:                    seedDir,
		IndexSetID:             "idx_test",
		RunID:                  "run_mixed",
		CreatedAt:              base,
		TargetRowsPerSegment:   1,
		AllowExistingIdentical: true,
	}, NewSliceOrderedRows(rows))
	require.Error(t, err)
	require.Equal(t, InternalManifest{}, man)

	after, err := os.ReadFile(seedPath)
	require.NoError(t, err)
	require.Equal(t, seedBytes, after)
	afterInfo, err := os.Stat(seedPath)
	require.NoError(t, err)
	require.Equal(t, seedInfo.Mode(), afterInfo.Mode())

	entries, err := os.ReadDir(seedDir)
	require.NoError(t, err)
	parquet := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".parquet") {
			parquet++
			require.Equal(t, seedMan.Segments[0].Path, e.Name())
		}
		require.False(t, strings.Contains(e.Name(), ".tmp"), "temp residue: %s", e.Name())
	}
	require.Equal(t, 1, parquet)
}

func TestWriteStreamingFromCurrentStateSourceLifecycle(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 15, 12, 35, 0, 0, time.UTC)
	size := int64(10)
	prior := []CurrentObjectRow{{
		IndexSetID: "idx_test", RelKey: "data/keep.xml", SizeBytes: 5,
		FirstSeenRunID: "run_old", FirstSeenAt: runStartedAt.Add(-time.Hour),
		LastChangedRunID: "run_old", LastChangedAt: runStartedAt.Add(-time.Hour),
		LastSeenRunID: "run_old", LastSeenAt: runStartedAt.Add(-time.Hour),
		ETag: `"old"`,
	}}
	j := journalWithRecords("idx_test", "run_cur", "jrn_a", []ObjectRecord{
		observe("jrn_a", 1, "data/a.xml", runStartedAt, size, nil, `"a"`, nil),
		observe("jrn_a", 2, "data/keep.xml", runStartedAt, size, nil, `"new"`, nil),
	})
	paths := writeSealedJournals(t, []Journal{j})
	coverage := []CoverageAttestation{{
		Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true,
	}}

	oracle, err := Compact(CompactionInput{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		PriorRows: prior, Journals: []Journal{j}, Coverage: coverage,
	})
	require.NoError(t, err)

	batchDir := t.TempDir()
	batchMan, err := WriteSegmentSet(SegmentWriterConfig{
		Dir: batchDir, IndexSetID: "idx_test", RunID: "run_cur", CreatedAt: runStartedAt,
		TargetRowsPerSegment: 2, Coverage: coverage,
	}, oracle.Rows)
	require.NoError(t, err)

	spillRoot := t.TempDir()
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_cur", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(prior), JournalPaths: paths,
		Coverage: coverage, SpillRoot: spillRoot,
	})
	require.NoError(t, err)
	ws := src.WorkspaceDir()
	require.DirExists(t, ws)

	streamDir := t.TempDir()
	streamMan, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir: streamDir, IndexSetID: "idx_test", RunID: "run_cur", CreatedAt: runStartedAt,
		TargetRowsPerSegment: 2, Coverage: coverage,
	}, src)
	require.NoError(t, err)

	// Writer owns Close: Complete and workspace cleaned without caller Close.
	require.True(t, src.Stats().Complete)
	_, statErr := os.Stat(ws)
	require.True(t, os.IsNotExist(statErr), "workspace must be removed by writer-owned Close")
	// Second Close is safe/idempotent on CurrentStateSource but not required.
	require.NoError(t, src.Close())

	requireEqualStreamingManifest(t, batchMan, streamMan)
	requireEqualSegmentFiles(t, batchDir, streamDir, batchMan, streamMan)

	var walked []CurrentObjectRow
	require.NoError(t, WalkManifestRows(streamDir, streamMan, func(row CurrentObjectRow) error {
		walked = append(walked, row)
		return nil
	}))
	require.Equal(t, oracle.Rows, walked)
}

func TestWriteStreamingFromCurrentStateSourceEmpty(t *testing.T) {
	runStartedAt := time.Date(2026, 7, 15, 12, 36, 0, 0, time.UTC)
	src, err := PrepareCurrentStateSource(context.Background(), SpillMergeConfig{
		IndexSetID: "idx_test", RunID: "run_empty", RunStartedAt: runStartedAt,
		Parent: NewSliceParentRows(nil), SpillRoot: t.TempDir(),
	})
	require.NoError(t, err)
	ws := src.WorkspaceDir()
	man, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir: t.TempDir(), IndexSetID: "idx_test", RunID: "run_empty", CreatedAt: runStartedAt,
	}, src)
	require.NoError(t, err)
	require.Empty(t, man.Segments)
	require.Equal(t, 0, man.Counts.Rows)
	require.True(t, src.Stats().Complete)
	_, statErr := os.Stat(ws)
	require.True(t, os.IsNotExist(statErr))
}

// --- remediation helpers ---

type failingNextSource struct {
	closeFn func() error
	err     error
}

func (f *failingNextSource) Next(ctx context.Context) (CurrentObjectRow, error) {
	if err := ctx.Err(); err != nil {
		return CurrentObjectRow{}, err
	}
	return CurrentObjectRow{}, f.err
}

func (f *failingNextSource) Close() error {
	if f.closeFn != nil {
		return f.closeFn()
	}
	return nil
}

func isSegmentTempPath(path string) bool {
	base := filepath.Base(path)
	return strings.Contains(base, ".segment-") && strings.HasSuffix(path, ".tmp")
}

func scopedSegmentOps(
	remove func(path string) error,
	stat func(path string) (os.FileInfo, error),
	postLink func(tempPath, finalPath string) error,
) *segmentFileOps {
	prod := productionSegmentFileOps()
	ops := &segmentFileOps{
		remove:   prod.remove,
		stat:     prod.stat,
		postLink: postLink,
	}
	if remove != nil {
		ops.remove = remove
	}
	if stat != nil {
		ops.stat = stat
	}
	return ops
}

func streamSegmentCategories(err error) []StreamSegmentCategory {
	var out []StreamSegmentCategory
	var walk func(error)
	walk = func(e error) {
		if e == nil {
			return
		}
		var ss *StreamSegmentError
		if errors.As(e, &ss) && ss != nil {
			// Collect this node then continue into its cause / join children.
			found := false
			for _, c := range out {
				if c == ss.Category {
					found = true
					break
				}
			}
			if !found {
				out = append(out, ss.Category)
			}
			// Prefer walking join lists and Unwrap chains without re-As looping forever.
			if uw, ok := e.(interface{ Unwrap() []error }); ok {
				for _, child := range uw.Unwrap() {
					walk(child)
				}
				return
			}
			if cause := errors.Unwrap(e); cause != nil && cause != e {
				// If e is StreamSegmentError, unwrap is Cause; also walk if e was the As target.
				walk(cause)
			}
			return
		}
		if uw, ok := e.(interface{ Unwrap() []error }); ok {
			for _, child := range uw.Unwrap() {
				walk(child)
			}
			return
		}
		if cause := errors.Unwrap(e); cause != nil {
			walk(cause)
		}
	}
	// Seed walk from join root without errors.As collapsing to first only.
	if uw, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range uw.Unwrap() {
			walk(child)
		}
		return out
	}
	walk(err)
	return out
}
