package indexsubstrate

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteSegmentSetWritesDigestBoundParquetSegments(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	storageClass := "STANDARD"
	contentType := "application/xml"
	restoreState := "available"
	restoreExpiry := base.Add(24 * time.Hour)
	deletedAt := base.Add(2 * time.Hour)
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/c.xml", 30, `"etag-c"`, base.Add(3*time.Minute), nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/a.xml", 10, `"etag-a"`, base.Add(time.Minute), &storageClass, &contentType, &restoreState, &restoreExpiry),
		segmentTestRow("idx_test", "data/b.xml", 20, `"etag-a"`, base.Add(2*time.Minute), &storageClass, nil, nil, nil),
		segmentTestRow("idx_test", "data/deleted.xml", 40, `"etag-deleted"`, base.Add(4*time.Minute), &storageClass, nil, nil, nil),
	}
	rows[3].DeletedAt = &deletedAt

	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:                  dir,
		IndexSetID:           "idx_test",
		RunID:                "run_test",
		CreatedAt:            base,
		TargetRowsPerSegment: 2,
	}, rows)
	require.NoError(t, err)

	require.Equal(t, ManifestType, manifest.Type)
	require.Equal(t, ManifestRenderType, manifest.Render)
	require.Equal(t, IndexSchemaVersion, manifest.IndexSchemaVersion)
	require.Empty(t, manifest.ParentManifests)
	require.Equal(t, DefaultManifestReachability(), manifest.Reachability)
	require.Equal(t, SegmentSizing{TargetRowsPerSegment: 2, Rationale: SegmentSizingRationale}, manifest.SegmentSizing)
	require.Equal(t, ManifestCounts{Rows: 4, ActiveRows: 3, Tombstones: 1, DistinctETags: 3}, manifest.Counts)
	require.Len(t, manifest.Segments, 2)

	first := manifest.Segments[0]
	require.Equal(t, "data/a.xml", first.MinRelKey)
	require.Equal(t, "data/b.xml", first.MaxRelKey)
	require.Equal(t, 2, first.Rows)
	require.Equal(t, 0, first.Tombstones)
	require.Equal(t, 1, first.DistinctETags)
	require.Equal(t, "sha256", first.Digest.Algorithm)
	require.Len(t, first.Digest.Hex, 64)
	require.Contains(t, first.Path, first.Digest.Hex[:16])
	require.Equal(t, "snappy", first.Compression)

	second := manifest.Segments[1]
	require.Equal(t, "data/c.xml", second.MinRelKey)
	require.Equal(t, "data/deleted.xml", second.MaxRelKey)
	require.Equal(t, 2, second.Rows)
	require.Equal(t, 1, second.Tombstones)
	require.Equal(t, 2, second.DistinctETags)

	var reconstructed []CurrentObjectRow
	for _, segment := range manifest.Segments {
		path := filepath.Join(dir, segment.Path)
		digest, err := sha256HexFile(path)
		require.NoError(t, err)
		require.Equal(t, segment.Digest.Hex, digest)
		require.Greater(t, segment.SizeBytes, int64(0))

		segmentRows, err := ReadSegmentFileVerified(dir, segment)
		require.NoError(t, err)
		reconstructed = append(reconstructed, segmentRows...)
	}

	expected, err := normalizeAndSortSegmentRows(rows, "idx_test")
	require.NoError(t, err)
	require.Equal(t, expected, reconstructed)
}

func TestWriteSegmentSetProgressHookDoesNotChangeArtifacts(t *testing.T) {
	// Side-effect-free gate: multi-segment write with/without OnSegmentProgress
	// must produce byte-identical segment digests and equivalent manifest bytes
	// (ignoring only non-persisted callback side effects).
	base := time.Date(2026, 7, 9, 15, 0, 0, 0, time.UTC)
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"etag-a"`, base.Add(time.Minute), nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/b.xml", 20, `"etag-b"`, base.Add(2*time.Minute), nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/c.xml", 30, `"etag-c"`, base.Add(3*time.Minute), nil, nil, nil, nil),
	}
	cfgBase := SegmentWriterConfig{
		IndexSetID:           "idx_test",
		RunID:                "run_test",
		CreatedAt:            base,
		TargetRowsPerSegment: 1, // force ≥2 segments (actually 3)
	}

	dirOff := t.TempDir()
	cfgOff := cfgBase
	cfgOff.Dir = dirOff
	manifestOff, err := WriteSegmentSet(cfgOff, rows)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(manifestOff.Segments), 2)

	dirOn := t.TempDir()
	cfgOn := cfgBase
	cfgOn.Dir = dirOn
	var calls []SegmentProgress
	cfgOn.OnSegmentProgress = func(p SegmentProgress) {
		calls = append(calls, p)
	}
	manifestOn, err := WriteSegmentSet(cfgOn, rows)
	require.NoError(t, err)
	require.Len(t, calls, len(manifestOn.Segments))
	require.Equal(t, 1, calls[0].Segment)
	require.Equal(t, len(manifestOn.Segments), calls[0].Total)

	require.Equal(t, len(manifestOff.Segments), len(manifestOn.Segments))
	for i := range manifestOff.Segments {
		require.Equal(t, manifestOff.Segments[i].Digest.Hex, manifestOn.Segments[i].Digest.Hex)
		require.Equal(t, manifestOff.Segments[i].Path, manifestOn.Segments[i].Path)
		require.Equal(t, manifestOff.Segments[i].Rows, manifestOn.Segments[i].Rows)
		offBytes, err := os.ReadFile(filepath.Join(dirOff, manifestOff.Segments[i].Path))
		require.NoError(t, err)
		onBytes, err := os.ReadFile(filepath.Join(dirOn, manifestOn.Segments[i].Path))
		require.NoError(t, err)
		require.Equal(t, offBytes, onBytes)
	}

	// Manifest JSON equality after clearing path-independent fields that are
	// identical by construction when digests/paths/counts match.
	offJSON, err := marshalIndentedJSON(manifestOff)
	require.NoError(t, err)
	onJSON, err := marshalIndentedJSON(manifestOn)
	require.NoError(t, err)
	require.Equal(t, string(offJSON), string(onJSON))
}

func TestWriteSegmentSetRejectsOverwritingImmutableSegment(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"etag-a"`, base.Add(time.Minute), nil, nil, nil, nil),
	}
	config := SegmentWriterConfig{
		Dir:                  dir,
		IndexSetID:           "idx_test",
		RunID:                "run_test",
		CreatedAt:            base,
		TargetRowsPerSegment: 1,
	}
	manifest, err := WriteSegmentSet(config, rows)
	require.NoError(t, err)
	require.Len(t, manifest.Segments, 1)

	_, err = WriteSegmentSet(config, rows)
	require.Error(t, err)
	require.Contains(t, err.Error(), "immutable segment")
}

func TestWriteInternalManifestFileIsImmutableJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.json")
	manifest := InternalManifest{
		Type:               ManifestType,
		Render:             ManifestRenderType,
		IndexSetID:         "idx_test",
		RunID:              "run_test",
		IndexSchemaVersion: IndexSchemaVersion,
		CreatedAt:          time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC),
		ParentManifests:    []ManifestReference{},
		Reachability:       DefaultManifestReachability(),
		SegmentSizing:      SegmentSizing{TargetRowsPerSegment: DefaultTargetRowsPerSegment, Rationale: SegmentSizingRationale},
	}

	require.NoError(t, WriteInternalManifestFile(path, manifest))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var got InternalManifest
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, manifest, got)

	err = WriteInternalManifestFile(path, manifest)
	require.Error(t, err)
	require.True(t, errors.Is(err, os.ErrExist), err)
}

func TestWriteSegmentSetUsesConservativeDefaultSizing(t *testing.T) {
	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:        t.TempDir(),
		IndexSetID: "idx_test",
		RunID:      "run_test",
	}, nil)
	require.NoError(t, err)
	require.Equal(t, DefaultTargetRowsPerSegment, manifest.SegmentSizing.TargetRowsPerSegment)
	require.Equal(t, SegmentSizingRationale, manifest.SegmentSizing.Rationale)
	require.Empty(t, manifest.Segments)
	require.Equal(t, ManifestCounts{}, manifest.Counts)
}

func TestWriteSegmentSetCarriesParentReachabilityMetadata(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:        dir,
		IndexSetID: "idx_test",
		RunID:      "run_child",
		CreatedAt:  base,
		ParentManifests: []ManifestReference{{
			IndexSetID:     "idx_test",
			RunID:          "run_parent",
			ManifestSHA256: strings.Repeat("a", 64),
		}},
	}, nil)
	require.NoError(t, err)
	require.Equal(t, []ManifestReference{{
		IndexSetID:     "idx_test",
		RunID:          "run_parent",
		ManifestSHA256: strings.Repeat("a", 64),
	}}, manifest.ParentManifests)
	require.Equal(t, ManifestReachability{
		Model:            ManifestReachabilityModel,
		SegmentNamespace: SegmentNamespaceShared,
		RefcountMode:     RefcountModeDerivedAudit,
		CompactOwner:     CompactOwnerIndexCompact,
	}, manifest.Reachability)

	path := filepath.Join(dir, "manifest.json")
	require.NoError(t, WriteInternalManifestFile(path, manifest))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), `"parent_manifests"`)
	require.Contains(t, string(data), `"retained_manifests_parent_chain_latest_pointers"`)
}

func TestWriteSegmentSetRejectsInvalidParentManifestBeforeCreatingDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "segments")
	_, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:        dir,
		IndexSetID: "idx_test",
		RunID:      "run_child",
		ParentManifests: []ManifestReference{{
			RunID: "run_parent",
		}},
	}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parent manifest")
	require.NoDirExists(t, dir)
}

func TestWriteSegmentSetRejectsInvalidRows(t *testing.T) {
	base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		rows []CurrentObjectRow
		want string
	}{
		{
			name: "missing rel key",
			rows: []CurrentObjectRow{
				segmentTestRow("idx_test", "", 10, `"etag-a"`, base, nil, nil, nil, nil),
			},
			want: "rel_key",
		},
		{
			name: "index set mismatch",
			rows: []CurrentObjectRow{
				segmentTestRow("idx_other", "data/a.xml", 10, `"etag-a"`, base, nil, nil, nil, nil),
			},
			want: "index_set_id mismatch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := WriteSegmentSet(SegmentWriterConfig{
				Dir:        t.TempDir(),
				IndexSetID: "idx_test",
				RunID:      "run_test",
			}, tt.rows)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestReadSegmentFileVerifiedRejectsDigestMismatch(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:        dir,
		IndexSetID: "idx_test",
		RunID:      "run_test",
		CreatedAt:  base,
	}, []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"etag-a"`, base, nil, nil, nil, nil),
	})
	require.NoError(t, err)
	require.Len(t, manifest.Segments, 1)
	manifest.Segments[0].Digest.Hex = strings.Repeat("0", 64)

	_, err = ReadSegmentFileVerified(dir, manifest.Segments[0])
	require.Error(t, err)
	require.Contains(t, err.Error(), "digest mismatch")
}

func segmentTestRow(indexSetID, relKey string, size int64, etag string, seenAt time.Time, storageClass, contentType, restoreState *string, restoreExpiry *time.Time) CurrentObjectRow {
	lastModified := seenAt.Add(-time.Hour)
	headEnrichedAt := seenAt.Add(time.Minute)
	return CurrentObjectRow{
		IndexSetID:       indexSetID,
		RelKey:           relKey,
		SizeBytes:        size,
		LastModified:     &lastModified,
		ETag:             etag,
		StorageClass:     storageClass,
		RestoreState:     restoreState,
		RestoreExpiry:    restoreExpiry,
		ContentType:      contentType,
		HeadEnrichedAt:   &headEnrichedAt,
		FirstSeenRunID:   "run_old",
		FirstSeenAt:      seenAt.Add(-24 * time.Hour),
		LastChangedRunID: "run_old",
		LastChangedAt:    seenAt.Add(-12 * time.Hour),
		LastSeenRunID:    "run_test",
		LastSeenAt:       seenAt,
	}
}
