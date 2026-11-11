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

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/durableindex"
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
	rows[3].LastChangedRunID = "run_del"
	rows[3].LastChangedAt = deletedAt
	rows[3].HeadEnrichedAt = nil

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
	require.Equal(t, "zstd", first.Compression)

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

func TestParquetInventoryV0ComplianceAndLegacyDualRead(t *testing.T) {
	dir := t.TempDir()
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	lm := t0.Add(-time.Hour)
	sc := "STANDARD"
	ct := "application/xml"
	enrichedAt := t1.Add(30 * time.Second)
	rows := []CurrentObjectRow{
		{
			IndexSetID:       "idx_test",
			RelKey:           "data/a.xml",
			SizeBytes:        100,
			LastModified:     &lm,
			ETag:             `"etag-a"`,
			StorageClass:     &sc,
			ContentType:      &ct,
			HeadEnrichedAt:   &enrichedAt,
			FirstSeenRunID:   "snap-0",
			FirstSeenAt:      t0,
			LastChangedRunID: "snap-0",
			LastChangedAt:    t0,
			LastSeenRunID:    "snap-1",
			LastSeenAt:       t1,
		},
		{
			IndexSetID:       "idx_test",
			RelKey:           "data/deleted.xml",
			SizeBytes:        200,
			ETag:             `"etag-del"`,
			FirstSeenRunID:   "snap-0",
			FirstSeenAt:      t0,
			LastChangedRunID: "snap-1",
			LastChangedAt:    t1,
			LastSeenRunID:    "snap-0",
			LastSeenAt:       t0,
			DeletedAt:        &t1,
		},
	}

	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:                  dir,
		IndexSetID:           "idx_test",
		RunID:                "snap-1",
		CreatedAt:            t1,
		TargetRowsPerSegment: 10,
	}, rows)
	require.NoError(t, err)
	require.Len(t, manifest.Segments, 1)

	segBytes, err := os.ReadFile(filepath.Join(dir, manifest.Segments[0].Path))
	require.NoError(t, err)
	logicalRows, err := durableindex.ReadParquetInventoryMember(segBytes, 0)
	require.NoError(t, err)
	require.Len(t, logicalRows, 2)
	require.Equal(t, "objects", logicalRows[0].Namespace)
	require.Equal(t, "data/a.xml", logicalRows[0].Key)
	require.Len(t, logicalRows[0].Enrichments, 1)
	require.Equal(t, "content_type", logicalRows[0].Enrichments[0].Name)
	require.Equal(t, "data/deleted.xml", logicalRows[1].Key)
	require.NotNil(t, logicalRows[1].Deleted)

	// Verify dual-read of legacy durable-v2 (segmentParquetRow Snappy) segments
	// through both ReadSegmentFileVerified and PublishedParentRowSource.
	legacyDir := t.TempDir()
	legacyPath := filepath.Join(legacyDir, "seg_legacy.parquet")
	legacyFile, err := os.OpenFile(legacyPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	require.NoError(t, err)
	legacyWriter := parquet.NewGenericWriter[segmentParquetRow](legacyFile, parquet.Compression(&parquet.Snappy))
	_, err = legacyWriter.Write([]segmentParquetRow{
		segmentParquetFromCurrentRow(rows[0]),
		segmentParquetFromCurrentRow(rows[1]),
	})
	require.NoError(t, err)
	require.NoError(t, legacyWriter.Close())
	require.NoError(t, legacyFile.Close())

	legacyBytes, err := os.ReadFile(legacyPath)
	require.NoError(t, err)
	sum := sha256.Sum256(legacyBytes)
	legacyDesc := SegmentDescriptor{
		SegmentID:   "seg_legacy",
		Path:        "seg_legacy.parquet",
		Format:      SegmentFormatParquet,
		Compression: "snappy",
		Rows:        2,
		Tombstones:  1,
		SizeBytes:   int64(len(legacyBytes)),
		Digest: SegmentDigest{
			Algorithm: "sha256",
			Hex:       hex.EncodeToString(sum[:]),
		},
	}

	gotWalk, err := ReadSegmentFileVerified(legacyDir, legacyDesc)
	require.NoError(t, err)
	expected, err := normalizeAndSortSegmentRows(rows, "idx_test")
	require.NoError(t, err)
	require.Equal(t, expected, gotWalk)

	parentSnap := PublishedSnapshot{
		SegmentDir: legacyDir,
		Manifest: InternalManifest{
			Counts:   ManifestCounts{Rows: 2, ActiveRows: 1, Tombstones: 1},
			Segments: []SegmentDescriptor{legacyDesc},
		},
	}
	src := NewPublishedParentRowSource(parentSnap)
	gotParent := drainParent(t, src)
	require.NoError(t, src.Close())
	require.Equal(t, expected, gotParent)
}

func TestWriteSegmentSetAndStreamingSplitByByteBudgetUnderReaderCeiling(t *testing.T) {
	base := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	std := "STANDARD"
	// Each row's estimated uncompressed payload is ~61 KiB (using a 60 KiB enrichment attribute,
	// which is > 4 KiB and <= 64 KiB MaxEnrichmentBytes). 1,200 rows (~73 MiB) must split into
	// 2 segments under the 64 MiB uncompressed budget.
	bigAttr := strings.Repeat("a", 60*1024)
	rows := make([]CurrentObjectRow, 0, 1200)
	for i := 0; i < 1200; i++ {
		r := segmentTestRow("idx_byte_budget", "data/"+pad3(i/100)+"/"+pad3(i%100)+"-"+strings.Repeat("k", 512)+".xml", int64(i+1), `"etag"`, base, &std, &bigAttr, nil, nil)
		rows = append(rows, r)
	}

	batchDir := t.TempDir()
	streamDir := t.TempDir()
	cfg := SegmentWriterConfig{
		IndexSetID:           "idx_byte_budget",
		RunID:                "run_test",
		CreatedAt:            base,
		TargetRowsPerSegment: DefaultTargetRowsPerSegment,
	}
	cfgBatch := cfg
	cfgBatch.Dir = batchDir
	cfgStream := cfg
	cfgStream.Dir = streamDir

	batchMan, err := WriteSegmentSet(cfgBatch, rows)
	require.NoError(t, err)
	require.Equal(t, 2, len(batchMan.Segments), "must split into 2 segments by byte budget despite TargetRowsPerSegment=500000")

	streamMan, err := WriteStreamingSegmentSet(context.Background(), cfgStream, NewSliceOrderedRows(rows))
	require.NoError(t, err)
	requireEqualStreamingManifest(t, batchMan, streamMan)
	requireEqualSegmentFiles(t, batchDir, streamDir, batchMan, streamMan)

	// Verify ReadSegmentFileVerified, PublishedParentRowSource, and durableindex.ReadParquetInventoryMember succeed on every segment.
	var totalRead int
	for _, seg := range batchMan.Segments {
		segRows, err := ReadSegmentFileVerified(batchDir, seg)
		require.NoError(t, err)
		totalRead += len(segRows)

		rawBytes, err := os.ReadFile(filepath.Join(batchDir, seg.Path))
		require.NoError(t, err)
		logical, err := durableindex.ReadParquetInventoryMember(rawBytes, 0)
		require.NoError(t, err)
		require.Len(t, logical, seg.Rows)
	}
	require.Equal(t, len(rows), totalRead)

	parentSrc := NewPublishedParentRowSource(PublishedSnapshot{
		SegmentDir: batchDir,
		Manifest:   batchMan,
	})
	defer func() { _ = parentSrc.Close() }()
	var parentCount int
	for {
		_, err := parentSrc.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		parentCount++
	}
	require.Equal(t, len(rows), parentCount)
}

func TestReadSegmentFileVerifiedAndParentReaderRefuseInvalidEnrichmentProvenance(t *testing.T) {
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	ts := durableTimeString(t0)
	ct := "application/xml"

	writeTamperedSegment := func(t *testing.T, pqRow durableParquetRow) (string, SegmentDescriptor) {
		t.Helper()
		dir := t.TempDir()
		segPath := filepath.Join(dir, "seg_tampered.parquet")
		f, err := os.OpenFile(segPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		require.NoError(t, err)
		cfg := SegmentWriterConfig{
			Dir:                  dir,
			IndexSetID:           "idx_test",
			RunID:                "snap-0",
			CreatedAt:            t0,
			TargetRowsPerSegment: 10,
		}
		require.NoError(t, writePreparedParquetRows(f, cfg, []durableParquetRow{pqRow}))
		require.NoError(t, f.Close())
		raw, err := os.ReadFile(segPath)
		require.NoError(t, err)
		sum := sha256.Sum256(raw)
		desc := SegmentDescriptor{
			SegmentID:   "seg_tampered",
			Path:        "seg_tampered.parquet",
			Format:      SegmentFormatParquet,
			Compression: "zstd",
			Rows:        1,
			SizeBytes:   int64(len(raw)),
			Digest: SegmentDigest{
				Algorithm: "sha256",
				Hex:       hex.EncodeToString(sum[:]),
			},
		}
		return dir, desc
	}

	cases := []struct {
		name string
		row  durableParquetRow
	}{
		{
			name: "against_revision_mismatch",
			row: durableParquetRow{
				Namespace:   "objects",
				Key:         "data/a.xml",
				Revision:    &durableParquetRevision{Kind: "opaque-etag", Value: `"rev-actual"`},
				SizeBytes:   10,
				FirstSeen:   durableParquetStamp{SnapshotID: "snap-0", At: ts},
				LastSeen:    durableParquetStamp{SnapshotID: "snap-0", At: ts},
				LastChanged: durableParquetStamp{SnapshotID: "snap-0", At: ts},
				Enrichments: []durableParquetEnrichment{{
					Name:            "content_type",
					Value:           &ct,
					Operation:       "head-object",
					ObservedAt:      ts,
					AgainstRevision: durableParquetRevision{Kind: "opaque-etag", Value: `"rev-wrong"`},
				}},
			},
		},
		{
			name: "tombstone_with_enrichment",
			row: durableParquetRow{
				Namespace:   "objects",
				Key:         "data/del.xml",
				Revision:    &durableParquetRevision{Kind: "opaque-etag", Value: `"rev-del"`},
				SizeBytes:   10,
				FirstSeen:   durableParquetStamp{SnapshotID: "snap-0", At: ts},
				LastSeen:    durableParquetStamp{SnapshotID: "snap-0", At: ts},
				LastChanged: durableParquetStamp{SnapshotID: "snap-0", At: ts},
				Deleted:     &durableParquetStamp{SnapshotID: "snap-0", At: ts},
				Enrichments: []durableParquetEnrichment{{
					Name:            "content_type",
					Value:           &ct,
					Operation:       "head-object",
					ObservedAt:      ts,
					AgainstRevision: durableParquetRevision{Kind: "opaque-etag", Value: `"rev-del"`},
				}},
			},
		},
		{
			name: "unsorted_enrichments",
			row: durableParquetRow{
				Namespace:   "objects",
				Key:         "data/unsorted.xml",
				Revision:    &durableParquetRevision{Kind: "opaque-etag", Value: `"rev-1"`},
				SizeBytes:   10,
				FirstSeen:   durableParquetStamp{SnapshotID: "snap-0", At: ts},
				LastSeen:    durableParquetStamp{SnapshotID: "snap-0", At: ts},
				LastChanged: durableParquetStamp{SnapshotID: "snap-0", At: ts},
				Enrichments: []durableParquetEnrichment{
					{
						Name:            "restore_state",
						Value:           &ct,
						Operation:       "head-object",
						ObservedAt:      ts,
						AgainstRevision: durableParquetRevision{Kind: "opaque-etag", Value: `"rev-1"`},
					},
					{
						Name:            "content_type",
						Value:           &ct,
						Operation:       "head-object",
						ObservedAt:      ts,
						AgainstRevision: durableParquetRevision{Kind: "opaque-etag", Value: `"rev-1"`},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir, desc := writeTamperedSegment(t, tc.row)
			_, err := ReadSegmentFileVerified(dir, desc)
			require.Error(t, err, "ReadSegmentFileVerified must refuse %s", tc.name)

			src := NewPublishedParentRowSource(PublishedSnapshot{
				SegmentDir: dir,
				Manifest: InternalManifest{
					Counts:   ManifestCounts{Rows: 1, ActiveRows: 1},
					Segments: []SegmentDescriptor{desc},
				},
			})
			defer func() { _ = src.Close() }()
			_, err = src.Next(context.Background())
			require.Error(t, err, "PublishedParentRowSource must refuse %s", tc.name)
		})
	}

	t.Run("enrichment_value_5000_bytes_accepted_65537_refused", func(t *testing.T) {
		val5000 := strings.Repeat("v", 5000)
		validRow := durableParquetRow{
			Namespace:   "objects",
			Key:         "data/valid5k.xml",
			Revision:    &durableParquetRevision{Kind: "opaque-etag", Value: `"rev-5k"`},
			SizeBytes:   10,
			FirstSeen:   durableParquetStamp{SnapshotID: "snap-0", At: ts},
			LastSeen:    durableParquetStamp{SnapshotID: "snap-0", At: ts},
			LastChanged: durableParquetStamp{SnapshotID: "snap-0", At: ts},
			Enrichments: []durableParquetEnrichment{{
				Name:            "content_type",
				Value:           &val5000,
				Operation:       "head-object",
				ObservedAt:      ts,
				AgainstRevision: durableParquetRevision{Kind: "opaque-etag", Value: `"rev-5k"`},
			}},
		}
		dir, desc := writeTamperedSegment(t, validRow)
		gotRows, err := ReadSegmentFileVerified(dir, desc)
		require.NoError(t, err)
		require.Len(t, gotRows, 1)
		require.NotNil(t, gotRows[0].ContentType)
		require.Equal(t, val5000, *gotRows[0].ContentType)

		src := NewPublishedParentRowSource(PublishedSnapshot{
			SegmentDir: dir,
			Manifest: InternalManifest{
				Counts:   ManifestCounts{Rows: 1, ActiveRows: 1},
				Segments: []SegmentDescriptor{desc},
			},
		})
		defer func() { _ = src.Close() }()
		gotParent, err := src.Next(context.Background())
		require.NoError(t, err)
		require.NotNil(t, gotParent.ContentType)
		require.Equal(t, val5000, *gotParent.ContentType)

		val65537 := strings.Repeat("v", 65537)
		oversizeRow := validRow
		oversizeRow.Enrichments = []durableParquetEnrichment{{
			Name:            "content_type",
			Value:           &val65537,
			Operation:       "head-object",
			ObservedAt:      ts,
			AgainstRevision: durableParquetRevision{Kind: "opaque-etag", Value: `"rev-5k"`},
		}}
		dirOver, descOver := writeTamperedSegment(t, oversizeRow)
		_, err = ReadSegmentFileVerified(dirOver, descOver)
		require.Error(t, err)
		srcOver := NewPublishedParentRowSource(PublishedSnapshot{
			SegmentDir: dirOver,
			Manifest: InternalManifest{
				Counts:   ManifestCounts{Rows: 1, ActiveRows: 1},
				Segments: []SegmentDescriptor{descOver},
			},
		})
		defer func() { _ = srcOver.Close() }()
		_, err = srcOver.Next(context.Background())
		require.Error(t, err)
	})
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
		LastChangedAt:    seenAt.Add(-24 * time.Hour),
		LastSeenRunID:    "run_test",
		LastSeenAt:       seenAt,
	}
}

func TestDotSegmentRowsExcludesTombstonesAndNonDotMarkersInBatchAndStreaming(t *testing.T) {
	base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	deletedAt := base.Add(time.Minute)

	tombstoneDotRow := segmentTestRow("idx_test", ".deleted_dotfile", 15, `"etag-del-dot"`, base, nil, nil, nil, nil)
	tombstoneDotRow.HeadEnrichedAt = nil
	tombstoneDotRow.LastChangedRunID = "run_test"
	tombstoneDotRow.LastChangedAt = deletedAt
	tombstoneDotRow.DeletedAt = &deletedAt

	rows := []CurrentObjectRow{
		tombstoneDotRow,
		segmentTestRow("idx_test", ".dotfile", 10, `"etag-dot"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "_SUCCESS", 0, `"etag-success"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/a.xml", 20, `"etag-a"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "sub/.dotdir/item.xml", 30, `"etag-subdot"`, base, nil, nil, nil, nil),
	}

	batchManifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:             t.TempDir(),
		IndexSetID:      "idx_test",
		RunID:           "run_test",
		GonimbusVersion: "v0.5.0-test",
		CreatedAt:       base,
	}, rows)
	require.NoError(t, err)
	require.Equal(t, DotSegmentPolicyInclude, batchManifest.DotSegmentPolicy)
	require.Equal(t, "v0.5.0-test", batchManifest.GonimbusVersion)
	require.Equal(t, 5, batchManifest.Counts.Rows)
	require.Equal(t, 4, batchManifest.Counts.ActiveRows)
	require.Equal(t, 1, batchManifest.Counts.Tombstones)
	require.Equal(t, 2, batchManifest.Counts.DotSegmentRows)

	streamManifest, err := WriteStreamingSegmentSet(context.Background(), SegmentWriterConfig{
		Dir:        t.TempDir(),
		IndexSetID: "idx_test",
		RunID:      "run_test",
		CreatedAt:  base,
	}, &sliceRowSource{rows: rows})
	require.NoError(t, err)
	require.Equal(t, DotSegmentPolicyInclude, streamManifest.DotSegmentPolicy)
	require.NotEmpty(t, streamManifest.GonimbusVersion)
	require.Equal(t, batchManifest.Counts, streamManifest.Counts)
}

type sliceRowSource struct {
	rows []CurrentObjectRow
	idx  int
}

func (s *sliceRowSource) Next(_ context.Context) (CurrentObjectRow, error) {
	if s.idx >= len(s.rows) {
		return CurrentObjectRow{}, io.EOF
	}
	row := s.rows[s.idx]
	s.idx++
	return row, nil
}

func (s *sliceRowSource) Close() error { return nil }

func TestLegacySegmentAllowsUncompressedColumnChunksAbove128MiB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg_legacy_large.parquet")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	require.NoError(t, err)

	writer := parquet.NewGenericWriter[segmentParquetRow](
		f,
		parquet.Compression(&parquet.Snappy),
		parquet.PageBufferSize(1024*1024),
	)
	// Write a legacy segmentParquetRow segment whose uncompressed column chunks
	// exceed durableindex.DefaultMaxParquetDecompressedBytes (128 MiB), matching
	// v0.4.3–v0.4.7 250k-row production segments (~164 MiB uncompressed).
	bigKeySuffix := strings.Repeat("k", 64*1024)
	bigETag := `"` + strings.Repeat("e", 64*1024) + `"`
	const rowCount = 1350 // ~172 MiB uncompressed across RelKey + ETag + metadata columns
	batch := make([]segmentParquetRow, 0, 150)
	for i := range rowCount {
		batch = append(batch, segmentParquetRow{
			IndexSetID:       "idx_legacy",
			RelKey:           "data/item_" + string(rune('a'+(i%26))) + "_" + bigKeySuffix,
			SizeBytes:        int64(i + 1),
			ETag:             bigETag,
			FirstSeenRunID:   "run_043",
			FirstSeenAt:      "2026-04-01T00:00:00Z",
			LastChangedRunID: "run_043",
			LastChangedAt:    "2026-04-01T00:00:00Z",
			LastSeenRunID:    "run_043",
			LastSeenAt:       "2026-04-01T00:00:00Z",
		})
		if len(batch) == 150 {
			_, err := writer.Write(batch)
			require.NoError(t, err)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		_, err := writer.Write(batch)
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	require.NoError(t, f.Close())

	info, err := os.Stat(path)
	require.NoError(t, err)
	digest, err := sha256HexFile(path)
	require.NoError(t, err)

	desc := SegmentDescriptor{
		SegmentID:     "seg_000001",
		Path:          "seg_legacy_large.parquet",
		Compression:   "snappy",
		Rows:          rowCount,
		Tombstones:    0,
		DistinctETags: 1,
		SizeBytes:     info.Size(),
		Digest:        SegmentDigest{Algorithm: "sha256", Hex: digest},
	}

	gotRows, err := ReadSegmentFileVerified(dir, desc)
	require.NoError(t, err)
	require.Len(t, gotRows, rowCount)

	src := NewPublishedParentRowSource(PublishedSnapshot{
		SegmentDir: dir,
		Manifest: InternalManifest{
			Counts:   ManifestCounts{Rows: rowCount, ActiveRows: rowCount},
			Segments: []SegmentDescriptor{desc},
		},
	})
	defer func() { _ = src.Close() }()
	firstRow, err := src.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, "idx_legacy", firstRow.IndexSetID)
}
