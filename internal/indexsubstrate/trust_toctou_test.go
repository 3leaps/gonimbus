package indexsubstrate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenLatestPublishedSnapshot_UsesSameManifestBytesAfterReplacement(t *testing.T) {
	root := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"etag-a"`, base, nil, nil, nil, nil),
	}
	segmentDir := filepath.Join(root, "segments")
	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:                  segmentDir,
		IndexSetID:           "idx_test",
		RunID:                "run_test",
		CreatedAt:            base,
		TargetRowsPerSegment: 10,
	}, rows)
	require.NoError(t, err)

	manifestPath := filepath.Join(root, "manifest.json")
	require.NoError(t, WriteInternalManifestFile(manifestPath, manifest))
	manifestData, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	sum := sha256.Sum256(manifestData)
	manifestSHA := hex.EncodeToString(sum[:])

	completePath := filepath.Join(root, "complete.json")
	require.NoError(t, os.WriteFile(completePath, mustJSON(t, map[string]any{
		"type":            "gonimbus.index.complete.v1",
		"index_set_id":    "idx_test",
		"run_id":          "run_test",
		"completed_at":    base.Format(time.RFC3339Nano),
		"manifest_path":   manifestPath,
		"manifest_sha256": manifestSHA,
		"segment_dir":     segmentDir,
		"segments":        len(manifest.Segments),
	}), 0o600))
	latestPath := filepath.Join(root, "latest.json")
	require.NoError(t, os.WriteFile(latestPath, mustJSON(t, map[string]any{
		"type":          "gonimbus.index.latest.v1",
		"index_set_id":  "idx_test",
		"run_id":        "run_test",
		"updated_at":    base.Format(time.RFC3339Nano),
		"complete_path": completePath,
	}), 0o600))

	// After the trusted bytes are in memory, replace the pathname with garbage.
	// Same-bytes verification must still parse the original content successfully.
	t.Cleanup(func() { afterManifestBytesReadForTest = nil })
	afterManifestBytesReadForTest = func(path string) {
		require.Equal(t, manifestPath, path)
		evil := path + ".evil"
		require.NoError(t, os.WriteFile(evil, []byte(`{"type":"tampered"}`), 0o600))
		require.NoError(t, os.Rename(evil, path))
	}

	snap, err := OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.Equal(t, "idx_test", snap.Manifest.IndexSetID)
	require.Equal(t, "run_test", snap.Manifest.RunID)
	require.Equal(t, 1, snap.Manifest.Counts.Rows)

	// Path now holds tampered bytes; a re-open would fail or disagree.
	var tampered map[string]any
	require.NoError(t, json.Unmarshal(mustRead(t, manifestPath), &tampered))
	require.Equal(t, "tampered", tampered["type"])
}

func TestWalkSegmentFileVerified_UsesSameFDAfterPathReplacement(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"etag-a"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "data/b.xml", 20, `"etag-b"`, base.Add(time.Minute), nil, nil, nil, nil),
	}
	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:                  dir,
		IndexSetID:           "idx_test",
		RunID:                "run_test",
		CreatedAt:            base,
		TargetRowsPerSegment: 10,
	}, rows)
	require.NoError(t, err)
	require.Len(t, manifest.Segments, 1)
	segment := manifest.Segments[0]
	path := filepath.Join(dir, segment.Path)

	t.Cleanup(func() { afterSegmentDigestVerifiedForTest = nil })
	afterSegmentDigestVerifiedForTest = func(gotPath string) {
		require.Equal(t, path, gotPath)
		// Pathname replacement via rename: open FD keeps the old inode, so same-FD
		// parse must still succeed. (In-place truncate would clobber the open FD.)
		evil := gotPath + ".evil"
		require.NoError(t, os.WriteFile(evil, []byte("not-a-parquet-file"), 0o600))
		require.NoError(t, os.Rename(evil, gotPath))
	}

	var seen []string
	err = WalkSegmentFileVerified(dir, segment, func(row CurrentObjectRow) error {
		seen = append(seen, row.RelKey)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"data/a.xml", "data/b.xml"}, seen)

	// Pathname content is garbage; a re-open path would fail.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("not-a-parquet-file"), data)
}

func TestWalkSegmentFileVerified_RejectsDigestMismatch(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "data/a.xml", 10, `"etag-a"`, base, nil, nil, nil, nil),
	}
	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:                  dir,
		IndexSetID:           "idx_test",
		RunID:                "run_test",
		CreatedAt:            base,
		TargetRowsPerSegment: 10,
	}, rows)
	require.NoError(t, err)
	segment := manifest.Segments[0]
	segment.Digest.Hex = "0000000000000000000000000000000000000000000000000000000000000000"
	err = WalkSegmentFileVerified(dir, segment, func(CurrentObjectRow) error { return nil })
	require.Error(t, err)
	require.Contains(t, err.Error(), "digest mismatch")
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	return append(data, '\n')
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
