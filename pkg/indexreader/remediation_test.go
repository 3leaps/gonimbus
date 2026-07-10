package indexreader

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

func TestDurableWalkObjects_StreamsWithoutFullMaterializeAPI(t *testing.T) {
	ctx := context.Background()
	// Force multi-segment via TargetRowsPerSegment=1 in setup helper variant.
	env := setupDurableTestEnvMultiSegment(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a.txt", 1, "e1", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
		durableRow("b.txt", 2, "e2", time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)),
		durableRow("c.txt", 3, "e3", time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC)),
	}, 1)

	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	require.Equal(t, FormatDurableV2, reader.Meta().Format)

	var seen []string
	_, err = reader.WalkObjects(ctx, indexstore.QueryParams{IndexSetID: env.indexSetID}, func(r indexstore.QueryResult) error {
		seen = append(seen, r.RelKey)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"a.txt", "b.txt", "c.txt"}, seen)
}

func TestDurableWalkObjects_LaterSegmentDigestFailure(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnvMultiSegment(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a.txt", 1, "e1", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
		durableRow("b.txt", 2, "e2", time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)),
	}, 1)

	// Tamper second segment after publish so verify-before-emit fails mid-walk.
	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Locate a segment file and corrupt it.
	runDir := filepath.Join(env.segmentRoot, "runs", env.runID)
	entries, err := os.ReadDir(runDir)
	require.NoError(t, err)
	var segmentFiles []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".parquet" {
			segmentFiles = append(segmentFiles, filepath.Join(runDir, e.Name()))
		}
	}
	require.GreaterOrEqual(t, len(segmentFiles), 2)
	// Corrupt last segment.
	require.NoError(t, os.WriteFile(segmentFiles[len(segmentFiles)-1], []byte("tampered"), 0o600))

	var seen []string
	_, err = reader.WalkObjects(ctx, indexstore.QueryParams{IndexSetID: env.indexSetID}, func(r indexstore.QueryResult) error {
		seen = append(seen, r.RelKey)
		return nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "digest")
	// May have emitted verified prefix from earlier segments.
	require.Less(t, len(seen), 2)
}

func TestDurableWalkObjects_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := setupDurableTestEnvMultiSegment(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a.txt", 1, "e1", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
		durableRow("b.txt", 2, "e2", time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)),
		durableRow("c.txt", 3, "e3", time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC)),
	}, 1)
	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	n := 0
	_, err = reader.WalkObjects(ctx, indexstore.QueryParams{IndexSetID: env.indexSetID}, func(r indexstore.QueryResult) error {
		_ = r
		n++
		if n == 1 {
			cancel()
		}
		return nil
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled))
}

func TestMarkerTypeRequired(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("x.txt", 1, "e1", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})

	// Missing type on latest.json.
	latestPath := filepath.Join(env.segmentRoot, "latest.json")
	writeJSON(t, latestPath, map[string]any{
		"index_set_id":  env.indexSetID,
		"run_id":        env.runID,
		"updated_at":    time.Now().UTC().Format(time.RFC3339Nano),
		"complete_path": filepath.Join(env.segmentRoot, "runs", env.runID, "complete.json"),
	})
	_, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.Error(t, err)
	require.Contains(t, err.Error(), "type is required")

	// Unknown type.
	writeJSON(t, latestPath, map[string]any{
		"type":          "gonimbus.index.latest.v999",
		"index_set_id":  env.indexSetID,
		"run_id":        env.runID,
		"updated_at":    time.Now().UTC().Format(time.RFC3339Nano),
		"complete_path": filepath.Join(env.segmentRoot, "runs", env.runID, "complete.json"),
	})
	_, err = ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not supported")

	// Also reject missing complete type via substrate open.
	writeJSON(t, latestPath, map[string]any{
		"type":          "gonimbus.index.latest.v1",
		"index_set_id":  env.indexSetID,
		"run_id":        env.runID,
		"updated_at":    time.Now().UTC().Format(time.RFC3339Nano),
		"complete_path": filepath.Join(env.segmentRoot, "runs", env.runID, "complete.json"),
	})
	writeJSON(t, filepath.Join(env.segmentRoot, "runs", env.runID, "complete.json"), map[string]any{
		"index_set_id":    env.indexSetID,
		"run_id":          env.runID,
		"completed_at":    time.Now().UTC().Format(time.RFC3339Nano),
		"manifest_path":   filepath.Join(env.segmentRoot, "runs", env.runID, "manifest.json"),
		"manifest_sha256": "deadbeef",
		"segment_dir":     filepath.Join(env.segmentRoot, "runs", env.runID),
		"segments":        0,
	})
	_, err = ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.Error(t, err)
	require.Contains(t, err.Error(), "type is required")
}

func TestDurableQuery_PredicateMatrixAndCount(t *testing.T) {
	ctx := context.Background()
	mod := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	scGlacier := "GLACIER"
	enriched := time.Date(2025, 6, 16, 0, 0, 0, 0, time.UTC)
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		func() indexsubstrate.CurrentObjectRow {
			r := durableRow("keep/a.json", 1000, "e1", mod)
			r.StorageClass = &scGlacier
			r.HeadEnrichedAt = &enriched
			r.ContentType = strPtr("application/json")
			return r
		}(),
		durableRow("keep/b.xml", 50, "e2", mod.Add(-24*time.Hour)),
		func() indexsubstrate.CurrentObjectRow {
			r := durableRow("drop/c.txt", 5000, "e3", mod)
			del := mod
			r.DeletedAt = &del
			return r
		}(),
	})
	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Pattern + min-size + storage-class + enriched-after
	params := indexstore.QueryParams{
		IndexSetID:     env.indexSetID,
		Pattern:        "keep/**",
		MinSize:        100,
		StorageClasses: []string{"GLACIER"},
		EnrichedAfter:  enriched.Add(-time.Hour),
	}
	results, _, err := reader.QueryObjects(ctx, params)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "keep/a.json", results[0].RelKey)
	require.NotNil(t, results[0].HeadEnrichedAt)
	require.NotNil(t, results[0].ContentType)

	count, err := reader.QueryObjectCount(ctx, params)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	// include-deleted
	all, _, err := reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID:     env.indexSetID,
		IncludeDeleted: true,
	})
	require.NoError(t, err)
	require.Len(t, all, 3)

	// limit
	limited, _, err := reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		Limit:      1,
	})
	require.NoError(t, err)
	require.Len(t, limited, 1)

	// key-regex
	reResults, _, err := reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		KeyRegex:   `\.xml$`,
	})
	require.NoError(t, err)
	require.Len(t, reResults, 1)
	require.Equal(t, "keep/b.xml", reResults[0].RelKey)
}

func setupDurableTestEnvMultiSegment(t *testing.T, rows []indexsubstrate.CurrentObjectRow, targetRows int) durableTestEnv {
	t.Helper()
	// Reuse setup then rewrite with smaller segment size.
	env := setupDurableTestEnv(t, rows)
	// Rebuild segments with target size.
	runDir := filepath.Join(env.segmentRoot, "runs", env.runID)
	// Clear old segment artifacts except identity chain by rewriting fully.
	require.NoError(t, os.RemoveAll(runDir))
	require.NoError(t, os.MkdirAll(runDir, 0o755))
	for i := range rows {
		rows[i].IndexSetID = env.indexSetID
	}
	createdAt := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir:                  runDir,
		IndexSetID:           env.indexSetID,
		RunID:                env.runID,
		CreatedAt:            createdAt,
		TargetRowsPerSegment: targetRows,
	}, rows)
	require.NoError(t, err)
	manifestPath := filepath.Join(runDir, "manifest.json")
	require.NoError(t, indexsubstrate.WriteInternalManifestFile(manifestPath, manifest))
	manifestSHA, err := hashFileSHA256(manifestPath)
	require.NoError(t, err)
	completePath := filepath.Join(runDir, "complete.json")
	writeJSON(t, completePath, map[string]any{
		"type":            "gonimbus.index.complete.v1",
		"index_set_id":    env.indexSetID,
		"run_id":          env.runID,
		"completed_at":    createdAt.Format(time.RFC3339Nano),
		"manifest_path":   manifestPath,
		"manifest_sha256": manifestSHA,
		"segment_dir":     runDir,
		"segments":        len(manifest.Segments),
	})
	writeJSON(t, filepath.Join(env.segmentRoot, "latest.json"), map[string]any{
		"type":          "gonimbus.index.latest.v1",
		"index_set_id":  env.indexSetID,
		"run_id":        env.runID,
		"updated_at":    createdAt.Format(time.RFC3339Nano),
		"complete_path": completePath,
	})
	return env
}

func strPtr(s string) *string { return &s }
