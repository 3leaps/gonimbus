package indexreader

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

func TestDurableQuery_WithoutIndexDB(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a/one.json", 100, "etag-a", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
		durableRow("a/two.xml", 200, "etag-b", time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)),
		durableRow("b/three.json", 300, "etag-c", time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)),
	})

	// No index.db anywhere — default-build gap must be closed.
	require.NoFileExists(t, filepath.Join(env.identityDir, "index.db"))

	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	require.Equal(t, FormatDurableV2, reader.Meta().Format)
	require.Equal(t, env.indexSetID, reader.Meta().IndexSetID)
	require.Equal(t, env.baseURI, reader.Meta().BaseURI)

	results, _, err := reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		Pattern:    "**/*.json",
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, "a/one.json", results[0].RelKey)
	require.Equal(t, "b/three.json", results[1].RelKey)

	count, err := reader.QueryObjectCount(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		Pattern:    "a/**",
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

func TestDurableQuery_BaseURIResolve(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("x.txt", 1, "e1", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})

	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{BaseURI: env.baseURI})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	require.Equal(t, FormatDurableV2, reader.Meta().Format)

	results, _, err := reader.QueryObjects(ctx, indexstore.QueryParams{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestDurableQuery_RejectLayoutGuessing(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	indexes := filepath.Join(root, "indexes")
	segments := filepath.Join(root, "segments")
	// Segment-shaped directory without latest/complete markers.
	require.NoError(t, os.MkdirAll(filepath.Join(segments, "idx_abcdef0123456789", "runs", "run_1"), 0o755))

	_, err := ResolveIndexReader(ctx, ResolveOptions{
		IndexesRoot:      indexes,
		SegmentCacheRoot: segments,
	}, ResolveTarget{IndexSetID: "idx_abcdef0123456789"})
	require.Error(t, err)
}

func TestDurableQuery_SinceRunUnsupportedFailClosed(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("x.txt", 1, "e1", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Sibling complete markers must not enable approximate --since-run.
	writeRunComplete(t, env, "run_unlinked_sibling", time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC))

	_, err = reader.ResolveSinceRunFilter(ctx, "run_unlinked_sibling")
	require.ErrorIs(t, err, ErrDurableSinceRunUnsupported)

	_, _, err = reader.QueryObjects(ctx, indexstore.QueryParams{
		IndexSetID: env.indexSetID,
		SinceRun:   &indexstore.SinceRunFilter{RunID: "run_unlinked_sibling", StartedAt: time.Now().UTC()},
	})
	require.ErrorIs(t, err, ErrDurableSinceRunUnsupported)
}

func TestDurableQuery_CanonicalByETag(t *testing.T) {
	ctx := context.Background()
	mod := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a.txt", 1, "same", mod),
		durableRow("b.txt", 2, "same", mod),
		durableRow("c.txt", 3, "", mod),
	})
	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	out, stats, err := reader.QueryCanonicalObjects(ctx, indexstore.QueryParams{
		IndexSetID:        env.indexSetID,
		CanonicalTieBreak: indexstore.CanonicalTieBreakMinKey,
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.CanonicalGroups)
	require.Equal(t, 1, stats.PassthroughRows)
	require.Equal(t, 2, stats.TotalRecords)
	require.Len(t, out, 2)
}

func TestDurablePreferredWhenBothPresent(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("a.txt", 1, "e", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	// Also write a sqlite index.db with same identity.
	dbPath := filepath.Join(env.identityDir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	_, _, err = indexstore.FindOrCreateIndexSet(ctx, db, env.params)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// A verified durable latest outranks the set-root index.db, including for
	// base-URI targets that discover the set through the identity directory.
	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{BaseURI: env.baseURI})
	require.NoError(t, err)
	require.Equal(t, FormatDurableV2, reader.Meta().Format)
	require.NoError(t, reader.Close())

	// Without a durable latest the set keeps its existing SQLite selection.
	latestPath := filepath.Join(env.segmentRoot, "latest.json")
	require.NoError(t, os.Rename(latestPath, latestPath+".bak"))
	reader, err = ResolveIndexReader(ctx, env.opts, ResolveTarget{BaseURI: env.baseURI})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	require.Equal(t, FormatSQLiteV1, reader.Meta().Format)
}

type durableTestEnv struct {
	opts        ResolveOptions
	baseURI     string
	indexSetID  string
	identityDir string
	segmentRoot string
	runID       string
	params      indexstore.IndexSetParams
}

func setupDurableTestEnv(t *testing.T, rows []indexsubstrate.CurrentObjectRow) durableTestEnv {
	t.Helper()
	root := t.TempDir()
	indexesRoot := filepath.Join(root, "indexes")
	segmentCacheRoot := filepath.Join(root, "segments")
	baseURI := "s3://test-bucket/data/"
	params := indexstore.IndexSetParams{
		BaseURI:         baseURI,
		Provider:        "s3",
		StorageProvider: "aws_s3",
		CloudProvider:   "aws",
		RegionKind:      "aws",
		Region:          "us-east-1",
		BuildParams: indexstore.BuildParams{
			SourceType:      "crawl",
			SchemaVersion:   indexstore.SchemaVersion,
			GonimbusVersion: "test",
			Includes:        []string{"**"},
		},
	}
	identity, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)

	identityDir := filepath.Join(indexesRoot, identity.DirName)
	require.NoError(t, os.MkdirAll(identityDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(identityDir, "identity.json"), []byte(identity.CanonicalJSON+"\n"), 0o600))

	runID := "run_test_1"
	segmentRoot := filepath.Join(segmentCacheRoot, identity.IndexSetID)
	runDir := filepath.Join(segmentRoot, "runs", runID)
	require.NoError(t, os.MkdirAll(runDir, 0o755))

	for i := range rows {
		rows[i].IndexSetID = identity.IndexSetID
		if rows[i].FirstSeenRunID == "" {
			rows[i].FirstSeenRunID = runID
		}
		if rows[i].LastChangedRunID == "" {
			rows[i].LastChangedRunID = runID
		}
		if rows[i].LastSeenRunID == "" {
			rows[i].LastSeenRunID = runID
		}
	}

	createdAt := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir:                  runDir,
		IndexSetID:           identity.IndexSetID,
		RunID:                runID,
		CreatedAt:            createdAt,
		TargetRowsPerSegment: 100,
	}, rows)
	require.NoError(t, err)

	manifestPath := filepath.Join(runDir, "manifest.json")
	require.NoError(t, indexsubstrate.WriteInternalManifestFile(manifestPath, manifest))
	manifestSHA, err := hashFileSHA256(manifestPath)
	require.NoError(t, err)

	complete := map[string]any{
		"type":            "gonimbus.index.complete.v1",
		"index_set_id":    identity.IndexSetID,
		"run_id":          runID,
		"completed_at":    createdAt.Format(time.RFC3339Nano),
		"manifest_path":   manifestPath,
		"manifest_sha256": manifestSHA,
		"segment_dir":     runDir,
		"segments":        len(manifest.Segments),
	}
	completePath := filepath.Join(runDir, "complete.json")
	writeJSON(t, completePath, complete)

	latest := map[string]any{
		"type":          "gonimbus.index.latest.v1",
		"index_set_id":  identity.IndexSetID,
		"run_id":        runID,
		"updated_at":    createdAt.Format(time.RFC3339Nano),
		"complete_path": completePath,
	}
	writeJSON(t, filepath.Join(segmentRoot, "latest.json"), latest)

	return durableTestEnv{
		opts: ResolveOptions{
			IndexesRoot:      indexesRoot,
			SegmentCacheRoot: segmentCacheRoot,
		},
		baseURI:     baseURI,
		indexSetID:  identity.IndexSetID,
		identityDir: identityDir,
		segmentRoot: segmentRoot,
		runID:       runID,
		params:      params,
	}
}

func writeRunComplete(t *testing.T, env durableTestEnv, runID string, createdAt time.Time) {
	t.Helper()
	runDir := filepath.Join(env.segmentRoot, "runs", runID)
	require.NoError(t, os.MkdirAll(runDir, 0o755))
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir:                  runDir,
		IndexSetID:           env.indexSetID,
		RunID:                runID,
		CreatedAt:            createdAt,
		TargetRowsPerSegment: 100,
	}, nil)
	require.NoError(t, err)
	manifestPath := filepath.Join(runDir, "manifest.json")
	require.NoError(t, indexsubstrate.WriteInternalManifestFile(manifestPath, manifest))
	manifestSHA, err := hashFileSHA256(manifestPath)
	require.NoError(t, err)
	writeJSON(t, filepath.Join(runDir, "complete.json"), map[string]any{
		"type":            "gonimbus.index.complete.v1",
		"index_set_id":    env.indexSetID,
		"run_id":          runID,
		"completed_at":    createdAt.Format(time.RFC3339Nano),
		"manifest_path":   manifestPath,
		"manifest_sha256": manifestSHA,
		"segment_dir":     runDir,
		"segments":        len(manifest.Segments),
	})
}

func durableRow(relKey string, size int64, etag string, mod time.Time) indexsubstrate.CurrentObjectRow {
	return indexsubstrate.CurrentObjectRow{
		RelKey:           relKey,
		SizeBytes:        size,
		LastModified:     &mod,
		ETag:             etag,
		FirstSeenRunID:   "run_test_1",
		FirstSeenAt:      mod,
		LastChangedRunID: "run_test_1",
		LastChangedAt:    mod,
		LastSeenRunID:    "run_test_1",
		LastSeenAt:       mod,
	}
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))
}

func hashFileSHA256(path string) (string, error) {
	// Use OpenLatest trust path hashing via reading publish helpers: recompute with os.
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256Sum(data)
	return sum, nil
}
