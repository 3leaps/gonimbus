package indexreader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

func TestPinnedRun_BypassesLatestAndRejectsTraversal(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("b-only.txt", 10, "eb", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	// Advance latest to a different run; pin must still open original.
	writeRunComplete(t, env, "run_later_0000000000000002", time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC))
	writeJSON(t, filepath.Join(env.segmentRoot, "latest.json"), map[string]any{
		"type":          "gonimbus.index.latest.v1",
		"index_set_id":  env.indexSetID,
		"run_id":        "run_later_0000000000000002",
		"updated_at":    time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		"complete_path": filepath.Join(env.segmentRoot, "runs", "run_later_0000000000000002", "complete.json"),
	})

	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{
		IndexSetID: env.indexSetID,
		RunID:      env.runID,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })
	require.Equal(t, env.runID, reader.Meta().RunID)
	results, _, err := reader.QueryObjects(ctx, indexstore.QueryParams{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "b-only.txt", results[0].RelKey)

	// Path traversal / multi-component run ids rejected at package seam.
	for _, bad := range []string{
		"../etc/passwd",
		"runs/../secret",
		"a/b",
		`a\b`,
		".",
		"..",
	} {
		_, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID, RunID: bad})
		require.Error(t, err, "run_id %q", bad)
		require.Contains(t, err.Error(), "run_id")
	}
}

func TestPinnedRun_RejectsIdentityMismatchMetadata(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("x.txt", 1, "e1", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})

	// Overwrite identity with a different base_uri payload so recomputed
	// IndexSetID cannot equal the pinned full set id. Metadata must not attach.
	wrongIdentity := map[string]any{
		"base_uri":         "s3://other-bucket/other/",
		"provider":         "s3",
		"storage_provider": "aws_s3",
		"build": map[string]any{
			"source_type":      "crawl",
			"schema_version":   indexstore.SchemaVersion,
			"gonimbus_version": "test",
			"includes":         []string{"**"},
		},
	}
	writeJSON(t, filepath.Join(env.identityDir, "identity.json"), wrongIdentity)

	reader, err := ResolveIndexReader(ctx, env.opts, ResolveTarget{
		IndexSetID: env.indexSetID,
		RunID:      env.runID,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })
	// Snapshot trust still succeeds; wrong identity must not attach metadata.
	require.Equal(t, "", reader.Meta().BaseURI)
	require.Equal(t, "", reader.Meta().IdentityDir)
}

func TestPinnedRun_RejectsCompleteSetRunMismatch(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("x.txt", 1, "e1", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	// Tamper complete marker identity.
	completePath := filepath.Join(env.segmentRoot, "runs", env.runID, "complete.json")
	data, err := os.ReadFile(completePath)
	require.NoError(t, err)
	var complete map[string]any
	require.NoError(t, json.Unmarshal(data, &complete))
	complete["run_id"] = "run_wrong_0000000000000009"
	writeJSON(t, completePath, complete)

	_, err = ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID, RunID: env.runID})
	require.Error(t, err)
}

func TestPinnedRun_RejectsDigestMismatch(t *testing.T) {
	ctx := context.Background()
	env := setupDurableTestEnv(t, []indexsubstrate.CurrentObjectRow{
		durableRow("x.txt", 1, "e1", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	completePath := filepath.Join(env.segmentRoot, "runs", env.runID, "complete.json")
	data, err := os.ReadFile(completePath)
	require.NoError(t, err)
	var complete map[string]any
	require.NoError(t, json.Unmarshal(data, &complete))
	// Wrong digest with otherwise valid JSON.
	sum := sha256.Sum256([]byte("not-the-manifest"))
	complete["manifest_sha256"] = hex.EncodeToString(sum[:])
	writeJSON(t, completePath, complete)

	_, err = ResolveIndexReader(ctx, env.opts, ResolveTarget{IndexSetID: env.indexSetID, RunID: env.runID})
	require.Error(t, err)
	require.Contains(t, err.Error(), "digest")
}

func TestPinnedRun_RequiresIndexSetID(t *testing.T) {
	_, err := ResolveIndexReader(context.Background(), ResolveOptions{
		SegmentCacheRoot: t.TempDir(),
	}, ResolveTarget{RunID: "run_1"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "run_id requires index_set_id")
}
