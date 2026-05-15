package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fulmenhq/gofulmen/appidentity"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/atlas"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
)

func TestRunAtlasBuildAndStats_LocalArtifact(t *testing.T) {
	ctx := context.Background()
	withAtlasCommandTestState(t)

	indexSet, run := createAtlasCommandIndex(t, ctx, "abc123")
	recipePath := filepath.Join(t.TempDir(), "atlas-recipe.yaml")
	require.NoError(t, os.WriteFile(recipePath, []byte(`version: "1.0"
coverage: full
dimensions:
  - name: event_date
    kind: temporal-day
    classification: 1-confidential
    extractor:
      type: json_path
      json_path: $.event_date
shard_by: [event_date]
`), 0644))

	oldFactory := newAtlasBuildProvider
	newAtlasBuildProvider = func(context.Context, s3.Config) (atlasBuildProvider, error) {
		return fakeAtlasBuildProvider{
			"prefix/a.json": []byte(`{"event_date":"2026-05-01"}`),
			"prefix/b.json": []byte(`{"event_date":"2026-05-01","record":"b"}`),
		}, nil
	}
	t.Cleanup(func() { newAtlasBuildProvider = oldFactory })

	outDir := filepath.Join(t.TempDir(), "atlas")
	buildCmd := newAtlasBuildTestCommand()
	var buildOut bytes.Buffer
	buildCmd.SetOut(&buildOut)
	buildCmd.SetArgs([]string{
		"--from-index", indexSet.IndexSetID,
		"--run", run.RunID,
		"--recipe", recipePath,
		"--output", outDir,
		"--json",
	})
	buildCmd.SetContext(ctx)
	require.NoError(t, buildCmd.Execute())
	require.FileExists(t, filepath.Join(outDir, atlas.HeaderFile))
	require.FileExists(t, filepath.Join(outDir, atlas.ShardsDir, "2026-05-01.jsonl"))

	var header atlas.Header
	require.NoError(t, json.Unmarshal(buildOut.Bytes(), &header))
	require.Equal(t, atlas.SchemaVersion, header.SchemaVersion)
	require.Equal(t, indexSet.IndexSetID, header.SourceIndexSetID)
	require.Equal(t, run.RunID, header.SourceRunID)
	require.Equal(t, "abc123", header.ScopeDigest)
	require.Equal(t, int64(2), header.Counts.RowsWritten)

	statsCmd := &cobra.Command{Use: "stats", Args: cobra.ExactArgs(1), RunE: runAtlasStats}
	statsCmd.Flags().Bool("json", false, "")
	var statsOut bytes.Buffer
	statsCmd.SetOut(&statsOut)
	statsCmd.SetArgs([]string{outDir, "--json"})
	require.NoError(t, statsCmd.Execute())

	var stats atlas.Stats
	require.NoError(t, json.Unmarshal(statsOut.Bytes(), &stats))
	require.Equal(t, int64(2), stats.Tier1Keys)
	require.Equal(t, int64(2), stats.Tier2Content)
	require.Equal(t, int64(2), stats.Tier3Shards)
}

func withAtlasCommandTestState(t *testing.T) {
	t.Helper()
	oldIdentity := appIdentity
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	appIdentity = &appidentity.Identity{BinaryName: "gonimbus", ConfigName: "gonimbus"}
	t.Cleanup(func() { appIdentity = oldIdentity })
}

func createAtlasCommandIndex(t *testing.T, ctx context.Context, scopeHash string) (*indexstore.IndexSet, *indexstore.IndexRun) {
	t.Helper()
	params := testIndexSetParams("s3://bucket/prefix/")
	params.BuildParams.ScopeHash = scopeHash
	identity, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)

	root, err := indexRootDir()
	require.NoError(t, err)
	indexDir := filepath.Join(root, identity.DirName)
	require.NoError(t, os.MkdirAll(indexDir, 0755))
	db, err := indexstore.Open(ctx, indexstore.Config{Path: filepath.Join(indexDir, "index.db")})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)
	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	seenAt := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	require.NoError(t, indexstore.BatchUpsertObjects(ctx, db, []indexstore.ObjectRow{
		{IndexSetID: indexSet.IndexSetID, RelKey: "a.json", SizeBytes: 31, LastSeenRunID: run.RunID, LastSeenAt: seenAt},
		{IndexSetID: indexSet.IndexSetID, RelKey: "b.json", SizeBytes: 31, LastSeenRunID: run.RunID, LastSeenAt: seenAt},
	}))
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusSuccess, nil))
	require.NoError(t, os.WriteFile(filepath.Join(indexDir, "identity.json"), []byte(identity.CanonicalJSON+"\n"), 0644))
	_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, db.Close())
	return indexSet, run
}

func newAtlasBuildTestCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "build", Args: cobra.NoArgs, RunE: runAtlasBuild}
	cmd.Flags().String("from-index", "", "")
	cmd.Flags().String("run", "", "")
	cmd.Flags().String("recipe", "", "")
	cmd.Flags().String("output", "", "")
	cmd.Flags().StringP("region", "r", "", "")
	cmd.Flags().StringP("profile", "p", "", "")
	cmd.Flags().String("endpoint", "", "")
	cmd.Flags().Bool("json", false, "")
	return cmd
}

type fakeAtlasBuildProvider map[string][]byte

func (p fakeAtlasBuildProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return nil, fmt.Errorf("unexpected List")
}

func (p fakeAtlasBuildProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, fmt.Errorf("unexpected Head")
}

func (p fakeAtlasBuildProvider) Close() error { return nil }

func (p fakeAtlasBuildProvider) GetObject(_ context.Context, key string) (io.ReadCloser, int64, error) {
	data, ok := p[key]
	if !ok {
		return nil, 0, fmt.Errorf("not found: %s", key)
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}
