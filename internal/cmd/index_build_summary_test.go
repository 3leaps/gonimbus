package cmd

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

func TestIndexBuildSummaryFlag_DefaultOff(t *testing.T) {
	flag := indexBuildCmd.Flags().Lookup("summary")
	require.NotNil(t, flag)
	require.Equal(t, "false", flag.DefValue)
}

func TestIndexBuildBackgroundSummaryRejected(t *testing.T) {
	withIndexBuildModes(t, true, false, true)

	err := validateIndexBuildBackgroundFlags()
	require.ErrorContains(t, err, "--background is not compatible with --summary")
}

func TestIndexBuildBackgroundDryRunRejected(t *testing.T) {
	withIndexBuildModes(t, true, true, false)

	err := validateIndexBuildBackgroundFlags()
	require.ErrorContains(t, err, "--background is not compatible with --dry-run")
}

func TestIndexBuildBackgroundFlagsAccepted(t *testing.T) {
	withIndexBuildModes(t, true, false, false)

	require.NoError(t, validateIndexBuildBackgroundFlags())
}

func withIndexBuildModes(t *testing.T, background, dryRun, summary bool) {
	t.Helper()

	oldBackground := indexBuildBackground
	oldDryRun := indexBuildDryRun
	oldSummary := indexBuildSummary
	indexBuildBackground = background
	indexBuildDryRun = dryRun
	indexBuildSummary = summary
	t.Cleanup(func() {
		indexBuildBackground = oldBackground
		indexBuildDryRun = oldDryRun
		indexBuildSummary = oldSummary
	})
}

func TestPrintIndexBuildSummary(t *testing.T) {
	ctx := context.Background()
	db, err := indexstore.Open(ctx, indexstore.Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, indexstore.Migrate(ctx, db))

	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, indexstore.IndexSetParams{
		BaseURI:  "s3://bucket/base/",
		Provider: "s3",
		BuildParams: indexstore.BuildParams{
			SourceType:    "crawl",
			SchemaVersion: indexstore.SchemaVersion,
		},
	})
	require.NoError(t, err)

	oldRun, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	now := time.Now().UTC()
	require.NoError(t, indexstore.BatchUpsertObjects(ctx, db, []indexstore.ObjectRow{
		{IndexSetID: indexSet.IndexSetID, RelKey: "data/a/file.json", SizeBytes: 100, LastSeenRunID: run.RunID, LastSeenAt: now},
		{IndexSetID: indexSet.IndexSetID, RelKey: "data/b/file.json", SizeBytes: 300, LastSeenRunID: run.RunID, LastSeenAt: now},
		{IndexSetID: indexSet.IndexSetID, RelKey: "logs/file.json", SizeBytes: 200, LastSeenRunID: run.RunID, LastSeenAt: now},
		{IndexSetID: indexSet.IndexSetID, RelKey: "old/file.json", SizeBytes: 999, LastSeenRunID: oldRun.RunID, LastSeenAt: now},
	}))

	var out bytes.Buffer
	require.NoError(t, printIndexBuildSummary(ctx, db, indexSet.IndexSetID, run.RunID, &out))

	rendered := out.String()
	require.Contains(t, rendered, "Top-level object summary:")
	require.Contains(t, rendered, "PREFIX")
	require.Contains(t, rendered, "OBJECTS")
	require.Contains(t, rendered, "SIZE")
	require.Contains(t, rendered, "data/")
	require.Contains(t, rendered, "2")
	require.Contains(t, rendered, "400 B")
	require.Contains(t, rendered, "logs/")
	require.NotContains(t, rendered, "old/")
}

func TestPrintIndexBuildSummary_Empty(t *testing.T) {
	ctx := context.Background()
	db, err := indexstore.Open(ctx, indexstore.Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, indexstore.Migrate(ctx, db))

	var out bytes.Buffer
	require.NoError(t, printIndexBuildSummary(ctx, db, "idx_missing", "run_missing", &out))
	require.Contains(t, out.String(), "(no objects seen in this run)")
}
