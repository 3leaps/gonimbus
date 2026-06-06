package cmd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/fulmenhq/gofulmen/appidentity"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
)

func TestOpenDefaultOperationCheckpointStoreRejectsRepoRootFromNestedCwd(t *testing.T) {
	repoRoot := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(repoRoot, "go.mod"), []byte("module example.test/gonimbus\n"), 0o644))
	nested := filepath.Join(repoRoot, "internal", "cmd")
	require.NoError(t, os.MkdirAll(nested, 0o755))

	originalWD, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(nested))
	t.Cleanup(func() { require.NoError(t, os.Chdir(originalWD)) })

	originalIdentity := appIdentity
	appIdentity = &appidentity.Identity{
		BinaryName: "gonimbus",
		ConfigName: "gonimbus",
		EnvPrefix:  "GONIMBUS_",
	}
	t.Cleanup(func() { appIdentity = originalIdentity })
	t.Setenv("XDG_DATA_HOME", repoRoot)

	_, err = openDefaultOperationCheckpointStore(context.Background())
	require.True(t, errors.Is(err, opcheckpoint.ErrPathInsideForbiddenRoot), "got error: %v", err)
}

func TestDiscoverRepositoryRootPrefersNearestMarker(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.test/root\n"), 0o644))
	nested := filepath.Join(root, "a", "b")
	require.NoError(t, os.MkdirAll(nested, 0o755))

	got, err := discoverRepositoryRoot(nested)
	require.NoError(t, err)
	require.Equal(t, root, got)
}

func TestIndexRunResumeCandidateAllowsRunningOnlyWithFailedResumableCheckpoint(t *testing.T) {
	ctx, db, indexSet := setupIndexBuildResumeDB(t)
	defer func() { _ = db.Close() }()

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	require.NoError(t, validateIndexRunResumeCandidate(run, indexSet, "crawl", "index build", opcheckpoint.StatusFailedResumable))
	require.ErrorContains(t,
		validateIndexRunResumeCandidate(run, indexSet, "crawl", "index build", opcheckpoint.StatusSuccess),
		"not a failed-resumable index build run",
	)
	require.ErrorContains(t,
		validateIndexRunResumeCandidate(run, indexSet, enrichHeadSourceType, "enrich-with-head", opcheckpoint.StatusFailedResumable),
		"not a failed-resumable enrich-with-head run",
	)

	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusSuccess, nil))
	successRun, err := indexstore.GetIndexRun(ctx, db, run.RunID)
	require.NoError(t, err)
	require.ErrorContains(t,
		validateIndexRunResumeCandidate(successRun, indexSet, "crawl", "index build", opcheckpoint.StatusFailedResumable),
		"not a failed-resumable index build run",
	)
}

func TestRecoverIndexRunResumeCrashRestoresFailedResumableBeforeMarkingResuming(t *testing.T) {
	ctx, db, indexSet := setupIndexBuildResumeDB(t)
	defer func() { _ = db.Close() }()

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.Equal(t, indexstore.RunStatus(indexstore.RunStatusRunning), run.Status)

	require.NoError(t, recoverIndexRunResumeCrash(context.Background(), db, run))

	recovered, err := indexstore.GetIndexRun(ctx, db, run.RunID)
	require.NoError(t, err)
	require.Equal(t, indexstore.RunStatus(indexstore.RunStatusFailedResumable), recovered.Status)
	require.NotNil(t, recovered.EndedAt)

	events, err := indexstore.ListRunEvents(ctx, db, run.RunID, nil)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "resume_recovered", events[0].EventType)

	require.NoError(t, indexstore.MarkIndexRunResuming(ctx, db, run.RunID))
	resuming, err := indexstore.GetIndexRun(ctx, db, run.RunID)
	require.NoError(t, err)
	require.Equal(t, indexstore.RunStatus(indexstore.RunStatusRunning), resuming.Status)
	require.Nil(t, resuming.EndedAt)
}
