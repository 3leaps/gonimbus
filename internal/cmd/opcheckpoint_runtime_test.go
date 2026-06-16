package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestWriteOperationErrorSummaryIncludesResumeHintAndSortedProgress(t *testing.T) {
	var buf bytes.Buffer
	writeOperationErrorSummary(&buf, "Index build failed with resumable checkpoint", operationIndexBuild, "run_123", opcheckpoint.ErrorClassCredentialsRefreshFailed, map[string]int64{
		"prefixes_ingested": 2,
		"objects_ingested":  10,
	})

	text := buf.String()
	require.Contains(t, text, "Index build failed with resumable checkpoint")
	require.Contains(t, text, "  run_id: run_123\n")
	require.Contains(t, text, "  status: failed-resumable\n")
	require.Contains(t, text, "  error_class: credentials_refresh_failed\n")
	require.Contains(t, text, "  resume_command: gonimbus index build --resume-run run_123\n")
	require.Less(t, strings.Index(text, "objects_ingested"), strings.Index(text, "prefixes_ingested"))
	require.NotContains(t, text, "Usage:")
	require.NotContains(t, text, "s3://")
}

func TestWriteOperationErrorSummaryIncludesCause(t *testing.T) {
	var buf bytes.Buffer
	writeOperationErrorSummaryWithCause(&buf, "Transfer reflow failed with resumable checkpoint", operationTransferReflow, "run_123", opcheckpoint.ErrorClassInterrupted, &opcheckpoint.ErrorCause{
		Code:        "TRANSIENT",
		Reason:      "transient.network",
		Message:     "context deadline exceeded",
		Resumable:   true,
		Disposition: "aborted_resumable_checkpoint",
	}, nil)

	text := buf.String()
	require.Contains(t, text, "  error_class: interrupted\n")
	require.Contains(t, text, "  cause_code: TRANSIENT\n")
	require.Contains(t, text, "  cause_reason: transient.network\n")
	require.Contains(t, text, "  cause_message: context deadline exceeded\n")
	require.Contains(t, text, "  cause_resumable: true\n")
	require.Contains(t, text, "  cause_disposition: aborted_resumable_checkpoint\n")
}

func TestStopResumeLeaseHeartbeatReturnsLeaseLoss(t *testing.T) {
	store := newRuntimeTestOperationStore(t)

	runID := "run_heartbeat_lost"
	lease, err := store.ClaimLease(context.Background(), operationIndexBuild, runID, "holder-a", time.Hour)
	require.NoError(t, err)
	heartbeat, err := store.StartLeaseHeartbeat(context.Background(), operationIndexBuild, lease, time.Millisecond, time.Hour)
	require.NoError(t, err)

	writeLeaseForRuntimeTest(t, filepath.Join(store.RootDir(), operationIndexBuild, runID, "resume.lease.json"), opcheckpoint.Lease{
		RunID:     runID,
		HolderID:  "holder-b",
		ClaimedAt: time.Now().UTC(),
		ExpiresAt: time.Now().Add(time.Hour).UTC(),
	})

	require.Eventually(t, func() bool {
		select {
		case <-heartbeat.Context().Done():
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, stopResumeLeaseHeartbeat(heartbeat), opcheckpoint.ErrLeaseHeld)
}

func TestLostResumeLeaseSkipsFailedResumableIndexCheckpointWrite(t *testing.T) {
	ctx, db, indexSet := setupIndexBuildResumeDB(t)
	defer func() { _ = db.Close() }()

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusFailedResumable, nil))
	require.NoError(t, indexstore.MarkIndexRunResuming(ctx, db, run.RunID))
	store := newRuntimeTestOperationStore(t)
	lease, err := store.ClaimLease(context.Background(), operationIndexBuild, run.RunID, "holder-a", time.Hour)
	require.NoError(t, err)
	heartbeat, err := store.StartLeaseHeartbeat(context.Background(), operationIndexBuild, lease, time.Millisecond, time.Hour)
	require.NoError(t, err)

	writeLeaseForRuntimeTest(t, filepath.Join(store.RootDir(), operationIndexBuild, run.RunID, "resume.lease.json"), opcheckpoint.Lease{
		RunID:     run.RunID,
		HolderID:  "holder-b",
		ClaimedAt: time.Now().UTC(),
		ExpiresAt: time.Now().Add(time.Hour).UTC(),
	})

	require.Eventually(t, func() bool {
		return heartbeat.Context().Err() != nil
	}, time.Second, time.Millisecond)

	err = stopResumeLeaseHeartbeatBeforeFailedResumableCheckpoint(heartbeat)
	require.ErrorIs(t, err, opcheckpoint.ErrLeaseHeld)
	if err == nil {
		require.NoError(t, writeIndexRunCheckpoint(context.Background(), store, db, run.RunID, operationIndexBuild, "fingerprint-a", opcheckpoint.ErrorClassInterrupted, nil, map[string]string{"status": "interrupted"}))
	}

	_, readErr := store.ReadCheckpoint(context.Background(), operationIndexBuild, run.RunID)
	require.True(t, os.IsNotExist(readErr), "got error: %v", readErr)
	updated, err := indexstore.GetIndexRun(context.Background(), db, run.RunID)
	require.NoError(t, err)
	require.Equal(t, indexstore.RunStatus(indexstore.RunStatusRunning), updated.Status)
}

func TestOperatorCancellationAllowsFailedResumableCheckpointWrite(t *testing.T) {
	ctx, db, indexSet := setupIndexBuildResumeDB(t)
	defer func() { _ = db.Close() }()

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusFailedResumable, nil))
	require.NoError(t, indexstore.MarkIndexRunResuming(ctx, db, run.RunID))
	store := newRuntimeTestOperationStore(t)
	lease, err := store.ClaimLease(context.Background(), operationIndexBuild, run.RunID, "holder-a", time.Hour)
	require.NoError(t, err)
	parentCtx, cancel := context.WithCancel(context.Background())
	heartbeat, err := store.StartLeaseHeartbeat(parentCtx, operationIndexBuild, lease, time.Hour, time.Hour)
	require.NoError(t, err)
	cancel()

	require.NoError(t, stopResumeLeaseHeartbeatBeforeFailedResumableCheckpoint(heartbeat))
	require.NoError(t, writeIndexRunCheckpoint(context.Background(), store, db, run.RunID, operationIndexBuild, "fingerprint-a", opcheckpoint.ErrorClassInterrupted, nil, map[string]string{"status": "interrupted"}))

	env, err := store.ReadCheckpoint(context.Background(), operationIndexBuild, run.RunID)
	require.NoError(t, err)
	require.Equal(t, opcheckpoint.StatusFailedResumable, env.Status)
	updated, err := indexstore.GetIndexRun(context.Background(), db, run.RunID)
	require.NoError(t, err)
	require.Equal(t, indexstore.RunStatus(indexstore.RunStatusFailedResumable), updated.Status)
}

func newRuntimeTestOperationStore(t *testing.T) *opcheckpoint.Store {
	t.Helper()
	store, err := opcheckpoint.Open(context.Background(), opcheckpoint.Config{AppDataDir: filepath.Join(t.TempDir(), "data")})
	require.NoError(t, err)
	return store
}

func writeLeaseForRuntimeTest(t *testing.T, path string, lease opcheckpoint.Lease) {
	t.Helper()
	data, err := json.Marshal(lease)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
}
