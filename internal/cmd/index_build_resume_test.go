package cmd

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
)

func TestIndexBuildResumeRunDoesNotRequireJobFlag(t *testing.T) {
	oldJob := indexBuildJobPath
	oldResumeRun := indexBuildResumeRun
	oldDryRun := indexBuildDryRun
	oldFormat := indexBuildFormat
	defer func() {
		indexBuildJobPath = oldJob
		indexBuildResumeRun = oldResumeRun
		indexBuildDryRun = oldDryRun
		indexBuildFormat = oldFormat
	}()

	// Resume is a SQLite-lifecycle path; keep format sqlite for this flag interaction test.
	indexBuildFormat = "sqlite"
	indexBuildJobPath = ""
	indexBuildResumeRun = ""
	indexBuildDryRun = false
	err := runIndexBuild(indexBuildCmd, nil)
	require.ErrorContains(t, err, "--job is required")

	indexBuildResumeRun = "run_123"
	indexBuildDryRun = true
	err = runIndexBuild(indexBuildCmd, nil)
	require.ErrorContains(t, err, "--dry-run is not compatible with --resume-run")
}

func TestIndexBuildResumeRunWithDefaultDurableFormatReachesResumePath(t *testing.T) {
	// After durable default flip, the printed operator hint
	// `gonimbus index build --resume-run <run_id>` (no --format) must still
	// enter the SQLite resume path, not fail durable format validation.
	oldJob := indexBuildJobPath
	oldResumeRun := indexBuildResumeRun
	oldDryRun := indexBuildDryRun
	oldFormat := indexBuildFormat
	oldExperimental := indexBuildExperimentalEngine
	defer func() {
		indexBuildJobPath = oldJob
		indexBuildResumeRun = oldResumeRun
		indexBuildDryRun = oldDryRun
		indexBuildFormat = oldFormat
		indexBuildExperimentalEngine = oldExperimental
	}()

	indexBuildJobPath = ""
	indexBuildResumeRun = "run_123"
	indexBuildDryRun = false
	indexBuildFormat = "durable" // default after Slice E
	indexBuildExperimentalEngine = false
	// Ensure cobra does not treat --format as operator-explicit.
	if f := indexBuildCmd.Flags().Lookup("format"); f != nil {
		_ = f.Value.Set("durable")
		f.Changed = false
	}

	err := runIndexBuild(indexBuildCmd, nil)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "--format durable is not compatible with --resume-run")
	require.NotContains(t, err.Error(), "not compatible with --resume-run in this slice")
	// Reaches runIndexBuildResume before format rejection. Without app data
	// identity / checkpoint store the resume path fails later — that is fine.
	require.True(t,
		strings.Contains(err.Error(), "read operation checkpoint") ||
			strings.Contains(err.Error(), "app identity is not available"),
		"expected resume-path error, got: %v", err)
}

func TestIndexBuildResumeRunRejectsExplicitDurableFormat(t *testing.T) {
	oldResumeRun := indexBuildResumeRun
	oldFormat := indexBuildFormat
	oldExperimental := indexBuildExperimentalEngine
	defer func() {
		indexBuildResumeRun = oldResumeRun
		indexBuildFormat = oldFormat
		indexBuildExperimentalEngine = oldExperimental
		if f := indexBuildCmd.Flags().Lookup("format"); f != nil {
			f.Changed = false
		}
	}()

	indexBuildResumeRun = "run_123"
	indexBuildFormat = "durable"
	indexBuildExperimentalEngine = false
	if f := indexBuildCmd.Flags().Lookup("format"); f != nil {
		_ = f.Value.Set("durable")
		f.Changed = true
	}

	err := validateIndexBuildResumeInvocation(indexBuildCmd)
	require.ErrorContains(t, err, "--resume-run is not compatible with --format durable")
}

func TestIndexBuildResumeIdentityRejectsTamperedCheckpointConfig(t *testing.T) {
	ctx, db, indexSet := setupIndexBuildResumeDB(t)
	defer func() { _ = db.Close() }()

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusFailedResumable, nil))

	original := indexBuildCheckpointConfig{
		IndexSetID:  indexSet.IndexSetID,
		SourceType:  "crawl",
		Manifest:    indexBuildResumeManifest(),
		Identity:    indexBuildIdentityState{StorageProvider: "aws_s3", CloudProvider: "aws", RegionKind: "aws", Region: "us-east-1"},
		FiltersHash: "filters-a",
		ScopeHash:   "scope-a",
	}
	originalFingerprint, err := checkpointFingerprint(original)
	require.NoError(t, err)
	require.NoError(t, recordIndexRunCheckpointIdentity(ctx, db, run.RunID, operationIndexBuild, originalFingerprint, time.Now().UTC()))

	_, err = validateCheckpointIdentityAgainstIndexRun(ctx, db, &opcheckpoint.Envelope{
		Operation:         operationIndexBuild,
		RunID:             run.RunID,
		ConfigFingerprint: originalFingerprint,
	}, operationIndexBuild, original)
	require.NoError(t, err)

	tampered := original
	tampered.Manifest.Build.Crawl.Concurrency = 16
	tamperedFingerprint, err := checkpointFingerprint(tampered)
	require.NoError(t, err)

	_, err = validateCheckpointIdentityAgainstIndexRun(ctx, db, &opcheckpoint.Envelope{
		Operation:         operationIndexBuild,
		RunID:             run.RunID,
		ConfigFingerprint: tamperedFingerprint,
	}, operationIndexBuild, tampered)
	require.True(t, errors.Is(err, opcheckpoint.ErrIdentityMismatch), "got error: %v", err)

	_, err = validateCheckpointIdentityAgainstIndexRun(ctx, db, &opcheckpoint.Envelope{
		Operation:         operationIndexBuild,
		RunID:             run.RunID,
		ConfigFingerprint: originalFingerprint,
	}, operationIndexBuild, tampered)
	require.True(t, errors.Is(err, opcheckpoint.ErrIdentityMismatch), "got error: %v", err)
}

func TestIndexBuildResumeIdentityRejectsTamperedCrawlPrefixes(t *testing.T) {
	ctx, db, indexSet := setupIndexBuildResumeDB(t)
	defer func() { _ = db.Close() }()

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusFailedResumable, nil))

	original := indexBuildCheckpointConfig{
		IndexSetID:  indexSet.IndexSetID,
		SourceType:  "crawl",
		Manifest:    indexBuildResumeManifest(),
		Identity:    indexBuildIdentityState{StorageProvider: "aws_s3", CloudProvider: "aws", RegionKind: "aws", Region: "us-east-1"},
		FiltersHash: "filters-a",
		ScopeHash:   "scope-a",
	}
	prefixes := []string{"prefix/a/", "prefix/b/"}
	require.NoError(t, bindIndexBuildCrawlPrefixes(&original, prefixes))
	originalFingerprint, err := checkpointFingerprint(original)
	require.NoError(t, err)
	require.NoError(t, recordIndexRunCheckpointIdentity(ctx, db, run.RunID, operationIndexBuild, originalFingerprint, time.Now().UTC()))

	env := &opcheckpoint.Envelope{
		Operation:         operationIndexBuild,
		RunID:             run.RunID,
		ConfigFingerprint: originalFingerprint,
	}
	_, err = validateIndexBuildCheckpointPayloadIdentity(ctx, db, env, indexBuildCheckpointPayload{
		Config:        original,
		CrawlPrefixes: prefixes,
	})
	require.NoError(t, err)

	_, err = validateIndexBuildCheckpointPayloadIdentity(ctx, db, env, indexBuildCheckpointPayload{
		Config:        original,
		CrawlPrefixes: []string{"prefix/a/"},
	})
	require.True(t, errors.Is(err, opcheckpoint.ErrIdentityMismatch), "got error: %v", err)

	_, err = validateIndexBuildCheckpointPayloadIdentity(ctx, db, env, indexBuildCheckpointPayload{
		Config:        original,
		CrawlPrefixes: []string{"prefix/a/ ", "prefix/b/"},
	})
	require.True(t, errors.Is(err, opcheckpoint.ErrIdentityMismatch), "got error: %v", err)
}

func TestIndexBuildCustomDBCheckpointIneligible(t *testing.T) {
	cfg := indexBuildCheckpointConfig{UsesDefaultIndexDB: false}
	require.False(t, indexBuildCheckpointEligible(cfg))

	cfg.UsesDefaultIndexDB = true
	require.True(t, indexBuildCheckpointEligible(cfg))
}

func TestIndexBuildResumableFailureHelpersAreNilResultSafe(t *testing.T) {
	require.Nil(t, indexBuildProgress(nil))
	require.Nil(t, indexBuildCrawlPrefixes(nil))

	summary := indexBuildSummaryFromResult(nil)
	require.Zero(t, summary.ObjectsIngested)
	require.Zero(t, summary.PrefixesIngested)
	require.Zero(t, summary.ObjectsDeleted)
	require.Empty(t, summary.FinalStatus)
}

func TestIndexBuildResumePromotionCanRunAfterHeartbeatStop(t *testing.T) {
	ctx, db, indexSet := setupIndexBuildResumeDB(t)
	defer func() { _ = db.Close() }()

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	store := newRuntimeTestOperationStore(t)
	lease, err := store.ClaimLease(context.Background(), operationIndexBuild, run.RunID, "holder-a", time.Hour)
	require.NoError(t, err)
	heartbeat, leaseCtx, err := startResumeLeaseHeartbeat(context.Background(), store, operationIndexBuild, lease)
	require.NoError(t, err)

	require.NoError(t, stopResumeLeaseHeartbeat(heartbeat))
	require.ErrorIs(t, leaseCtx.Err(), context.Canceled)

	result := &indexBuildResult{FinalStatus: indexstore.RunStatusSuccess}
	require.NoError(t, finalizeIndexRun(context.Background(), db, indexSet.IndexSetID, run, result, false, nil))
	updated, err := indexstore.GetIndexRun(context.Background(), db, run.RunID)
	require.NoError(t, err)
	require.Equal(t, indexstore.RunStatus(indexstore.RunStatusSuccess), updated.Status)
}

func TestIndexBuildFinalizeRollsBackResumeCompletedWhenSoftDeleteFails(t *testing.T) {
	ctx, db, indexSet := setupIndexBuildResumeDB(t)
	defer func() { _ = db.Close() }()

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusFailedResumable, nil))
	_, err = db.ExecContext(ctx, `DROP TABLE objects_current`)
	require.NoError(t, err)

	result := &indexBuildResult{FinalStatus: indexstore.RunStatusSuccess}
	err = finalizeIndexRun(context.Background(), db, indexSet.IndexSetID, run, result, true, []indexstore.RunEvent{
		indexRunLifecycleEvent(run.RunID, "resume_completed", "", time.Now().UTC()),
	})
	require.ErrorContains(t, err, "mark deleted")

	updated, err := indexstore.GetIndexRun(context.Background(), db, run.RunID)
	require.NoError(t, err)
	require.Equal(t, indexstore.RunStatus(indexstore.RunStatusFailedResumable), updated.Status)

	events, err := indexstore.ListRunEvents(context.Background(), db, run.RunID, nil)
	require.NoError(t, err)
	for _, event := range events {
		require.NotEqual(t, "resume_completed", event.EventType)
	}
}

func setupIndexBuildResumeDB(t *testing.T) (context.Context, *sql.DB, *indexstore.IndexSet) {
	t.Helper()
	ctx := context.Background()
	db, err := indexstore.Open(ctx, indexstore.Config{Path: ":memory:"})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, indexstore.IndexSetParams{
		BaseURI:         "s3://bucket/prefix/",
		Provider:        "s3",
		StorageProvider: "aws_s3",
		CloudProvider:   "aws",
		RegionKind:      "aws",
		Region:          "us-east-1",
		BuildParams: indexstore.BuildParams{
			SourceType:      "crawl",
			SchemaVersion:   indexstore.SchemaVersion,
			GonimbusVersion: versionInfo.Version,
			Includes:        []string{"**"},
			FiltersHash:     "filters-a",
			ScopeHash:       "scope-a",
		},
	})
	require.NoError(t, err)
	return ctx, db, indexSet
}

func indexBuildResumeManifest() manifest.IndexManifest {
	return manifest.IndexManifest{
		Version: "1.0",
		Connection: manifest.IndexConnectionConfig{
			Provider: "s3",
			Bucket:   "bucket",
			BaseURI:  "s3://bucket/prefix/",
			Region:   "us-east-1",
		},
		Identity: &manifest.IndexIdentityConfig{
			StorageProvider: "aws_s3",
			CloudProvider:   "aws",
			RegionKind:      "aws",
			Region:          "us-east-1",
		},
		Build: &manifest.IndexBuildConfig{
			Source: "crawl",
			Match: &manifest.IndexMatchConfig{
				Includes: []string{"**"},
			},
			Crawl: &manifest.IndexCrawlBuildConfig{
				Concurrency:   4,
				ProgressEvery: 1000,
			},
		},
	}
}
