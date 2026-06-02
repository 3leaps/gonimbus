package cmd

import (
	"context"
	"database/sql"
	"errors"
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
	defer func() {
		indexBuildJobPath = oldJob
		indexBuildResumeRun = oldResumeRun
		indexBuildDryRun = oldDryRun
	}()

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
