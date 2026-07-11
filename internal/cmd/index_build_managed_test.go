package cmd

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

func TestCurrentIndexBuildInvocationNormalizesExperimentalAlias(t *testing.T) {
	reset := withIndexBuildExperimentalEngineTestState(t)
	reset()
	indexBuildExperimentalEngine = true
	indexBuildFormat = "durable"
	inv := currentIndexBuildInvocation()
	require.Equal(t, "experimental-engine", inv.RequestedFormat)
	require.Equal(t, "durable", inv.EffectiveFormat)
}

func TestManagedIndexBuildErrorSanitizesCredentialMaterial(t *testing.T) {
	err := sanitizeManagedIndexBuildError(errors.New("GET https://objects.example.test/key?X-Amz-Signature=sentinel-secret failed"))
	require.NotContains(t, err.Error(), "sentinel-secret")
	require.NotContains(t, err.Error(), "X-Amz-Signature")
}

func TestClaimManagedIndexBuildJobRejectsChangedManifest(t *testing.T) {
	reset := withIndexBuildExperimentalEngineTestState(t)
	reset()
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte("version: 1\n"), 0o600))
	inv, fingerprint, err := jobregistry.PrepareIndexBuildInvocation(manifestPath, "", &jobregistry.IndexBuildInvocation{
		SchemaVersion:     jobregistry.IndexBuildInvocationVersion,
		RequestedFormat:   "durable",
		EffectiveFormat:   "durable",
		ScopeWarnPrefixes: jobregistry.DefaultScopeWarnPrefixes,
		ScopeMaxPrefixes:  jobregistry.DefaultScopeMaxPrefixes,
	})
	require.NoError(t, err)
	store := jobregistry.NewStore(t.TempDir())
	rec := &jobregistry.JobRecord{
		JobID:                 "11111111-1111-4111-8111-111111111111",
		Type:                  jobregistry.JobTypeIndexBuild,
		State:                 jobregistry.JobStateQueued,
		ManifestPath:          inv.ManifestPath,
		CreatedAt:             time.Now().UTC(),
		Invocation:            inv,
		InvocationFingerprint: fingerprint,
	}
	require.NoError(t, store.Write(rec))
	require.NoError(t, os.WriteFile(manifestPath, []byte("version: 2\n"), 0o600))
	indexBuildJobPath = manifestPath
	_, _, err = claimManagedIndexBuildJob(store, rec.JobID)
	require.ErrorContains(t, err, "manifest changed after enqueue")
	failed, err := store.Get(rec.JobID)
	require.NoError(t, err)
	require.Equal(t, jobregistry.JobStateFailed, failed.State)
	require.NotNil(t, failed.Invocation)
}

func TestClaimManagedIndexBuildJobOwnsQueuedTransition(t *testing.T) {
	reset := withIndexBuildExperimentalEngineTestState(t)
	reset()
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(managedIndexManifestYAML("bucket-a")), 0o600))
	inv, fingerprint, err := jobregistry.PrepareIndexBuildInvocation(manifestPath, "managed", &jobregistry.IndexBuildInvocation{
		SchemaVersion:     jobregistry.IndexBuildInvocationVersion,
		RequestedFormat:   "both",
		EffectiveFormat:   "both",
		Name:              "managed",
		DBPath:            "custom.db",
		StorageProvider:   "generic_s3",
		CloudProvider:     "other",
		RegionKind:        "aws",
		Region:            "us-east-1",
		EndpointHost:      "objects.example.test",
		ScopeWarnPrefixes: 12,
		ScopeMaxPrefixes:  34,
	})
	require.NoError(t, err)
	store := jobregistry.NewStore(t.TempDir())
	rec := &jobregistry.JobRecord{
		JobID:                 "22222222-2222-4222-8222-222222222222",
		Type:                  jobregistry.JobTypeIndexBuild,
		State:                 jobregistry.JobStateQueued,
		ManifestPath:          inv.ManifestPath,
		CreatedAt:             time.Now().UTC(),
		Invocation:            inv,
		InvocationFingerprint: fingerprint,
	}
	require.NoError(t, store.Write(rec))
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"
	indexBuildName = "managed"
	indexBuildDBPath = "custom.db"
	indexBuildStorageProv = "generic_s3"
	indexBuildCloudProv = "other"
	indexBuildRegionKind = "aws"
	indexBuildRegion = "us-east-1"
	indexBuildEndpointHost = "objects.example.test"
	indexBuildScopeWarnPrefix = 12
	indexBuildScopeMaxPrefix = 34

	claimed, parsed, err := claimManagedIndexBuildJob(store, rec.JobID)
	require.NoError(t, err)
	require.Equal(t, jobregistry.JobStateRunning, claimed.State)
	require.Equal(t, os.Getpid(), claimed.PID)
	require.NotNil(t, claimed.StartedAt)
	require.Equal(t, "bucket-a", parsed.Connection.Bucket)
}

func TestClaimManagedIndexBuildJobParsesTheHashedBytes(t *testing.T) {
	reset := withIndexBuildExperimentalEngineTestState(t)
	reset()
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(managedIndexManifestYAML("bucket-a")), 0o600))
	inv, fingerprint, err := jobregistry.PrepareIndexBuildInvocation(manifestPath, "", &jobregistry.IndexBuildInvocation{
		SchemaVersion:     jobregistry.IndexBuildInvocationVersion,
		RequestedFormat:   "durable",
		EffectiveFormat:   "durable",
		ScopeWarnPrefixes: jobregistry.DefaultScopeWarnPrefixes,
		ScopeMaxPrefixes:  jobregistry.DefaultScopeMaxPrefixes,
	})
	require.NoError(t, err)
	store := jobregistry.NewStore(t.TempDir())
	rec := &jobregistry.JobRecord{
		JobID:                 "33333333-3333-4333-8333-333333333333",
		Type:                  jobregistry.JobTypeIndexBuild,
		State:                 jobregistry.JobStateQueued,
		ManifestPath:          inv.ManifestPath,
		CreatedAt:             time.Now().UTC(),
		Invocation:            inv,
		InvocationFingerprint: fingerprint,
	}
	require.NoError(t, store.Write(rec))
	indexBuildJobPath = manifestPath
	oldHook := afterManagedManifestRead
	afterManagedManifestRead = func() {
		require.NoError(t, os.WriteFile(manifestPath, []byte(managedIndexManifestYAML("bucket-b")), 0o600))
	}
	t.Cleanup(func() { afterManagedManifestRead = oldHook })

	_, parsed, err := claimManagedIndexBuildJob(store, rec.JobID)
	require.NoError(t, err)
	require.Equal(t, "bucket-a", parsed.Connection.Bucket)
	onDisk, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	require.Contains(t, string(onDisk), "bucket-b")
}

func TestJobBuildReceiptIdentityIsMetadataOnly(t *testing.T) {
	receipt := jobBuildReceiptIdentity(indexBuildResultRecord{
		Type:             indexBuildResultType,
		SchemaVersion:    indexBuildResultVersion,
		Status:           "success",
		RequestedFormat:  "both",
		FormatsCommitted: []string{"sqlite-v1", "durable-v2"},
		IndexSetID:       testFullIndexSetID,
		RunID:            "run_1700000000000000000",
		ScopeHash:        "scope-digest",
		ManifestSHA256:   "artifact-digest",
	})
	require.Equal(t, testFullIndexSetID, receipt.IndexSetID)
	require.Equal(t, "run_1700000000000000000", receipt.RunID)
	require.Equal(t, []string{"sqlite-v1", "durable-v2"}, receipt.FormatsCommitted)
}

func TestPersistCommittedIndexBuildJobSurfacesFailure(t *testing.T) {
	blockingPath := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blockingPath, []byte("x"), 0o600))
	store := jobregistry.NewStore(filepath.Join(blockingPath, "jobs"))
	err := persistCommittedIndexBuildJob(store, &jobregistry.JobRecord{
		JobID:     "44444444-4444-4444-8444-444444444444",
		State:     jobregistry.JobStateSuccess,
		CreatedAt: time.Now().UTC(),
		Receipt: &jobregistry.BuildReceiptIdentity{
			Type:       indexBuildResultType,
			IndexSetID: testFullIndexSetID,
			RunID:      "run_1700000000000000000",
		},
	})
	require.ErrorContains(t, err, "index build committed but persist terminal job identity")
}

func managedIndexManifestYAML(bucket string) string {
	return `version: "1.0"
connection:
  provider: s3
  bucket: ` + bucket + `
  base_uri: s3://` + bucket + `/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
`
}
