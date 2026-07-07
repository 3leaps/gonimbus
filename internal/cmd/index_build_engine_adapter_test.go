package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/indexbuild"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

func TestIndexBuildEngineAdapterMatchesLibraryManifestAndSegments(t *testing.T) {
	ctx := context.Background()
	libraryCfg := indexBuildEngineAdapterTestConfig(t, "library")
	adapterCfg := indexBuildEngineAdapterTestConfig(t, "adapter")

	librarySummary, err := indexbuild.NewRunner(libraryCfg).Build(ctx)
	require.NoError(t, err)
	adapterSummary, err := runIndexBuildEngine(ctx, adapterCfg)
	require.NoError(t, err)

	libraryManifest, err := os.ReadFile(libraryCfg.Paths.ManifestPath)
	require.NoError(t, err)
	adapterManifest, err := os.ReadFile(adapterCfg.Paths.ManifestPath)
	require.NoError(t, err)
	require.Equal(t, libraryManifest, adapterManifest)
	require.Equal(t, librarySummary.Manifest, adapterSummary.Manifest)
}

func TestIndexBuildExperimentalEngineCommandBuildsSnapshot(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  match:
    includes: ["**"]
  crawl:
    concurrency: 1
    progress_every: 100
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildExperimentalEngine = true

	var gotSrc *uri.ObjectURI
	var gotOpts providerdispatch.SourceOptions
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(_ context.Context, src *uri.ObjectURI, opts providerdispatch.SourceOptions) (provider.Provider, error) {
		gotSrc = src
		gotOpts = opts
		return indexBuildEngineFakeProvider{objects: indexBuildEngineTestObjects(base)}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	require.NoError(t, runIndexBuild(cmd, nil))

	require.Equal(t, "s3", gotSrc.Provider)
	require.Equal(t, "bucket", gotSrc.Bucket)
	require.Equal(t, operationIndexBuild, gotOpts.Command)

	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
	manifestSummary, rows, err := indexbuild.ReadLatest(latestFiles[0])
	require.NoError(t, err)
	require.Equal(t, 2, manifestSummary.Rows)
	require.Len(t, rows, 2)
	require.Equal(t, "a.xml", rows[0].RelKey)
	require.Equal(t, "b.xml", rows[1].RelKey)
	require.NoFileExists(t, filepath.Join(dataRoot, "indexes", "index.db"))
}

func indexBuildEngineAdapterTestConfig(t *testing.T, name string) indexbuild.Config {
	t.Helper()
	root := filepath.Join(t.TempDir(), name)
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	return indexbuild.Config{
		IndexSetID: "idx_test",
		RunID:      "run_test",
		BaseURI:    "s3://bucket/data/",
		Source: indexbuild.Source{
			Provider:     indexBuildEngineFakeProvider{objects: indexBuildEngineTestObjects(base)},
			ProviderName: "s3",
		},
		Match: indexbuild.MatchConfig{Includes: []string{"**"}},
		Paths: indexbuild.PathConfig{
			JournalDir:   filepath.Join(root, "journals"),
			SegmentDir:   filepath.Join(root, "segments"),
			ManifestPath: filepath.Join(root, "manifest.json"),
			CompletePath: filepath.Join(root, "complete.json"),
			LatestPath:   filepath.Join(root, "latest.json"),
			IndexDBDir:   filepath.Join(root, "indexes", "idx_test"),
		},
		Coverage: []indexbuild.CoverageAttestation{{
			Scope:    &indexbuild.Scope{Prefix: "data/"},
			Basis:    indexbuild.CoverageBasisConfirmed,
			Complete: true,
		}},
		RunStartedAt:         base,
		CreatedAt:            base.Add(time.Minute),
		Clock:                func() time.Time { return base.Add(2 * time.Minute) },
		TargetRowsPerSegment: 1,
	}
}

func indexBuildEngineTestObjects(base time.Time) []provider.ObjectSummary {
	return []provider.ObjectSummary{
		{Key: "data/a.xml", Size: 10, ETag: `"a"`, LastModified: base.Add(-time.Hour), StorageClass: "STANDARD"},
		{Key: "data/b.xml", Size: 11, ETag: `"b"`, LastModified: base.Add(-time.Minute), StorageClass: "STANDARD"},
	}
}

type indexBuildEngineFakeProvider struct {
	objects []provider.ObjectSummary
}

func (p indexBuildEngineFakeProvider) List(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	var out []provider.ObjectSummary
	for _, obj := range p.objects {
		if strings.HasPrefix(obj.Key, opts.Prefix) {
			out = append(out, obj)
		}
	}
	return &provider.ListResult{Objects: out}, nil
}

func (indexBuildEngineFakeProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (indexBuildEngineFakeProvider) Close() error { return nil }

func withIndexBuildExperimentalEngineTestState(t *testing.T) func() {
	t.Helper()
	oldJobPath := indexBuildJobPath
	oldDBPath := indexBuildDBPath
	oldDryRun := indexBuildDryRun
	oldBackground := indexBuildBackground
	oldDedupe := indexBuildDedupe
	oldManagedJobID := indexBuildManagedJobID
	oldStorageProv := indexBuildStorageProv
	oldCloudProv := indexBuildCloudProv
	oldRegionKind := indexBuildRegionKind
	oldRegion := indexBuildRegion
	oldEndpointHost := indexBuildEndpointHost
	oldScopeWarnPrefix := indexBuildScopeWarnPrefix
	oldScopeMaxPrefix := indexBuildScopeMaxPrefix
	oldName := indexBuildName
	oldSummary := indexBuildSummary
	oldResumeRun := indexBuildResumeRun
	oldSince := indexBuildSince
	oldExperimentalEngine := indexBuildExperimentalEngine
	t.Cleanup(func() {
		indexBuildJobPath = oldJobPath
		indexBuildDBPath = oldDBPath
		indexBuildDryRun = oldDryRun
		indexBuildBackground = oldBackground
		indexBuildDedupe = oldDedupe
		indexBuildManagedJobID = oldManagedJobID
		indexBuildStorageProv = oldStorageProv
		indexBuildCloudProv = oldCloudProv
		indexBuildRegionKind = oldRegionKind
		indexBuildRegion = oldRegion
		indexBuildEndpointHost = oldEndpointHost
		indexBuildScopeWarnPrefix = oldScopeWarnPrefix
		indexBuildScopeMaxPrefix = oldScopeMaxPrefix
		indexBuildName = oldName
		indexBuildSummary = oldSummary
		indexBuildResumeRun = oldResumeRun
		indexBuildSince = oldSince
		indexBuildExperimentalEngine = oldExperimentalEngine
	})
	return func() {
		indexBuildJobPath = ""
		indexBuildDBPath = ""
		indexBuildDryRun = false
		indexBuildBackground = false
		indexBuildDedupe = false
		indexBuildManagedJobID = ""
		indexBuildStorageProv = ""
		indexBuildCloudProv = ""
		indexBuildRegionKind = ""
		indexBuildRegion = ""
		indexBuildEndpointHost = ""
		indexBuildScopeWarnPrefix = 10000
		indexBuildScopeMaxPrefix = 50000
		indexBuildName = ""
		indexBuildSummary = false
		indexBuildResumeRun = ""
		indexBuildSince = ""
		indexBuildExperimentalEngine = false
	}
}
