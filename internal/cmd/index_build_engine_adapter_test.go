package cmd

import (
	"context"
	"encoding/json"
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

func TestIndexBuildFormatBothUsesSingleObservedProviderStream(t *testing.T) {
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
	indexBuildFormat = "both"

	prov := &countingIndexBuildProvider{objects: indexBuildEngineTestObjects(base)}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	var stdout strings.Builder
	cmd.SetOut(&stdout)
	require.NoError(t, runIndexBuild(cmd, nil))
	require.Equal(t, 1, prov.listCalls)

	var report map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(stdout.String())), &report))
	require.Equal(t, "gonimbus.index.compare_result.v1", report["type"])
	semantics, ok := report["projection_semantics"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "LIST-projection fidelity (sqlite vs durable row projection over one crawl)", semantics["certifies"])
	require.Equal(t, "reflow-input readiness (HEAD-enrichment parity)", semantics["does_not_certify"])
	require.Equal(t, []any{"rel_key", "size_bytes", "last_modified", "storage_class"}, semantics["included_fields"])
	require.Contains(t, semantics["content_identity"], "not a portable content hash")
	require.Equal(t, true, report["sqlite_materialized"])
	require.Equal(t, true, report["durable_published"])
	require.Equal(t, true, report["comparison_ran"])
	require.Equal(t, true, report["parity_passed"])

	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
}

func TestIndexBuildBothFormatsFailureReportCarriesProjectionSemantics(t *testing.T) {
	report := indexBuildBothFormatsFailureReport(nil, nil, resolvedIndexDB{Path: "/tmp/index.db"}, indexbuild.PathConfig{ManifestPath: "/tmp/manifest.json"}, true, false)
	require.Equal(t, "gonimbus.index.compare_result.v1", report.Type)
	require.Equal(t, "LIST-projection fidelity (sqlite vs durable row projection over one crawl)", report.ProjectionSemantics.Certifies)
	require.Equal(t, "reflow-input readiness (HEAD-enrichment parity)", report.ProjectionSemantics.DoesNotCertify)
	require.Equal(t, []string{"rel_key", "size_bytes", "last_modified", "storage_class"}, report.ProjectionSemantics.IncludedFields)
	require.Contains(t, report.ProjectionSemantics.ContentIdentity, "not a portable content hash")
	require.Len(t, report.ProjectionSemantics.ExcludedFields, 4)
	require.False(t, report.ComparisonRan)
	require.False(t, report.ParityPassed)
}

func TestIndexBuildFormatBothRejectsBuildScopeBeforeDurablePublish(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
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
  scope:
    type: prefix_list
    prefixes: ["hot/"]
  match:
    includes: ["**"]
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"

	prov := &countingIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	err := runIndexBuild(cmd, nil)
	require.ErrorContains(t, err, "--format both does not support build.scope in this slice")
	require.Zero(t, prov.listCalls)

	latestFiles, globErr := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, globErr)
	require.Empty(t, latestFiles)
}

func TestIndexBuildFormatBothRejectsNarrowMatchIncludesBeforeDurablePublish(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
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
    includes: ["hot/**"]
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"

	prov := &countingIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	err := runIndexBuild(cmd, nil)
	require.ErrorContains(t, err, `--format both supports only default build.match.includes "**" in this slice`)
	require.Zero(t, prov.listCalls)

	latestFiles, globErr := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, globErr)
	require.Empty(t, latestFiles)
}

func TestIndexBuildFormatBothRejectsWhitespacePaddedDefaultIncludeBeforeDurablePublish(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
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
    includes: [" ** "]
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"

	prov := &countingIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	err := runIndexBuild(cmd, nil)
	require.ErrorContains(t, err, `--format both supports only default build.match.includes "**" in this slice`)
	require.Zero(t, prov.listCalls)

	latestFiles, globErr := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, globErr)
	require.Empty(t, latestFiles)
}

func TestIndexBuildFormatBothRejectsMatchExcludesBeforeDurablePublish(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
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
    excludes: ["tmp/**"]
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"

	prov := &countingIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	err := runIndexBuild(cmd, nil)
	require.ErrorContains(t, err, "--format both does not support build.match.excludes in this slice")
	require.Zero(t, prov.listCalls)

	latestFiles, globErr := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, globErr)
	require.Empty(t, latestFiles)
}

func TestIndexBuildFormatBothRejectsMatchFiltersBeforeDurablePublish(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
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
    filters:
      size:
        min: "1B"
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "both"

	prov := &countingIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	err := runIndexBuild(cmd, nil)
	require.ErrorContains(t, err, "--format both does not support build.match.filters in this slice")
	require.Zero(t, prov.listCalls)

	latestFiles, globErr := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, globErr)
	require.Empty(t, latestFiles)
}

func TestIndexBuildExperimentalEngineRejectsNarrowMatchBeforeDurablePublish(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
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
    excludes: ["tmp/**"]
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildExperimentalEngine = true

	prov := &countingIndexBuildProvider{}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return prov, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	err := runIndexBuild(cmd, nil)
	require.ErrorContains(t, err, "--experimental-engine does not support build.match.excludes in this slice")
	require.Zero(t, prov.listCalls)

	latestFiles, globErr := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, globErr)
	require.Empty(t, latestFiles)
}

func TestIndexBuildFormatBothRejectsExperimentalEngineFlag(t *testing.T) {
	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildFormat = "both"
	indexBuildExperimentalEngine = true

	err := validateIndexBuildFormatFlags("")
	require.ErrorContains(t, err, "--format both is not compatible with --experimental-engine")
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

type countingIndexBuildProvider struct {
	objects   []provider.ObjectSummary
	listCalls int
}

func (p *countingIndexBuildProvider) List(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	p.listCalls++
	var out []provider.ObjectSummary
	for _, obj := range p.objects {
		if strings.HasPrefix(obj.Key, opts.Prefix) {
			out = append(out, obj)
		}
	}
	return &provider.ListResult{Objects: out}, nil
}

func (*countingIndexBuildProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (*countingIndexBuildProvider) Close() error { return nil }

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
	oldFormat := indexBuildFormat
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
		indexBuildFormat = oldFormat
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
		indexBuildFormat = "sqlite"
		indexBuildExperimentalEngine = false
	}
}
