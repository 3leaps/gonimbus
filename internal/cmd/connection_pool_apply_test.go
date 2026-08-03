package cmd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/crawler"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

func TestApplySourceConnectionPoolExactAssign(t *testing.T) {
	t.Parallel()
	opts := providerdispatch.SourceOptions{}
	require.NoError(t, applySourceConnectionPool(&opts, 16))
	require.Equal(t, 16, opts.S3.MaxIdleConnsPerHost)
	require.Equal(t, 16, opts.S3.MaxConnsPerHost)
	require.Equal(t, 16, opts.GCS.MaxIdleConnsPerHost)
	require.Equal(t, 16, opts.GCS.MaxConnsPerHost)

	opts = providerdispatch.SourceOptions{}
	require.NoError(t, applySourceConnectionPool(&opts, 1))
	require.Equal(t, 0, opts.S3.MaxIdleConnsPerHost)
	require.Equal(t, 0, opts.S3.MaxConnsPerHost)
}

func TestResolvedCrawlAdmittedN(t *testing.T) {
	t.Parallel()
	require.Equal(t, crawler.DefaultConfig().Concurrency, resolvedCrawlAdmittedN(0))
	require.Equal(t, crawler.DefaultConfig().Concurrency, resolvedCrawlAdmittedN(-1))
	require.Equal(t, 12, resolvedCrawlAdmittedN(12))
}

func TestIndexBuildEngineSourceAppliesCrawlPool(t *testing.T) {
	var got providerdispatch.SourceOptions
	old := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(_ context.Context, _ *uri.ObjectURI, opts providerdispatch.SourceOptions) (provider.Provider, error) {
		got = opts
		return nopPoolProvider{}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = old })

	m := &manifest.IndexManifest{
		Connection: manifest.IndexConnectionConfig{
			Provider: string(provider.ProviderS3),
			Bucket:   "b",
			BaseURI:  "s3://b/prefix/",
			Region:   "us-east-1",
		},
		Build: &manifest.IndexBuildConfig{
			Crawl: &manifest.IndexCrawlBuildConfig{Concurrency: 8},
		},
	}
	opts := providerdispatch.SourceOptions{Command: operationIndexBuild}
	require.NoError(t, applySourceConnectionPool(&opts, resolvedCrawlAdmittedN(indexBuildEngineCrawlConfig(m).Concurrency)))
	_, err := newIndexBuildEngineSource(context.Background(), &uri.ObjectURI{
		Provider: m.Connection.Provider,
		Bucket:   m.Connection.Bucket,
	}, opts)
	require.NoError(t, err)
	require.Equal(t, 8, got.S3.MaxIdleConnsPerHost)
	require.Equal(t, 8, got.S3.MaxConnsPerHost)
}

func TestCreateProviderAppliesCrawlPool(t *testing.T) {
	m := &manifest.Manifest{
		Connection: manifest.ConnectionConfig{
			Provider: string(provider.ProviderFile),
			BaseDir:  t.TempDir(),
		},
		Crawl: manifest.CrawlConfig{Concurrency: 6},
	}
	prov, err := createProvider(context.Background(), m)
	require.NoError(t, err)
	require.NotNil(t, prov)
	_ = prov.Close()
}

func TestReconstructEnrichHeadProviderAppliesParallelPool(t *testing.T) {
	var got providerdispatch.SourceOptions
	old := newEnrichHeadProvider
	newEnrichHeadProvider = func(_ context.Context, _ *uri.ObjectURI, opts providerdispatch.SourceOptions) (provider.Provider, error) {
		got = opts
		return nopPoolProvider{}, nil
	}
	t.Cleanup(func() { newEnrichHeadProvider = old })

	set := &indexstore.IndexSet{
		IndexSetID: "idx_test",
		Provider:   string(provider.ProviderS3),
		BaseURI:    "s3://bucket/prefix/",
		Region:     "us-east-1",
	}
	_, err := reconstructEnrichHeadProvider(context.Background(), set, enrichHeadProviderOptions{}, 32)
	require.NoError(t, err)
	require.Equal(t, 32, got.S3.MaxIdleConnsPerHost)
	require.Equal(t, 32, got.S3.MaxConnsPerHost)
}

type nopPoolProvider struct{}

func (nopPoolProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}
func (nopPoolProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return &provider.ObjectMeta{}, nil
}
func (nopPoolProvider) Close() error { return nil }
