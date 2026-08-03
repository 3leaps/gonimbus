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
	"github.com/3leaps/gonimbus/pkg/provider/s3"
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

func TestIndexBuildSourceOptionsProductionBoundary(t *testing.T) {
	t.Parallel()
	m := &manifest.IndexManifest{
		Connection: manifest.IndexConnectionConfig{
			Provider: string(provider.ProviderS3),
			Bucket:   "b",
			BaseURI:  "s3://b/prefix/",
			Region:   "us-east-1",
			Endpoint: "https://s3.example.test",
			Profile:  "p",
		},
		Build: &manifest.IndexBuildConfig{
			Crawl: &manifest.IndexCrawlBuildConfig{Concurrency: 8},
		},
	}
	opts, err := indexBuildSourceOptions(m)
	require.NoError(t, err)
	require.Equal(t, operationIndexBuild, opts.Command)
	require.Equal(t, 8, opts.S3.MaxIdleConnsPerHost)
	require.Equal(t, 8, opts.S3.MaxConnsPerHost)
	require.Equal(t, 8, opts.GCS.MaxIdleConnsPerHost)
	require.Equal(t, 8, opts.GCS.MaxConnsPerHost)
	require.Equal(t, "us-east-1", opts.S3.Region)
	require.True(t, opts.S3.ForcePathStyle)

	// N=1 after resolve → SDK default zeros (crawl concurrency 1).
	m.Build.Crawl.Concurrency = 1
	opts, err = indexBuildSourceOptions(m)
	require.NoError(t, err)
	require.Equal(t, 0, opts.S3.MaxIdleConnsPerHost)
	require.Equal(t, 0, opts.S3.MaxConnsPerHost)

	// Zero concurrency resolves to crawler default (4) → pool 4.
	m.Build.Crawl.Concurrency = 0
	opts, err = indexBuildSourceOptions(m)
	require.NoError(t, err)
	require.Equal(t, crawler.DefaultConfig().Concurrency, opts.S3.MaxIdleConnsPerHost)
}

func TestCreateProviderAppliesPoolViaS3Factory(t *testing.T) {
	var got s3.Config
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			got = cfg
			return nopPoolProvider{}, nil
		},
	})
	t.Cleanup(restore)

	m := &manifest.Manifest{
		Connection: manifest.ConnectionConfig{
			Provider: string(provider.ProviderS3),
			Bucket:   "b",
			Region:   "us-east-1",
		},
		Crawl: manifest.CrawlConfig{Concurrency: 6},
	}
	prov, err := createProvider(context.Background(), m)
	require.NoError(t, err)
	require.NotNil(t, prov)
	require.Equal(t, 6, got.MaxIdleConnsPerHost)
	require.Equal(t, 6, got.MaxConnsPerHost)
	_ = prov.Close()

	// N=1 → SDK default zeros
	got = s3.Config{}
	m.Crawl.Concurrency = 1
	prov, err = createProvider(context.Background(), m)
	require.NoError(t, err)
	require.Equal(t, 0, got.MaxIdleConnsPerHost)
	require.Equal(t, 0, got.MaxConnsPerHost)
	_ = prov.Close()
}

func TestReconstructEnrichHeadProviderAppliesParallelPool(t *testing.T) {
	var got providerdispatch.SourceOptions
	var constructed int
	old := newEnrichHeadProvider
	newEnrichHeadProvider = func(_ context.Context, _ *uri.ObjectURI, opts providerdispatch.SourceOptions) (provider.Provider, error) {
		constructed++
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
	require.Equal(t, 1, constructed)
	require.Equal(t, 32, got.S3.MaxIdleConnsPerHost)
	require.Equal(t, 32, got.S3.MaxConnsPerHost)

	// N=1 → SDK default zeros still constructs.
	got = providerdispatch.SourceOptions{}
	_, err = reconstructEnrichHeadProvider(context.Background(), set, enrichHeadProviderOptions{}, 1)
	require.NoError(t, err)
	require.Equal(t, 2, constructed)
	require.Equal(t, 0, got.S3.MaxIdleConnsPerHost)
	require.Equal(t, 0, got.S3.MaxConnsPerHost)
}

func TestReconstructEnrichHeadProviderRefusesInvalidParallel(t *testing.T) {
	var constructed int
	old := newEnrichHeadProvider
	newEnrichHeadProvider = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		constructed++
		return nopPoolProvider{}, nil
	}
	t.Cleanup(func() { newEnrichHeadProvider = old })

	set := &indexstore.IndexSet{
		IndexSetID: "idx_test",
		Provider:   string(provider.ProviderS3),
		BaseURI:    "s3://bucket/prefix/",
		Region:     "us-east-1",
	}
	for _, n := range []int{0, -1, -32} {
		_, err := reconstructEnrichHeadProvider(context.Background(), set, enrichHeadProviderOptions{}, n)
		require.Error(t, err)
		require.Contains(t, err.Error(), "parallel")
	}
	require.Equal(t, 0, constructed, "invalid parallel must not construct a provider")
}

type nopPoolProvider struct{}

func (nopPoolProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}
func (nopPoolProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return &provider.ObjectMeta{}, nil
}
func (nopPoolProvider) Close() error { return nil }
