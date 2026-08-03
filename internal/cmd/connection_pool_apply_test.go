package cmd

import (
	"context"
	"errors"
	"math"
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

func TestTransferSourceAndDestAdmittedN(t *testing.T) {
	t.Parallel()
	m := &manifest.TransferManifest{
		Transfer: manifest.TransferConfig{
			Concurrency: 16,
			Sharding: manifest.ShardingConfig{
				Enabled:         false,
				ListConcurrency: 16,
			},
		},
	}
	src, err := transferSourceAdmittedN(m)
	require.NoError(t, err)
	require.Equal(t, 32, src) // C+L
	dst, err := transferDestAdmittedN(m)
	require.NoError(t, err)
	require.Equal(t, 16, dst)

	m.Transfer.Sharding.Enabled = true
	src, err = transferSourceAdmittedN(m)
	require.NoError(t, err)
	require.Equal(t, 48, src) // C+2L
}

func TestCreateTransferProviderAppliesPoolViaS3Factory(t *testing.T) {
	var got s3.Config
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			got = cfg
			return nopPoolProvider{}, nil
		},
	})
	t.Cleanup(restore)

	conn := manifest.ConnectionConfig{
		Provider: string(provider.ProviderS3),
		Bucket:   "b",
		Region:   "us-east-1",
	}
	// Source composite N=32
	prov, err := createTransferProvider(context.Background(), conn, 32)
	require.NoError(t, err)
	require.Equal(t, 32, got.MaxIdleConnsPerHost)
	require.Equal(t, 32, got.MaxConnsPerHost)
	_ = prov.Close()

	// Dest N=1 → SDK-default zeros
	got = s3.Config{}
	prov, err = createTransferProvider(context.Background(), conn, 1)
	require.NoError(t, err)
	require.Equal(t, 0, got.MaxIdleConnsPerHost)
	require.Equal(t, 0, got.MaxConnsPerHost)
	_ = prov.Close()
}

func TestContentEnumeratorByClientAndAdmittedN(t *testing.T) {
	t.Parallel()
	// Exact keys only → no enum for that client → N
	enum := contentEnumeratorByClient([]string{"s3://b/key.txt"})
	require.False(t, enum["s3:b"])
	n, err := contentAdmittedNForClient(16, "s3:b", enum)
	require.NoError(t, err)
	require.Equal(t, 16, n)

	// Prefix on A → A gets N+1
	enum = contentEnumeratorByClient([]string{"s3://bucket-a/prefix/"})
	require.True(t, enum["s3:bucket-a"])
	n, err = contentAdmittedNForClient(16, "s3:bucket-a", enum)
	require.NoError(t, err)
	require.Equal(t, 17, n)

	// Glob → N+1
	enum = contentEnumeratorByClient([]string{"s3://b/pre*/*.json"})
	require.True(t, enum["s3:b"])
	n, err = contentAdmittedNForClient(8, "s3:b", enum)
	require.NoError(t, err)
	require.Equal(t, 9, n)

	// JSONL does not force enumerator
	enum = contentEnumeratorByClient([]string{`{"type":"gonimbus.index.object.v1"}`})
	require.Empty(t, enum)
	n, err = contentAdmittedNForClient(4, "s3:any", enum)
	require.NoError(t, err)
	require.Equal(t, 4, n)

	// Mixed clients: prefix on A + exact key on B → A=N+1, B=N
	enum = contentEnumeratorByClient([]string{
		"s3://bucket-a/prefix/",
		"s3://bucket-b/exact-key.txt",
	})
	require.True(t, enum["s3:bucket-a"])
	require.False(t, enum["s3:bucket-b"])
	nA, err := contentAdmittedNForClient(16, "s3:bucket-a", enum)
	require.NoError(t, err)
	require.Equal(t, 17, nA)
	nB, err := contentAdmittedNForClient(16, "s3:bucket-b", enum)
	require.NoError(t, err)
	require.Equal(t, 16, nB)
}

func TestNewContentHeadProviderAppliesPool(t *testing.T) {
	var got s3.Config
	var constructed int
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			constructed++
			got = cfg
			return nopPoolProvider{}, nil
		},
	})
	t.Cleanup(restore)

	src := &uri.ObjectURI{Provider: string(provider.ProviderS3), Bucket: "b", Key: "k"}
	prov, err := newContentHeadProvider(context.Background(), src, 17)
	require.NoError(t, err)
	require.Equal(t, 1, constructed)
	require.Equal(t, 17, got.MaxIdleConnsPerHost)
	require.Equal(t, 17, got.MaxConnsPerHost)
	_ = prov.Close()

	got = s3.Config{}
	prov, err = newContentHeadProvider(context.Background(), src, 1)
	require.NoError(t, err)
	require.Equal(t, 0, got.MaxIdleConnsPerHost)
	require.Equal(t, 0, got.MaxConnsPerHost)
	_ = prov.Close()
}

func TestNewContentProbeProviderAppliesPool(t *testing.T) {
	var got s3.Config
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			got = cfg
			return nopPoolProvider{}, nil
		},
	})
	t.Cleanup(restore)

	src := &uri.ObjectURI{Provider: string(provider.ProviderS3), Bucket: "b", Key: "k"}
	prov, err := newContentProbeProvider(context.Background(), src, 9)
	require.NoError(t, err)
	require.Equal(t, 9, got.MaxIdleConnsPerHost)
	require.Equal(t, 9, got.MaxConnsPerHost)
	_ = prov.Close()
}

func TestTreeAdmittedN(t *testing.T) {
	t.Parallel()
	n, err := treeAdmittedN(0, 8)
	require.NoError(t, err)
	require.Equal(t, 1, n) // depth 0 → SDK-default path

	n, err = treeAdmittedN(2, 8)
	require.NoError(t, err)
	require.Equal(t, 8, n)

	_, err = treeAdmittedN(1, 0)
	require.Error(t, err)
}

func TestCreateTreeProviderAppliesPoolViaS3Factory(t *testing.T) {
	// Mutates package-level tree flag vars — not parallel-safe.
	var got s3.Config
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			got = cfg
			return nopPoolProvider{}, nil
		},
	})
	t.Cleanup(restore)

	oldRegion, oldEndpoint, oldProfile := treeRegion, treeEndpoint, treeProfile
	treeRegion, treeEndpoint, treeProfile = "us-east-1", "", ""
	t.Cleanup(func() {
		treeRegion, treeEndpoint, treeProfile = oldRegion, oldEndpoint, oldProfile
	})

	src := &uri.ObjectURI{Provider: string(provider.ProviderS3), Bucket: "b", Key: "p/"}
	prov, err := createTreeProvider(context.Background(), src, 8)
	require.NoError(t, err)
	require.Equal(t, 8, got.MaxIdleConnsPerHost)
	require.Equal(t, 8, got.MaxConnsPerHost)
	_ = prov.Close()

	got = s3.Config{}
	prov, err = createTreeProvider(context.Background(), src, 1)
	require.NoError(t, err)
	require.Equal(t, 0, got.MaxIdleConnsPerHost)
	require.Equal(t, 0, got.MaxConnsPerHost)
	_ = prov.Close()
}

func TestOpenTransferProvidersProductionBoundary(t *testing.T) {
	// Captures pool values through the production join used by executeTransfer.
	type capture struct {
		bucket string
		idle   int
		conns  int
	}
	var got []capture
	var constructed int
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			constructed++
			got = append(got, capture{bucket: cfg.Bucket, idle: cfg.MaxIdleConnsPerHost, conns: cfg.MaxConnsPerHost})
			return nopPoolProvider{}, nil
		},
	})
	t.Cleanup(restore)

	m := &manifest.TransferManifest{
		Source: manifest.ConnectionConfig{Provider: string(provider.ProviderS3), Bucket: "src-bucket", Region: "us-east-1"},
		Target: manifest.ConnectionConfig{Provider: string(provider.ProviderS3), Bucket: "dst-bucket", Region: "us-east-1"},
		Transfer: manifest.TransferConfig{
			Concurrency: 16,
			Sharding:    manifest.ShardingConfig{Enabled: false, ListConcurrency: 16},
		},
	}
	src, dst, err := openTransferProviders(context.Background(), m)
	require.NoError(t, err)
	require.NotNil(t, src)
	require.NotNil(t, dst)
	_ = src.Close()
	_ = dst.Close()
	require.Equal(t, 2, constructed)
	require.Equal(t, capture{"src-bucket", 32, 32}, got[0]) // C+L
	require.Equal(t, capture{"dst-bucket", 16, 16}, got[1]) // C

	// Sharding on → source C+2L=48
	got = nil
	constructed = 0
	m.Transfer.Sharding.Enabled = true
	src, dst, err = openTransferProviders(context.Background(), m)
	require.NoError(t, err)
	_ = src.Close()
	_ = dst.Close()
	require.Equal(t, capture{"src-bucket", 48, 48}, got[0])
	require.Equal(t, capture{"dst-bucket", 16, 16}, got[1])
}

func TestOpenTransferProvidersOverflowNoConstruction(t *testing.T) {
	var constructed int
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(context.Context, s3.Config) (provider.Provider, error) {
			constructed++
			return nopPoolProvider{}, nil
		},
	})
	t.Cleanup(restore)

	m := &manifest.TransferManifest{
		Source: manifest.ConnectionConfig{Provider: string(provider.ProviderS3), Bucket: "src", Region: "us-east-1"},
		Target: manifest.ConnectionConfig{Provider: string(provider.ProviderS3), Bucket: "dst", Region: "us-east-1"},
		Transfer: manifest.TransferConfig{
			Concurrency: math.MaxInt,
			Sharding:    manifest.ShardingConfig{Enabled: false, ListConcurrency: 2},
		},
	}
	src, dst, err := openTransferProviders(context.Background(), m)
	require.Error(t, err)
	require.True(t, isConnPoolAdmission(err), "formula overflow must be typed admission error")
	require.Nil(t, src)
	require.Nil(t, dst)
	require.Equal(t, 0, constructed, "overflow must refuse before provider construction")
}

func TestOpenTransferProvidersConstructionErrorNotAdmission(t *testing.T) {
	// Provider failure whose text contains a former admission sentinel must NOT
	// be typed as errConnPoolAdmission (no error-text classification).
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(context.Context, s3.Config) (provider.Provider, error) {
			return nil, errors.New("connection overflow while dialing host")
		},
	})
	t.Cleanup(restore)

	m := &manifest.TransferManifest{
		Source: manifest.ConnectionConfig{Provider: string(provider.ProviderS3), Bucket: "src", Region: "us-east-1"},
		Target: manifest.ConnectionConfig{Provider: string(provider.ProviderS3), Bucket: "dst", Region: "us-east-1"},
		Transfer: manifest.TransferConfig{
			Concurrency: 16,
			Sharding:    manifest.ShardingConfig{Enabled: false, ListConcurrency: 16},
		},
	}
	src, dst, err := openTransferProviders(context.Background(), m)
	require.Error(t, err)
	require.False(t, isConnPoolAdmission(err), "construction errors stay non-admission even if text matches old sentinels")
	require.Contains(t, err.Error(), "overflow")
	require.Nil(t, src)
	require.Nil(t, dst)
}

func TestOpenContentProvidersPerClientAndMixed(t *testing.T) {
	type cap struct {
		bucket string
		idle   int
	}
	var got []cap
	var constructed int
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			constructed++
			got = append(got, cap{bucket: cfg.Bucket, idle: cfg.MaxIdleConnsPerHost})
			return nopPoolProvider{}, nil
		},
	})
	t.Cleanup(restore)

	inputs := []string{
		"s3://bucket-a/prefix/",
		"s3://bucket-b/exact-key.txt",
	}
	admitted, err := resolveContentAdmittedByClient(16, inputs)
	require.NoError(t, err)
	require.Equal(t, 17, admitted["s3:bucket-a"])
	require.Equal(t, 16, admitted["s3:bucket-b"])

	a := &uri.ObjectURI{Provider: string(provider.ProviderS3), Bucket: "bucket-a", Key: "prefix/"}
	b := &uri.ObjectURI{Provider: string(provider.ProviderS3), Bucket: "bucket-b", Key: "exact-key.txt"}
	pA, err := openContentHeadProvider(context.Background(), a, admitted)
	require.NoError(t, err)
	_ = pA.Close()
	pB, err := openContentHeadProvider(context.Background(), b, admitted)
	require.NoError(t, err)
	_ = pB.Close()
	require.Equal(t, 2, constructed)
	require.Equal(t, cap{"bucket-a", 17}, got[0]) // N+1
	require.Equal(t, cap{"bucket-b", 16}, got[1]) // N

	// Probe path same pre-resolved map
	got = nil
	constructed = 0
	admitted8, err := resolveContentAdmittedByClient(8, inputs)
	require.NoError(t, err)
	pA, err = openContentProbeProvider(context.Background(), a, admitted8)
	require.NoError(t, err)
	_ = pA.Close()
	pB, err = openContentProbeProvider(context.Background(), b, admitted8)
	require.NoError(t, err)
	_ = pB.Close()
	require.Equal(t, cap{"bucket-a", 9}, got[0])
	require.Equal(t, cap{"bucket-b", 8}, got[1])
}

func TestResolveContentAdmittedMixedOverflowNoConstruction(t *testing.T) {
	// Mixed clients: exact B + enumerating A with MaxInt workers → N+1 overflows
	// at pre-resolve, before any construction.
	var constructed int
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(context.Context, s3.Config) (provider.Provider, error) {
			constructed++
			return nopPoolProvider{}, nil
		},
	})
	t.Cleanup(restore)

	inputs := []string{
		"s3://bucket-a/prefix/",
		"s3://bucket-b/exact-key.txt",
	}
	admitted, err := resolveContentAdmittedByClient(math.MaxInt, inputs)
	require.Error(t, err)
	require.True(t, isConnPoolAdmission(err))
	require.Nil(t, admitted)
	require.Equal(t, 0, constructed)

	// Even if a caller attempted open with an empty map, no formula runs at open —
	// still no construction for missing pre-resolve.
	a := &uri.ObjectURI{Provider: string(provider.ProviderS3), Bucket: "bucket-a", Key: "prefix/"}
	_, err = openContentHeadProvider(context.Background(), a, map[string]int{})
	require.Error(t, err)
	require.True(t, isConnPoolAdmission(err))
	require.Equal(t, 0, constructed)
}

func TestOpenTreeProviderProductionBoundary(t *testing.T) {
	var got s3.Config
	var constructed int
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			constructed++
			got = cfg
			return nopPoolProvider{}, nil
		},
	})
	t.Cleanup(restore)

	oldRegion, oldEndpoint, oldProfile := treeRegion, treeEndpoint, treeProfile
	treeRegion, treeEndpoint, treeProfile = "us-east-1", "", ""
	t.Cleanup(func() {
		treeRegion, treeEndpoint, treeProfile = oldRegion, oldEndpoint, oldProfile
	})

	src := &uri.ObjectURI{Provider: string(provider.ProviderS3), Bucket: "b", Key: "p/"}
	// depth 0 → SDK-default zeros
	prov, err := openTreeProvider(context.Background(), src, 0, 8)
	require.NoError(t, err)
	require.Equal(t, 0, got.MaxIdleConnsPerHost)
	require.Equal(t, 0, got.MaxConnsPerHost)
	_ = prov.Close()

	// depth > 0 → treeParallel
	got = s3.Config{}
	prov, err = openTreeProvider(context.Background(), src, 2, 8)
	require.NoError(t, err)
	require.Equal(t, 8, got.MaxIdleConnsPerHost)
	require.Equal(t, 8, got.MaxConnsPerHost)
	_ = prov.Close()

	// invalid parallel → no construction beyond prior two
	before := constructed
	_, err = openTreeProvider(context.Background(), src, 1, 0)
	require.Error(t, err)
	require.Equal(t, before, constructed)
}

type nopPoolProvider struct{}

func (nopPoolProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}
func (nopPoolProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return &provider.ObjectMeta{}, nil
}
func (nopPoolProvider) Close() error { return nil }
