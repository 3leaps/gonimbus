package providerdispatch

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/uri"
)

type dispatchTestProvider struct{}

func (dispatchTestProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return nil, nil
}

func (dispatchTestProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (dispatchTestProvider) Close() error {
	return nil
}

type dispatchTestPutter struct {
	dispatchTestProvider
}

func (dispatchTestPutter) PutObject(context.Context, string, io.Reader, int64) error {
	return nil
}

func TestNewSourceBuildsS3ProviderFromParsedURI(t *testing.T) {
	var got s3.Config
	restore := UseFactoriesForTest(Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			got = cfg
			return dispatchTestProvider{}, nil
		},
	})
	t.Cleanup(restore)

	src := &uri.ObjectURI{Provider: string(provider.ProviderS3), Bucket: "source-bucket", Key: "prefix/object.txt"}
	p, err := NewSource(context.Background(), src, SourceOptions{
		Command: "transfer-reflow",
		S3: S3Options{
			Region:              "us-east-1",
			Profile:             "source-profile",
			Endpoint:            "https://s3.example.test",
			ForcePathStyle:      true,
			MaxIdleConnsPerHost: 32,
			MaxConnsPerHost:     32,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, p)
	require.Equal(t, "source-bucket", got.Bucket)
	require.Equal(t, "us-east-1", got.Region)
	require.Equal(t, "source-profile", got.Profile)
	require.Equal(t, "https://s3.example.test", got.Endpoint)
	require.True(t, got.ForcePathStyle)
	require.Equal(t, 32, got.MaxIdleConnsPerHost)
	require.Equal(t, 32, got.MaxConnsPerHost)
}

func TestNewSourceBuildsFileProviderFromParsedURI(t *testing.T) {
	var got providerfile.Config
	restore := UseFactoriesForTest(Factories{
		File: func(cfg providerfile.Config) (provider.Provider, error) {
			got = cfg
			return dispatchTestProvider{}, nil
		},
	})
	t.Cleanup(restore)

	src := &uri.ObjectURI{Provider: string(provider.ProviderFile), Bucket: "local", Key: "/tmp/source-root"}
	p, err := NewSource(context.Background(), src, SourceOptions{
		Command:             "transfer-reflow",
		FileMetadataSidecar: ".meta.json",
		FileSymlinkPolicy:   providerfile.SymlinkPolicyFollow,
	})

	require.NoError(t, err)
	require.NotNil(t, p)
	require.Equal(t, "/tmp/source-root", got.BaseDir)
	require.Equal(t, ".meta.json", got.MetadataSidecarSuffix)
	require.Equal(t, providerfile.SymlinkPolicyFollow, got.SymlinkPolicy)
}

func TestNewDestinationBuildsS3Provider(t *testing.T) {
	var got s3.Config
	restore := UseFactoriesForTest(Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			got = cfg
			return dispatchTestProvider{}, nil
		},
	})
	t.Cleanup(restore)

	p, err := NewDestination(context.Background(), DestinationOptions{
		Command:  "transfer-reflow",
		Provider: string(provider.ProviderS3),
		S3Bucket: "dest-bucket",
		S3: S3Options{
			Region:              "us-west-2",
			Profile:             "dest-profile",
			Endpoint:            "https://dest.example.test",
			ForcePathStyle:      true,
			MaxIdleConnsPerHost: 64,
			MaxConnsPerHost:     64,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, p)
	require.Equal(t, "dest-bucket", got.Bucket)
	require.Equal(t, "us-west-2", got.Region)
	require.Equal(t, "dest-profile", got.Profile)
	require.Equal(t, "https://dest.example.test", got.Endpoint)
	require.True(t, got.ForcePathStyle)
	require.Equal(t, 64, got.MaxIdleConnsPerHost)
	require.Equal(t, 64, got.MaxConnsPerHost)
}

func TestNewDestinationBuildsFileProviderAndCreatesBaseDir(t *testing.T) {
	var got providerfile.Config
	restore := UseFactoriesForTest(Factories{
		File: func(cfg providerfile.Config) (provider.Provider, error) {
			got = cfg
			return dispatchTestProvider{}, nil
		},
	})
	t.Cleanup(restore)

	baseDir := filepath.Join(t.TempDir(), "nested", "dest")
	p, err := NewDestination(context.Background(), DestinationOptions{
		Command:             "transfer-reflow",
		Provider:            string(provider.ProviderFile),
		FileBaseDir:         baseDir,
		FileMetadataSidecar: ".meta.json",
	})

	require.NoError(t, err)
	require.NotNil(t, p)
	require.Equal(t, baseDir, got.BaseDir)
	require.Equal(t, ".meta.json", got.MetadataSidecarSuffix)
	info, err := os.Stat(baseDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestNewSourceUnsupportedProviderFailsClosedWithContext(t *testing.T) {
	_, err := NewSource(context.Background(), &uri.ObjectURI{Provider: "gcs", Bucket: "bucket"}, SourceOptions{Command: "transfer-reflow"})

	require.Error(t, err)
	require.Contains(t, err.Error(), "transfer-reflow")
	require.Contains(t, err.Error(), "source")
	require.Contains(t, err.Error(), "gcs")
}

func TestRequireCapabilityFailsClosedWithCommandProviderAndCapability(t *testing.T) {
	_, err := RequireCapability[provider.ObjectPutter](dispatchTestProvider{}, "transfer-reflow", "file", "ObjectPutter")
	require.Error(t, err)
	require.Contains(t, err.Error(), "transfer-reflow")
	require.Contains(t, err.Error(), "file")
	require.Contains(t, err.Error(), "ObjectPutter")

	putter, err := RequireCapability[provider.ObjectPutter](dispatchTestPutter{}, "transfer-reflow", "file", "ObjectPutter")
	require.NoError(t, err)
	require.NotNil(t, putter)
}
