package cmd

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

func TestCommandSourceTargetForReadFileExactObject(t *testing.T) {
	raw := fileURI(filepath.Join(t.TempDir(), "nested", "object.txt"))
	parsed, err := uri.ParseURI(raw)
	require.NoError(t, err)

	target := commandSourceTargetForRead(parsed)

	require.Equal(t, string(provider.ProviderFile), target.ProviderURI.Provider)
	require.Equal(t, filepath.ToSlash(filepath.Dir(parsed.Key)), filepath.ToSlash(target.ProviderURI.Key))
	require.Equal(t, "object.txt", target.QueryURI.Key)
	require.Contains(t, target.ProviderID, "file:")
}

func TestCommandSourceTargetForReadFilePrefix(t *testing.T) {
	root := t.TempDir()
	parsed, err := uri.ParseURI(fileURI(root) + "/")
	require.NoError(t, err)

	target := commandSourceTargetForRead(parsed)

	require.Equal(t, filepath.ToSlash(filepath.Clean(root)), filepath.ToSlash(target.ProviderURI.Key))
	require.Empty(t, target.QueryURI.Key)
}

func TestCommandSourceTargetForReadS3PreservesBucketAndKey(t *testing.T) {
	parsed, err := uri.ParseURI("s3://bucket/prefix/object.txt")
	require.NoError(t, err)

	target := commandSourceTargetForRead(parsed)

	require.Equal(t, parsed, target.ProviderURI)
	require.Equal(t, parsed, target.QueryURI)
	require.Equal(t, "s3:bucket", target.ProviderID)
}

func TestCommandSourceTargetForReadGCSPreservesBucketAndKey(t *testing.T) {
	parsed, err := uri.ParseURI("gs://bucket/prefix/object.txt")
	require.NoError(t, err)

	target := commandSourceTargetForRead(parsed)

	require.Equal(t, parsed, target.ProviderURI)
	require.Equal(t, parsed, target.QueryURI)
	require.Equal(t, "gcs:bucket", target.ProviderID)
}

func TestCommandOutputProviderForInputsUsesFirstURIProvider(t *testing.T) {
	require.Equal(t, string(provider.ProviderGCS), commandOutputProviderForInputs([]string{"gs://bucket/object.txt"}, string(provider.ProviderS3)))
	require.Equal(t, string(provider.ProviderS3), commandOutputProviderForInputs([]string{`{"type":"gonimbus.index.object.v1"}`}, string(provider.ProviderS3)))
}
