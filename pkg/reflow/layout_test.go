package reflow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDestLayoutObjectStore(t *testing.T) {
	l, err := ParseDestLayout("s3://dest-bucket/data/")
	require.NoError(t, err)
	require.Equal(t, "s3", l.ProviderID)
	require.Equal(t, "dest-bucket", l.Bucket)
	require.Equal(t, "data/", l.Prefix)
	require.Equal(t, "s3://dest-bucket/data/", l.BaseURI)
	require.Equal(t, "data/source/file.xml", l.DestKey("source/file.xml"))
	require.Equal(t, "s3://dest-bucket/data/source/file.xml", l.DestURI("data/source/file.xml"))
}

func TestParseDestLayoutNormalizesPrefixAndGCS(t *testing.T) {
	// A non-prefix object URI is normalized to a prefix.
	l, err := ParseDestLayout("gs://bucket/base")
	require.NoError(t, err)
	require.Equal(t, "gcs", l.ProviderID)
	require.Equal(t, "base/", l.Prefix)
	require.Equal(t, "gs://bucket/base/", l.BaseURI)
	require.Equal(t, "gs://bucket/base/k", l.DestURI(l.DestKey("k")))
}

func TestParseDestLayoutFile(t *testing.T) {
	l, err := ParseDestLayout("file:///tmp/out/")
	require.NoError(t, err)
	require.Equal(t, "file", l.ProviderID)
	require.Equal(t, "/tmp/out", l.BaseDir)
	require.Equal(t, "nested/o.txt", l.DestKey("nested/o.txt"))
	require.Equal(t, "file:///tmp/out/nested/o.txt", l.DestURI("nested/o.txt"))
}

func TestParseDestLayoutErrors(t *testing.T) {
	_, err := ParseDestLayout("")
	require.Error(t, err)
	_, err = ParseDestLayout("s3://bucket/a/**/*.xml")
	require.ErrorContains(t, err, "prefix")
}

func TestQuarantineDestRel(t *testing.T) {
	require.Equal(t, "quar/src/k", QuarantineDestRel("quar", "src/k"))
	require.Equal(t, "src/k", QuarantineDestRel("", "src/k"))
	require.Equal(t, "quar", QuarantineDestRel("/quar/", "/"))
}

func TestIsRelativeQuarantinePrefix(t *testing.T) {
	require.True(t, IsRelativeQuarantinePrefix("quarantine/sub"))
	require.False(t, IsRelativeQuarantinePrefix("/abs"))
	require.False(t, IsRelativeQuarantinePrefix("s3://bucket/x"))
}
