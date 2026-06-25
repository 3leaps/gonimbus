package manifest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadIndexManifestAcceptsGCSConnection(t *testing.T) {
	m, err := LoadIndexManifestFromBytes([]byte(`version: "1.0"
connection:
  provider: gcs
  bucket: test-bucket
  base_uri: gs://test-bucket/base/
  project: test-project
`), "index.yaml")

	require.NoError(t, err)
	require.Equal(t, "gcs", m.Connection.Provider)
	require.Equal(t, "test-bucket", m.Connection.Bucket)
	require.Equal(t, "gs://test-bucket/base/", m.Connection.BaseURI)
	require.Equal(t, "test-project", m.Connection.Project)
}
