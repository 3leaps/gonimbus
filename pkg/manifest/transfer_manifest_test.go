package manifest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validTransferManifestYAML() string {
	return `version: "1.0"
source:
  provider: s3
  bucket: src-bucket

target:
  provider: s3
  bucket: dst-bucket

match:
  includes:
    - "**/*"

transfer: {}
`
}

func TestLoadTransferFromBytes_YAML(t *testing.T) {
	m, err := LoadTransferFromBytes([]byte(validTransferManifestYAML()), "transfer.yaml")
	require.NoError(t, err)

	assert.Equal(t, "1.0", m.Version)
	assert.Equal(t, "s3", m.Source.Provider)
	assert.Equal(t, "src-bucket", m.Source.Bucket)
	assert.Equal(t, "s3", m.Target.Provider)
	assert.Equal(t, "dst-bucket", m.Target.Bucket)

	// Defaults
	assert.Equal(t, DefaultTransferMode, m.Transfer.Mode)
	assert.Equal(t, DefaultTransferConcurrency, m.Transfer.Concurrency)
	assert.Equal(t, DefaultOnExists, m.Transfer.OnExists)
	assert.True(t, m.Transfer.Dedup.DedupEnabled())
	assert.Equal(t, DefaultDedupStrategy, m.Transfer.Dedup.Strategy)
	assert.Equal(t, DefaultPreflightMode, m.Transfer.Preflight.Mode)
	assert.Equal(t, DefaultProbeStrategy, m.Transfer.Preflight.ProbeStrategy)
	assert.Equal(t, DefaultProbePrefix, m.Transfer.Preflight.ProbePrefix)
	assert.Equal(t, DefaultDestination, m.Output.Destination)
	assert.True(t, m.Output.ProgressEnabled())
}

func TestLoadTransferFromBytes_UnknownFieldRejected(t *testing.T) {
	bad := `version: "1.0"
source:
  provider: s3
  bucket: src-bucket

target:
  provider: s3
  bucket: dst-bucket

match:
  includes:
    - "**/*"

transfer:
  unknown_field: true
`

	_, err := LoadTransferFromBytes([]byte(bad), "transfer.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown_field")
}
