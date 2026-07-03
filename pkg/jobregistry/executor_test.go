package jobregistry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexBuildBackgroundMetadataIncludesSince(t *testing.T) {
	metadata := indexBuildBackgroundMetadata(BackgroundOptions{
		Since:    "auto",
		Metadata: map[string]string{"site": "s1"},
	})

	require.Equal(t, "auto", metadata["since"])
	require.Equal(t, "s1", metadata["site"])
}

func TestIndexBuildBackgroundMetadataOmitsBlankSince(t *testing.T) {
	metadata := indexBuildBackgroundMetadata(BackgroundOptions{
		Since: " ",
	})

	require.Nil(t, metadata)
}
