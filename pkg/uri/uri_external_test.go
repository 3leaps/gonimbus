package uri_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/uri"
)

func TestParseURIExternalConsumerShape(t *testing.T) {
	parsed, err := uri.ParseURI("s3://bucket/data/2026/**/*.xml")
	require.NoError(t, err)

	require.Equal(t, "s3", parsed.Provider)
	require.Equal(t, "bucket", parsed.Bucket)
	require.Equal(t, "data/2026/", parsed.Key)
	require.Equal(t, "data/2026/**/*.xml", parsed.Pattern)
	require.True(t, parsed.IsPattern())
	require.Equal(t, "s3://bucket/data/2026/**/*.xml", parsed.String())
}
