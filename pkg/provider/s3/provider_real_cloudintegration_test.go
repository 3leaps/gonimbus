//go:build cloudintegration

package s3_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/test/cloudtest"
)

func TestProviderRealS3ReadWriteRoundTrip_CloudIntegration(t *testing.T) {
	ctx := context.Background()
	cfg, prefix := cloudtest.CreateS3ObjectPrefix(t, ctx)

	p := cloudtest.RealS3Provider(t, ctx, cfg)
	defer func() { _ = p.Close() }()

	key := prefix + "objects/roundtrip.txt"
	content := "hello from real s3-compatible storage"
	require.NoError(t, p.PutObjectWithOptions(ctx, key, strings.NewReader(content), int64(len(content)), provider.PutOptions{
		ContentType:  "text/plain",
		UserMetadata: map[string]string{"gonimbus-test": "real-s3"},
	}))

	meta, err := p.Head(ctx, key)
	require.NoError(t, err)
	require.Equal(t, key, meta.Key)
	require.Equal(t, int64(len(content)), meta.Size)
	require.Equal(t, "text/plain", meta.ContentType)
	require.Equal(t, "real-s3", meta.Metadata["gonimbus-test"])
	require.NotEmpty(t, meta.ETag)

	body, size, err := p.GetObject(ctx, key)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), size)
	got, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, content, string(got))

	rangeBody, rangeSize, err := p.GetRange(ctx, key, 6, 10)
	require.NoError(t, err)
	require.Equal(t, int64(5), rangeSize)
	rangeBytes, err := io.ReadAll(rangeBody)
	require.NoError(t, err)
	require.NoError(t, rangeBody.Close())
	require.Equal(t, "from ", string(rangeBytes))

	list, err := p.List(ctx, provider.ListOptions{Prefix: prefix + "objects/", MaxKeys: 10})
	require.NoError(t, err)
	require.Contains(t, realS3ObjectKeys(list.Objects), key)

	require.NoError(t, p.DeleteObject(ctx, key))
	_, err = p.Head(ctx, key)
	require.Error(t, err)
	require.True(t, provider.IsNotFound(err), "got %v", err)
}

func realS3ObjectKeys(objects []provider.ObjectSummary) []string {
	keys := make([]string, 0, len(objects))
	for _, object := range objects {
		keys = append(keys, object.Key)
	}
	return keys
}
