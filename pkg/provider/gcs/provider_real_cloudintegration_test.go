//go:build cloudintegration

package gcs_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/gcs"
	"github.com/3leaps/gonimbus/test/cloudtest"
)

func TestProviderRealGCSReadWriteRoundTrip_CloudIntegration(t *testing.T) {
	ctx := context.Background()
	bucket, prefix := cloudtest.CreateGCSObjectPrefix(t, ctx)

	p, err := gcs.New(ctx, gcs.Config{
		Bucket:               bucket,
		Project:              cloudtest.RealGCSProject(),
		WriterChunkSizeBytes: gcs.MinWriterChunkSizeBytes,
	})
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	key := prefix + "objects/roundtrip.txt"
	content := "hello from real gcs"
	require.NoError(t, p.PutObjectWithOptions(ctx, key, strings.NewReader(content), int64(len(content)), provider.PutOptions{
		ContentType:  "text/plain",
		UserMetadata: map[string]string{"gonimbus-test": "real-gcs"},
	}))

	meta, err := p.Head(ctx, key)
	require.NoError(t, err)
	require.Equal(t, key, meta.Key)
	require.Equal(t, int64(len(content)), meta.Size)
	require.Equal(t, "text/plain", meta.ContentType)
	require.Equal(t, "real-gcs", meta.Metadata["gonimbus-test"])
	require.NotEmpty(t, meta.ETag)
	require.NotEmpty(t, meta.Version)

	body, size, err := p.GetObject(ctx, key)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), size)
	got, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, content, string(got))

	rangeBody, rangeSize, err := p.GetRange(ctx, key, 6, 9)
	require.NoError(t, err)
	require.Equal(t, int64(4), rangeSize)
	rangeBytes, err := io.ReadAll(rangeBody)
	require.NoError(t, err)
	require.NoError(t, rangeBody.Close())
	require.Equal(t, "from", string(rangeBytes))

	list, err := p.List(ctx, provider.ListOptions{Prefix: prefix + "objects/", MaxKeys: 10})
	require.NoError(t, err)
	require.Contains(t, objectKeys(list.Objects), key)

	createOnlyKey := prefix + "objects/create-only.txt"
	first := "first"
	result, err := p.PutObjectConditional(ctx, createOnlyKey, strings.NewReader(first), int64(len(first)), provider.PutPrecondition{IfAbsent: true})
	require.NoError(t, err)
	require.NotEmpty(t, result.ETag)
	require.NotEmpty(t, result.Version)

	second := "second"
	_, err = p.PutObjectConditional(ctx, createOnlyKey, strings.NewReader(second), int64(len(second)), provider.PutPrecondition{IfAbsent: true})
	require.Error(t, err)
	require.True(t, provider.IsAlreadyExists(err), "got %v", err)

	createOnlyBody, _, err := p.GetObject(ctx, createOnlyKey)
	require.NoError(t, err)
	createOnlyBytes, err := io.ReadAll(createOnlyBody)
	require.NoError(t, err)
	require.NoError(t, createOnlyBody.Close())
	require.Equal(t, first, string(createOnlyBytes))

	_, err = p.PutObjectConditional(ctx, key, strings.NewReader("replace"), int64(len("replace")), provider.PutPrecondition{IfMatchETag: &meta.ETag})
	require.Error(t, err)
	require.True(t, provider.IsUnsupportedPrecondition(err), "got %v", err)

	require.NoError(t, p.DeleteObject(ctx, key))
	_, err = p.Head(ctx, key)
	require.Error(t, err)
	require.True(t, provider.IsNotFound(err), "got %v", err)
}

func TestProviderRealGCSDelimiterListing_CloudIntegration(t *testing.T) {
	ctx := context.Background()
	bucket, prefix := cloudtest.CreateGCSObjectPrefix(t, ctx)

	p, err := gcs.New(ctx, gcs.Config{
		Bucket:  bucket,
		Project: cloudtest.RealGCSProject(),
	})
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	require.NoError(t, p.PutObject(ctx, prefix+"tree/root.txt", strings.NewReader("root"), int64(len("root"))))
	require.NoError(t, p.PutObject(ctx, prefix+"tree/child/a.txt", strings.NewReader("child"), int64(len("child"))))

	result, err := p.ListWithDelimiter(ctx, provider.ListWithDelimiterOptions{Prefix: prefix + "tree/", Delimiter: "/"})
	require.NoError(t, err)
	require.Contains(t, objectKeys(result.Objects), prefix+"tree/root.txt")
	require.Contains(t, result.CommonPrefixes, prefix+"tree/child/")
}

func objectKeys(objects []provider.ObjectSummary) []string {
	keys := make([]string, 0, len(objects))
	for _, object := range objects {
		keys = append(keys, object.Key)
	}
	return keys
}
