package gcs

import (
	"context"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"
)

func TestUnconditionalPutResult(t *testing.T) {
	p := newFakeProvider(t, []fakestorage.Object{{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "seed"}, Content: []byte("seed")}})
	result, err := p.PutObjectResultWithOptions(context.Background(), "receipt.txt", strings.NewReader("payload"), 7, provider.PutOptions{ContentType: "text/plain"})
	require.NoError(t, err)
	require.NotEmpty(t, result.Version)
	meta, err := p.Head(context.Background(), "receipt.txt")
	require.NoError(t, err)
	require.Equal(t, meta.Version, result.Version)
	require.Equal(t, meta.ETag, result.ETag)
	require.Equal(t, "text/plain", meta.ContentType)
}
