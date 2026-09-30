package gcs

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	"github.com/3leaps/gonimbus/pkg/transfer"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

type countingReadTransport struct {
	base http.RoundTripper
	gets atomic.Int64
}

func (tr *countingReadTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodGet {
		tr.gets.Add(1)
	}
	return tr.base.RoundTrip(r)
}

func TestUnpinnedCopyRetainsOneGCSReadRequest(t *testing.T) {
	ctx := context.Background()
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{InitialObjects: []fakestorage.Object{{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "source"}, Content: []byte("payload")}}})
	require.NoError(t, err)
	t.Cleanup(server.Stop)
	httpClient := server.HTTPClient()
	tr := &countingReadTransport{base: httpClient.Transport}
	httpClient.Transport = tr
	client, err := storage.NewClient(ctx, option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL()+"/storage/v1/"), storage.WithJSONReads())
	require.NoError(t, err)
	src := &Provider{client: client, bucket: "bucket", maxKeys: DefaultMaxKeys}
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	// Negative control: the richer getter really adds an Attrs request, so a
	// method-call-only mock would miss the wire-level regression.
	body, _, err := src.GetObjectVersioned(ctx, "source")
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, int64(2), tr.gets.Load())

	for _, receipt := range []bool{false, true} {
		tr.gets.Store(0)
		root := t.TempDir()
		dst, err := providerfile.New(providerfile.Config{BaseDir: root})
		require.NoError(t, err)
		if receipt {
			result, copyErr := transfer.CopyObjectWithReceipt(ctx, src, dst, "source", "dest", 7, transfer.CopyReceiptOptions{})
			require.NoError(t, copyErr)
			require.True(t, result.SourceLastModified.IsZero())
		} else {
			_, err = transfer.CopyObject(ctx, src, dst, "source", "dest", 7, 0)
			require.NoError(t, err)
		}
		require.Equal(t, int64(1), tr.gets.Load())
		payload, err := os.ReadFile(filepath.Join(root, "dest"))
		require.NoError(t, err)
		require.Equal(t, "payload", string(payload))
	}
}
