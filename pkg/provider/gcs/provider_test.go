package gcs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"

	"github.com/3leaps/gonimbus/pkg/provider"
)

func TestProviderReadOperationsWithFakeServer(t *testing.T) {
	ctx := context.Background()
	p := newFakeProvider(t, []fakestorage.Object{
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "page/a.txt"}, Content: []byte("a")},
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "page/b.txt"}, Content: []byte("b")},
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "page/c.txt"}, Content: []byte("c")},
	})

	writer := p.client.Bucket("bucket").Object("objects/with-attrs.txt").NewWriter(ctx)
	writer.ContentType = "text/plain"
	writer.Metadata = map[string]string{"source": "slice1"}
	writer.StorageClass = "COLDLINE"
	_, err := writer.Write([]byte("abcdef"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	firstPage, err := p.List(ctx, provider.ListOptions{Prefix: "page/", MaxKeys: 2})
	require.NoError(t, err)
	require.True(t, firstPage.IsTruncated)
	require.NotEmpty(t, firstPage.ContinuationToken)
	require.Equal(t, []string{"page/a.txt", "page/b.txt"}, summaryKeys(firstPage.Objects))
	require.Equal(t, int64(1), firstPage.Objects[0].Size)

	secondPage, err := p.List(ctx, provider.ListOptions{Prefix: "page/", MaxKeys: 2, ContinuationToken: firstPage.ContinuationToken})
	require.NoError(t, err)
	require.False(t, secondPage.IsTruncated)
	require.Empty(t, secondPage.ContinuationToken)
	require.Equal(t, []string{"page/c.txt"}, summaryKeys(secondPage.Objects))

	meta, err := p.Head(ctx, "objects/with-attrs.txt")
	require.NoError(t, err)
	require.Equal(t, "objects/with-attrs.txt", meta.Key)
	require.Equal(t, int64(6), meta.Size)
	require.Equal(t, "text/plain", meta.ContentType)
	require.Equal(t, "slice1", meta.Metadata["source"])
	require.Equal(t, "COLDLINE", meta.StorageClass)
	require.NotEmpty(t, meta.Version)

	body, n, err := p.GetObject(ctx, "objects/with-attrs.txt")
	require.NoError(t, err)
	require.Equal(t, int64(6), n)
	got, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, []byte("abcdef"), got)

	versionedBody, versionedMeta, err := p.GetObjectVersioned(ctx, "objects/with-attrs.txt")
	require.NoError(t, err)
	require.Equal(t, int64(6), versionedMeta.Size)
	require.NotEmpty(t, versionedMeta.ETag)
	require.NotEmpty(t, versionedMeta.Version)
	versionedBytes, err := io.ReadAll(versionedBody)
	require.NoError(t, err)
	require.NoError(t, versionedBody.Close())
	require.Equal(t, []byte("abcdef"), versionedBytes)

	rangeBody, rangeSize, err := p.GetRange(ctx, "objects/with-attrs.txt", 1, 3)
	require.NoError(t, err)
	require.Equal(t, int64(3), rangeSize)
	rangeBytes, err := io.ReadAll(rangeBody)
	require.NoError(t, err)
	require.NoError(t, rangeBody.Close())
	require.Equal(t, []byte("bcd"), rangeBytes)
}

func TestProviderReadOperationsMapNotFound(t *testing.T) {
	ctx := context.Background()
	p := newFakeProvider(t, []fakestorage.Object{
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "exists.txt"}, Content: []byte("x")},
	})

	_, err := p.Head(ctx, "missing.txt")
	require.Error(t, err)
	require.True(t, provider.IsNotFound(err), "got %v", err)

	_, _, err = p.GetObject(ctx, "missing.txt")
	require.Error(t, err)
	require.True(t, provider.IsNotFound(err), "got %v", err)
}

func TestProviderReadInterfacesOnlyForSlice1(t *testing.T) {
	var p any = &Provider{}

	require.Implements(t, (*provider.Provider)(nil), p)
	require.Implements(t, (*provider.ObjectGetter)(nil), p)
	require.Implements(t, (*provider.VersionedGetter)(nil), p)
	require.Implements(t, (*provider.ObjectRanger)(nil), p)
	require.NotImplements(t, (*provider.ObjectPutter)(nil), p)
	require.NotImplements(t, (*provider.ConditionalPutter)(nil), p)
	require.NotImplements(t, (*provider.ObjectDeleter)(nil), p)
}

func TestWrapGCSErrorMapsGoogleAPICodes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		key  string
		want error
	}{
		{name: "object sentinel", err: storage.ErrObjectNotExist, key: "key", want: provider.ErrNotFound},
		{name: "bucket sentinel", err: storage.ErrBucketNotExist, want: provider.ErrBucketNotFound},
		{name: "unauthorized", err: &googleapi.Error{Code: http.StatusUnauthorized}, want: provider.ErrInvalidCredentials},
		{name: "forbidden", err: &googleapi.Error{Code: http.StatusForbidden}, want: provider.ErrAccessDenied},
		{name: "forbidden rate limit reason", err: &googleapi.Error{Code: http.StatusForbidden, Errors: []googleapi.ErrorItem{{Reason: "rateLimitExceeded"}}}, want: provider.ErrThrottled},
		{name: "forbidden user rate limit reason", err: &googleapi.Error{Code: http.StatusForbidden, Errors: []googleapi.ErrorItem{{Reason: "userRateLimitExceeded"}}}, want: provider.ErrThrottled},
		{name: "forbidden quota reason", err: &googleapi.Error{Code: http.StatusForbidden, Errors: []googleapi.ErrorItem{{Reason: "quotaExceeded"}}}, want: provider.ErrThrottled},
		{name: "resource exhausted reason", err: &googleapi.Error{Code: http.StatusForbidden, Errors: []googleapi.ErrorItem{{Reason: "RESOURCE_EXHAUSTED"}}}, want: provider.ErrThrottled},
		{name: "object 404", err: &googleapi.Error{Code: http.StatusNotFound}, key: "key", want: provider.ErrNotFound},
		{name: "bucket 404", err: &googleapi.Error{Code: http.StatusNotFound}, want: provider.ErrBucketNotFound},
		{name: "throttled", err: &googleapi.Error{Code: http.StatusTooManyRequests}, want: provider.ErrThrottled},
		{name: "unavailable", err: &googleapi.Error{Code: http.StatusServiceUnavailable}, want: provider.ErrProviderUnavailable},
		{name: "credentials refresh", err: errors.New("failed to refresh credential token"), want: provider.ErrCredentialsRefreshFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := wrapGCSError("Head", "bucket", tt.key, tt.err)
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestTransportTunedHTTPClientUsesClonedTransport(t *testing.T) {
	require.Nil(t, transportTunedHTTPClient(Config{}))

	client := transportTunedHTTPClient(Config{MaxIdleConnsPerHost: 16, MaxConnsPerHost: 32})
	require.NotNil(t, client)
	tr, ok := client.Transport.(*http.Transport)
	require.True(t, ok)
	require.Equal(t, 16, tr.MaxIdleConnsPerHost)
	require.GreaterOrEqual(t, tr.MaxIdleConns, 16)
	require.Equal(t, 32, tr.MaxConnsPerHost)
	require.NotSame(t, http.DefaultTransport, tr)
}

func TestClampMaxKeys(t *testing.T) {
	require.Equal(t, 25, clampMaxKeys(0, 25))
	require.Equal(t, DefaultMaxKeys, clampMaxKeys(0, 0))
	require.Equal(t, 10, clampMaxKeys(10, 25))
	require.Equal(t, MaxAllowedKeys, clampMaxKeys(MaxAllowedKeys+1, 25))
}

func newFakeProvider(t *testing.T, objects []fakestorage.Object) *Provider {
	t.Helper()
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{InitialObjects: objects})
	require.NoError(t, err)
	t.Cleanup(server.Stop)

	client := server.Client()
	p := &Provider{client: client, bucket: "bucket", maxKeys: DefaultMaxKeys}
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	return p
}

func summaryKeys(objects []provider.ObjectSummary) []string {
	keys := make([]string, 0, len(objects))
	for _, obj := range objects {
		keys = append(keys, obj.Key)
	}
	return keys
}
