package gcs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
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

	prefixes, err := p.ListCommonPrefixes(ctx, provider.ListCommonPrefixesOptions{Prefix: "page/", Delimiter: "/"})
	require.NoError(t, err)
	require.Empty(t, prefixes.Prefixes)

	delimited, err := p.ListWithDelimiter(ctx, provider.ListWithDelimiterOptions{Prefix: "page/", Delimiter: "/"})
	require.NoError(t, err)
	require.Empty(t, delimited.CommonPrefixes)
	require.Equal(t, []string{"page/a.txt", "page/b.txt", "page/c.txt"}, summaryKeys(delimited.Objects))
}

func TestProviderDelimiterListingWithFakeServer(t *testing.T) {
	ctx := context.Background()
	p := newFakeProvider(t, []fakestorage.Object{
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "root/a.txt"}, Content: []byte("a")},
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "root/child/b.txt"}, Content: []byte("b")},
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "root/child/c.txt"}, Content: []byte("c")},
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "root/other/d.txt"}, Content: []byte("d")},
	})

	prefixes, err := p.ListCommonPrefixes(ctx, provider.ListCommonPrefixesOptions{Prefix: "root/", Delimiter: "/"})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"root/child/", "root/other/"}, prefixes.Prefixes)

	delimited, err := p.ListWithDelimiter(ctx, provider.ListWithDelimiterOptions{Prefix: "root/", Delimiter: "/"})
	require.NoError(t, err)
	require.Equal(t, []string{"root/a.txt"}, summaryKeys(delimited.Objects))
	require.ElementsMatch(t, []string{"root/child/", "root/other/"}, delimited.CommonPrefixes)
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

func TestProviderReadWriteInterfaces(t *testing.T) {
	var p any = &Provider{}

	require.Implements(t, (*provider.Provider)(nil), p)
	require.Implements(t, (*provider.PrefixLister)(nil), p)
	require.Implements(t, (*provider.DelimiterLister)(nil), p)
	require.Implements(t, (*provider.ObjectGetter)(nil), p)
	require.Implements(t, (*provider.VersionedGetter)(nil), p)
	require.Implements(t, (*provider.ObjectRanger)(nil), p)
	require.Implements(t, (*provider.ObjectPutter)(nil), p)
	require.Implements(t, (*provider.ConditionalPutter)(nil), p)
	require.Implements(t, (*provider.MetadataAwarePutter)(nil), p)
	require.Implements(t, (*provider.ObjectDeleter)(nil), p)
}

func TestProviderWriteOperationsWithFakeServer(t *testing.T) {
	ctx := context.Background()
	p := newFakeProvider(t, []fakestorage.Object{
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: ".seed"}, Content: []byte{}},
	})

	err := p.PutObjectWithOptions(ctx, "objects/written.txt", strings.NewReader("hello"), int64(len("hello")), provider.PutOptions{
		ContentType:  "text/plain",
		UserMetadata: map[string]string{"source": "slice3"},
		StorageClass: "COLDLINE",
	})
	require.NoError(t, err)

	meta, err := p.Head(ctx, "objects/written.txt")
	require.NoError(t, err)
	require.Equal(t, int64(5), meta.Size)
	require.Equal(t, "text/plain", meta.ContentType)
	require.Equal(t, "slice3", meta.Metadata["source"])
	require.Equal(t, "COLDLINE", meta.StorageClass)

	result, err := p.PutObjectConditional(ctx, "objects/create-only.txt", strings.NewReader("first"), int64(len("first")), provider.PutPrecondition{IfAbsent: true})
	require.NoError(t, err)
	require.NotEmpty(t, result.ETag)
	require.NotEmpty(t, result.Version)

	_, err = p.PutObjectConditional(ctx, "objects/create-only.txt", strings.NewReader("second"), int64(len("second")), provider.PutPrecondition{IfAbsent: true})
	require.Error(t, err)
	require.True(t, provider.IsAlreadyExists(err), "got %v", err)

	require.NoError(t, p.DeleteObject(ctx, "objects/written.txt"))
	_, err = p.Head(ctx, "objects/written.txt")
	require.Error(t, err)
	require.True(t, provider.IsNotFound(err), "got %v", err)
}

func TestProviderConditionalPutIfMatchETagUnsupported(t *testing.T) {
	ctx := context.Background()
	p := newFakeProvider(t, []fakestorage.Object{
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: ".seed"}, Content: []byte{}},
	})

	etag := "etag"
	_, err := p.PutObjectConditional(ctx, "objects/etag.txt", strings.NewReader("body"), int64(len("body")), provider.PutPrecondition{IfMatchETag: &etag})
	require.Error(t, err)
	require.True(t, provider.IsUnsupportedPrecondition(err), "got %v", err)
}

func TestAnonymousWriteMethodsFailClosedBeforeRequest(t *testing.T) {
	ctx := context.Background()
	p := newFakeProvider(t, []fakestorage.Object{
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: ".seed"}, Content: []byte{}},
	})
	p.anonymous = true

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "PutObject",
			run: func() error {
				return p.PutObject(ctx, "objects/write.txt", strings.NewReader("body"), int64(len("body")))
			},
		},
		{
			name: "PutObjectWithOptions",
			run: func() error {
				return p.PutObjectWithOptions(ctx, "objects/write.txt", strings.NewReader("body"), int64(len("body")), provider.PutOptions{ContentType: "text/plain"})
			},
		},
		{
			name: "PutObjectConditional",
			run: func() error {
				_, err := p.PutObjectConditional(ctx, "objects/write.txt", strings.NewReader("body"), int64(len("body")), provider.PutPrecondition{IfAbsent: true})
				return err
			},
		},
		{
			name: "PutObjectConditionalWithOptions",
			run: func() error {
				_, err := p.PutObjectConditionalWithOptions(ctx, "objects/write.txt", strings.NewReader("body"), int64(len("body")), provider.PutPrecondition{IfAbsent: true}, provider.PutOptions{ContentType: "text/plain"})
				return err
			},
		},
		{
			name: "DeleteObject",
			run: func() error {
				return p.DeleteObject(ctx, "objects/write.txt")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			require.Error(t, err)
			require.True(t, provider.IsAnonymousReadOnly(err), "got %v", err)
			require.True(t, provider.IsAccessDenied(err), "got %v", err)
		})
	}

	_, err := p.Head(ctx, "objects/write.txt")
	require.Error(t, err)
	require.True(t, provider.IsNotFound(err), "got %v", err)
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

func TestClientOptionsForConfigKeepsUntunedADCOnSDKDefaultPath(t *testing.T) {
	old := adcTokenSource
	t.Cleanup(func() { adcTokenSource = old })
	adcTokenSource = func(context.Context, ...string) (oauth2.TokenSource, error) {
		t.Fatal("untuned ADC path should remain storage SDK default")
		return nil, nil
	}

	opts, err := clientOptionsForConfig(context.Background(), Config{Bucket: "bucket"})
	require.NoError(t, err)
	require.Empty(t, opts)
}

func TestClientOptionsForConfigUsesTunedHTTPClientForADC(t *testing.T) {
	old := adcTokenSource
	t.Cleanup(func() { adcTokenSource = old })

	var gotClient *http.Client
	var gotScopes []string
	adcTokenSource = func(ctx context.Context, scopes ...string) (oauth2.TokenSource, error) {
		gotClient, _ = ctx.Value(oauth2.HTTPClient).(*http.Client)
		gotScopes = append([]string(nil), scopes...)
		return staticTokenSource{token: "SECRET-TOKEN"}, nil
	}

	opts, err := clientOptionsForConfig(context.Background(), Config{
		Bucket:              "bucket",
		MaxIdleConnsPerHost: 16,
		MaxConnsPerHost:     32,
	})
	require.NoError(t, err)
	require.Len(t, opts, 1)
	require.NotNil(t, gotClient)
	tr, ok := gotClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.Equal(t, 16, tr.MaxIdleConnsPerHost)
	require.Equal(t, 32, tr.MaxConnsPerHost)
	require.Equal(t, DefaultScopes(), gotScopes)
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
