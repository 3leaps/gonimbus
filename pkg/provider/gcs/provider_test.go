package gcs

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

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

	listedObject, err := p.List(ctx, provider.ListOptions{Prefix: "objects/with-attrs.txt"})
	require.NoError(t, err)
	require.Len(t, listedObject.Objects, 1)
	require.NotEmpty(t, listedObject.Objects[0].Revision)
	revisionBody, revisionMeta, err := p.GetObjectRevision(ctx, "objects/with-attrs.txt", provider.SourceRevision{
		Kind: provider.RevisionNative, Value: listedObject.Objects[0].Revision,
	})
	require.NoError(t, err)
	require.Equal(t, listedObject.Objects[0].Revision, revisionMeta.Version)
	require.NoError(t, revisionBody.Close())

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

func TestGetObjectRevisionETagWithFakeServer(t *testing.T) {
	ctx := context.Background()
	p := newFakeProvider(t, []fakestorage.Object{
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "doc.txt"}, Content: []byte("alpha")},
	})

	head, err := p.Head(ctx, "doc.txt")
	require.NoError(t, err)
	require.NotEmpty(t, head.ETag)
	require.NotEmpty(t, head.Version)

	// Unchanged read with RevisionETag (quoted or unquoted) succeeds and returns read-bound metadata.
	body, meta, err := p.GetObjectRevision(ctx, "doc.txt", provider.SourceRevision{
		Kind:  provider.RevisionETag,
		Value: `"` + head.ETag + `"`,
	})
	require.NoError(t, err)
	raw, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, "alpha", string(raw))
	require.Equal(t, head.ETag, meta.ETag)
	require.Equal(t, head.Version, meta.Version)

	// Same-size replacement returns ErrSourceChanged (not ErrNotFound).
	require.NoError(t, p.PutObject(ctx, "doc.txt", strings.NewReader("bravo"), 5))
	_, _, err = p.GetObjectRevision(ctx, "doc.txt", provider.SourceRevision{
		Kind:  provider.RevisionETag,
		Value: head.ETag,
	})
	require.ErrorIs(t, err, provider.ErrSourceChanged)
	require.False(t, provider.IsNotFound(err))

	// Different-size replacement returns ErrSourceChanged.
	require.NoError(t, p.PutObject(ctx, "doc.txt", strings.NewReader("charlie-longer"), int64(len("charlie-longer"))))
	_, _, err = p.GetObjectRevision(ctx, "doc.txt", provider.SourceRevision{
		Kind:  provider.RevisionETag,
		Value: head.ETag,
	})
	require.ErrorIs(t, err, provider.ErrSourceChanged)
	require.False(t, provider.IsNotFound(err))

	// Deleted object returns ErrNotFound (not ErrSourceChanged).
	require.NoError(t, p.DeleteObject(ctx, "doc.txt"))
	_, _, err = p.GetObjectRevision(ctx, "doc.txt", provider.SourceRevision{
		Kind:  provider.RevisionETag,
		Value: head.ETag,
	})
	require.True(t, provider.IsNotFound(err), "expected ErrNotFound, got %v", err)
	require.False(t, provider.IsSourceChanged(err))
}

func TestGetObjectRevisionNativeNotFoundVsChangedWithFakeServer(t *testing.T) {
	ctx := context.Background()
	p := newFakeProvider(t, []fakestorage.Object{
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "doc.txt"}, Content: []byte("alpha")},
	})

	head, err := p.Head(ctx, "doc.txt")
	require.NoError(t, err)
	require.NotEmpty(t, head.Version)

	// Matching generation succeeds.
	body, meta, err := p.GetObjectRevision(ctx, "doc.txt", provider.SourceRevision{
		Kind:  provider.RevisionNative,
		Value: head.Version,
	})
	require.NoError(t, err)
	raw, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, "alpha", string(raw))
	require.Equal(t, head.Version, meta.Version)

	// Negative and zero generations are refused before any GCS request.
	_, _, err = p.GetObjectRevision(ctx, "doc.txt", provider.SourceRevision{Kind: provider.RevisionNative, Value: "-1"})
	require.True(t, provider.IsReplayUnverifiable(err), "negative generation must fail closed with ErrReplayUnverifiable, got %v", err)
	_, err = p.HeadRevision(ctx, "doc.txt", provider.SourceRevision{Kind: provider.RevisionNative, Value: "-1"})
	require.True(t, provider.IsReplayUnverifiable(err), "negative generation HeadRevision must fail closed with ErrReplayUnverifiable, got %v", err)

	// Non-existent generation while live object exists reports ErrSourceChanged.
	_, _, err = p.GetObjectRevision(ctx, "doc.txt", provider.SourceRevision{
		Kind:  provider.RevisionNative,
		Value: "999999999999",
	})
	require.True(t, provider.IsSourceChanged(err), "expected ErrSourceChanged, got %v", err)
	require.False(t, provider.IsNotFound(err))

	// Non-existent generation after object deletion reports ErrNotFound.
	require.NoError(t, p.DeleteObject(ctx, "doc.txt"))
	_, _, err = p.GetObjectRevision(ctx, "doc.txt", provider.SourceRevision{
		Kind:  provider.RevisionNative,
		Value: "999999999999",
	})
	require.True(t, provider.IsNotFound(err), "expected ErrNotFound, got %v", err)
	require.False(t, provider.IsSourceChanged(err))
}

func TestGetObjectRevisionGCSMediaIgnoresGenerationFailsClosed(t *testing.T) {
	ctx := context.Background()
	v1Body := []byte("alpha")
	v2Body := []byte("bravo") // same length (5 bytes), different content
	v1CRC := crc32.Checksum(v1Body, gcsCRC32CTable)
	v2CRC := crc32.Checksum(v2Body, gcsCRC32CTable)
	encodeCRC := func(c uint32) string {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], c)
		return base64.StdEncoding.EncodeToString(buf[:])
	}

	for _, withMediaHeaders := range []bool{true, false} {
		name := "headers-omitted-stream-crc-check"
		if withMediaHeaders {
			name = "headers-present-attr-check"
		}
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				isMetadata := strings.HasPrefix(r.URL.Path, "/storage/v1/") && r.URL.Query().Get("alt") != "media"
				if !isMetadata {
					// Broken media endpoint ignores generation=101 and serves v2 bytes!
					if withMediaHeaders {
						w.Header().Set("X-Goog-Generation", "202")
						w.Header().Set("X-Goog-Metageneration", "2")
						w.Header().Set("X-Goog-Hash", "crc32c="+encodeCRC(v2CRC))
					}
					w.Header().Set("Content-Length", fmt.Sprintf("%d", len(v2Body)))
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(v2Body)
					return
				}
				// Metadata lookup honors generation=101 and returns v1 attrs.
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"bucket":         "bucket",
					"name":           "doc.txt",
					"generation":     "101",
					"metageneration": "1",
					"size":           fmt.Sprintf("%d", len(v1Body)),
					"etag":           "etag-v1",
					"crc32c":         encodeCRC(v1CRC),
				})
			}))
			defer srv.Close()

			client, err := storage.NewClient(ctx,
				option.WithEndpoint(srv.URL+"/storage/v1/"),
				option.WithHTTPClient(srv.Client()),
				option.WithoutAuthentication(),
			)
			require.NoError(t, err)
			defer func() { _ = client.Close() }()

			p := &Provider{client: client, bucket: "bucket", maxKeys: DefaultMaxKeys}

			// HeadRevision honors generation 101.
			headMeta, err := p.HeadRevision(ctx, "doc.txt", provider.SourceRevision{Kind: provider.RevisionNative, Value: "101"})
			require.NoError(t, err)
			require.Equal(t, "101", headMeta.Version)
			require.Equal(t, "etag-v1", headMeta.ETag)

			// GetObjectRevision must fail closed with ErrSourceChanged when media serves v2 bytes.
			body, _, err := p.GetObjectRevision(ctx, "doc.txt", provider.SourceRevision{Kind: provider.RevisionNative, Value: "101"})
			if err == nil {
				_, readErr := io.ReadAll(body)
				_ = body.Close()
				err = readErr
			}
			require.Error(t, err)
			require.True(t, provider.IsSourceChanged(err), "media serving unpinned v2 bytes must fail closed with ErrSourceChanged, got %v", err)
		})
	}

	t.Run("valid-zero-crc32c-object-rejects-different-same-length-body", func(t *testing.T) {
		zeroCRCBytes := []byte{0xab, 0x9b, 0xe0, 0x9b}
		require.Equal(t, uint32(0), crc32.Checksum(zeroCRCBytes, gcsCRC32CTable), "sanity: fixture must have Castagnoli CRC32C 0x00000000")
		wrongBody := []byte{0x01, 0x02, 0x03, 0x04}

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			isMetadata := strings.HasPrefix(r.URL.Path, "/storage/v1/") && r.URL.Query().Get("alt") != "media"
			if !isMetadata {
				// Media endpoint omits X-Goog-Hash and serves different 4-byte body.
				w.Header().Set("Content-Length", fmt.Sprintf("%d", len(wrongBody)))
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(wrongBody)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"bucket":         "bucket",
				"name":           "zero.bin",
				"generation":     "303",
				"metageneration": "1",
				"size":           fmt.Sprintf("%d", len(zeroCRCBytes)),
				"etag":           "etag-zero",
				"crc32c":         encodeCRC(0),
			})
		}))
		defer srv.Close()

		client, err := storage.NewClient(ctx,
			option.WithEndpoint(srv.URL+"/storage/v1/"),
			option.WithHTTPClient(srv.Client()),
			option.WithoutAuthentication(),
		)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		p := &Provider{client: client, bucket: "bucket", maxKeys: DefaultMaxKeys}
		body, _, err := p.GetObjectRevision(ctx, "zero.bin", provider.SourceRevision{Kind: provider.RevisionNative, Value: "303"})
		if err == nil {
			_, readErr := io.ReadAll(body)
			_ = body.Close()
			err = readErr
		}
		require.Error(t, err)
		require.True(t, provider.IsSourceChanged(err), "valid zero CRC32C object must reject wrong body, got %v", err)
	})

	t.Run("transparently-decompressed-response-refused-as-unverifiable", func(t *testing.T) {
		var gzBuf bytes.Buffer
		gz := gzip.NewWriter(&gzBuf)
		_, err := gz.Write([]byte("plain-payload"))
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		storedBytes := gzBuf.Bytes()
		storedCRC := crc32.Checksum(storedBytes, gcsCRC32CTable)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			isMetadata := strings.HasPrefix(r.URL.Path, "/storage/v1/") && r.URL.Query().Get("alt") != "media"
			if !isMetadata {
				// Simulate GCS server-side transparent decompression (transcoding):
				// stored as gzip, served uncompressed without Content-Encoding.
				w.Header().Set("X-Goog-Stored-Content-Encoding", "gzip")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("plain-payload"))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"bucket":          "bucket",
				"name":            "compressed.txt",
				"generation":      "404",
				"metageneration":  "1",
				"contentEncoding": "gzip",
				"size":            fmt.Sprintf("%d", len(storedBytes)),
				"etag":            "etag-gz",
				"crc32c":          encodeCRC(storedCRC),
			})
		}))
		defer srv.Close()

		client, err := storage.NewClient(ctx,
			option.WithEndpoint(srv.URL+"/storage/v1/"),
			option.WithHTTPClient(srv.Client()),
			option.WithoutAuthentication(),
		)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		p := &Provider{client: client, bucket: "bucket", maxKeys: DefaultMaxKeys}
		_, _, err = p.GetObjectRevision(ctx, "compressed.txt", provider.SourceRevision{Kind: provider.RevisionNative, Value: "404"})
		require.Error(t, err)
		require.True(t, provider.IsReplayUnverifiable(err), "transparently decompressed response must fail closed with ErrReplayUnverifiable, got %v", err)
	})
}
