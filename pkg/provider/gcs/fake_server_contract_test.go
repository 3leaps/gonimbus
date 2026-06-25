package gcs

import (
	"context"
	"errors"
	"io"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

func TestFakeGCSServerContractForSlice0(t *testing.T) {
	ctx := context.Background()
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects: []fakestorage.Object{
			{
				ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "page/a.txt"},
				Content:     []byte("a"),
			},
			{
				ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "page/b.txt"},
				Content:     []byte("b"),
			},
			{
				ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "bucket", Name: "page/c.txt"},
				Content:     []byte("c"),
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(server.Stop)

	client := server.Client()
	t.Cleanup(func() { _ = client.Close() })
	bucket := client.Bucket("bucket")

	writer := bucket.Object("objects/with-attrs.txt").If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	writer.ContentType = "text/plain"
	writer.Metadata = map[string]string{"source": "slice0"}
	writer.StorageClass = "COLDLINE"
	_, err = writer.Write([]byte("abcdef"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	attrs, err := bucket.Object("objects/with-attrs.txt").Attrs(ctx)
	require.NoError(t, err)
	require.Equal(t, "text/plain", attrs.ContentType)
	require.Equal(t, "slice0", attrs.Metadata["source"])
	require.Equal(t, "COLDLINE", attrs.StorageClass)
	require.NotZero(t, attrs.Generation)

	ranger, err := bucket.Object("objects/with-attrs.txt").NewRangeReader(ctx, 1, 3)
	require.NoError(t, err)
	rangeBody, err := io.ReadAll(ranger)
	require.NoError(t, err)
	require.NoError(t, ranger.Close())
	require.Equal(t, "bcd", string(rangeBody))

	secondWriter := bucket.Object("objects/with-attrs.txt").If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	_, err = secondWriter.Write([]byte("overwrite"))
	require.NoError(t, err)
	err = secondWriter.Close()
	requireGoogleAPICode(t, err, 412)

	attrsAfterFailedWrite, err := bucket.Object("objects/with-attrs.txt").Attrs(ctx)
	require.NoError(t, err)
	require.Equal(t, attrs.Generation, attrsAfterFailedWrite.Generation)

	iter := bucket.Objects(ctx, &storage.Query{Prefix: "page/"})
	pager := iterator.NewPager(iter, 2, "")
	var firstPage []*storage.ObjectAttrs
	nextToken, err := pager.NextPage(&firstPage)
	require.NoError(t, err)
	require.Len(t, firstPage, 2)
	require.NotEmpty(t, nextToken)

	iter = bucket.Objects(ctx, &storage.Query{Prefix: "page/"})
	pager = iterator.NewPager(iter, 2, nextToken)
	var secondPage []*storage.ObjectAttrs
	finalToken, err := pager.NextPage(&secondPage)
	require.NoError(t, err)
	require.Len(t, secondPage, 1)
	require.Empty(t, finalToken)
}

func requireGoogleAPICode(t *testing.T, err error, code int) {
	t.Helper()
	var apiErr *googleapi.Error
	require.True(t, errors.As(err, &apiErr), "got %T %[1]v", err)
	require.Equal(t, code, apiErr.Code)
}
