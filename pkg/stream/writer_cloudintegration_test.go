//go:build cloudintegration

package stream_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/stream"
	"github.com/3leaps/gonimbus/test/cloudtest"
	"github.com/stretchr/testify/require"
)

func TestStreamWriter_CloudToLocal_RoundTrip(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()

	bucket := cloudtest.CreateBucket(t, ctx)
	content := []byte("stream content test")
	cloudtest.PutObject(t, ctx, bucket, "a.txt", content)

	prov, err := s3.New(ctx, s3.Config{
		Bucket:          bucket,
		Region:          cloudtest.Region,
		Endpoint:        cloudtest.Endpoint,
		ForcePathStyle:  true,
		AccessKeyID:     cloudtest.TestAccessKeyID,
		SecretAccessKey: cloudtest.TestSecretAccessKey,
	})
	require.NoError(t, err)
	defer func() { _ = prov.Close() }()

	body, size, err := prov.GetObject(ctx, "a.txt")
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), size)

	var out bytes.Buffer
	sw := stream.NewWriter(&out, "job-1", "s3")

	now := time.Now().UTC()
	open := &stream.Open{StreamID: "s1", URI: "s3://" + bucket + "/a.txt", Size: &size, LastModified: &now}
	require.NoError(t, sw.WriteOpen(ctx, open))
	require.NoError(t, sw.WriteChunk(ctx, &stream.Chunk{StreamID: "s1", Seq: 0, NBytes: size}, body))
	require.NoError(t, sw.WriteClose(ctx, &stream.Close{StreamID: "s1", Status: "success", Chunks: 1, Bytes: size}))

	d := stream.NewDecoder(&out)

	// open
	ev, err := d.Next()
	require.NoError(t, err)
	var gotOpen stream.Open
	require.NoError(t, json.Unmarshal(ev.Record.Data, &gotOpen))
	require.Equal(t, open.URI, gotOpen.URI)

	// chunk
	ev, err = d.Next()
	require.NoError(t, err)
	b, err := io.ReadAll(ev.Chunk.Body)
	require.NoError(t, err)
	require.NoError(t, ev.Chunk.Body.Close())
	require.Equal(t, content, b)

	// close
	ev, err = d.Next()
	require.NoError(t, err)
	_ = ev
}

func TestStreamWriter_CloudToCloud_ViaDecode(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()

	srcBucket := cloudtest.CreateBucket(t, ctx)
	dstBucket := cloudtest.CreateBucket(t, ctx)

	content := []byte("copy via stream helper")
	cloudtest.PutObject(t, ctx, srcBucket, "x.txt", content)

	src, err := s3.New(ctx, s3.Config{
		Bucket:          srcBucket,
		Region:          cloudtest.Region,
		Endpoint:        cloudtest.Endpoint,
		ForcePathStyle:  true,
		AccessKeyID:     cloudtest.TestAccessKeyID,
		SecretAccessKey: cloudtest.TestSecretAccessKey,
	})
	require.NoError(t, err)
	defer func() { _ = src.Close() }()

	dst, err := s3.New(ctx, s3.Config{
		Bucket:          dstBucket,
		Region:          cloudtest.Region,
		Endpoint:        cloudtest.Endpoint,
		ForcePathStyle:  true,
		AccessKeyID:     cloudtest.TestAccessKeyID,
		SecretAccessKey: cloudtest.TestSecretAccessKey,
	})
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()

	body, size, err := src.GetObject(ctx, "x.txt")
	require.NoError(t, err)

	var streamBuf bytes.Buffer
	sw := stream.NewWriter(&streamBuf, "job-1", "s3")
	require.NoError(t, sw.WriteOpen(ctx, &stream.Open{StreamID: "s1", URI: "s3://" + srcBucket + "/x.txt", Size: &size}))
	require.NoError(t, sw.WriteChunk(ctx, &stream.Chunk{StreamID: "s1", Seq: 0, NBytes: size}, body))
	require.NoError(t, sw.WriteClose(ctx, &stream.Close{StreamID: "s1", Status: "success", Chunks: 1, Bytes: size}))

	d := stream.NewDecoder(&streamBuf)
	_, err = d.Next() // open
	require.NoError(t, err)
	ev, err := d.Next() // chunk
	require.NoError(t, err)
	decoded, err := io.ReadAll(ev.Chunk.Body)
	require.NoError(t, err)
	require.NoError(t, ev.Chunk.Body.Close())

	require.NoError(t, dst.PutObject(ctx, "x.txt", bytes.NewReader(decoded), int64(len(decoded))))

	meta, err := dst.Head(ctx, "x.txt")
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), meta.Size)
}
