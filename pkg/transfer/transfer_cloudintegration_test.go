//go:build cloudintegration

package transfer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	providers3 "github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/transfer"
	"github.com/3leaps/gonimbus/test/cloudtest"
)

func TestTransfer_Copy_SkipOnExists(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()

	srcBucket := cloudtest.CreateBucket(t, ctx)
	dstBucket := cloudtest.CreateBucket(t, ctx)

	cloudtest.PutObjects(t, ctx, srcBucket, []string{"foo/bar/a.txt", "foo/bar/b.txt"})
	cloudtest.PutObjects(t, ctx, dstBucket, []string{"foo/b.txt"})

	src, err := providers3.New(ctx, providers3.Config{Bucket: srcBucket, Endpoint: cloudtest.Endpoint, Region: cloudtest.Region, AccessKeyID: cloudtest.TestAccessKeyID, SecretAccessKey: cloudtest.TestSecretAccessKey, ForcePathStyle: true})
	require.NoError(t, err)
	defer src.Close()

	dst, err := providers3.New(ctx, providers3.Config{Bucket: dstBucket, Endpoint: cloudtest.Endpoint, Region: cloudtest.Region, AccessKeyID: cloudtest.TestAccessKeyID, SecretAccessKey: cloudtest.TestSecretAccessKey, ForcePathStyle: true})
	require.NoError(t, err)
	defer dst.Close()

	m, err := match.New(match.Config{Includes: []string{"foo/bar/**"}})
	require.NoError(t, err)

	var buf bytes.Buffer
	w := output.NewJSONLWriter(&buf, "job-123", "s3")
	defer w.Close()

	tx := transfer.New(src, dst, m, w, "job-123", transfer.Config{Concurrency: 4, OnExists: "skip", Mode: "copy", PathTemplate: "{dir[0]}/{filename}", Sharding: transfer.ShardingConfig{Enabled: true, Depth: 1, ListConcurrency: 4, Delimiter: "/"}, Dedup: transfer.DedupConfig{Enabled: false, Strategy: "none"}})
	_, err = tx.Run(ctx)
	require.NoError(t, err)

	// Verify objects landed under template mapping.
	client := cloudtest.ClientT(t)
	listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(dstBucket)})
	require.NoError(t, err)
	keys := make([]string, 0, len(listOut.Contents))
	for _, o := range listOut.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}
	assert.Contains(t, keys, "foo/a.txt")
	assert.Contains(t, keys, "foo/b.txt")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	var transfers, skips int
	for _, line := range lines {
		var rec output.Record
		require.NoError(t, json.Unmarshal(line, &rec))
		switch rec.Type {
		case output.TypeTransfer:
			transfers++
		case output.TypeSkip:
			skips++
		}
	}
	assert.Equal(t, 1, transfers)
	assert.Equal(t, 1, skips)
}

// staleSizeProvider wraps a provider and mutates List results to simulate
// stale index/list metadata.
//
// It forces a wrong size for a specific key, while GetObject still returns the
// real object and size.
// This triggers transfer's validate=size check deterministically.
type staleSizeProvider struct {
	base      provider.Provider
	baseGet   provider.ObjectGetter
	key       string
	staleSize int64
}

func (p *staleSizeProvider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	res, err := p.base.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	for i := range res.Objects {
		if res.Objects[i].Key == p.key {
			res.Objects[i].Size = p.staleSize
		}
	}
	return res, nil
}

func (p *staleSizeProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	return p.base.Head(ctx, key)
}

func (p *staleSizeProvider) Close() error {
	return p.base.Close()
}

func (p *staleSizeProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	return p.baseGet.GetObject(ctx, key)
}

// TestTransfer_ValidateSize_StaleMismatch verifies validate=size catches
// stale list/index metadata by forcing a size mismatch between List and GetObject.
func TestTransfer_ValidateSize_StaleMismatch(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()

	srcBucket := cloudtest.CreateBucket(t, ctx)
	dstBucket := cloudtest.CreateBucket(t, ctx)

	key := "data/test.txt"
	content := []byte("real content size")
	cloudtest.PutObject(t, ctx, srcBucket, key, content)

	src, err := providers3.New(ctx, providers3.Config{
		Bucket:          srcBucket,
		Endpoint:        cloudtest.Endpoint,
		Region:          cloudtest.Region,
		AccessKeyID:     cloudtest.TestAccessKeyID,
		SecretAccessKey: cloudtest.TestSecretAccessKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer src.Close()

	dst, err := providers3.New(ctx, providers3.Config{
		Bucket:          dstBucket,
		Endpoint:        cloudtest.Endpoint,
		Region:          cloudtest.Region,
		AccessKeyID:     cloudtest.TestAccessKeyID,
		SecretAccessKey: cloudtest.TestSecretAccessKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer dst.Close()

	getter, ok := interface{}(src).(provider.ObjectGetter)
	require.True(t, ok)

	// Force a wrong size in List results.
	wrapped := &staleSizeProvider{base: src, baseGet: getter, key: key, staleSize: int64(len(content)) + 7}

	m, err := match.New(match.Config{Includes: []string{"data/**"}})
	require.NoError(t, err)

	var buf bytes.Buffer
	w := output.NewJSONLWriter(&buf, "job-stale", "s3")
	defer w.Close()

	tx := transfer.New(wrapped, dst, m, w, "job-stale", transfer.Config{Concurrency: 1, OnExists: "overwrite", Mode: "copy"})
	summary, err := tx.Run(ctx)
	require.NoError(t, err)
	require.NotNil(t, summary)
	assert.Equal(t, int64(1), summary.Errors)

	// Destination should not have been written.
	_, err = dst.Head(ctx, key)
	assert.True(t, provider.IsNotFound(err))

	// Ensure we emitted an error record mapped to NOT_FOUND.
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	var errRecs []output.Record
	for _, line := range lines {
		var rec output.Record
		require.NoError(t, json.Unmarshal(line, &rec))
		if rec.Type == output.TypeError {
			errRecs = append(errRecs, rec)
		}
	}
	require.Len(t, errRecs, 1)
	var payload output.ErrorRecord
	require.NoError(t, json.Unmarshal(errRecs[0].Data, &payload))
	assert.Equal(t, output.ErrCodeNotFound, payload.Code)
	assert.Contains(t, payload.Message, "source size mismatch")
}
