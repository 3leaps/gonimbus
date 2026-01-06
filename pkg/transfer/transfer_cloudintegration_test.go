//go:build cloudintegration

package transfer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
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
