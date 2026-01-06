//go:build cloudintegration

package transfer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/transfer"
	"github.com/3leaps/gonimbus/test/cloudtest"
)

func TestTransfer_Copy_SkipOnExists(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()

	srcBucket := cloudtest.CreateBucket(t, ctx)
	dstBucket := cloudtest.CreateBucket(t, ctx)

	cloudtest.PutObjects(t, ctx, srcBucket, []string{"data/a.txt", "data/b.txt"})
	cloudtest.PutObjects(t, ctx, dstBucket, []string{"data/b.txt"})

	src, err := s3.New(ctx, s3.Config{Bucket: srcBucket, Endpoint: cloudtest.Endpoint, Region: cloudtest.Region, AccessKeyID: cloudtest.TestAccessKeyID, SecretAccessKey: cloudtest.TestSecretAccessKey, ForcePathStyle: true})
	require.NoError(t, err)
	defer src.Close()

	dst, err := s3.New(ctx, s3.Config{Bucket: dstBucket, Endpoint: cloudtest.Endpoint, Region: cloudtest.Region, AccessKeyID: cloudtest.TestAccessKeyID, SecretAccessKey: cloudtest.TestSecretAccessKey, ForcePathStyle: true})
	require.NoError(t, err)
	defer dst.Close()

	m, err := match.New(match.Config{Includes: []string{"data/**"}})
	require.NoError(t, err)

	var buf bytes.Buffer
	w := output.NewJSONLWriter(&buf, "job-123", "s3")
	defer w.Close()

	tx := transfer.New(src, dst, m, w, "job-123", transfer.Config{Concurrency: 4, OnExists: "skip", Mode: "copy", Dedup: transfer.DedupConfig{Enabled: false, Strategy: "none"}})
	_, err = tx.Run(ctx)
	require.NoError(t, err)

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
