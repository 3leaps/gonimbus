//go:build cloudintegration

package s3_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
	providers3 "github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/test/cloudtest"
)

func TestProvider_GetRange(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()

	bucket := cloudtest.CreateBucket(t, ctx)
	key := "r.txt"
	content := []byte("hello range world")
	cloudtest.PutObject(t, ctx, bucket, key, content)

	p, err := providers3.New(ctx, providers3.Config{
		Bucket:          bucket,
		Endpoint:        cloudtest.Endpoint,
		Region:          cloudtest.Region,
		AccessKeyID:     cloudtest.TestAccessKeyID,
		SecretAccessKey: cloudtest.TestSecretAccessKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	r, ok := interface{}(p).(provider.ObjectRanger)
	require.True(t, ok)

	body, n, err := r.GetRange(ctx, key, 6, 10) // "range"
	require.NoError(t, err)
	defer func() { _ = body.Close() }()

	b, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, int64(5), n)
	require.Equal(t, []byte("range"), b)
}
