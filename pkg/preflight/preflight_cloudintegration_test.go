//go:build cloudintegration

package preflight_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/preflight"
	providers3 "github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/test/cloudtest"
)

func TestWriteProbe_MultipartAbort_Allowed(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()

	bucket := cloudtest.CreateBucket(t, ctx)
	p, err := providers3.New(ctx, providers3.Config{
		Bucket:          bucket,
		Endpoint:        cloudtest.Endpoint,
		Region:          cloudtest.Region,
		AccessKeyID:     cloudtest.TestAccessKeyID,
		SecretAccessKey: cloudtest.TestSecretAccessKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer p.Close()

	rec, err := preflight.WriteProbe(ctx, p, preflight.Spec{
		Mode:          preflight.ModeWriteProbe,
		ProbeStrategy: preflight.ProbeMultipartAbort,
		ProbePrefix:   "_gonimbus/probe/",
	})
	require.NoError(t, err)
	require.NotNil(t, rec)

	// Expect at least one successful target.write result.
	var sawWrite bool
	for _, r := range rec.Results {
		if r.Capability == preflight.CapTargetWrite {
			sawWrite = true
			assert.True(t, r.Allowed)
			assert.Contains(t, r.Method, "CreateMultipartUpload")
		}
	}
	assert.True(t, sawWrite)
}

func TestWriteProbe_MultipartAbort_Denied(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()

	bucket := cloudtest.CreateBucket(t, ctx)

	policy := fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyMultipartCreate",
      "Effect": "Deny",
      "Principal": "*",
      "Action": ["s3:CreateMultipartUpload"],
      "Resource": ["arn:aws:s3:::%s/_gonimbus/probe/*"]
    }
  ]
}`, bucket)
	cloudtest.PutBucketPolicy(t, ctx, bucket, policy)

	p, err := providers3.New(ctx, providers3.Config{
		Bucket:          bucket,
		Endpoint:        cloudtest.Endpoint,
		Region:          cloudtest.Region,
		AccessKeyID:     cloudtest.TestAccessKeyID,
		SecretAccessKey: cloudtest.TestSecretAccessKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer p.Close()

	rec, err := preflight.WriteProbe(ctx, p, preflight.Spec{
		Mode:          preflight.ModeWriteProbe,
		ProbeStrategy: preflight.ProbeMultipartAbort,
		ProbePrefix:   "_gonimbus/probe/",
	})
	require.Error(t, err)
	require.NotNil(t, rec)

	var sawDenied bool
	for _, r := range rec.Results {
		if r.Capability == preflight.CapTargetWrite {
			sawDenied = true
			assert.False(t, r.Allowed)
			assert.Equal(t, "ACCESS_DENIED", r.ErrorCode)
		}
	}
	assert.True(t, sawDenied)
}

func TestWriteProbe_PutDelete_Allowed_CleansUp(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()

	bucket := cloudtest.CreateBucket(t, ctx)
	p, err := providers3.New(ctx, providers3.Config{
		Bucket:          bucket,
		Endpoint:        cloudtest.Endpoint,
		Region:          cloudtest.Region,
		AccessKeyID:     cloudtest.TestAccessKeyID,
		SecretAccessKey: cloudtest.TestSecretAccessKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer p.Close()

	rec, err := preflight.WriteProbe(ctx, p, preflight.Spec{
		Mode:          preflight.ModeWriteProbe,
		ProbeStrategy: preflight.ProbePutDelete,
		ProbePrefix:   "_gonimbus/probe/",
	})
	require.NoError(t, err)
	require.NotNil(t, rec)

	// Verify probe prefix is empty after put-delete.
	client := cloudtest.ClientT(t)
	out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("_gonimbus/probe/"),
	})
	require.NoError(t, err)
	assert.Len(t, out.Contents, 0)
}

func TestWriteProbe_PutDenied(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()

	bucket := cloudtest.CreateBucket(t, ctx)

	policy := fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyPut",
      "Effect": "Deny",
      "Principal": "*",
      "Action": ["s3:PutObject"],
      "Resource": ["arn:aws:s3:::%s/_gonimbus/probe/*"]
    }
  ]
}`, bucket)
	cloudtest.PutBucketPolicy(t, ctx, bucket, policy)

	p, err := providers3.New(ctx, providers3.Config{
		Bucket:          bucket,
		Endpoint:        cloudtest.Endpoint,
		Region:          cloudtest.Region,
		AccessKeyID:     cloudtest.TestAccessKeyID,
		SecretAccessKey: cloudtest.TestSecretAccessKey,
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer p.Close()

	rec, err := preflight.WriteProbe(ctx, p, preflight.Spec{
		Mode:          preflight.ModeWriteProbe,
		ProbeStrategy: preflight.ProbePutDelete,
		ProbePrefix:   "_gonimbus/probe/",
	})
	require.Error(t, err)
	require.NotNil(t, rec)

	var sawDenied bool
	for _, r := range rec.Results {
		if r.Capability == preflight.CapTargetWrite {
			sawDenied = true
			assert.False(t, r.Allowed)
			assert.Equal(t, "ACCESS_DENIED", r.ErrorCode)
		}
	}
	assert.True(t, sawDenied)
}
