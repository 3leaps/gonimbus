//go:build cloudintegration

package s3_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/test/cloudtest"
)

func TestProvider_New_CloudIntegration(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()

	t.Run("creates provider with static credentials", func(t *testing.T) {
		bucket := cloudtest.CreateBucket(t, ctx)

		p, err := s3.New(ctx, s3.Config{
			Bucket:          bucket,
			Endpoint:        cloudtest.Endpoint,
			Region:          cloudtest.Region,
			AccessKeyID:     cloudtest.TestAccessKeyID,
			SecretAccessKey: cloudtest.TestSecretAccessKey,
			ForcePathStyle:  true,
		})
		require.NoError(t, err)
		defer p.Close()

		// Verify provider can list (empty bucket)
		result, err := p.List(ctx, provider.ListOptions{})
		require.NoError(t, err)
		assert.Empty(t, result.Objects)
	})

	t.Run("returns error for non-existent bucket", func(t *testing.T) {
		p, err := s3.New(ctx, s3.Config{
			Bucket:          "nonexistent-bucket-12345",
			Endpoint:        cloudtest.Endpoint,
			Region:          cloudtest.Region,
			AccessKeyID:     cloudtest.TestAccessKeyID,
			SecretAccessKey: cloudtest.TestSecretAccessKey,
			ForcePathStyle:  true,
		})
		require.NoError(t, err) // New succeeds; error happens on List
		defer p.Close()

		_, err = p.List(ctx, provider.ListOptions{})
		require.Error(t, err)

		var provErr *provider.ProviderError
		require.ErrorAs(t, err, &provErr)
		assert.ErrorIs(t, provErr.Err, provider.ErrBucketNotFound)
	})
}

func TestProvider_List_CloudIntegration(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()

	t.Run("lists objects in bucket", func(t *testing.T) {
		bucket := cloudtest.CreateBucket(t, ctx)
		cloudtest.PutObjects(t, ctx, bucket, []string{
			"data/file1.txt",
			"data/file2.txt",
			"other/file3.txt",
		})

		p, err := s3.New(ctx, s3.Config{
			Bucket:          bucket,
			Endpoint:        cloudtest.Endpoint,
			Region:          cloudtest.Region,
			AccessKeyID:     cloudtest.TestAccessKeyID,
			SecretAccessKey: cloudtest.TestSecretAccessKey,
			ForcePathStyle:  true,
		})
		require.NoError(t, err)
		defer p.Close()

		result, err := p.List(ctx, provider.ListOptions{})
		require.NoError(t, err)
		assert.Len(t, result.Objects, 3)
	})

	t.Run("filters by prefix", func(t *testing.T) {
		bucket := cloudtest.CreateBucket(t, ctx)
		cloudtest.PutObjects(t, ctx, bucket, []string{
			"data/file1.txt",
			"data/file2.txt",
			"other/file3.txt",
		})

		p, err := s3.New(ctx, s3.Config{
			Bucket:          bucket,
			Endpoint:        cloudtest.Endpoint,
			Region:          cloudtest.Region,
			AccessKeyID:     cloudtest.TestAccessKeyID,
			SecretAccessKey: cloudtest.TestSecretAccessKey,
			ForcePathStyle:  true,
		})
		require.NoError(t, err)
		defer p.Close()

		result, err := p.List(ctx, provider.ListOptions{Prefix: "data/"})
		require.NoError(t, err)
		assert.Len(t, result.Objects, 2)

		for _, obj := range result.Objects {
			assert.Contains(t, obj.Key, "data/")
		}
	})

	t.Run("respects MaxKeys limit", func(t *testing.T) {
		bucket := cloudtest.CreateBucket(t, ctx)
		cloudtest.PutObjects(t, ctx, bucket, []string{
			"file1.txt",
			"file2.txt",
			"file3.txt",
			"file4.txt",
			"file5.txt",
		})

		p, err := s3.New(ctx, s3.Config{
			Bucket:          bucket,
			Endpoint:        cloudtest.Endpoint,
			Region:          cloudtest.Region,
			AccessKeyID:     cloudtest.TestAccessKeyID,
			SecretAccessKey: cloudtest.TestSecretAccessKey,
			ForcePathStyle:  true,
		})
		require.NoError(t, err)
		defer p.Close()

		result, err := p.List(ctx, provider.ListOptions{MaxKeys: 2})
		require.NoError(t, err)
		assert.Len(t, result.Objects, 2)
		assert.True(t, result.IsTruncated)
		assert.NotEmpty(t, result.ContinuationToken)
	})

	t.Run("paginates with continuation token", func(t *testing.T) {
		bucket := cloudtest.CreateBucket(t, ctx)
		cloudtest.PutObjects(t, ctx, bucket, []string{
			"file1.txt",
			"file2.txt",
			"file3.txt",
		})

		p, err := s3.New(ctx, s3.Config{
			Bucket:          bucket,
			Endpoint:        cloudtest.Endpoint,
			Region:          cloudtest.Region,
			AccessKeyID:     cloudtest.TestAccessKeyID,
			SecretAccessKey: cloudtest.TestSecretAccessKey,
			ForcePathStyle:  true,
		})
		require.NoError(t, err)
		defer p.Close()

		// First page
		result1, err := p.List(ctx, provider.ListOptions{MaxKeys: 2})
		require.NoError(t, err)
		assert.Len(t, result1.Objects, 2)
		assert.True(t, result1.IsTruncated)

		// Second page
		result2, err := p.List(ctx, provider.ListOptions{
			MaxKeys:           2,
			ContinuationToken: result1.ContinuationToken,
		})
		require.NoError(t, err)
		assert.Len(t, result2.Objects, 1)
		assert.False(t, result2.IsTruncated)
	})
}

func TestProvider_Head_CloudIntegration(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()

	t.Run("returns object metadata", func(t *testing.T) {
		bucket := cloudtest.CreateBucket(t, ctx)
		content := []byte("hello world")
		cloudtest.PutObject(t, ctx, bucket, "test.txt", content)

		p, err := s3.New(ctx, s3.Config{
			Bucket:          bucket,
			Endpoint:        cloudtest.Endpoint,
			Region:          cloudtest.Region,
			AccessKeyID:     cloudtest.TestAccessKeyID,
			SecretAccessKey: cloudtest.TestSecretAccessKey,
			ForcePathStyle:  true,
		})
		require.NoError(t, err)
		defer p.Close()

		meta, err := p.Head(ctx, "test.txt")
		require.NoError(t, err)

		assert.Equal(t, "test.txt", meta.Key)
		assert.Equal(t, int64(len(content)), meta.Size)
		assert.NotEmpty(t, meta.ETag)
		assert.False(t, meta.LastModified.IsZero())
	})

	t.Run("returns ErrNotFound for non-existent key", func(t *testing.T) {
		bucket := cloudtest.CreateBucket(t, ctx)

		p, err := s3.New(ctx, s3.Config{
			Bucket:          bucket,
			Endpoint:        cloudtest.Endpoint,
			Region:          cloudtest.Region,
			AccessKeyID:     cloudtest.TestAccessKeyID,
			SecretAccessKey: cloudtest.TestSecretAccessKey,
			ForcePathStyle:  true,
		})
		require.NoError(t, err)
		defer p.Close()

		_, err = p.Head(ctx, "nonexistent.txt")
		require.Error(t, err)

		var provErr *provider.ProviderError
		require.ErrorAs(t, err, &provErr)
		assert.ErrorIs(t, provErr.Err, provider.ErrNotFound)
	})
}

func TestProvider_Close_CloudIntegration(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()

	t.Run("close is idempotent", func(t *testing.T) {
		bucket := cloudtest.CreateBucket(t, ctx)

		p, err := s3.New(ctx, s3.Config{
			Bucket:          bucket,
			Endpoint:        cloudtest.Endpoint,
			Region:          cloudtest.Region,
			AccessKeyID:     cloudtest.TestAccessKeyID,
			SecretAccessKey: cloudtest.TestSecretAccessKey,
			ForcePathStyle:  true,
		})
		require.NoError(t, err)

		// Close multiple times should not error
		require.NoError(t, p.Close())
		require.NoError(t, p.Close())
	})
}
