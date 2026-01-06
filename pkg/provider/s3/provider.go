package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// Provider implements provider.Provider for AWS S3 and S3-compatible storage.
type Provider struct {
	client  *s3.Client
	bucket  string
	maxKeys int
}

// Ensure Provider implements the interfaces.
var (
	_ provider.Provider          = (*Provider)(nil)
	_ provider.ObjectPutter      = (*Provider)(nil)
	_ provider.ObjectDeleter     = (*Provider)(nil)
	_ provider.MultipartUploader = (*Provider)(nil)
)

// New creates a new S3 provider with the given configuration.
//
// The provider uses AWS SDK v2's default credential chain unless explicit
// credentials are provided in the config.
func New(ctx context.Context, cfg Config) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	awsCfg, err := loadAWSConfig(ctx, cfg)
	if err != nil {
		return nil, &provider.ProviderError{
			Op:       "New",
			Provider: provider.ProviderS3,
			Bucket:   cfg.Bucket,
			Err:      err,
		}
	}

	// Build S3 client options
	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			if cfg.ForcePathStyle {
				o.UsePathStyle = true
			}
		},
	}

	// Custom endpoint for S3-compatible stores
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	maxKeys := cfg.MaxKeys
	if maxKeys <= 0 {
		maxKeys = DefaultMaxKeys
	}

	return &Provider{
		client:  client,
		bucket:  cfg.Bucket,
		maxKeys: maxKeys,
	}, nil
}

// loadAWSConfig builds the AWS configuration with appropriate credentials.
func loadAWSConfig(ctx context.Context, cfg Config) (aws.Config, error) {
	var opts []func(*config.LoadOptions) error

	// Only apply explicit region if user set one in config.
	// Let SDK resolve from env/profile first.
	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}

	// Set profile if specified
	if cfg.Profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(cfg.Profile))
	}

	// Use explicit credentials if provided
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		staticCreds := credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			"", // session token (empty for long-term credentials)
		)
		opts = append(opts, config.WithCredentialsProvider(staticCreds))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, err
	}

	// Apply region defaulting logic
	awsCfg.Region = resolveRegion(cfg.Region, cfg.Endpoint, awsCfg.Region)

	return awsCfg, nil
}

// List returns a page of objects with the given prefix.
func (p *Provider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	maxKeys := clampMaxKeys(opts.MaxKeys, p.maxKeys)

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(p.bucket),
		MaxKeys: aws.Int32(int32(maxKeys)),
	}

	if opts.Prefix != "" {
		input.Prefix = aws.String(opts.Prefix)
	}

	if opts.ContinuationToken != "" {
		input.ContinuationToken = aws.String(opts.ContinuationToken)
	}

	output, err := p.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, p.wrapError("List", "", err)
	}

	objects := make([]provider.ObjectSummary, 0, len(output.Contents))
	for _, obj := range output.Contents {
		objects = append(objects, provider.ObjectSummary{
			Key:          aws.ToString(obj.Key),
			Size:         aws.ToInt64(obj.Size),
			ETag:         cleanETag(aws.ToString(obj.ETag)),
			LastModified: aws.ToTime(obj.LastModified),
		})
	}

	result := &provider.ListResult{
		Objects:     objects,
		IsTruncated: aws.ToBool(output.IsTruncated),
	}

	if output.NextContinuationToken != nil {
		result.ContinuationToken = *output.NextContinuationToken
	}

	return result, nil
}

// Head returns metadata for a single object.
func (p *Provider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
	}

	output, err := p.client.HeadObject(ctx, input)
	if err != nil {
		return nil, p.wrapError("Head", key, err)
	}

	meta := &provider.ObjectMeta{
		ObjectSummary: provider.ObjectSummary{
			Key:          key,
			Size:         aws.ToInt64(output.ContentLength),
			ETag:         cleanETag(aws.ToString(output.ETag)),
			LastModified: aws.ToTime(output.LastModified),
		},
		ContentType: aws.ToString(output.ContentType),
		Metadata:    output.Metadata,
	}

	return meta, nil
}

// PutObject uploads an object.
//
// This is used for write-probe preflight and future transfer operations.
func (p *Provider) PutObject(ctx context.Context, key string, body io.Reader, contentLength int64) error {
	input := &s3.PutObjectInput{
		Bucket:        aws.String(p.bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: &contentLength,
	}

	_, err := p.client.PutObject(ctx, input)
	if err != nil {
		return p.wrapError("PutObject", key, err)
	}
	return nil
}

// DeleteObject deletes an object.
//
// This is used for write-probe preflight and future move operations.
func (p *Provider) DeleteObject(ctx context.Context, key string) error {
	_, err := p.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(p.bucket), Key: aws.String(key)})
	if err != nil {
		return p.wrapError("DeleteObject", key, err)
	}
	return nil
}

// CreateMultipartUpload starts a multipart upload.
//
// This is used for minimal-side-effect write probes.
func (p *Provider) CreateMultipartUpload(ctx context.Context, key string) (string, error) {
	out, err := p.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(p.bucket), Key: aws.String(key)})
	if err != nil {
		return "", p.wrapError("CreateMultipartUpload", key, err)
	}
	return aws.ToString(out.UploadId), nil
}

// AbortMultipartUpload aborts a multipart upload.
func (p *Provider) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	_, err := p.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(p.bucket), Key: aws.String(key), UploadId: aws.String(uploadID)})
	if err != nil {
		return p.wrapError("AbortMultipartUpload", key, err)
	}
	return nil
}

// Close releases any resources held by the provider.
// The S3 client doesn't require explicit cleanup, but this satisfies the interface.
func (p *Provider) Close() error {
	return nil
}

// PutObjectEmpty uploads a 0-byte object.
//
// This helper exists for probe operations.
func (p *Provider) PutObjectEmpty(ctx context.Context, key string) error {
	return p.PutObject(ctx, key, bytes.NewReader(nil), 0)
}

// wrapError converts S3 errors to provider errors with appropriate sentinel errors.
func (p *Provider) wrapError(op, key string, err error) error {
	wrapped := &provider.ProviderError{
		Op:       op,
		Provider: provider.ProviderS3,
		Bucket:   p.bucket,
		Key:      key,
		Err:      err,
	}

	// Check for specific S3 error types first
	var notFound *types.NotFound
	var noSuchKey *types.NoSuchKey
	var noSuchBucket *types.NoSuchBucket

	switch {
	case errors.As(err, &notFound), errors.As(err, &noSuchKey):
		wrapped.Err = provider.ErrNotFound
		return wrapped
	case errors.As(err, &noSuchBucket):
		wrapped.Err = provider.ErrBucketNotFound
		return wrapped
	}

	// Check smithy API errors for error codes
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		switch code {
		case "NoSuchKey", "NotFound":
			wrapped.Err = provider.ErrNotFound
		case "NoSuchBucket":
			wrapped.Err = provider.ErrBucketNotFound
		case "AccessDenied", "Forbidden":
			wrapped.Err = provider.ErrAccessDenied
		case "InvalidAccessKeyId", "SignatureDoesNotMatch":
			wrapped.Err = provider.ErrInvalidCredentials
		case "SlowDown", "Throttling", "RequestLimitExceeded":
			wrapped.Err = provider.ErrThrottled
		case "ServiceUnavailable", "InternalError":
			wrapped.Err = provider.ErrProviderUnavailable
		}
		return wrapped
	}

	// Fallback: check error message for common cases
	errMsg := err.Error()
	switch {
	case strings.Contains(errMsg, "NoSuchKey") || strings.Contains(errMsg, "NotFound") || strings.Contains(errMsg, "404"):
		wrapped.Err = provider.ErrNotFound
	case strings.Contains(errMsg, "NoSuchBucket"):
		wrapped.Err = provider.ErrBucketNotFound
	case strings.Contains(errMsg, "AccessDenied") || strings.Contains(errMsg, "Forbidden") || strings.Contains(errMsg, "403"):
		wrapped.Err = provider.ErrAccessDenied
	case strings.Contains(errMsg, "InvalidAccessKeyId") || strings.Contains(errMsg, "SignatureDoesNotMatch"):
		wrapped.Err = provider.ErrInvalidCredentials
	case strings.Contains(errMsg, "SlowDown") || strings.Contains(errMsg, "Throttling") || strings.Contains(errMsg, "429"):
		wrapped.Err = provider.ErrThrottled
	case strings.Contains(errMsg, "ServiceUnavailable") || strings.Contains(errMsg, "503"):
		wrapped.Err = provider.ErrProviderUnavailable
	}

	return wrapped
}

// cleanETag removes surrounding quotes from an ETag value.
// S3 returns ETags with quotes, e.g., "d41d8cd98f00b204e9800998ecf8427e".
func cleanETag(etag string) string {
	return strings.Trim(etag, "\"")
}

// clampMaxKeys applies defaults and limits to maxKeys values.
// If requested is <= 0, uses providerDefault. Result is clamped to MaxAllowedKeys.
func clampMaxKeys(requested, providerDefault int) int {
	if requested <= 0 {
		requested = providerDefault
	}
	if requested > MaxAllowedKeys {
		return MaxAllowedKeys
	}
	return requested
}

// resolveRegion determines the final region to use after SDK config loading.
//
// The sdkRegion parameter is the region after SDK loading, which already
// incorporates explicit cfgRegion (if set) or env/profile resolution.
//
// Priority (already applied by SDK before this function):
//  1. Explicit cfgRegion (passed to SDK via config.WithRegion)
//  2. Environment variables (AWS_REGION, AWS_DEFAULT_REGION)
//  3. Shared config/credentials profile
//
// This function only applies the fallback default:
//   - If sdkRegion is still empty AND no custom endpoint, default to us-east-1
//   - For S3-compatible stores (endpoint set), no defaulting occurs
func resolveRegion(cfgRegion, endpoint, sdkRegion string) string {
	// SDK already resolved region (from explicit config, env, or profile)
	if sdkRegion != "" {
		return sdkRegion
	}

	// Only default for AWS S3 (no custom endpoint)
	if endpoint == "" {
		return DefaultAWSRegion
	}

	// S3-compatible: no default, provider may not need region
	return ""
}
