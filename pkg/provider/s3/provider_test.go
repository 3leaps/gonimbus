package s3

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// mockAPIError implements smithy.APIError for testing error code mapping.
type mockAPIError struct {
	code    string
	message string
}

func (e *mockAPIError) Error() string                 { return fmt.Sprintf("%s: %s", e.code, e.message) }
func (e *mockAPIError) ErrorCode() string             { return e.code }
func (e *mockAPIError) ErrorMessage() string          { return e.message }
func (e *mockAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }

var _ smithy.APIError = (*mockAPIError)(nil)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name:    "empty bucket",
			config:  Config{},
			wantErr: "bucket name is required",
		},
		{
			name: "valid minimal config",
			config: Config{
				Bucket: "my-bucket",
			},
			wantErr: "",
		},
		{
			name: "valid config with region",
			config: Config{
				Bucket: "my-bucket",
				Region: "us-east-1",
			},
			wantErr: "",
		},
		{
			name: "valid config with explicit creds",
			config: Config{
				Bucket:          "my-bucket",
				AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
				SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			wantErr: "",
		},
		{
			name: "access key without secret",
			config: Config{
				Bucket:      "my-bucket",
				AccessKeyID: "AKIAIOSFODNN7EXAMPLE",
			},
			wantErr: "both access key ID and secret access key must be provided together",
		},
		{
			name: "secret without access key",
			config: Config{
				Bucket:          "my-bucket",
				SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			wantErr: "both access key ID and secret access key must be provided together",
		},
		{
			name: "valid S3-compatible config",
			config: Config{
				Bucket:          "my-bucket",
				Endpoint:        "https://s3.wasabisys.com",
				ForcePathStyle:  true,
				AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
				SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestConfigError_Error(t *testing.T) {
	err := &ConfigError{
		Field:   "Bucket",
		Message: "bucket name is required",
	}
	assert.Equal(t, "s3 config: Bucket: bucket name is required", err.Error())
}

func TestProviderError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *provider.ProviderError
		expected string
	}{
		{
			name: "with key",
			err: &provider.ProviderError{
				Op:       "Head",
				Provider: provider.ProviderS3,
				Bucket:   "my-bucket",
				Key:      "path/to/file.txt",
				Err:      provider.ErrNotFound,
			},
			expected: "s3 Head: my-bucket/path/to/file.txt: object not found",
		},
		{
			name: "without key",
			err: &provider.ProviderError{
				Op:       "List",
				Provider: provider.ProviderS3,
				Bucket:   "my-bucket",
				Err:      provider.ErrAccessDenied,
			},
			expected: "s3 List: my-bucket: access denied",
		},
		{
			name: "without bucket",
			err: &provider.ProviderError{
				Op:       "New",
				Provider: provider.ProviderS3,
				Err:      errors.New("failed to load config"),
			},
			expected: "s3 New: failed to load config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.err.Error())
		})
	}
}

func TestProviderError_Unwrap(t *testing.T) {
	underlying := provider.ErrNotFound
	err := &provider.ProviderError{
		Op:       "Head",
		Provider: provider.ProviderS3,
		Bucket:   "my-bucket",
		Key:      "file.txt",
		Err:      underlying,
	}

	// Test errors.Is
	assert.True(t, errors.Is(err, provider.ErrNotFound))
	assert.False(t, errors.Is(err, provider.ErrAccessDenied))

	// Test Unwrap
	assert.Equal(t, underlying, err.Unwrap())
}

func TestIsNotFound(t *testing.T) {
	assert.True(t, provider.IsNotFound(provider.ErrNotFound))
	assert.True(t, provider.IsNotFound(&provider.ProviderError{Err: provider.ErrNotFound}))
	assert.False(t, provider.IsNotFound(provider.ErrAccessDenied))
	assert.False(t, provider.IsNotFound(errors.New("some error")))
}

func TestIsAccessDenied(t *testing.T) {
	assert.True(t, provider.IsAccessDenied(provider.ErrAccessDenied))
	assert.True(t, provider.IsAccessDenied(&provider.ProviderError{Err: provider.ErrAccessDenied}))
	assert.False(t, provider.IsAccessDenied(provider.ErrNotFound))
}

func TestIsBucketNotFound(t *testing.T) {
	assert.True(t, provider.IsBucketNotFound(provider.ErrBucketNotFound))
	assert.True(t, provider.IsBucketNotFound(&provider.ProviderError{Err: provider.ErrBucketNotFound}))
	assert.False(t, provider.IsBucketNotFound(provider.ErrNotFound))
}

func TestIsInvalidCredentials(t *testing.T) {
	assert.True(t, provider.IsInvalidCredentials(provider.ErrInvalidCredentials))
	assert.True(t, provider.IsInvalidCredentials(&provider.ProviderError{Err: provider.ErrInvalidCredentials}))
	assert.False(t, provider.IsInvalidCredentials(provider.ErrNotFound))
}

func TestIsProviderUnavailable(t *testing.T) {
	assert.True(t, provider.IsProviderUnavailable(provider.ErrProviderUnavailable))
	assert.True(t, provider.IsProviderUnavailable(&provider.ProviderError{Err: provider.ErrProviderUnavailable}))
	assert.False(t, provider.IsProviderUnavailable(provider.ErrNotFound))
}

func TestIsThrottled(t *testing.T) {
	assert.True(t, provider.IsThrottled(provider.ErrThrottled))
	assert.True(t, provider.IsThrottled(&provider.ProviderError{Err: provider.ErrThrottled}))
	assert.False(t, provider.IsThrottled(provider.ErrNotFound))
	assert.False(t, provider.IsThrottled(provider.ErrProviderUnavailable))
}

func TestCleanETag(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`"d41d8cd98f00b204e9800998ecf8427e"`, "d41d8cd98f00b204e9800998ecf8427e"},
		{"d41d8cd98f00b204e9800998ecf8427e", "d41d8cd98f00b204e9800998ecf8427e"},
		{`""`, ""},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, cleanETag(tt.input))
		})
	}
}

func TestProviderType_String(t *testing.T) {
	assert.Equal(t, "s3", provider.ProviderS3.String())
	assert.Equal(t, "gcs", provider.ProviderGCS.String())
}

func TestProvider_InterfaceCompliance(t *testing.T) {
	// Verify that *Provider implements provider.Provider
	var _ provider.Provider = (*Provider)(nil)
}

func TestListResult_Empty(t *testing.T) {
	result := &provider.ListResult{
		Objects:     []provider.ObjectSummary{},
		IsTruncated: false,
	}
	assert.Empty(t, result.Objects)
	assert.False(t, result.IsTruncated)
	assert.Empty(t, result.ContinuationToken)
}

func TestObjectSummary_Fields(t *testing.T) {
	now := time.Now()
	obj := provider.ObjectSummary{
		Key:          "path/to/file.txt",
		Size:         1024,
		ETag:         "abc123",
		LastModified: now,
	}

	assert.Equal(t, "path/to/file.txt", obj.Key)
	assert.Equal(t, int64(1024), obj.Size)
	assert.Equal(t, "abc123", obj.ETag)
	assert.Equal(t, now, obj.LastModified)
}

func TestObjectMeta_Embedding(t *testing.T) {
	now := time.Now()
	meta := provider.ObjectMeta{
		ObjectSummary: provider.ObjectSummary{
			Key:          "path/to/file.txt",
			Size:         2048,
			ETag:         "def456",
			LastModified: now,
		},
		ContentType: "application/json",
		Metadata: map[string]string{
			"author": "test",
		},
	}

	// Access embedded fields directly
	assert.Equal(t, "path/to/file.txt", meta.Key)
	assert.Equal(t, int64(2048), meta.Size)
	assert.Equal(t, "application/json", meta.ContentType)
	assert.Equal(t, "test", meta.Metadata["author"])
}

func TestListOptions_Defaults(t *testing.T) {
	opts := provider.ListOptions{}
	assert.Empty(t, opts.Prefix)
	assert.Empty(t, opts.ContinuationToken)
	assert.Zero(t, opts.MaxKeys)
}

func TestWrapError_NotFound(t *testing.T) {
	p := &Provider{bucket: "test-bucket"}

	// Test NoSuchKey error type
	noSuchKey := &types.NoSuchKey{}
	err := p.wrapError("Head", "missing.txt", noSuchKey)

	var provErr *provider.ProviderError
	require.True(t, errors.As(err, &provErr))
	assert.Equal(t, "Head", provErr.Op)
	assert.Equal(t, provider.ProviderS3, provErr.Provider)
	assert.Equal(t, "test-bucket", provErr.Bucket)
	assert.Equal(t, "missing.txt", provErr.Key)
	assert.True(t, errors.Is(err, provider.ErrNotFound))
}

func TestWrapError_BucketNotFound(t *testing.T) {
	p := &Provider{bucket: "missing-bucket"}

	noSuchBucket := &types.NoSuchBucket{}
	err := p.wrapError("List", "", noSuchBucket)

	assert.True(t, errors.Is(err, provider.ErrBucketNotFound))
}

func TestWrapError_FromMessage(t *testing.T) {
	p := &Provider{bucket: "test-bucket"}

	tests := []struct {
		name     string
		errMsg   string
		expected error
	}{
		{"access denied", "AccessDenied: Access Denied", provider.ErrAccessDenied},
		{"forbidden", "Forbidden: you don't have access", provider.ErrAccessDenied},
		{"403", "operation error: https response error StatusCode: 403", provider.ErrAccessDenied},
		{"no such key", "NoSuchKey: The specified key does not exist", provider.ErrNotFound},
		{"404", "operation error: https response error StatusCode: 404", provider.ErrNotFound},
		{"no such bucket", "NoSuchBucket: bucket does not exist", provider.ErrBucketNotFound},
		{"invalid access key", "InvalidAccessKeyId: key not found", provider.ErrInvalidCredentials},
		{"signature mismatch", "SignatureDoesNotMatch: invalid signature", provider.ErrInvalidCredentials},
		{"slow down", "SlowDown: Please reduce your request rate", provider.ErrThrottled},
		{"throttling", "Throttling: Rate exceeded", provider.ErrThrottled},
		{"429", "operation error: https response error StatusCode: 429", provider.ErrThrottled},
		{"service unavailable", "ServiceUnavailable: try again", provider.ErrProviderUnavailable},
		{"503", "operation error: https response error StatusCode: 503", provider.ErrProviderUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := p.wrapError("Test", "key", errors.New(tt.errMsg))
			assert.True(t, errors.Is(err, tt.expected))
		})
	}
}

func TestWrapError_APIError(t *testing.T) {
	p := &Provider{bucket: "test-bucket"}

	tests := []struct {
		name     string
		code     string
		expected error
	}{
		{"NoSuchKey", "NoSuchKey", provider.ErrNotFound},
		{"NotFound", "NotFound", provider.ErrNotFound},
		{"NoSuchBucket", "NoSuchBucket", provider.ErrBucketNotFound},
		{"AccessDenied", "AccessDenied", provider.ErrAccessDenied},
		{"Forbidden", "Forbidden", provider.ErrAccessDenied},
		{"InvalidAccessKeyId", "InvalidAccessKeyId", provider.ErrInvalidCredentials},
		{"SignatureDoesNotMatch", "SignatureDoesNotMatch", provider.ErrInvalidCredentials},
		{"SlowDown", "SlowDown", provider.ErrThrottled},
		{"Throttling", "Throttling", provider.ErrThrottled},
		{"RequestLimitExceeded", "RequestLimitExceeded", provider.ErrThrottled},
		{"ServiceUnavailable", "ServiceUnavailable", provider.ErrProviderUnavailable},
		{"InternalError", "InternalError", provider.ErrProviderUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apiErr := &mockAPIError{code: tt.code, message: "test message"}
			err := p.wrapError("Test", "key", apiErr)
			assert.True(t, errors.Is(err, tt.expected), "expected %v for code %s", tt.expected, tt.code)
		})
	}
}

// Integration test placeholder - requires real S3 or LocalStack
func TestProvider_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// This test would run against LocalStack or a real S3 bucket
	// Environment variables would configure the endpoint and credentials
	t.Skip("integration tests require LocalStack or real S3 - run manually")
}

func TestNew_ValidationError(t *testing.T) {
	ctx := context.Background()

	// Test that invalid config returns error before AWS config load
	_, err := New(ctx, Config{})
	require.Error(t, err)

	var configErr *ConfigError
	assert.True(t, errors.As(err, &configErr))
}

func TestDefaultMaxKeys(t *testing.T) {
	assert.Equal(t, 1000, DefaultMaxKeys)
}

func TestMaxAllowedKeys(t *testing.T) {
	assert.Equal(t, 1000, MaxAllowedKeys)
}

func TestDefaultAWSRegion(t *testing.T) {
	assert.Equal(t, "us-east-1", DefaultAWSRegion)
}

func TestMaxKeysClamping(t *testing.T) {
	// Test that clampMaxKeys properly limits values
	tests := []struct {
		name     string
		input    int
		pMaxKeys int
		expected int
	}{
		{"zero uses provider default", 0, DefaultMaxKeys, DefaultMaxKeys},
		{"negative uses provider default", -1, DefaultMaxKeys, DefaultMaxKeys},
		{"within limit unchanged", 500, DefaultMaxKeys, 500},
		{"at limit unchanged", 1000, DefaultMaxKeys, 1000},
		{"over limit clamped", 2000, DefaultMaxKeys, MaxAllowedKeys},
		{"way over limit clamped", 10000, DefaultMaxKeys, MaxAllowedKeys},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := clampMaxKeys(tt.input, tt.pMaxKeys)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestResolveRegion(t *testing.T) {
	// Note: sdkRegion is the region AFTER SDK loading, which already incorporates
	// explicit cfgRegion if it was set. The cfgRegion param is only used for
	// documentation/debugging - the actual value comes through sdkRegion.
	tests := []struct {
		name      string
		cfgRegion string // what user set in Config (for context)
		endpoint  string
		sdkRegion string // region after SDK loaded (already includes cfgRegion if set)
		expected  string
	}{
		{
			name:      "SDK resolved region from env/profile",
			cfgRegion: "",
			endpoint:  "",
			sdkRegion: "eu-west-1",
			expected:  "eu-west-1",
		},
		{
			name:      "explicit config region (SDK already applied it)",
			cfgRegion: "us-west-2",
			endpoint:  "",
			sdkRegion: "us-west-2", // SDK applied cfgRegion
			expected:  "us-west-2",
		},
		{
			name:      "AWS S3 defaults to us-east-1 when SDK has no region",
			cfgRegion: "",
			endpoint:  "",
			sdkRegion: "",
			expected:  "us-east-1",
		},
		{
			name:      "S3-compatible with endpoint does not default",
			cfgRegion: "",
			endpoint:  "https://s3.wasabisys.com",
			sdkRegion: "",
			expected:  "",
		},
		{
			name:      "S3-compatible respects SDK-resolved region",
			cfgRegion: "",
			endpoint:  "https://s3.wasabisys.com",
			sdkRegion: "us-east-2",
			expected:  "us-east-2",
		},
		{
			name:      "S3-compatible with explicit config region",
			cfgRegion: "eu-central-1",
			endpoint:  "https://minio.local:9000",
			sdkRegion: "eu-central-1", // SDK applied cfgRegion
			expected:  "eu-central-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := resolveRegion(tt.cfgRegion, tt.endpoint, tt.sdkRegion)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Benchmark for cleanETag since it's called frequently
func BenchmarkCleanETag(b *testing.B) {
	etag := `"d41d8cd98f00b204e9800998ecf8427e"`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cleanETag(etag)
	}
}
