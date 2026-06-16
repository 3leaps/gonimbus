package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
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
			name: "valid anonymous config",
			config: Config{
				Bucket:    "my-bucket",
				Anonymous: true,
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
		{
			name: "anonymous with injected credentials rejected",
			config: Config{
				Bucket:              "my-bucket",
				Anonymous:           true,
				CredentialsProvider: testCredentialsProvider{},
			},
			wantErr: "cannot be combined with CredentialsProvider",
		},
		{
			name: "anonymous with static credentials rejected",
			config: Config{
				Bucket:          "my-bucket",
				Anonymous:       true,
				AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
				SecretAccessKey: "secret",
			},
			wantErr: "cannot be combined with AccessKeyID/SecretAccessKey",
		},
		{
			name: "anonymous with profile rejected",
			config: Config{
				Bucket:    "my-bucket",
				Anonymous: true,
				Profile:   "default",
			},
			wantErr: "cannot be combined with Profile",
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

type testCredentialsProvider struct {
	accessKeyID     string
	secretAccessKey string
	calls           *atomic.Int32
}

func (p testCredentialsProvider) Retrieve(context.Context) (aws.Credentials, error) {
	if p.calls != nil {
		p.calls.Add(1)
	}
	return aws.Credentials{
		AccessKeyID:     p.accessKeyID,
		SecretAccessKey: p.secretAccessKey,
		Source:          "gonimbus-test",
	}, nil
}

func TestConfigStringRedactsCredentials(t *testing.T) {
	const (
		accessKey = "AKIASTRING00000001"
		secretKey = "secret-value-that-must-not-appear"
	)
	cfg := Config{
		Bucket:              "test-bucket",
		Region:              "us-east-1",
		AccessKeyID:         accessKey,
		SecretAccessKey:     secretKey,
		CredentialsProvider: testCredentialsProvider{},
	}

	for _, formatted := range []string{
		fmt.Sprintf("%+v", cfg),
		fmt.Sprintf("%#v", cfg),
		cfg.String(),
	} {
		require.NotContains(t, formatted, accessKey)
		require.NotContains(t, formatted, secretKey)
		require.Contains(t, formatted, "<redacted>")
	}
}

func TestNewValidationErrorDoesNotLeakExplicitCredentials(t *testing.T) {
	const (
		accessKey = "AKIAERROR000000001"
		secretKey = "secret-value-that-must-not-appear"
	)

	_, err := New(context.Background(), Config{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
	})
	require.Error(t, err)
	require.NotContains(t, err.Error(), accessKey)
	require.NotContains(t, err.Error(), secretKey)
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

func TestIsCredentialsRefreshFailed(t *testing.T) {
	assert.True(t, provider.IsCredentialsRefreshFailed(provider.ErrCredentialsRefreshFailed))
	assert.True(t, provider.IsCredentialsRefreshFailed(&provider.ProviderError{Err: provider.ErrCredentialsRefreshFailed}))
	assert.False(t, provider.IsCredentialsRefreshFailed(provider.ErrInvalidCredentials))
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

func TestConditionETag(t *testing.T) {
	assert.Equal(t, `"abc123"`, conditionETag("abc123"))
	assert.Equal(t, `"abc123"`, conditionETag(`"abc123"`))
	assert.Equal(t, `W/"abc123"`, conditionETag(`W/"abc123"`))
}

func TestWrapConditionalPutErrorMapsIfMatchPrecondition(t *testing.T) {
	p := &Provider{bucket: "bucket"}
	etag := "abc123"

	err := p.wrapConditionalPutError("key", provider.PutPrecondition{IfMatchETag: &etag}, &mockAPIError{code: "PreconditionFailed", message: "stale etag"})
	require.Error(t, err)
	require.True(t, provider.IsPreconditionFailed(err), "got %v", err)
	require.False(t, provider.IsAlreadyExists(err))
}

func TestWrapConditionalPutErrorPreservesIfAbsentAlreadyExists(t *testing.T) {
	p := &Provider{bucket: "bucket"}

	err := p.wrapConditionalPutError("key", provider.PutPrecondition{IfAbsent: true}, &mockAPIError{code: "PreconditionFailed", message: "exists"})
	require.Error(t, err)
	require.True(t, provider.IsAlreadyExists(err), "got %v", err)
	require.False(t, provider.IsPreconditionFailed(err))
}

func TestProviderType_String(t *testing.T) {
	assert.Equal(t, "s3", provider.ProviderS3.String())
	assert.Equal(t, "gcs", provider.ProviderGCS.String())
}

func TestProvider_InterfaceCompliance(t *testing.T) {
	// Verify that *Provider implements provider.Provider
	var _ provider.Provider = (*Provider)(nil)
	var _ provider.VersionedGetter = (*Provider)(nil)
	var _ provider.ConditionalPutter = (*Provider)(nil)
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
		StorageClass: "STANDARD_IA",
	}

	assert.Equal(t, "path/to/file.txt", obj.Key)
	assert.Equal(t, int64(1024), obj.Size)
	assert.Equal(t, "abc123", obj.ETag)
	assert.Equal(t, now, obj.LastModified)
	assert.Equal(t, "STANDARD_IA", obj.StorageClass)
}

func TestProviderListCapturesStorageClass(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "2", r.URL.Query().Get("list-type"))
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Prefix></Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>archive/object.txt</Key>
    <LastModified>2026-05-25T12:00:00.000Z</LastModified>
    <ETag>&quot;etag-1&quot;</ETag>
    <Size>10</Size>
    <StorageClass>GLACIER</StorageClass>
  </Contents>
  <Contents>
    <Key>standard/object.txt</Key>
    <LastModified>2026-05-25T12:00:00.000Z</LastModified>
    <ETag>&quot;etag-2&quot;</ETag>
    <Size>20</Size>
  </Contents>
</ListBucketResult>`))
	}))
	defer server.Close()

	p, err := New(ctx, Config{
		Bucket:          "test-bucket",
		Endpoint:        server.URL,
		Region:          "us-east-1",
		AccessKeyID:     "AKIATEST0000000001",
		SecretAccessKey: "test-secret",
		ForcePathStyle:  true,
	})
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	result, err := p.List(ctx, provider.ListOptions{})
	require.NoError(t, err)
	require.Len(t, result.Objects, 2)
	require.Equal(t, "GLACIER", result.Objects[0].StorageClass)
	require.Empty(t, result.Objects[1].StorageClass)

	delimited, err := p.ListWithDelimiter(ctx, provider.ListWithDelimiterOptions{Delimiter: "/"})
	require.NoError(t, err)
	require.Len(t, delimited.Objects, 2)
	require.Equal(t, "GLACIER", delimited.Objects[0].StorageClass)
	require.Empty(t, delimited.Objects[1].StorageClass)
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

func TestParseRestoreHeader(t *testing.T) {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	future := `ongoing-request="false", expiry-date="Wed, 03 Jun 2026 00:00:00 GMT"`
	past := `ongoing-request="false", expiry-date="Wed, 01 Apr 2026 00:00:00 GMT"`

	require.Equal(t, "ongoing", parseRestoreState(`ongoing-request="true"`, now))
	require.Equal(t, "completed", parseRestoreState(future, now))
	require.Equal(t, "expired", parseRestoreState(past, now))
	require.Equal(t, "unknown", parseRestoreState(`provider-specific-garbage`, now))
	require.Empty(t, parseRestoreState("", now))

	expiry := parseRestoreExpiry(future)
	require.NotNil(t, expiry)
	require.Equal(t, time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC), *expiry)
	require.Nil(t, parseRestoreExpiry(`ongoing-request="true"`))
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

func TestWrapError_CredentialRefreshFailure(t *testing.T) {
	p := &Provider{bucket: "test-bucket"}

	err := p.wrapError("List", "", fmt.Errorf("failed to refresh cached credentials, %w", errors.New("invalid_grant")))

	assert.True(t, errors.Is(err, provider.ErrCredentialsRefreshFailed))
	assert.True(t, provider.IsCredentialsRefreshFailed(err))
	assert.False(t, errors.Is(err, provider.ErrInvalidCredentials))
}

func TestWrapError_CredentialRefreshFailurePreservesInnerCause(t *testing.T) {
	p := &Provider{bucket: "test-bucket"}

	inner := fmt.Errorf("failed to refresh cached credentials, %w", provider.ErrInvalidCredentials)
	err := p.wrapError("List", "", inner)

	assert.True(t, errors.Is(err, provider.ErrCredentialsRefreshFailed))
	assert.True(t, errors.Is(err, provider.ErrInvalidCredentials))
}

func TestWrapError_APITextCannotSpoofCredentialRefreshFailure(t *testing.T) {
	p := &Provider{bucket: "test-bucket"}
	apiErr := &mockAPIError{
		code:    "InternalError",
		message: "failed to refresh cached credentials, invalid_grant",
	}

	err := p.wrapError("List", "", apiErr)

	assert.False(t, errors.Is(err, provider.ErrCredentialsRefreshFailed))
	assert.True(t, errors.Is(err, provider.ErrProviderUnavailable))
}

func TestWrapConditionalPutError_IfAbsentPreconditionFailure(t *testing.T) {
	p := &Provider{bucket: "test-bucket"}

	err := p.wrapConditionalPutError("key", provider.PutPrecondition{IfAbsent: true}, &mockAPIError{code: "PreconditionFailed", message: "object exists"})

	var provErr *provider.ProviderError
	require.True(t, errors.As(err, &provErr))
	assert.Equal(t, "PutObjectConditional", provErr.Op)
	assert.Equal(t, "key", provErr.Key)
	assert.True(t, errors.Is(err, provider.ErrAlreadyExists))
}

func TestWrapConditionalPutError_NonPreconditionFailure(t *testing.T) {
	p := &Provider{bucket: "test-bucket"}

	err := p.wrapConditionalPutError("key", provider.PutPrecondition{IfAbsent: true}, &mockAPIError{code: "AccessDenied", message: "denied"})

	assert.True(t, errors.Is(err, provider.ErrAccessDenied))
	assert.False(t, errors.Is(err, provider.ErrAlreadyExists))
}

// Integration tests are in provider_cloudintegration_test.go
// Run with: make test-cloud (requires moto server)
func TestProvider_Integration(t *testing.T) {
	t.Skip("see provider_cloudintegration_test.go - run with: make test-cloud")
}

func TestNew_ValidationError(t *testing.T) {
	ctx := context.Background()

	// Test that invalid config returns error before AWS config load
	_, err := New(ctx, Config{})
	require.Error(t, err)

	var configErr *ConfigError
	assert.True(t, errors.As(err, &configErr))
}

func TestNew_DoesNotLogExplicitCredentials(t *testing.T) {
	const (
		accessKey = "AKIADOESNOTLOG00001"
		secretKey = "secret-value-that-must-not-appear-in-logs"
	)

	var logs bytes.Buffer
	originalWriter := log.Writer()
	originalFlags := log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(originalWriter)
		log.SetFlags(originalFlags)
	})

	_, err := New(context.Background(), Config{
		Bucket:          "test-bucket",
		Region:          "us-east-1",
		Endpoint:        "http://127.0.0.1:1",
		ForcePathStyle:  true,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
	})
	require.NoError(t, err)

	assert.NotContains(t, logs.String(), accessKey)
	assert.NotContains(t, logs.String(), secretKey)
}

type observedS3Request struct {
	host          string
	authorization string
}

func TestNew_MultiCredentialCoexistenceUsesIndependentEndpointAndCredentials(t *testing.T) {
	newServer := func(ch chan<- observedS3Request) *httptest.Server {
		t.Helper()
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ch <- observedS3Request{
				host:          r.Host,
				authorization: r.Header.Get("Authorization"),
			}
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test-bucket</Name><IsTruncated>false</IsTruncated></ListBucketResult>`))
		}))
	}

	reqs1 := make(chan observedS3Request, 2)
	reqs2 := make(chan observedS3Request, 2)
	server1 := newServer(reqs1)
	defer server1.Close()
	server2 := newServer(reqs2)
	defer server2.Close()

	provider1, err := New(context.Background(), Config{
		Bucket:          "test-bucket",
		Region:          "us-east-1",
		Endpoint:        server1.URL,
		ForcePathStyle:  true,
		AccessKeyID:     "AKIAFIRST000000001",
		SecretAccessKey: "first-secret",
	})
	require.NoError(t, err)
	defer func() {
		_ = provider1.Close()
	}()

	provider2, err := New(context.Background(), Config{
		Bucket:          "test-bucket",
		Region:          "us-west-2",
		Endpoint:        server2.URL,
		ForcePathStyle:  true,
		AccessKeyID:     "AKIASECOND00000002",
		SecretAccessKey: "second-secret",
	})
	require.NoError(t, err)
	defer func() {
		_ = provider2.Close()
	}()

	_, err = provider1.List(context.Background(), provider.ListOptions{})
	require.NoError(t, err)
	_, err = provider2.List(context.Background(), provider.ListOptions{})
	require.NoError(t, err)

	got1 := receiveObservedRequest(t, reqs1, "provider1 endpoint")
	got2 := receiveObservedRequest(t, reqs2, "provider2 endpoint")

	assert.Contains(t, got1.authorization, "Credential=AKIAFIRST000000001/")
	assert.Contains(t, got1.authorization, "/us-east-1/s3/aws4_request")
	assert.Contains(t, got1.host, strings.TrimPrefix(server1.URL, "http://"))

	assert.Contains(t, got2.authorization, "Credential=AKIASECOND00000002/")
	assert.Contains(t, got2.authorization, "/us-west-2/s3/aws4_request")
	assert.Contains(t, got2.host, strings.TrimPrefix(server2.URL, "http://"))
}

func TestAnonymousReadRequestsAreUnsigned(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAAMBIENT0000001")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret-that-must-not-sign")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	reqs := make(chan *http.Request, 5)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs <- r.Clone(context.Background())
		w.Header().Set("ETag", `"read-etag"`)
		w.Header().Set("Last-Modified", "Fri, 12 Jun 2026 21:00:00 GMT")

		switch {
		case r.Method == "GET" && r.URL.Query().Get("list-type") == "2":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test-bucket</Name><IsTruncated>false</IsTruncated><Contents><Key>object.txt</Key><LastModified>2026-06-12T21:00:00.000Z</LastModified><ETag>&quot;read-etag&quot;</ETag><Size>5</Size></Contents></ListBucketResult>`))
		case r.Method == "HEAD":
			w.Header().Set("Content-Length", "5")
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
		case r.Method == "GET" && r.Header.Get("Range") != "":
			require.Equal(t, "bytes=1-3", r.Header.Get("Range"))
			w.Header().Set("Content-Length", "3")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("ell"))
		case r.Method == "GET":
			w.Header().Set("Content-Length", "5")
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("hello"))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	p, err := New(context.Background(), Config{
		Bucket:         "test-bucket",
		Region:         "us-east-1",
		Endpoint:       server.URL,
		ForcePathStyle: true,
		Anonymous:      true,
	})
	require.NoError(t, err)

	list, err := p.List(context.Background(), provider.ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Objects, 1)

	head, err := p.Head(context.Background(), "object.txt")
	require.NoError(t, err)
	require.Equal(t, int64(5), head.Size)

	body, size, err := p.GetObject(context.Background(), "object.txt")
	require.NoError(t, err)
	require.Equal(t, int64(5), size)
	gotBody, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, "hello", string(gotBody))

	versionedBody, versionedMeta, err := p.GetObjectVersioned(context.Background(), "object.txt")
	require.NoError(t, err)
	require.Equal(t, int64(5), versionedMeta.Size)
	require.NoError(t, versionedBody.Close())

	rangeBody, rangeSize, err := p.GetRange(context.Background(), "object.txt", 1, 3)
	require.NoError(t, err)
	require.Equal(t, int64(3), rangeSize)
	gotRange, err := io.ReadAll(rangeBody)
	require.NoError(t, err)
	require.NoError(t, rangeBody.Close())
	require.Equal(t, "ell", string(gotRange))

	for i := 0; i < 5; i++ {
		req := receiveRequest(t, reqs, fmt.Sprintf("anonymous read request %d", i+1))
		require.Empty(t, req.Header.Get("Authorization"), "anonymous request was signed: %s", req.Header.Get("Authorization"))
	}
}

func TestInjectedCredentialsProviderWinsOverStaticKeys(t *testing.T) {
	var calls atomic.Int32
	reqs := make(chan observedS3Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs <- observedS3Request{
			host:          r.Host,
			authorization: r.Header.Get("Authorization"),
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test-bucket</Name><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	defer server.Close()

	p, err := New(context.Background(), Config{
		Bucket:              "test-bucket",
		Region:              "us-east-1",
		Endpoint:            server.URL,
		ForcePathStyle:      true,
		AccessKeyID:         "AKIASTATIC0000001",
		SecretAccessKey:     "static-secret",
		CredentialsProvider: testCredentialsProvider{accessKeyID: "AKIAINJECTED0001", secretAccessKey: "injected-secret", calls: &calls},
	})
	require.NoError(t, err)

	_, err = p.List(context.Background(), provider.ListOptions{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, calls.Load(), int32(1))

	req := receiveObservedRequest(t, reqs, "injected credentials")
	require.Contains(t, req.authorization, "Credential=AKIAINJECTED0001/")
	require.NotContains(t, req.authorization, "AKIASTATIC0000001")
}

func TestAnonymousWriteMethodsFailClosedBeforeRequest(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	p, err := New(context.Background(), Config{
		Bucket:         "test-bucket",
		Region:         "us-east-1",
		Endpoint:       server.URL,
		ForcePathStyle: true,
		Anonymous:      true,
	})
	require.NoError(t, err)

	etag := "etag"
	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "PutObject",
			call: func() error {
				return p.PutObject(context.Background(), "object.txt", strings.NewReader("body"), int64(len("body")))
			},
		},
		{
			name: "PutObjectWithOptions",
			call: func() error {
				return p.PutObjectWithOptions(context.Background(), "object.txt", strings.NewReader("body"), int64(len("body")), provider.PutOptions{ContentType: "text/plain"})
			},
		},
		{
			name: "PutObjectConditional",
			call: func() error {
				_, err := p.PutObjectConditional(context.Background(), "object.txt", strings.NewReader("body"), int64(len("body")), provider.PutPrecondition{IfAbsent: true})
				return err
			},
		},
		{
			name: "PutObjectConditionalWithOptions",
			call: func() error {
				_, err := p.PutObjectConditionalWithOptions(context.Background(), "object.txt", strings.NewReader("body"), int64(len("body")), provider.PutPrecondition{IfMatchETag: &etag}, provider.PutOptions{ContentType: "text/plain"})
				return err
			},
		},
		{
			name: "PutObjectEmpty",
			call: func() error {
				return p.PutObjectEmpty(context.Background(), "empty.txt")
			},
		},
		{
			name: "DeleteObject",
			call: func() error {
				return p.DeleteObject(context.Background(), "object.txt")
			},
		},
		{
			name: "CreateMultipartUpload",
			call: func() error {
				_, err := p.CreateMultipartUpload(context.Background(), "object.txt")
				return err
			},
		},
		{
			name: "CreateMultipartUploadWithOptions",
			call: func() error {
				_, err := p.CreateMultipartUploadWithOptions(context.Background(), "object.txt", provider.PutOptions{ContentType: "text/plain"})
				return err
			},
		},
		{
			name: "UploadPart",
			call: func() error {
				_, err := p.UploadPart(context.Background(), "object.txt", "upload-id", 1, strings.NewReader("part"), int64(len("part")))
				return err
			},
		},
		{
			name: "CompleteMultipartUpload",
			call: func() error {
				_, err := p.CompleteMultipartUpload(context.Background(), "object.txt", "upload-id", []provider.PartETag{{PartNumber: 1, ETag: "part-etag"}})
				return err
			},
		},
		{
			name: "CompleteMultipartUploadConditional",
			call: func() error {
				_, err := p.CompleteMultipartUploadConditional(context.Background(), "object.txt", "upload-id", []provider.PartETag{{PartNumber: 1, ETag: "part-etag"}}, provider.PutPrecondition{IfAbsent: true})
				return err
			},
		},
		{
			name: "AbortMultipartUpload",
			call: func() error {
				return p.AbortMultipartUpload(context.Background(), "object.txt", "upload-id")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			require.Error(t, err)
			require.True(t, provider.IsAnonymousReadOnly(err), "got %v", err)
			require.True(t, provider.IsAccessDenied(err), "got %v", err)
		})
	}
	require.Equal(t, int32(0), requests.Load())
}

func TestCreateMultipartUploadWithOptionsSendsMetadataHeaders(t *testing.T) {
	reqs := make(chan *http.Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs <- r.Clone(context.Background())
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>test-bucket</Bucket><Key>object.txt</Key><UploadId>upload-123</UploadId></InitiateMultipartUploadResult>`))
	}))
	defer server.Close()

	p, err := New(context.Background(), Config{
		Bucket:          "test-bucket",
		Region:          "us-east-1",
		Endpoint:        server.URL,
		ForcePathStyle:  true,
		AccessKeyID:     "AKIAFIRST000000001",
		SecretAccessKey: "first-secret",
	})
	require.NoError(t, err)

	uploadID, err := p.CreateMultipartUploadWithOptions(context.Background(), "object.txt", provider.PutOptions{
		UserMetadata: map[string]string{"owner": "team-a"},
		ContentType:  "text/plain",
		StorageClass: "STANDARD_IA",
	})
	require.NoError(t, err)
	require.Equal(t, "upload-123", uploadID)
	req := receiveRequest(t, reqs, "multipart initiate")
	require.Equal(t, "POST", req.Method)
	require.Contains(t, req.URL.RawQuery, "uploads")
	require.Equal(t, "team-a", req.Header.Get("X-Amz-Meta-Owner"))
	require.Equal(t, "text/plain", req.Header.Get("Content-Type"))
	require.Equal(t, "STANDARD_IA", req.Header.Get("X-Amz-Storage-Class"))
}

func TestMultipartUploadPartAndConditionalComplete(t *testing.T) {
	reqs := make(chan *http.Request, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs <- r.Clone(context.Background())
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case r.Method == "PUT" && strings.Contains(r.URL.RawQuery, "partNumber=1"):
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Equal(t, "part-body", string(body))
			w.Header().Set("ETag", `"part-etag"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == "POST" && strings.Contains(r.URL.RawQuery, "uploadId=upload-123"):
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>test-bucket</Bucket><Key>object.txt</Key><ETag>"complete-etag"</ETag></CompleteMultipartUploadResult>`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	p, err := New(context.Background(), Config{
		Bucket:          "test-bucket",
		Region:          "us-east-1",
		Endpoint:        server.URL,
		ForcePathStyle:  true,
		AccessKeyID:     "AKIAFIRST000000001",
		SecretAccessKey: "first-secret",
	})
	require.NoError(t, err)

	part, err := p.UploadPart(context.Background(), "object.txt", "upload-123", 1, strings.NewReader("part-body"), int64(len("part-body")))
	require.NoError(t, err)
	require.Equal(t, provider.PartETag{PartNumber: 1, ETag: "part-etag"}, part)
	partReq := receiveRequest(t, reqs, "multipart part")
	require.Equal(t, "PUT", partReq.Method)
	require.Contains(t, partReq.URL.RawQuery, "partNumber=1")
	require.Contains(t, partReq.URL.RawQuery, "uploadId=upload-123")

	result, err := p.CompleteMultipartUploadConditional(context.Background(), "object.txt", "upload-123", []provider.PartETag{part}, provider.PutPrecondition{IfAbsent: true})
	require.NoError(t, err)
	require.Equal(t, "complete-etag", result.ETag)
	completeReq := receiveRequest(t, reqs, "multipart complete")
	require.Equal(t, "POST", completeReq.Method)
	require.Contains(t, completeReq.URL.RawQuery, "uploadId=upload-123")
	require.Equal(t, "*", completeReq.Header.Get("If-None-Match"))
}

func receiveObservedRequest(t *testing.T, ch <-chan observedS3Request, label string) observedS3Request {
	t.Helper()

	select {
	case req := <-ch:
		return req
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for request on %s; provider endpoint isolation may have regressed", label)
		return observedS3Request{}
	}
}

func receiveRequest(t *testing.T, ch <-chan *http.Request, label string) *http.Request {
	t.Helper()

	select {
	case req := <-ch:
		return req
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for request on %s", label)
		return nil
	}
}

func TestEndpointConfiguredURLSuppression(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "aws-config")
	require.NoError(t, os.WriteFile(configPath, []byte("[default]\nregion = us-east-1\nendpoint_url = https://evil-shared.example.com\n"), 0o600))

	baseEnv := []string{
		"GONIMBUS_S3_ENDPOINT_PROBE_HELPER=1",
		"AWS_EC2_METADATA_DISABLED=true",
		"AWS_SDK_LOAD_CONFIG=1",
		"AWS_CONFIG_FILE=" + configPath,
		"AWS_ENDPOINT_URL=https://evil-global.example.com",
		"AWS_ENDPOINT_URL_S3=https://evil-s3.example.com",
	}

	unmitigated := runEndpointProbeHelper(t, baseEnv)
	require.Contains(t, unmitigated, "evil", "empty cfg.Endpoint should document ambient endpoint redirection")

	mitigated := runEndpointProbeHelper(t, append(baseEnv, "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=true"))
	assert.NotContains(t, mitigated, "evil")
	assert.Empty(t, mitigated)
}

func TestEndpointProbeHelper(t *testing.T) {
	if os.Getenv("GONIMBUS_S3_ENDPOINT_PROBE_HELPER") != "1" {
		return
	}

	p, err := New(context.Background(), Config{
		Bucket:          "test-bucket",
		Region:          "us-east-1",
		AccessKeyID:     "AKIAHELPER00000001",
		SecretAccessKey: "helper-secret",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer func() {
		_ = p.Close()
	}()

	_, _ = fmt.Fprint(os.Stdout, reflectedS3BaseEndpoint(p.client))
	os.Exit(0)
}

func runEndpointProbeHelper(t *testing.T, env []string) string {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=TestEndpointProbeHelper")
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	return strings.TrimSpace(string(out))
}

func reflectedS3BaseEndpoint(client *awss3.Client) string {
	clientValue := reflect.ValueOf(client).Elem()
	options := clientValue.FieldByName("options")
	if !options.IsValid() {
		return ""
	}
	baseEndpoint := options.FieldByName("BaseEndpoint")
	if !baseEndpoint.IsValid() || baseEndpoint.IsNil() {
		return ""
	}
	return baseEndpoint.Elem().String()
}

func TestApplyS3ClientOptions_S3CompatibleEndpoint(t *testing.T) {
	opts := &awss3.Options{}

	applyS3ClientOptions(opts, Config{
		Endpoint:       "https://s3.wasabisys.com",
		ForcePathStyle: true,
	})

	assert.True(t, opts.UsePathStyle)
	require.NotNil(t, opts.BaseEndpoint)
	assert.Equal(t, "https://s3.wasabisys.com", *opts.BaseEndpoint)
	assert.True(t, opts.DisableLogOutputChecksumValidationSkipped)
}

func TestApplyS3ClientOptions_NativeAWS(t *testing.T) {
	opts := &awss3.Options{}

	applyS3ClientOptions(opts, Config{})

	assert.False(t, opts.UsePathStyle)
	assert.Nil(t, opts.BaseEndpoint)
	assert.False(t, opts.DisableLogOutputChecksumValidationSkipped)
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

// newSigningObserverServer returns a fake S3 endpoint that records the
// Authorization header of each request so a test can prove which credentials
// signed it.
func newSigningObserverServer(t *testing.T, ch chan<- observedS3Request) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ch <- observedS3Request{host: r.Host, authorization: r.Header.Get("Authorization")}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test-bucket</Name><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
}

// TestProfileCredentialsStillSign is the GON-045 AC#5 regression for the shared
// config Profile auth path: a Config with only Profile set must resolve the
// profile's keys from the shared credentials file and sign the request with
// them. Hermetic — a temp credentials file, no ambient env/IMDS.
func TestProfileCredentialsStillSign(t *testing.T) {
	dir := t.TempDir()
	credsFile := filepath.Join(dir, "credentials")
	require.NoError(t, os.WriteFile(credsFile, []byte(
		"[gon-test-profile]\n"+
			"aws_access_key_id = AKIAPROFILE0000001\n"+
			"aws_secret_access_key = profile-secret\n"), 0o600))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credsFile)
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config")) // absent → no ambient profile
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	// Neutralize ambient env credentials so the profile path is exercised.
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	reqs := make(chan observedS3Request, 2)
	server := newSigningObserverServer(t, reqs)
	defer server.Close()

	p, err := New(context.Background(), Config{
		Bucket:         "test-bucket",
		Region:         "us-east-1",
		Endpoint:       server.URL,
		ForcePathStyle: true,
		Profile:        "gon-test-profile",
	})
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	_, err = p.List(context.Background(), provider.ListOptions{})
	require.NoError(t, err)

	got := receiveObservedRequest(t, reqs, "profile endpoint")
	assert.Contains(t, got.authorization, "Credential=AKIAPROFILE0000001/",
		"profile credentials must sign the request")
	assert.Contains(t, got.authorization, "/us-east-1/s3/aws4_request")
}

// TestDefaultChainEnvCredentialsStillSign is the GON-045 AC#5 regression for the
// SDK default credential chain (environment leg): a Config with no explicit
// credentials, Profile, or CredentialsProvider must fall through to the default
// chain and sign with the ambient env credentials. IMDS and shared files are
// disabled so the env leg is the only resolvable source.
func TestDefaultChainEnvCredentialsStillSign(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials")) // absent
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))                  // absent
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAENVCHAIN000001")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-chain-secret")

	reqs := make(chan observedS3Request, 2)
	server := newSigningObserverServer(t, reqs)
	defer server.Close()

	p, err := New(context.Background(), Config{
		Bucket:         "test-bucket",
		Region:         "us-east-1",
		Endpoint:       server.URL,
		ForcePathStyle: true,
		// no AccessKeyID/SecretAccessKey, Profile, or CredentialsProvider → default chain
	})
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	_, err = p.List(context.Background(), provider.ListOptions{})
	require.NoError(t, err)

	got := receiveObservedRequest(t, reqs, "default-chain endpoint")
	assert.Contains(t, got.authorization, "Credential=AKIAENVCHAIN000001/",
		"default-chain env credentials must sign the request")
	assert.Contains(t, got.authorization, "/us-east-1/s3/aws4_request")
}
