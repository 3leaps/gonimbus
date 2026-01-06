// Package cloudtest provides helpers for cloud integration tests using moto.
//
// These helpers enable testing against a local S3-compatible endpoint without
// requiring real AWS credentials. Tests using this package should be tagged
// with //go:build cloudintegration.
//
// Usage:
//
//	func TestMyS3Function(t *testing.T) {
//	    cloudtest.SkipIfUnavailable(t)
//	    bucket := cloudtest.CreateBucket(t, ctx)
//	    cloudtest.PutObject(t, ctx, bucket, "key", []byte("content"))
//	    // ... test code ...
//	}
package cloudtest

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	// DefaultEndpoint is the default moto server endpoint.
	// Port 5555 avoids conflict with macOS AirTunes on 5000.
	DefaultEndpoint = "http://localhost:5555"

	// DefaultRegion is the default AWS region for tests.
	DefaultRegion = "us-east-1"

	// TestAccessKeyID is the access key used for moto (accepts any).
	TestAccessKeyID = "testing"

	// TestSecretAccessKey is the secret key used for moto (accepts any).
	TestSecretAccessKey = "testing"
)

var (
	// Endpoint is the moto server endpoint, configurable via MOTO_ENDPOINT env var.
	Endpoint = getEnvOrDefault("MOTO_ENDPOINT", DefaultEndpoint)

	// Region is the AWS region for tests, configurable via MOTO_REGION env var.
	Region = getEnvOrDefault("MOTO_REGION", DefaultRegion)

	// client caches the S3 client for reuse across tests.
	client     *s3.Client
	clientOnce sync.Once
	clientErr  error
)

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// Available checks if the moto server is reachable.
func Available() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, Endpoint+"/moto-api/", nil)
	if err != nil {
		return false
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()

	return resp.StatusCode == http.StatusOK
}

// SkipIfUnavailable skips the test if moto server is not available.
func SkipIfUnavailable(t *testing.T) {
	t.Helper()
	if !Available() {
		t.Skipf("moto server not available at %s (start with: make moto-start)", Endpoint)
	}
}

// Reset clears all moto state. Call this between tests for isolation.
func Reset(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, Endpoint+"/moto-api/reset", nil)
	if err != nil {
		return fmt.Errorf("create reset request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("reset request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("reset returned status %d", resp.StatusCode)
	}

	return nil
}

// ResetT resets moto state, failing the test on error.
func ResetT(t *testing.T, ctx context.Context) {
	t.Helper()
	if err := Reset(ctx); err != nil {
		t.Fatalf("failed to reset moto: %v", err)
	}
}

// Client returns a shared S3 client configured for moto.
func Client() (*s3.Client, error) {
	clientOnce.Do(func() {
		ctx := context.Background()
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(Region),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				TestAccessKeyID,
				TestSecretAccessKey,
				"",
			)),
		)
		if err != nil {
			clientErr = fmt.Errorf("load config: %w", err)
			return
		}

		client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(Endpoint)
			o.UsePathStyle = true
		})
	})

	return client, clientErr
}

// ClientT returns the S3 client, failing the test on error.
func ClientT(t *testing.T) *s3.Client {
	t.Helper()
	c, err := Client()
	if err != nil {
		t.Fatalf("failed to create S3 client: %v", err)
	}
	return c
}

// CreateBucket creates a test bucket with a unique name and registers cleanup.
func CreateBucket(t *testing.T, ctx context.Context) string {
	t.Helper()

	c := ClientT(t)

	// Generate unique bucket name using test name
	name := strings.ToLower(t.Name())
	name = strings.ReplaceAll(name, "/", "-")
	name = strings.ReplaceAll(name, "_", "-")
	// Truncate if too long (S3 bucket names max 63 chars)
	if len(name) > 50 {
		name = name[:50]
	}
	name = fmt.Sprintf("%s-%d", name, time.Now().UnixNano()%100000)

	_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		t.Fatalf("failed to create bucket %s: %v", name, err)
	}

	t.Cleanup(func() {
		DeleteBucket(t, context.Background(), name)
	})

	return name
}

// DeleteBucket deletes a bucket and all its contents.
func DeleteBucket(t *testing.T, ctx context.Context, bucket string) {
	t.Helper()

	c := ClientT(t)

	// List and delete all objects first
	paginator := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Logf("warning: failed to list objects in bucket %s: %v", bucket, err)
			return
		}

		for _, obj := range page.Contents {
			_, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			})
			if err != nil {
				t.Logf("warning: failed to delete object %s: %v", *obj.Key, err)
			}
		}
	}

	// Delete the bucket
	_, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Logf("warning: failed to delete bucket %s: %v", bucket, err)
	}
}

// PutBucketPolicy sets a bucket policy.
func PutBucketPolicy(t *testing.T, ctx context.Context, bucket, policyJSON string) {
	t.Helper()

	c := ClientT(t)

	_, err := c.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket),
		Policy: aws.String(policyJSON),
	})
	if err != nil {
		t.Fatalf("failed to put bucket policy for %s: %v", bucket, err)
	}
}

// PutObject uploads an object to the bucket.
func PutObject(t *testing.T, ctx context.Context, bucket, key string, content []byte) {
	t.Helper()

	c := ClientT(t)

	_, err := c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(string(content)),
	})
	if err != nil {
		t.Fatalf("failed to put object %s/%s: %v", bucket, key, err)
	}
}

// PutObjects uploads multiple objects with the given keys and empty content.
func PutObjects(t *testing.T, ctx context.Context, bucket string, keys []string) {
	t.Helper()

	for _, key := range keys {
		PutObject(t, ctx, bucket, key, []byte("test content for "+key))
	}
}

// PutObjectsWithContent uploads multiple objects with specified content.
func PutObjectsWithContent(t *testing.T, ctx context.Context, bucket string, objects map[string][]byte) {
	t.Helper()

	for key, content := range objects {
		PutObject(t, ctx, bucket, key, content)
	}
}
