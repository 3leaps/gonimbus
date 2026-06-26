package cloudtest

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	// RealGCSBucketEnv is the opt-in bucket for real GCS integration tests.
	RealGCSBucketEnv = "GONIMBUS_GCS_TEST_BUCKET"

	// RealGCSProjectEnv is an optional GCP project hint for real GCS tests.
	RealGCSProjectEnv = "GONIMBUS_GCS_TEST_PROJECT"

	// RealGCSPrefixEnv optionally scopes real GCS test objects within the
	// supplied bucket. Tests still append a unique per-test suffix.
	RealGCSPrefixEnv = "GONIMBUS_GCS_TEST_PREFIX"
)

// RealGCSBucket returns the bring-your-own GCS test bucket, or skips when the
// opt-in environment is absent.
func RealGCSBucket(t *testing.T) string {
	t.Helper()
	bucket := strings.TrimSpace(os.Getenv(RealGCSBucketEnv))
	if bucket == "" {
		t.Skipf("%s not set; skipping real GCS integration test", RealGCSBucketEnv)
	}
	return bucket
}

// RealGCSProject returns the optional project hint for GCS client construction.
func RealGCSProject() string {
	return strings.TrimSpace(os.Getenv(RealGCSProjectEnv))
}

// CreateGCSObjectPrefix returns the configured bucket and a unique object prefix
// for a real GCS test. Cleanup deletes only objects under that generated prefix.
func CreateGCSObjectPrefix(t *testing.T, ctx context.Context) (bucket string, prefix string) {
	t.Helper()

	bucket = RealGCSBucket(t)
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("failed to create GCS client from ambient credentials: %v", err)
	}

	root := strings.Trim(strings.TrimSpace(os.Getenv(RealGCSPrefixEnv)), "/")
	if root == "" {
		root = "gonimbus-real-gcs"
	}
	testName := strings.ToLower(t.Name())
	testName = strings.NewReplacer("/", "-", "_", "-", " ", "-").Replace(testName)
	prefix = fmt.Sprintf("%s/%s-%d/", root, testName, time.Now().UnixNano())

	t.Cleanup(func() {
		defer func() { _ = client.Close() }()
		deleteGCSObjectsWithPrefix(t, context.Background(), client, bucket, prefix)
	})

	return bucket, prefix
}

func deleteGCSObjectsWithPrefix(t *testing.T, ctx context.Context, client *storage.Client, bucket, prefix string) {
	t.Helper()

	iter := client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := iter.Next()
		if err == iterator.Done {
			return
		}
		if err != nil {
			t.Logf("warning: failed to list GCS cleanup prefix gs://%s/%s: %v", bucket, prefix, err)
			return
		}
		if err := client.Bucket(bucket).Object(attrs.Name).Delete(ctx); err != nil {
			t.Logf("warning: failed to delete GCS object gs://%s/%s: %v", bucket, attrs.Name, err)
		}
	}
}
