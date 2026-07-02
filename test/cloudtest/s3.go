package cloudtest

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
	providers3 "github.com/3leaps/gonimbus/pkg/provider/s3"
)

const (
	// RealS3BucketEnv is the opt-in bucket for real S3-compatible integration
	// tests.
	RealS3BucketEnv = "GONIMBUS_S3_TEST_BUCKET"

	// RealS3EndpointEnv optionally points tests at an S3-compatible endpoint.
	RealS3EndpointEnv = "GONIMBUS_S3_TEST_ENDPOINT"

	// RealS3RegionEnv optionally supplies the AWS region or S3-compatible
	// signing region.
	RealS3RegionEnv = "GONIMBUS_S3_TEST_REGION"

	// RealS3ProfileEnv optionally selects an AWS shared-config profile for
	// tests. If unset, the SDK default credential chain is used.
	RealS3ProfileEnv = "GONIMBUS_S3_TEST_PROFILE"

	// RealS3PrefixEnv optionally scopes real S3 test objects within the
	// supplied bucket. Tests still append a unique per-test suffix.
	RealS3PrefixEnv = "GONIMBUS_S3_TEST_PREFIX"

	// RealS3ForcePathStyleEnv optionally overrides the default path-style
	// behavior. When unset, endpoint-backed S3-compatible tests use path-style.
	RealS3ForcePathStyleEnv = "GONIMBUS_S3_TEST_FORCE_PATH_STYLE"
)

type RealS3Config struct {
	Bucket         string
	Endpoint       string
	Region         string
	Profile        string
	Prefix         string
	ForcePathStyle bool
}

// RealS3ConfigFromEnv returns the bring-your-own S3-compatible test
// configuration, or skips when the opt-in bucket environment is absent.
func RealS3ConfigFromEnv(t *testing.T) RealS3Config {
	t.Helper()

	bucket := strings.TrimSpace(os.Getenv(RealS3BucketEnv))
	if bucket == "" {
		t.Skipf("%s not set; skipping real S3-compatible integration test", RealS3BucketEnv)
	}

	endpoint := strings.TrimSpace(os.Getenv(RealS3EndpointEnv))
	forcePathStyle := endpoint != ""
	if raw := strings.TrimSpace(os.Getenv(RealS3ForcePathStyleEnv)); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			t.Fatalf("%s must be a boolean: %v", RealS3ForcePathStyleEnv, err)
		}
		forcePathStyle = parsed
	}

	return RealS3Config{
		Bucket:         bucket,
		Endpoint:       endpoint,
		Region:         strings.TrimSpace(os.Getenv(RealS3RegionEnv)),
		Profile:        strings.TrimSpace(os.Getenv(RealS3ProfileEnv)),
		Prefix:         strings.Trim(strings.TrimSpace(os.Getenv(RealS3PrefixEnv)), "/"),
		ForcePathStyle: forcePathStyle,
	}
}

func (cfg RealS3Config) ProviderConfig() providers3.Config {
	return providers3.Config{
		Bucket:         cfg.Bucket,
		Endpoint:       cfg.Endpoint,
		Region:         cfg.Region,
		Profile:        cfg.Profile,
		ForcePathStyle: cfg.ForcePathStyle,
	}
}

func (cfg RealS3Config) ObjectURI(key string) string {
	return "s3://" + cfg.Bucket + "/" + strings.TrimPrefix(key, "/")
}

// CreateS3ObjectPrefix returns the configured bucket and a unique object prefix
// for a real S3-compatible test. Cleanup deletes only objects under that
// generated prefix.
func CreateS3ObjectPrefix(t *testing.T, ctx context.Context) (RealS3Config, string) {
	t.Helper()

	cfg := RealS3ConfigFromEnv(t)
	root := cfg.Prefix
	if root == "" {
		root = "gonimbus-real-s3"
	}

	testName := strings.ToLower(t.Name())
	testName = strings.NewReplacer("/", "-", "_", "-", " ", "-").Replace(testName)
	prefix := fmt.Sprintf("%s/%s-%d/", root, testName, time.Now().UnixNano())

	t.Cleanup(func() {
		deleteS3ObjectsWithPrefix(t, context.Background(), cfg, prefix)
	})

	// Fail early if the credentials cannot see the bucket.
	p := RealS3Provider(t, ctx, cfg)
	defer func() { _ = p.Close() }()
	if _, err := p.List(ctx, provider.ListOptions{Prefix: prefix, MaxKeys: 1}); err != nil {
		t.Fatalf("failed to list S3 test prefix s3://%s/%s: %v", cfg.Bucket, prefix, err)
	}

	return cfg, prefix
}

func RealS3Provider(t *testing.T, ctx context.Context, cfg RealS3Config) *providers3.Provider {
	t.Helper()

	p, err := providers3.New(ctx, cfg.ProviderConfig())
	if err != nil {
		t.Fatalf("failed to create real S3-compatible provider: %v", err)
	}
	return p
}

func deleteS3ObjectsWithPrefix(t *testing.T, ctx context.Context, cfg RealS3Config, prefix string) {
	t.Helper()

	p := RealS3Provider(t, ctx, cfg)
	defer func() { _ = p.Close() }()

	token := ""
	for {
		res, err := p.List(ctx, provider.ListOptions{Prefix: prefix, ContinuationToken: token})
		if err != nil {
			t.Logf("warning: failed to list S3 cleanup prefix s3://%s/%s: %v", cfg.Bucket, prefix, err)
			return
		}
		for _, obj := range res.Objects {
			if err := p.DeleteObject(ctx, obj.Key); err != nil {
				t.Logf("warning: failed to delete S3 object s3://%s/%s: %v", cfg.Bucket, obj.Key, err)
			}
		}
		if !res.IsTruncated || res.ContinuationToken == "" {
			return
		}
		token = res.ContinuationToken
	}
}
