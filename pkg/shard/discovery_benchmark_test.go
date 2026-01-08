package shard

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	s3provider "github.com/3leaps/gonimbus/pkg/provider/s3"
)

// TestDiscoveryBenchmark_Live runs against a real S3-compatible bucket
// to measure parallel shard discovery performance.
//
// Requires environment variables:
//   - AWS_ACCESS_KEY_ID
//   - AWS_SECRET_ACCESS_KEY
//   - TEST_BUCKET (default: gonimbus-qa)
//   - TEST_ENDPOINT (default: https://s3.us-east-2.wasabisys.com)
//   - TEST_REGION (default: us-east-2)
//   - TEST_PREFIX (default: fixtures/v0.1.2/sharding/)
//
// Run with: go test -v -run TestDiscoveryBenchmark_Live ./pkg/shard/...
func TestDiscoveryBenchmark_Live(t *testing.T) {
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if accessKey == "" || secretKey == "" {
		t.Skip("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY required for live test")
	}

	bucket := os.Getenv("TEST_BUCKET")
	if bucket == "" {
		bucket = "gonimbus-qa"
	}
	endpoint := os.Getenv("TEST_ENDPOINT")
	if endpoint == "" {
		endpoint = "https://s3.us-east-2.wasabisys.com"
	}
	region := os.Getenv("TEST_REGION")
	if region == "" {
		region = "us-east-2"
	}
	prefix := os.Getenv("TEST_PREFIX")
	if prefix == "" {
		prefix = "fixtures/v0.1.2/sharding/"
	}

	ctx := context.Background()

	// Create S3 provider
	p, err := s3provider.New(ctx, s3provider.Config{
		Bucket:          bucket,
		Region:          region,
		Endpoint:        endpoint,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		ForcePathStyle:  true,
	})
	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}
	defer func() { _ = p.Close() }()

	// Test configurations
	tests := []struct {
		name            string
		depth           int
		listConcurrency int
	}{
		// Depth 1: 256 prefixes (single level expansion)
		{"sequential_depth1", 1, 1},
		{"parallel_4_depth1", 1, 4},
		{"parallel_8_depth1", 1, 8},
		{"parallel_16_depth1", 1, 16},
		// Depth 2: ~4096 prefixes (two level expansion) - this is where parallel shines
		{"sequential_depth2", 2, 1},
		{"parallel_4_depth2", 2, 4},
		{"parallel_8_depth2", 2, 8},
		{"parallel_16_depth2", 2, 16},
		{"parallel_32_depth2", 2, 32},
	}

	fmt.Println("\n=== Shard Discovery Benchmark ===")
	fmt.Printf("Bucket:   %s\n", bucket)
	fmt.Printf("Endpoint: %s\n", endpoint)
	fmt.Printf("Prefix:   %s\n\n", prefix)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()

			shards, err := Discover(ctx, p, prefix, Config{
				Enabled:         true,
				Depth:           tc.depth,
				ListConcurrency: tc.listConcurrency,
				Delimiter:       "/",
			})
			if err != nil {
				t.Fatalf("Discover failed: %v", err)
			}

			elapsed := time.Since(start)
			fmt.Printf("%-20s: %d shards in %v\n", tc.name, len(shards), elapsed)
		})
	}
}
