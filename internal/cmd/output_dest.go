package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/transfer"
)

// outputDestSpec describes an exact output object destination (s3://bucket/key.jsonl,
// gs://bucket/key.jsonl, or file:///abs/path/file.jsonl). Unlike reflowDestSpec
// which describes a prefix for template expansion, this targets a single file.
type outputDestSpec struct {
	Provider string

	// Object-store destination
	Bucket         string
	Key            string
	Region         string
	Profile        string
	Endpoint       string
	ForcePathStyle bool
	GCPProject     string

	// File destination
	BaseDir string
}

// parseOutputDest parses a URI into an exact output destination.
//
// Supported schemes:
//   - s3://bucket/path/to/file.jsonl
//   - gs://bucket/path/to/file.jsonl
//   - file:///absolute/path/file.jsonl
func parseOutputDest(uri string) (*outputDestSpec, error) {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return nil, fmt.Errorf("output destination is required")
	}

	lower := strings.ToLower(uri)

	if strings.HasPrefix(lower, "file://") {
		path := uri[len("file://"):]
		path = strings.TrimSpace(path)
		if path == "" {
			return nil, fmt.Errorf("file output path is empty")
		}
		if strings.HasSuffix(path, "/") {
			return nil, fmt.Errorf("file output path must include a filename, not a directory: %s", path)
		}
		path = filepath.Clean(path)
		if !filepath.IsAbs(path) {
			return nil, fmt.Errorf("file output path must be absolute: %s", path)
		}
		base := filepath.Base(path)
		if base == "." || base == "/" {
			return nil, fmt.Errorf("file output path must include a filename: %s", path)
		}
		return &outputDestSpec{
			Provider: string(provider.ProviderFile),
			Key:      base,
			BaseDir:  filepath.Dir(path),
		}, nil
	}

	if strings.HasPrefix(lower, "s3://") || strings.HasPrefix(lower, "gs://") {
		scheme := "s3"
		providerName := string(provider.ProviderS3)
		prefixLen := len("s3://")
		if strings.HasPrefix(lower, "gs://") {
			scheme = "gs"
			providerName = string(provider.ProviderGCS)
			prefixLen = len("gs://")
		}
		remainder := uri[prefixLen:]
		if remainder == "" {
			return nil, fmt.Errorf("%s output URI missing bucket: %s", scheme, uri)
		}
		slashIdx := strings.Index(remainder, "/")
		if slashIdx == -1 || slashIdx == len(remainder)-1 {
			return nil, fmt.Errorf("%s output URI must include an object key: %s", scheme, uri)
		}
		bucket := remainder[:slashIdx]
		key := remainder[slashIdx+1:]
		if bucket == "" {
			return nil, fmt.Errorf("%s output URI missing bucket: %s", scheme, uri)
		}
		if key == "" || strings.HasSuffix(key, "/") {
			return nil, fmt.Errorf("%s output URI must be an exact object key, not a prefix: %s", scheme, uri)
		}
		return &outputDestSpec{
			Provider: providerName,
			Bucket:   bucket,
			Key:      key,
		}, nil
	}

	scheme := uri
	if idx := strings.Index(uri, "://"); idx != -1 {
		scheme = uri[:idx]
	}
	return nil, fmt.Errorf("unsupported output scheme %q (supported: s3, gs, file)", scheme)
}

// newOutputProvider creates a provider capable of PutObject for the given destination spec.
func newOutputProvider(ctx context.Context, spec *outputDestSpec) (provider.ObjectPutter, error) {
	if spec.Provider == string(provider.ProviderFile) {
		if err := os.MkdirAll(spec.BaseDir, 0o750); err != nil {
			return nil, fmt.Errorf("create output directory: %w", err)
		}
	}
	p, err := providerdispatch.NewDestination(ctx, providerdispatch.DestinationOptions{
		Command:     "output",
		Provider:    spec.Provider,
		S3Bucket:    spec.Bucket,
		S3Prefix:    spec.Key,
		GCSBucket:   spec.Bucket,
		GCSPrefix:   spec.Key,
		FileBaseDir: spec.BaseDir,
		S3: providerdispatch.S3Options{
			Region:         spec.Region,
			Endpoint:       spec.Endpoint,
			Profile:        spec.Profile,
			ForcePathStyle: spec.ForcePathStyle,
		},
		GCS: providerdispatch.GCSOptions{
			Project: strings.TrimSpace(spec.GCPProject),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create output provider: %w", err)
	}
	putter, err := providerdispatch.RequireCapability[provider.ObjectPutter](p, "output", spec.Provider, "ObjectPutter")
	if err != nil {
		return nil, err
	}
	return putter, nil
}

// uploadToOutputDest opens a temp file and uploads it to the output destination.
func uploadToOutputDest(ctx context.Context, putter provider.ObjectPutter, key string, tempFilePath string) error {
	if _, err := transfer.UploadFile(ctx, putter, key, tempFilePath, transfer.UploadOptions{}); err != nil {
		return fmt.Errorf("upload output: %w", err)
	}
	return nil
}

func uploadConditionallyToOutputDest(ctx context.Context, putter provider.ObjectPutter, key string, tempFilePath string, precond provider.PutPrecondition) error {
	if _, err := transfer.UploadFile(ctx, putter, key, tempFilePath, transfer.UploadOptions{Precondition: precond}); err != nil {
		return fmt.Errorf("conditional upload output: %w", err)
	}
	return nil
}
