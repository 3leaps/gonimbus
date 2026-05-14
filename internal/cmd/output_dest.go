package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
)

// outputDestSpec describes an exact output object destination (s3://bucket/key.jsonl
// or file:///abs/path/file.jsonl). Unlike reflowDestSpec which describes a prefix
// for template expansion, this targets a single file.
type outputDestSpec struct {
	Provider string

	// S3 destination
	Bucket         string
	Key            string
	Region         string
	Profile        string
	Endpoint       string
	ForcePathStyle bool

	// File destination
	BaseDir string
}

// parseOutputDest parses a URI into an exact output destination.
//
// Supported schemes:
//   - s3://bucket/path/to/file.jsonl
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

	if strings.HasPrefix(lower, "s3://") {
		remainder := uri[len("s3://"):]
		if remainder == "" {
			return nil, fmt.Errorf("s3 output URI missing bucket: %s", uri)
		}
		slashIdx := strings.Index(remainder, "/")
		if slashIdx == -1 || slashIdx == len(remainder)-1 {
			return nil, fmt.Errorf("s3 output URI must include an object key: %s", uri)
		}
		bucket := remainder[:slashIdx]
		key := remainder[slashIdx+1:]
		if bucket == "" {
			return nil, fmt.Errorf("s3 output URI missing bucket: %s", uri)
		}
		if key == "" || strings.HasSuffix(key, "/") {
			return nil, fmt.Errorf("s3 output URI must be an exact object key, not a prefix: %s", uri)
		}
		return &outputDestSpec{
			Provider: string(provider.ProviderS3),
			Bucket:   bucket,
			Key:      key,
		}, nil
	}

	scheme := uri
	if idx := strings.Index(uri, "://"); idx != -1 {
		scheme = uri[:idx]
	}
	return nil, fmt.Errorf("unsupported output scheme %q (supported: s3, file)", scheme)
}

// newOutputProvider creates a provider capable of PutObject for the given destination spec.
func newOutputProvider(ctx context.Context, spec *outputDestSpec) (provider.ObjectPutter, error) {
	switch spec.Provider {
	case string(provider.ProviderS3):
		p, err := s3.New(ctx, s3.Config{
			Bucket:         spec.Bucket,
			Region:         spec.Region,
			Endpoint:       spec.Endpoint,
			Profile:        spec.Profile,
			ForcePathStyle: spec.ForcePathStyle,
		})
		if err != nil {
			return nil, fmt.Errorf("create output S3 provider: %w", err)
		}
		return p, nil
	case string(provider.ProviderFile):
		if err := os.MkdirAll(spec.BaseDir, 0o755); err != nil {
			return nil, fmt.Errorf("create output directory: %w", err)
		}
		p, err := providerfile.New(providerfile.Config{BaseDir: spec.BaseDir})
		if err != nil {
			return nil, fmt.Errorf("create output file provider: %w", err)
		}
		return p, nil
	default:
		return nil, fmt.Errorf("unsupported output provider %q", spec.Provider)
	}
}

// uploadToOutputDest opens a temp file and uploads it to the output destination.
func uploadToOutputDest(ctx context.Context, putter provider.ObjectPutter, key string, tempFilePath string) error {
	f, err := os.Open(tempFilePath) // #nosec G304 -- tempFilePath is an internal spool path created by stream put.
	if err != nil {
		return fmt.Errorf("open temp file for upload: %w", err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat temp file for upload: %w", err)
	}

	if err := putter.PutObject(ctx, key, io.Reader(f), info.Size()); err != nil {
		return fmt.Errorf("upload output: %w", err)
	}
	return nil
}

func uploadConditionallyToOutputDest(ctx context.Context, putter provider.ObjectPutter, key string, tempFilePath string, precond provider.PutPrecondition) error {
	conditionalPutter, ok := putter.(provider.ConditionalPutter)
	if !ok {
		return fmt.Errorf("destination provider does not support conditional writes")
	}

	f, err := os.Open(tempFilePath) // #nosec G304 -- tempFilePath is an internal spool path created by stream put.
	if err != nil {
		return fmt.Errorf("open temp file for upload: %w", err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat temp file for upload: %w", err)
	}

	if _, err := conditionalPutter.PutObjectConditional(ctx, key, io.Reader(f), info.Size(), precond); err != nil {
		return fmt.Errorf("conditional upload output: %w", err)
	}
	return nil
}
