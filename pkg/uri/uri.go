// Package uri provides parsing of cloud object-storage URIs in the
// scheme-bucket-key/prefix/pattern shape used throughout gonimbus.
//
// Supported schemes as of v0.2.x: s3, file.
//
// Glob pattern characters in the key, including *, ?, [, ], {, }, and **, are
// recognized and surface via ObjectURI.IsPattern. The original pattern is
// preserved in ObjectURI.Pattern; the longest pre-glob listing prefix is
// available in ObjectURI.Key. Escaped glob metacharacters are treated as
// literal key characters, and non-pattern keys are unescaped for object-store
// lookup.
//
// API stability: gonimbus is pre-v1.0. Breaking changes are possible across
// minor versions and will be telegraphed in release-channel coordination for
// known embedded library consumers. Pin to specific release tags for embedded
// use.
package uri

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/3leaps/gonimbus/pkg/match"
)

// URI parsing errors
var (
	// ErrInvalidURI indicates the URI could not be parsed.
	ErrInvalidURI = errors.New("invalid URI")

	// ErrUnsupportedProvider indicates the URI scheme is not supported.
	ErrUnsupportedProvider = errors.New("unsupported provider")

	// ErrMissingBucket indicates the URI is missing a bucket name.
	ErrMissingBucket = errors.New("missing bucket name")

	// ErrInvalidFileURI indicates a file:// URI is not a supported local absolute path.
	ErrInvalidFileURI = errors.New("invalid file URI")
)

// ObjectURI represents a parsed object URI.
//
// Example URIs:
//   - s3://bucket/key/path.txt
//   - s3://bucket/prefix/
//   - s3://bucket/prefix/**/*.parquet
//   - file:///absolute/path
type ObjectURI struct {
	// Provider is the storage provider (e.g., "s3" or "file").
	Provider string

	// Bucket is the bucket name. For file:// URIs this is the sentinel "local".
	Bucket string

	// Key is the object key or prefix. For file:// URIs this is the absolute
	// local filesystem path.
	Key string

	// Pattern is set if Key contains glob characters.
	// When set, Key is the prefix before the first glob character.
	Pattern string
}

// String returns the URI in canonical form.
func (u *ObjectURI) String() string {
	if u.Provider == "file" {
		key := filepath.ToSlash(u.Key)
		if key == "" {
			key = "/"
		}
		if !strings.HasPrefix(key, "/") {
			key = "/" + key
		}
		return "file://" + key
	}
	if u.Pattern != "" {
		return fmt.Sprintf("%s://%s/%s", u.Provider, u.Bucket, u.Pattern)
	}
	if u.Key != "" {
		return fmt.Sprintf("%s://%s/%s", u.Provider, u.Bucket, u.Key)
	}
	return fmt.Sprintf("%s://%s/", u.Provider, u.Bucket)
}

// IsPattern returns true if the URI contains glob pattern characters.
func (u *ObjectURI) IsPattern() bool {
	return u.Pattern != ""
}

// IsPrefix returns true if the URI represents a prefix (ends with /).
func (u *ObjectURI) IsPrefix() bool {
	return strings.HasSuffix(u.Key, "/") || u.Key == ""
}

// ParseURI parses a cloud storage URI into its components.
//
// Supported formats:
//   - s3://bucket
//   - s3://bucket/
//   - s3://bucket/key
//   - s3://bucket/prefix/
//   - s3://bucket/prefix/**/*.parquet
//   - file:///absolute/path
//   - file:///absolute/path/
//
// Returns an error if the URI is malformed or uses an unsupported provider.
func ParseURI(uri string) (*ObjectURI, error) {
	if uri == "" {
		return nil, fmt.Errorf("%w: empty URI", ErrInvalidURI)
	}

	// Parse manually to handle glob characters like ? which url.Parse treats as query delimiter
	// Expected format: scheme://bucket/key
	schemeEnd := strings.Index(uri, "://")
	if schemeEnd == -1 {
		return nil, fmt.Errorf("%w: missing scheme (expected s3://...)", ErrInvalidURI)
	}

	provider := strings.ToLower(uri[:schemeEnd])
	if provider == "file" {
		return parseFileURI(uri, schemeEnd)
	}
	if provider != "s3" {
		return nil, fmt.Errorf("%w: %s (supported: s3, file)", ErrUnsupportedProvider, provider)
	}

	// Everything after ://
	remainder := uri[schemeEnd+3:]
	if remainder == "" {
		return nil, fmt.Errorf("%w: in %s", ErrMissingBucket, uri)
	}

	// Split bucket from key at first /
	var bucket, key string
	slashIdx := strings.Index(remainder, "/")
	if slashIdx == -1 {
		bucket = remainder
		key = ""
	} else {
		bucket = remainder[:slashIdx]
		key = remainder[slashIdx+1:]
	}

	if bucket == "" {
		return nil, fmt.Errorf("%w: in %s", ErrMissingBucket, uri)
	}

	// Validate bucket name doesn't contain invalid characters
	// (basic validation - S3 bucket names can't contain most special chars)
	if _, err := url.Parse("s3://" + bucket + "/"); err != nil {
		return nil, fmt.Errorf("%w: invalid bucket name %q", ErrInvalidURI, bucket)
	}

	result := &ObjectURI{
		Provider: provider,
		Bucket:   bucket,
	}

	// Use escape-aware glob detection from match package.
	// This correctly handles escaped metacharacters (e.g., \* for literal asterisk).
	if match.IsGlobPattern(key) {
		// Glob pattern: Key is the prefix for listing, Pattern is the full glob
		result.Pattern = key
		result.Key = match.DerivePrefix(key)
	} else {
		// No glob: unescape for S3 key (e.g., "file\*.txt" -> "file*.txt")
		result.Key = match.DerivePrefix(key)
	}

	return result, nil
}

func parseFileURI(raw string, schemeEnd int) (*ObjectURI, error) {
	remainder := raw[schemeEnd+3:]
	if remainder == "" {
		return nil, fmt.Errorf("%w: empty path in %s", ErrInvalidFileURI, raw)
	}
	if !strings.HasPrefix(remainder, "/") {
		return nil, fmt.Errorf("%w: file URI must use an absolute local path", ErrInvalidFileURI)
	}

	// file:///abs/path parses as remainder "/abs/path". A non-empty hostname
	// form such as file://host/path parses as remainder "host/path" and is
	// rejected above. The output-only audit form file://local/path is also
	// intentionally rejected here.
	path := filepath.Clean(remainder)
	if strings.HasSuffix(remainder, "/") && path != string(filepath.Separator) {
		path += string(filepath.Separator)
	}
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("%w: file URI must use an absolute local path", ErrInvalidFileURI)
	}
	return &ObjectURI{Provider: "file", Bucket: "local", Key: filepath.ToSlash(path)}, nil
}
