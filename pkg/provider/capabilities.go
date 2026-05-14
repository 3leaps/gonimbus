package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// Optional provider capability interfaces.
//
// These interfaces are used for feature detection (type assertions). The core
// Provider interface remains intentionally small.

// ObjectPutter can create/overwrite objects.
//
// For v0.1.x this is primarily used for write-probe preflight operations.
type ObjectPutter interface {
	PutObject(ctx context.Context, key string, body io.Reader, contentLength int64) error
}

// ConditionalPutter can create/replace objects only when a write precondition
// holds atomically at the provider.
type ConditionalPutter interface {
	PutObjectConditional(ctx context.Context, key string, body io.Reader, contentLength int64, precond PutPrecondition) (PutResult, error)
}

// PutPrecondition describes the predicate that must hold for a conditional put.
//
// Exactly one predicate must be set. Callers that want unconditional replacement
// should use ObjectPutter.PutObject instead.
type PutPrecondition struct {
	IfAbsent    bool
	IfMatchETag *string
}

// Validate checks that exactly one precondition predicate is set.
func (p PutPrecondition) Validate() error {
	count := 0
	if p.IfAbsent {
		count++
	}
	if p.IfMatchETag != nil {
		if *p.IfMatchETag == "" {
			return errors.New("IfMatchETag precondition must not be empty")
		}
		count++
	}
	if count != 1 {
		return fmt.Errorf("exactly one put precondition must be set, got %d", count)
	}
	return nil
}

// PutResult contains provider version handles returned by a successful put.
type PutResult struct {
	ETag    string
	Version string
}

// ObjectDeleter can delete objects.
//
// For v0.1.x this is primarily used for write-probe preflight operations.
type ObjectDeleter interface {
	DeleteObject(ctx context.Context, key string) error
}

// MultipartUploader can create and abort multipart uploads.
//
// This provides a low-side-effect write probe when supported.
type MultipartUploader interface {
	CreateMultipartUpload(ctx context.Context, key string) (uploadID string, err error)
	AbortMultipartUpload(ctx context.Context, key, uploadID string) error
}

// ObjectGetter can download objects as a stream.
//
// For v0.1.x this is used for streaming transfer operations and stream helpers.
type ObjectGetter interface {
	GetObject(ctx context.Context, key string) (body io.ReadCloser, contentLength int64, err error)
}

// ObjectRanger can download a specific byte range of an object.
//
// This is the foundational primitive for content inspection operations.
//
// start and endInclusive are inclusive offsets following HTTP Range semantics.
// Implementations SHOULD return the content length of the returned range.
type ObjectRanger interface {
	GetRange(ctx context.Context, key string, start, endInclusive int64) (body io.ReadCloser, contentLength int64, err error)
}

// PrefixLister supports delimiter-based prefix discovery.
//
// For S3 this maps to ListObjectsV2 with a Delimiter.
type PrefixLister interface {
	ListCommonPrefixes(ctx context.Context, opts ListCommonPrefixesOptions) (*ListCommonPrefixesResult, error)
}

type ListCommonPrefixesOptions struct {
	Prefix            string
	Delimiter         string
	ContinuationToken string
	MaxKeys           int
}

type ListCommonPrefixesResult struct {
	Prefixes          []string
	ContinuationToken string
	IsTruncated       bool
}
