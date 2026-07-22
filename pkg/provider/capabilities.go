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
// holds atomically at the provider. Implementations that expose this capability
// must honor every supported PutPrecondition predicate atomically.
// Unsupported validated predicates must fail with ErrUnsupportedPrecondition
// rather than falling back to an unconditional write or returning
// ErrPreconditionFailed, which means a supported predicate evaluated false.
type ConditionalPutter interface {
	PutObjectConditional(ctx context.Context, key string, body io.Reader, contentLength int64, precond PutPrecondition) (PutResult, error)
}

// PutOptions carries optional destination object attributes for providers that
// can persist caller-controlled metadata on PUT.
type PutOptions struct {
	// UserMetadata contains user-defined object metadata keys without provider
	// wire prefixes such as x-amz-meta-. Keys should already be canonicalized by
	// callers that expose user input.
	UserMetadata map[string]string

	// ContentType is the destination object's content type. Empty means provider
	// default.
	ContentType string

	// StorageClass is the destination object's provider-native storage class.
	// Empty means provider default.
	StorageClass string
}

// Empty returns true when no optioned destination attributes are requested.
func (o PutOptions) Empty() bool {
	return len(o.UserMetadata) == 0 && o.ContentType == "" && o.StorageClass == ""
}

// MetadataAwarePutter can persist caller-controlled metadata, content type, and
// storage class on both unconditional and conditional PUTs.
type MetadataAwarePutter interface {
	PutObjectWithOptions(ctx context.Context, key string, body io.Reader, contentLength int64, opts PutOptions) error
	PutObjectConditionalWithOptions(ctx context.Context, key string, body io.Reader, contentLength int64, precond PutPrecondition, opts PutOptions) (PutResult, error)
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

// PartETag identifies one successfully uploaded multipart part.
type PartETag struct {
	PartNumber int32
	ETag       string
}

// MultipartUploader can create, upload, complete, and abort multipart uploads.
//
// CreateMultipartUpload+AbortMultipartUpload also provide a low-side-effect
// write probe when supported.
type MultipartUploader interface {
	CreateMultipartUpload(ctx context.Context, key string) (uploadID string, err error)
	UploadPart(ctx context.Context, key, uploadID string, partNumber int32, body io.Reader, size int64) (PartETag, error)
	CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []PartETag) (PutResult, error)
	AbortMultipartUpload(ctx context.Context, key, uploadID string) error
}

// ConditionalMultipartCompleter can complete multipart uploads only when a
// write precondition holds atomically at the provider.
type ConditionalMultipartCompleter interface {
	CompleteMultipartUploadConditional(ctx context.Context, key, uploadID string, parts []PartETag, precond PutPrecondition) (PutResult, error)
}

// ConditionalWriteCapabilities enumerates the conditional-write predicates a
// destination provider honors atomically. It exists so a caller can validate
// predicate support up front — before any read or destination mutation — instead
// of inferring support from the presence of ConditionalPutter, which cannot
// distinguish an IfAbsent-only implementation from one that also honors IfMatch
// or conditional multipart completion. A provider that advertises a predicate
// here promises to honor it (or return ErrPreconditionFailed when the predicate
// evaluates false); it must never fall back to an unconditional write or return
// ErrUnsupportedPrecondition for an advertised predicate.
type ConditionalWriteCapabilities struct {
	// IfAbsent reports that PutObjectConditional honors an IfAbsent precondition.
	IfAbsent bool
	// IfMatchETag reports that PutObjectConditional honors an IfMatchETag
	// precondition (atomic compare-and-swap against the destination ETag).
	IfMatchETag bool
	// ConditionalMultipartCompletion reports that CompleteMultipartUploadConditional
	// honors its precondition, so a large conditional overwrite completes
	// atomically instead of uploading parts and discovering the predicate is
	// unsupported only at completion time.
	ConditionalMultipartCompletion bool
}

// ConditionalCapabilityReporter reports which conditional-write predicates the
// provider honors. Providers that implement ConditionalPutter (and, where
// applicable, ConditionalMultipartCompleter) should implement this so callers
// can validate predicate support before touching the destination. A provider
// that does not implement this interface cannot prove it honors any conditional
// predicate and must be treated as unable to honor them — the injected adapter's
// declared capability, not the mere presence of ConditionalPutter, is the
// authority. A remote endpoint reached through a declaring adapter remains a
// documented trust boundary.
type ConditionalCapabilityReporter interface {
	ConditionalWriteCapabilities() ConditionalWriteCapabilities
}

// MetadataAwareMultipartUploader can start multipart uploads with the same
// destination object attributes as a single-part PUT.
type MetadataAwareMultipartUploader interface {
	CreateMultipartUploadWithOptions(ctx context.Context, key string, opts PutOptions) (uploadID string, err error)
}

// ObjectGetter can download objects as a stream.
//
// For v0.1.x this is used for streaming transfer operations and stream helpers.
type ObjectGetter interface {
	GetObject(ctx context.Context, key string) (body io.ReadCloser, contentLength int64, err error)
}

// VersionedGetter can download an object with a version handle that belongs to
// the bytes returned by the same read operation.
type VersionedGetter interface {
	GetObjectVersioned(ctx context.Context, key string) (body io.ReadCloser, meta ObjectMeta, err error)
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
