// Package provider defines abstractions for cloud object storage operations.
//
// Providers implement a minimal surface area focused on listing and metadata
// retrieval. Authentication uses SDK default credential chains - providers
// should not implement custom auth logic.
package provider

import (
	"context"
	"time"
)

// Provider abstracts cloud storage listing operations.
//
// Implementations should:
//   - Use SDK default credential chains (AWS default config, GCP ADC)
//   - Support pagination via continuation tokens
//   - Be safe for concurrent use
type Provider interface {
	// List returns a page of objects with the given prefix.
	// Use ContinuationToken from ListResult for subsequent pages.
	List(ctx context.Context, opts ListOptions) (*ListResult, error)

	// Head returns metadata for a single object.
	// Returns ErrNotFound if the object does not exist.
	Head(ctx context.Context, key string) (*ObjectMeta, error)

	// Close releases any resources held by the provider.
	Close() error
}

// ListOptions configures a List operation.
type ListOptions struct {
	// Prefix filters results to keys starting with this value.
	// Empty string lists all objects.
	Prefix string

	// ContinuationToken resumes listing from a previous ListResult.
	// Empty string starts from the beginning.
	ContinuationToken string

	// MaxKeys limits the number of objects returned per page.
	// Zero uses provider default (typically 1000).
	MaxKeys int
}

// ListResult contains a page of objects from a List operation.
type ListResult struct {
	// Objects contains the object summaries for this page.
	Objects []ObjectSummary

	// ContinuationToken is used to retrieve the next page.
	// Empty string indicates no more pages.
	ContinuationToken string

	// IsTruncated indicates whether more results are available.
	IsTruncated bool
}

// ObjectSummary contains basic metadata returned from List operations.
type ObjectSummary struct {
	// Key is the full object key (path) in the bucket.
	Key string

	// Size is the object size in bytes.
	Size int64

	// ETag is the entity tag, typically an MD5 hash of the object.
	ETag string

	// LastModified is when the object was last modified.
	LastModified time.Time
}

// ObjectMeta contains full metadata for a single object.
// Returned by Head operations.
type ObjectMeta struct {
	ObjectSummary

	// ContentType is the MIME type of the object.
	ContentType string

	// Metadata contains user-defined metadata key-value pairs.
	Metadata map[string]string
}

// ProviderType identifies a cloud storage provider.
type ProviderType string

const (
	// ProviderS3 represents AWS S3 or S3-compatible storage.
	ProviderS3 ProviderType = "s3"

	// ProviderGCS represents Google Cloud Storage (future).
	ProviderGCS ProviderType = "gcs"
)

// String returns the string representation of the provider type.
func (p ProviderType) String() string {
	return string(p)
}
