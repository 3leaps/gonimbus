package provider

import (
	"errors"
	"fmt"
)

// Sentinel errors for provider operations.
var (
	// ErrNotFound indicates the requested object does not exist.
	ErrNotFound = errors.New("object not found")

	// ErrAccessDenied indicates insufficient permissions.
	ErrAccessDenied = errors.New("access denied")

	// ErrBucketNotFound indicates the bucket does not exist.
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrInvalidCredentials indicates authentication failed.
	ErrInvalidCredentials = errors.New("invalid credentials")

	// ErrProviderUnavailable indicates the provider service is unavailable.
	ErrProviderUnavailable = errors.New("provider unavailable")

	// ErrThrottled indicates the request was rate limited by the provider.
	ErrThrottled = errors.New("request throttled")
)

// ProviderError wraps provider-specific errors with context.
type ProviderError struct {
	// Op is the operation that failed (e.g., "List", "Head").
	Op string

	// Provider is the provider type (e.g., "s3").
	Provider ProviderType

	// Bucket is the bucket name, if applicable.
	Bucket string

	// Key is the object key, if applicable.
	Key string

	// Err is the underlying error.
	Err error
}

// Error implements the error interface.
func (e *ProviderError) Error() string {
	if e.Key != "" {
		return fmt.Sprintf("%s %s: %s/%s: %v", e.Provider, e.Op, e.Bucket, e.Key, e.Err)
	}
	if e.Bucket != "" {
		return fmt.Sprintf("%s %s: %s: %v", e.Provider, e.Op, e.Bucket, e.Err)
	}
	return fmt.Sprintf("%s %s: %v", e.Provider, e.Op, e.Err)
}

// Unwrap returns the underlying error for errors.Is/As support.
func (e *ProviderError) Unwrap() error {
	return e.Err
}

// IsNotFound returns true if the error indicates an object was not found.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsAccessDenied returns true if the error indicates insufficient permissions.
func IsAccessDenied(err error) bool {
	return errors.Is(err, ErrAccessDenied)
}

// IsBucketNotFound returns true if the error indicates the bucket does not exist.
func IsBucketNotFound(err error) bool {
	return errors.Is(err, ErrBucketNotFound)
}

// IsInvalidCredentials returns true if the error indicates authentication failed.
func IsInvalidCredentials(err error) bool {
	return errors.Is(err, ErrInvalidCredentials)
}

// IsProviderUnavailable returns true if the error indicates the provider service is unavailable.
func IsProviderUnavailable(err error) bool {
	return errors.Is(err, ErrProviderUnavailable)
}

// IsThrottled returns true if the error indicates the request was rate limited.
func IsThrottled(err error) bool {
	return errors.Is(err, ErrThrottled)
}
