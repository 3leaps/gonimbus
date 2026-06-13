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

	// ErrAnonymousReadOnly indicates an anonymous provider was asked to perform
	// a mutating operation. It is joined with ErrAccessDenied by providers that
	// support unsigned read-only construction.
	ErrAnonymousReadOnly = errors.New("anonymous provider is read-only")

	// ErrBucketNotFound indicates the bucket does not exist.
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrInvalidCredentials indicates authentication failed.
	ErrInvalidCredentials = errors.New("invalid credentials")

	// ErrCredentialsRefreshFailed indicates a provider credential cache or
	// token provider could not refresh credentials for the operation.
	ErrCredentialsRefreshFailed = errors.New("credentials refresh failed")

	// ErrProviderUnavailable indicates the provider service is unavailable.
	ErrProviderUnavailable = errors.New("provider unavailable")

	// ErrThrottled indicates the request was rate limited by the provider.
	ErrThrottled = errors.New("request throttled")

	// ErrAlreadyExists indicates an atomic create was refused because the object exists.
	ErrAlreadyExists = errors.New("object already exists")

	// ErrPreconditionFailed indicates an atomic write predicate did not hold.
	ErrPreconditionFailed = errors.New("write precondition failed")
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

// IsAnonymousReadOnly returns true if the error indicates an anonymous provider
// rejected a mutating operation.
func IsAnonymousReadOnly(err error) bool {
	return errors.Is(err, ErrAnonymousReadOnly)
}

// IsBucketNotFound returns true if the error indicates the bucket does not exist.
func IsBucketNotFound(err error) bool {
	return errors.Is(err, ErrBucketNotFound)
}

// IsInvalidCredentials returns true if the error indicates authentication failed.
func IsInvalidCredentials(err error) bool {
	return errors.Is(err, ErrInvalidCredentials)
}

// IsCredentialsRefreshFailed returns true if the error indicates a provider-side
// credential refresh failure.
func IsCredentialsRefreshFailed(err error) bool {
	return errors.Is(err, ErrCredentialsRefreshFailed)
}

// IsProviderUnavailable returns true if the error indicates the provider service is unavailable.
func IsProviderUnavailable(err error) bool {
	return errors.Is(err, ErrProviderUnavailable)
}

// IsThrottled returns true if the error indicates the request was rate limited.
func IsThrottled(err error) bool {
	return errors.Is(err, ErrThrottled)
}

// IsAlreadyExists returns true if the error indicates the target already exists.
func IsAlreadyExists(err error) bool {
	return errors.Is(err, ErrAlreadyExists)
}

// IsPreconditionFailed returns true if the error indicates a write precondition failed.
func IsPreconditionFailed(err error) bool {
	return errors.Is(err, ErrPreconditionFailed)
}
