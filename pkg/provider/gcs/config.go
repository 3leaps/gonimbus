// Package gcs defines the Google Cloud Storage provider contract.
//
// Slice 0 intentionally pins the stable configuration surface and SDK intake
// without enabling command dispatch or implementing provider operations.
package gcs

import (
	"fmt"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

const (
	// DefaultMaxKeys is the default page size for List operations.
	DefaultMaxKeys = 1000

	// MaxAllowedKeys is the maximum page size accepted by the JSON API list
	// maxResults parameter.
	MaxAllowedKeys = 1000

	// DefaultWriterChunkSizeBytes is the GCS SDK's default storage.Writer
	// ChunkSize. Each active writer allocates this much buffer memory unless a
	// provider config overrides WriterChunkSizeBytes.
	DefaultWriterChunkSizeBytes = 16 * 1024 * 1024
)

// Config configures a Google Cloud Storage provider.
//
// Authentication priority:
//  1. Anonymous unauthenticated reads when Anonymous is true; this is mutually
//     exclusive with TokenSource and must fail closed for mutating operations.
//  2. Caller-injected TokenSource, if provided.
//  3. Google Application Default Credentials from the operator environment.
//
// Credential source file paths are intentionally absent. GCP ADC external
// account files may contain credential_source.executable hooks, so manifest,
// CLI, and other untrusted config surfaces must never choose a credentials
// filepath for this package.
type Config struct {
	// Bucket is the GCS bucket name (required).
	Bucket string

	// Project is the optional GCP project for client construction. Empty lets
	// the SDK infer project context when supported.
	Project string

	// Anonymous issues unauthenticated read requests for public buckets. It is
	// mutually exclusive with TokenSource and never falls back to ADC.
	Anonymous bool

	// TokenSource overrides credential resolution with caller-managed OAuth2
	// tokens. It takes precedence over ADC and intentionally carries an already
	// resolved credential handle rather than a credentials filepath.
	TokenSource oauth2.TokenSource

	// MaxKeys is the default page size for List operations.
	// Zero uses the provider default (1000). Values over 1000 are clamped.
	MaxKeys int

	// MaxIdleConnsPerHost optionally sizes the per-host idle HTTP connection
	// pool for high-concurrency transfer paths. Zero leaves SDK defaults.
	MaxIdleConnsPerHost int

	// MaxConnsPerHost optionally caps total HTTP connections per host. Zero
	// leaves SDK defaults.
	MaxConnsPerHost int

	// WriterChunkSizeBytes optionally overrides storage.Writer.ChunkSize for
	// high-concurrency write paths. Zero keeps the SDK default; positive values
	// are rounded by the SDK to a 256 KiB multiple; negative values are invalid.
	WriterChunkSizeBytes int
}

// Validate checks that required configuration is present.
func (c *Config) Validate() error {
	if c.Bucket == "" {
		return &ConfigError{Field: "Bucket", Message: "bucket name is required"}
	}
	if c.Anonymous && c.TokenSource != nil {
		return &ConfigError{Field: "Anonymous", Message: "cannot be combined with TokenSource"}
	}
	if c.WriterChunkSizeBytes < 0 {
		return &ConfigError{Field: "WriterChunkSizeBytes", Message: "must be non-negative"}
	}
	return nil
}

// AuthClientOptions returns the SDK auth options implied by the config. It is
// intentionally limited to typed handles and anonymous mode; ADC remains the
// SDK default when no auth option is returned.
func (c Config) AuthClientOptions() []option.ClientOption {
	switch {
	case c.Anonymous:
		return []option.ClientOption{option.WithoutAuthentication()}
	case c.TokenSource != nil:
		return []option.ClientOption{option.WithTokenSource(c.TokenSource)}
	default:
		return nil
	}
}

// DefaultScopes returns the provider's default OAuth scope set for ADC/token
// construction points that require explicit scopes.
func DefaultScopes() []string {
	return []string{storage.ScopeReadWrite}
}

// String returns a redacted representation of the config suitable for logs.
func (c Config) String() string {
	tokenSource := "<nil>"
	if c.TokenSource != nil {
		tokenSource = fmt.Sprintf("<set:%T>", c.TokenSource)
	}
	return fmt.Sprintf(
		"gcs.Config{Bucket:%q Project:%q Anonymous:%t TokenSource:%s MaxKeys:%d MaxIdleConnsPerHost:%d MaxConnsPerHost:%d WriterChunkSizeBytes:%d}",
		c.Bucket,
		c.Project,
		c.Anonymous,
		tokenSource,
		c.MaxKeys,
		c.MaxIdleConnsPerHost,
		c.MaxConnsPerHost,
		c.WriterChunkSizeBytes,
	)
}

// GoString returns a redacted representation for %#v formatting.
func (c Config) GoString() string {
	return c.String()
}

// ConfigError represents a configuration validation error.
type ConfigError struct {
	Field   string
	Message string
}

// Error implements the error interface.
func (e *ConfigError) Error() string {
	return "gcs config: " + e.Field + ": " + e.Message
}
