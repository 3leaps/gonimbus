package s3

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// Config configures an S3 provider.
//
// Authentication priority:
//  1. Anonymous unsigned reads when Anonymous is true; this is mutually
//     exclusive with every credential source and never falls back to ambient
//     credentials.
//  2. Caller-injected CredentialsProvider, if provided.
//  3. Explicit AccessKeyID/SecretAccessKey, if provided.
//  4. Shared config Profile, if provided.
//  5. AWS SDK v2 default credential chain.
//
// Region handling:
//   - For AWS S3: If Region is empty and not set via environment/profile,
//     defaults to us-east-1 (standard AWS convention).
//   - For S3-compatible stores: Region is typically ignored by the endpoint,
//     but some providers (e.g., Wasabi) may require it. When Endpoint is set,
//     no default region is applied.
//
// For S3-compatible stores (Wasabi, MinIO, DigitalOcean Spaces), set
// Endpoint and typically ForcePathStyle.
//
// For hermetic embedded use, remember that explicit credentials only suppress
// credential-chain lookup. If Endpoint is empty, AWS SDK configured endpoint
// sources such as AWS_ENDPOINT_URL, AWS_ENDPOINT_URL_S3, and shared-config
// endpoint_url can still redirect requests unless the embedding process sets
// AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=true.
type Config struct {
	// Bucket is the S3 bucket name (required).
	Bucket string

	// Region is the AWS region.
	// For AWS S3: defaults to us-east-1 if not specified via config or environment.
	// For S3-compatible (when Endpoint is set): no default applied.
	Region string

	// Endpoint is a custom endpoint URL for S3-compatible stores.
	// Leave empty for AWS S3.
	// Examples:
	//   - Wasabi: https://s3.wasabisys.com
	//   - MinIO: http://localhost:9000
	//   - DigitalOcean: https://nyc3.digitaloceanspaces.com
	Endpoint string

	// Profile is the AWS profile name to use from shared config.
	// Leave empty to use the default profile or environment credentials.
	Profile string

	// Anonymous issues unsigned read requests for public buckets. It supports
	// List, Head, GetObject, GetObjectVersioned, and GetRange only. Mutating
	// methods fail closed with provider.ErrAnonymousReadOnly joined with
	// provider.ErrAccessDenied. Anonymous is mutually exclusive with
	// CredentialsProvider, AccessKeyID/SecretAccessKey, and Profile, and it
	// never falls back to ambient environment, profile, or instance credentials.
	Anonymous bool

	// CredentialsProvider overrides credential resolution with caller-managed
	// AWS credentials. It takes precedence over AccessKeyID/SecretAccessKey,
	// Profile, and the SDK default chain. When set, Profile is not loaded for
	// credentials or profile-derived non-credential config such as region; pass
	// Region, Endpoint, and ForcePathStyle directly when those values are needed.
	CredentialsProvider aws.CredentialsProvider

	// AccessKeyID is an explicit access key. If set, SecretAccessKey must also be set.
	// This takes precedence over Profile and the default credential chain.
	AccessKeyID string

	// SecretAccessKey is an explicit secret key. Required if AccessKeyID is set.
	SecretAccessKey string

	// ForcePathStyle forces path-style URLs (bucket in path, not subdomain).
	// Required for most S3-compatible stores and useful for local development.
	// AWS S3 uses virtual-hosted style by default.
	ForcePathStyle bool

	// MaxKeys is the default page size for List operations.
	// Zero uses the provider default (1000). Values over 1000 are clamped.
	MaxKeys int

	// MaxIdleConnsPerHost optionally sizes the per-host idle HTTP connection
	// pool for high-concurrency transfer paths. Zero leaves SDK defaults.
	MaxIdleConnsPerHost int

	// MaxConnsPerHost optionally caps total HTTP connections per host. Zero
	// leaves SDK defaults.
	MaxConnsPerHost int
}

// DefaultMaxKeys is the default page size for List operations.
const DefaultMaxKeys = 1000

// MaxAllowedKeys is the maximum page size allowed by S3.
const MaxAllowedKeys = 1000

// DefaultAWSRegion is the fallback region for AWS S3 when not specified.
const DefaultAWSRegion = "us-east-1"

// Validate checks that required configuration is present.
func (c *Config) Validate() error {
	if c.Bucket == "" {
		return &ConfigError{Field: "Bucket", Message: "bucket name is required"}
	}

	if c.Anonymous {
		switch {
		case c.CredentialsProvider != nil:
			return &ConfigError{Field: "Anonymous", Message: "cannot be combined with CredentialsProvider"}
		case c.AccessKeyID != "" || c.SecretAccessKey != "":
			return &ConfigError{Field: "Anonymous", Message: "cannot be combined with AccessKeyID/SecretAccessKey"}
		case c.Profile != "":
			return &ConfigError{Field: "Anonymous", Message: "cannot be combined with Profile"}
		}
	}

	// If one explicit credential is set, both must be set
	if (c.AccessKeyID != "") != (c.SecretAccessKey != "") {
		return &ConfigError{
			Field:   "AccessKeyID/SecretAccessKey",
			Message: "both access key ID and secret access key must be provided together",
		}
	}

	return nil
}

// String returns a redacted representation of the config suitable for logs.
func (c Config) String() string {
	credsProvider := "<nil>"
	if c.CredentialsProvider != nil {
		credsProvider = fmt.Sprintf("<set:%T>", c.CredentialsProvider)
	}
	return fmt.Sprintf(
		"s3.Config{Bucket:%q Region:%q Endpoint:%q Profile:%q Anonymous:%t CredentialsProvider:%s AccessKeyID:%s SecretAccessKey:%s ForcePathStyle:%t MaxKeys:%d MaxIdleConnsPerHost:%d MaxConnsPerHost:%d}",
		c.Bucket,
		c.Region,
		c.Endpoint,
		c.Profile,
		c.Anonymous,
		credsProvider,
		redactedCredential(c.AccessKeyID),
		redactedCredential(c.SecretAccessKey),
		c.ForcePathStyle,
		c.MaxKeys,
		c.MaxIdleConnsPerHost,
		c.MaxConnsPerHost,
	)
}

// GoString returns a redacted representation for %#v formatting.
func (c Config) GoString() string {
	return c.String()
}

func redactedCredential(value string) string {
	if value == "" {
		return "<empty>"
	}
	return "<redacted>"
}

// ConfigError represents a configuration validation error.
type ConfigError struct {
	Field   string
	Message string
}

// Error implements the error interface.
func (e *ConfigError) Error() string {
	return "s3 config: " + e.Field + ": " + e.Message
}
