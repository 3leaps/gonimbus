// Package s3 implements the provider interface for AWS S3 and S3-compatible storage.
package s3

// Config configures an S3 provider.
//
// Authentication priority (AWS SDK v2 default chain):
//  1. Explicit AccessKeyID/SecretAccessKey (if provided)
//  2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
//  3. Shared credentials file (~/.aws/credentials)
//  4. Shared config file (~/.aws/config) with profile
//  5. EC2 instance metadata / ECS task role / EKS IRSA
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

	// AccessKeyID is an explicit access key. If set, SecretAccessKey must also be set.
	// This takes precedence over the default credential chain.
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

	// If one explicit credential is set, both must be set
	if (c.AccessKeyID != "") != (c.SecretAccessKey != "") {
		return &ConfigError{
			Field:   "AccessKeyID/SecretAccessKey",
			Message: "both access key ID and secret access key must be provided together",
		}
	}

	return nil
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
