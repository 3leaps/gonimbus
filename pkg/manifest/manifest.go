// Package manifest provides loading and validation of gonimbus job manifests.
//
// A job manifest is a YAML or JSON file that configures all aspects of a
// crawl job: provider connection, pattern matching, crawl behavior, and output.
//
// Manifests are validated against a JSON Schema to ensure correctness before
// execution. The schema enforces strict typing and disallows unknown properties.
//
// Example manifest (YAML):
//
//	version: "1.0"
//	connection:
//	  provider: s3
//	  bucket: my-data-bucket
//	  region: us-east-1
//	match:
//	  includes:
//	    - "data/2024/**/*.parquet"
//	  excludes:
//	    - "**/_temporary/**"
//	crawl:
//	  concurrency: 4
//	output:
//	  destination: stdout
//	  progress: true
package manifest

// Manifest represents a validated job manifest.
//
// A manifest configures all aspects of a crawl job. Required fields are
// Version, Connection, and Match. Crawl and Output are optional with sensible
// defaults.
type Manifest struct {
	// Schema is an optional JSON Schema reference for editor support.
	// Example: "https://schemas.3leaps.dev/gonimbus/v1.0.0/job-manifest.schema.json"
	Schema string `json:"$schema,omitempty" yaml:"$schema,omitempty"`

	// Version is the manifest schema version. Must be "1.0".
	Version string `json:"version" yaml:"version"`

	// Connection configures the cloud storage provider.
	Connection ConnectionConfig `json:"connection" yaml:"connection"`

	// Match configures object filtering by glob patterns.
	Match MatchConfig `json:"match" yaml:"match"`

	// Crawl configures crawl behavior (optional).
	Crawl CrawlConfig `json:"crawl,omitempty" yaml:"crawl,omitempty"`

	// Output configures output destination and format (optional).
	Output OutputConfig `json:"output,omitempty" yaml:"output,omitempty"`
}

// ConnectionConfig configures the cloud storage provider connection.
type ConnectionConfig struct {
	// Provider is the storage provider type. Currently only "s3" is supported.
	Provider string `json:"provider" yaml:"provider"`

	// Bucket is the bucket name to crawl.
	Bucket string `json:"bucket" yaml:"bucket"`

	// Region is the AWS region (e.g., "us-east-1"). Optional.
	Region string `json:"region,omitempty" yaml:"region,omitempty"`

	// Endpoint is a custom endpoint URL for S3-compatible storage. Optional.
	// Example: "https://s3.wasabisys.com"
	Endpoint string `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`

	// Profile is the AWS credential profile name. Optional.
	Profile string `json:"profile,omitempty" yaml:"profile,omitempty"`
}

// MatchConfig configures object filtering by glob patterns and metadata filters.
type MatchConfig struct {
	// Includes is a list of glob patterns for objects to include.
	// At least one pattern is required.
	Includes []string `json:"includes" yaml:"includes"`

	// Excludes is a list of glob patterns for objects to exclude. Optional.
	Excludes []string `json:"excludes,omitempty" yaml:"excludes,omitempty"`

	// IncludeHidden includes hidden files (starting with .). Default: false.
	IncludeHidden bool `json:"include_hidden,omitempty" yaml:"include_hidden,omitempty"`

	// Filters specifies additional metadata-based filters. Optional.
	// Filters are applied after glob pattern matching with AND semantics.
	Filters *FilterConfig `json:"filters,omitempty" yaml:"filters,omitempty"`
}

// FilterConfig specifies metadata-based object filters.
// All filters are optional and compose with AND semantics.
type FilterConfig struct {
	// Size specifies min/max size constraints.
	// Supports human-readable values: "1KB", "100MiB", "1GB".
	Size *SizeFilterConfig `json:"size,omitempty" yaml:"size,omitempty"`

	// Modified specifies last-modified date range constraints.
	// Dates are in ISO 8601 format: "2024-01-15" or "2024-01-15T10:30:00Z".
	Modified *DateFilterConfig `json:"modified,omitempty" yaml:"modified,omitempty"`

	// ContentType specifies allowed MIME types.
	// Requires enrichment (HEAD calls) to evaluate.
	ContentType []string `json:"content_type,omitempty" yaml:"content_type,omitempty"`

	// KeyRegex is a regex pattern applied to object keys after glob matching.
	// Use for patterns not expressible with globs, e.g., "TXN-\\d{8}".
	KeyRegex string `json:"key_regex,omitempty" yaml:"key_regex,omitempty"`
}

// SizeFilterConfig specifies size constraints.
type SizeFilterConfig struct {
	// Min is the minimum size (inclusive).
	// Supports: raw bytes "1024", base-10 "1KB", base-2 "1KiB".
	Min string `json:"min,omitempty" yaml:"min,omitempty"`

	// Max is the maximum size (inclusive).
	Max string `json:"max,omitempty" yaml:"max,omitempty"`
}

// DateFilterConfig specifies date range constraints.
type DateFilterConfig struct {
	// After filters to objects modified at or after this time (inclusive).
	After string `json:"after,omitempty" yaml:"after,omitempty"`

	// Before filters to objects modified before this time (exclusive end).
	Before string `json:"before,omitempty" yaml:"before,omitempty"`
}

// CrawlConfig configures crawl behavior.
//
// All fields are optional with sensible defaults applied during loading.
type CrawlConfig struct {
	// Concurrency is the number of concurrent list operations.
	// Range: 1-32. Default: 4.
	Concurrency int `json:"concurrency,omitempty" yaml:"concurrency,omitempty"`

	// RateLimit is the maximum requests per second (0 = unlimited).
	// Default: 0.
	RateLimit float64 `json:"rate_limit,omitempty" yaml:"rate_limit,omitempty"`

	// ProgressEvery controls progress record frequency.
	// A progress record is emitted every N matched objects.
	// Default: 1000.
	ProgressEvery int `json:"progress_every,omitempty" yaml:"progress_every,omitempty"`

	// Preflight configures permission checks and provider probes.
	//
	// This is part of the plan/inspect/execute model for long-running jobs.
	Preflight PreflightConfig `json:"preflight,omitempty" yaml:"preflight,omitempty"`
}

// PreflightConfig controls how aggressively gonimbus probes permissions.
//
// Preflight is a capability contract, not a data operation.
// - plan-only: no provider calls
// - read-safe: no writes/deletes
// - write-probe: explicit opt-in minimal side effects (for transfer operations)
//
// Note: write-probe fields are included for schema consistency, but crawl jobs
// should generally use plan-only/read-safe.
//
// Values are schema-validated.
type PreflightConfig struct {
	Mode          string `json:"mode,omitempty" yaml:"mode,omitempty"`
	ProbeStrategy string `json:"probe_strategy,omitempty" yaml:"probe_strategy,omitempty"`
	ProbePrefix   string `json:"probe_prefix,omitempty" yaml:"probe_prefix,omitempty"`
}

// OutputConfig configures output destination and format.
//
// All fields are optional with sensible defaults applied during loading.
type OutputConfig struct {
	// Destination is the output target.
	// Values: "stdout" or "file:/path/to/output.jsonl"
	// Default: "stdout".
	Destination string `json:"destination,omitempty" yaml:"destination,omitempty"`

	// Progress enables progress record emission during crawl.
	// Default: true.
	Progress *bool `json:"progress,omitempty" yaml:"progress,omitempty"`
}

// Default values for optional configuration fields.
const (
	// DefaultVersion is the current manifest schema version.
	DefaultVersion = "1.0"

	// DefaultConcurrency is the default number of concurrent list operations.
	DefaultConcurrency = 4

	// DefaultRateLimit is the default rate limit (0 = unlimited).
	DefaultRateLimit = 0.0

	// DefaultProgressEvery is the default progress emission frequency.
	DefaultProgressEvery = 1000

	// DefaultDestination is the default output destination.
	DefaultDestination = "stdout"

	// DefaultProgress is the default value for progress emission.
	DefaultProgress = true

	// DefaultPreflightMode is the default preflight mode.
	DefaultPreflightMode = "read-safe"

	// DefaultProbeStrategy is the default provider probe strategy.
	DefaultProbeStrategy = "multipart-abort"

	// DefaultProbePrefix is the default prefix under which probe keys are created.
	DefaultProbePrefix = "_gonimbus/probe/"
)

// ApplyDefaults fills in default values for optional fields.
//
// This should be called after loading and validating the manifest to ensure
// all optional fields have sensible values.
func (m *Manifest) ApplyDefaults() {
	// Crawl defaults
	if m.Crawl.Concurrency == 0 {
		m.Crawl.Concurrency = DefaultConcurrency
	}
	if m.Crawl.ProgressEvery == 0 {
		m.Crawl.ProgressEvery = DefaultProgressEvery
	}
	// RateLimit: 0 is a valid value (unlimited), so no default needed

	// Preflight defaults (schema applies defaults too, but we normalize here
	// so callers don't need to reason about empty strings).
	if m.Crawl.Preflight.Mode == "" {
		m.Crawl.Preflight.Mode = DefaultPreflightMode
	}
	if m.Crawl.Preflight.ProbeStrategy == "" {
		m.Crawl.Preflight.ProbeStrategy = DefaultProbeStrategy
	}
	if m.Crawl.Preflight.ProbePrefix == "" {
		m.Crawl.Preflight.ProbePrefix = DefaultProbePrefix
	}

	// Output defaults
	if m.Output.Destination == "" {
		m.Output.Destination = DefaultDestination
	}
	if m.Output.Progress == nil {
		defaultProgress := DefaultProgress
		m.Output.Progress = &defaultProgress
	}
}

// ProgressEnabled returns whether progress records should be emitted.
// Returns the configured value, or DefaultProgress if not set.
func (o *OutputConfig) ProgressEnabled() bool {
	if o.Progress == nil {
		return DefaultProgress
	}
	return *o.Progress
}
