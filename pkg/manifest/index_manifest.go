package manifest

import (
	"encoding/json"
	"fmt"
	"os"

	schemasassets "github.com/3leaps/gonimbus/internal/assets/schemas"
	"github.com/fulmenhq/gofulmen/schema"
	"gopkg.in/yaml.v3"
)

// IndexManifest represents a validated index build manifest.
//
// A manifest configures all aspects of an index build job.
// Required fields are Version and Connection.
type IndexManifest struct {
	// Schema is an optional JSON Schema reference for editor support.
	Schema string `json:"$schema,omitempty" yaml:"$schema,omitempty"`

	// Version is the manifest schema version. Must be "1.0".
	Version string `json:"version" yaml:"version"`

	// Connection configures cloud storage provider and base URI.
	Connection IndexConnectionConfig `json:"connection" yaml:"connection"`

	// Identity provides explicit provider identity (ENTARCH: never infer as authoritative).
	Identity *IndexIdentityConfig `json:"identity,omitempty" yaml:"identity,omitempty"`

	// Build configures to index build behavior.
	Build *IndexBuildConfig `json:"build,omitempty" yaml:"build,omitempty"`

	// PathDate configures to date extraction from object key paths.
	PathDate *PathDateConfig `json:"path_date,omitempty" yaml:"path_date,omitempty"`
}

// IndexConnectionConfig configures to cloud storage connection.
type IndexConnectionConfig struct {
	// Provider is the storage provider type. Currently only "s3" is supported.
	Provider string `json:"provider" yaml:"provider"`

	// Bucket is the bucket name to index.
	Bucket string `json:"bucket" yaml:"bucket"`

	// BaseURI is the base prefix URI for the index. Must end with '/'.
	BaseURI string `json:"base_uri" yaml:"base_uri"`

	// Region is the AWS region. Optional.
	Region string `json:"region,omitempty" yaml:"region,omitempty"`

	// Endpoint is a custom endpoint URL for S3-compatible storage. Optional.
	Endpoint string `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`

	// Profile is the AWS credential profile name. Optional.
	Profile string `json:"profile,omitempty" yaml:"profile,omitempty"`
}

// IndexIdentityConfig provides explicit provider identity.
type IndexIdentityConfig struct {
	// StorageProvider is the canonical backend/variant (e.g., aws_s3, r2, wasabi).
	StorageProvider string `json:"storage_provider,omitempty" yaml:"storage_provider,omitempty"`

	// CloudProvider is the broader cloud (e.g., aws, gcp, azure).
	CloudProvider string `json:"cloud_provider,omitempty" yaml:"cloud_provider,omitempty"`

	// RegionKind disambiguates region naming schemes (e.g., aws, gcp, azure).
	RegionKind string `json:"region_kind,omitempty" yaml:"region_kind,omitempty"`

	// Region is the region name.
	Region string `json:"region,omitempty" yaml:"region,omitempty"`

	// EndpointHost is the host part of the endpoint URL.
	EndpointHost string `json:"endpoint_host,omitempty" yaml:"endpoint_host,omitempty"`
}

// IndexBuildConfig configures to index build behavior.
type IndexBuildConfig struct {
	// Source is the index source type (crawl|inventory|auto).
	// v0.1.3: crawl-only, inventory reserved for future.
	Source string `json:"source,omitempty" yaml:"source,omitempty"`

	// Match configures to object filtering during build (defaults to index everything).
	Match *IndexMatchConfig `json:"match,omitempty" yaml:"match,omitempty"`

	// Crawl configures to crawl-specific build settings.
	Crawl *IndexCrawlBuildConfig `json:"crawl,omitempty" yaml:"crawl,omitempty"`
}

// IndexMatchConfig configures to object matching for index build.
type IndexMatchConfig struct {
	// Includes is a list of glob patterns for objects to include.
	// Default: ["**"] (index everything under base_uri).
	Includes []string `json:"includes,omitempty" yaml:"includes,omitempty"`

	// Excludes is a list of glob patterns for objects to exclude. Optional.
	Excludes []string `json:"excludes,omitempty" yaml:"excludes,omitempty"`

	// IncludeHidden includes hidden files (starting with .). Default: false.
	IncludeHidden bool `json:"include_hidden,omitempty" yaml:"include_hidden,omitempty"`

	// Filters specifies additional metadata-based filters. Optional.
	Filters *FilterConfig `json:"filters,omitempty" yaml:"filters,omitempty"`
}

// IndexCrawlBuildConfig configures to crawl-specific build behavior.
type IndexCrawlBuildConfig struct {
	// Concurrency is the number of concurrent list operations.
	// Range: 1-32. Default: 4.
	Concurrency int `json:"concurrency,omitempty" yaml:"concurrency,omitempty"`

	// RateLimit is the maximum requests per second (0 = unlimited).
	// Default: 0.
	RateLimit float64 `json:"rate_limit,omitempty" yaml:"rate_limit,omitempty"`

	// ProgressEvery controls the progress emission frequency.
	// A progress record is emitted every N indexed objects.
	// Default: 1000.
	ProgressEvery int `json:"progress_every,omitempty" yaml:"progress_every,omitempty"`
}

// PathDateConfig configures to date extraction from object key paths.
type PathDateConfig struct {
	// Method is the extraction method (regex or segment).
	Method string `json:"method,omitempty" yaml:"method,omitempty"`

	// Regex is the regex pattern for extracting date from key path.
	Regex string `json:"regex,omitempty" yaml:"regex,omitempty"`

	// SegmentIndex is the zero-indexed segment number containing date.
	SegmentIndex int `json:"segment_index,omitempty" yaml:"segment_index,omitempty"`
}

// Index manifest defaults.
const (
	// DefaultIndexVersion is the current index manifest schema version.
	DefaultIndexVersion = "1.0"

	// DefaultIndexSource is the default index source type.
	DefaultIndexSource = "crawl"

	// DefaultIndexConcurrency is the default number of concurrent list operations for index build.
	DefaultIndexConcurrency = 4

	// DefaultIndexProgressEvery is the default progress emission frequency for index build.
	DefaultIndexProgressEvery = 1000

	// DefaultIndexIncludes is the default include pattern (index everything).
	DefaultIndexIncludes = "**"
)

// ApplyDefaults fills in default values for optional index manifest fields.
func (m *IndexManifest) ApplyDefaults() {
	// Build defaults
	if m.Build == nil {
		m.Build = &IndexBuildConfig{}
	}
	if m.Build.Source == "" {
		m.Build.Source = DefaultIndexSource
	}

	// Match defaults
	if m.Build.Match == nil {
		m.Build.Match = &IndexMatchConfig{}
	}
	if len(m.Build.Match.Includes) == 0 {
		m.Build.Match.Includes = []string{DefaultIndexIncludes}
	}

	// Crawl defaults
	if m.Build.Crawl == nil {
		m.Build.Crawl = &IndexCrawlBuildConfig{}
	}
	if m.Build.Crawl.Concurrency == 0 {
		m.Build.Crawl.Concurrency = DefaultIndexConcurrency
	}
	if m.Build.Crawl.ProgressEvery == 0 {
		m.Build.Crawl.ProgressEvery = DefaultIndexProgressEvery
	}
}

// LoadIndexManifest loads and validates an index manifest from given file path.
func LoadIndexManifest(path string) (*IndexManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("index manifest file not found: %s", path)
		}
		if os.IsPermission(err) {
			return nil, fmt.Errorf("permission denied reading index manifest: %s", path)
		}
		return nil, fmt.Errorf("failed to read index manifest file: %w", err)
	}

	return LoadIndexManifestFromBytes(data, path)
}

// LoadIndexManifestFromBytes parses and validates an index manifest from raw bytes.
func LoadIndexManifestFromBytes(data []byte, path string) (*IndexManifest, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("index manifest file is empty")
	}

	jsonData, err := indexYamlToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("failed to convert index manifest to JSON: %w", err)
	}

	if err := validateIndexManifestRaw(jsonData); err != nil {
		return nil, err
	}

	var manifest IndexManifest
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("invalid YAML in index manifest: %w", err)
	}

	manifest.ApplyDefaults()
	return &manifest, nil
}

// validateIndexManifestRaw validates raw JSON data against index manifest schema.
func validateIndexManifestRaw(jsonData []byte) error {
	if len(schemasassets.IndexManifestSchema) == 0 {
		return fmt.Errorf("embedded index-manifest schema is empty")
	}

	validator, err := schema.NewValidator(schemasassets.IndexManifestSchema)
	if err != nil {
		return fmt.Errorf("failed to compile index manifest schema: %w", err)
	}

	diags, err := validator.ValidateJSON(jsonData)
	if err != nil {
		return fmt.Errorf("schema validation error: %w", err)
	}

	if len(diags) == 0 {
		return nil
	}

	var errs ValidationErrors
	for _, d := range diags {
		if d.Severity == schema.SeverityError {
			errs = append(errs, ValidationError{
				Path:    d.Pointer,
				Message: d.Message,
			})
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return errs
}

// indexYamlToJSON converts YAML data to JSON.
func indexYamlToJSON(data []byte) ([]byte, error) {
	var raw any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid YAML in index manifest: %w", err)
	}

	jsonData, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to convert index manifest to JSON: %w", err)
	}

	return jsonData, nil
}
