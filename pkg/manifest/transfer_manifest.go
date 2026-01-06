package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	schemasassets "github.com/3leaps/gonimbus/internal/assets/schemas"
	"github.com/fulmenhq/gofulmen/schema"
	"gopkg.in/yaml.v3"
)

// TransferSchemaID is the schema identifier for transfer manifests.
const TransferSchemaID = "gonimbus/v1.0.0/transfer-manifest"

// Transfer validation errors.
var (
	// ErrTransferSchemaNotFound indicates the transfer schema file could not be located.
	ErrTransferSchemaNotFound = errors.New("transfer manifest schema not found")

	// ErrTransferValidationFailed indicates the manifest failed schema validation.
	ErrTransferValidationFailed = errors.New("transfer manifest validation failed")
)

// TransferManifest represents a validated transfer job manifest.
//
// This is the contract for multi-step copy/move operations.
// Fields are schema-validated using the embedded transfer-manifest schema.
type TransferManifest struct {
	// Schema is an optional JSON Schema reference for editor support.
	Schema string `json:"$schema,omitempty" yaml:"$schema,omitempty"`

	// Version is the manifest schema version. Must be "1.0".
	Version string `json:"version" yaml:"version"`

	// Source configures the source storage provider.
	Source ConnectionConfig `json:"source" yaml:"source"`

	// Target configures the target storage provider.
	Target ConnectionConfig `json:"target" yaml:"target"`

	// Match configures which source objects are eligible for transfer.
	Match MatchConfig `json:"match" yaml:"match"`

	// Transfer configures transfer behavior.
	Transfer TransferConfig `json:"transfer" yaml:"transfer"`

	// Output configures output destination and format.
	Output OutputConfig `json:"output,omitempty" yaml:"output,omitempty"`
}

// TransferConfig configures copy/move behavior.
type TransferConfig struct {
	// Mode controls transfer semantics.
	// Values: "copy" or "move".
	Mode string `json:"mode,omitempty" yaml:"mode,omitempty"`

	// Concurrency controls concurrent transfer workers.
	Concurrency int `json:"concurrency,omitempty" yaml:"concurrency,omitempty"`

	// OnExists controls behavior when the target key already exists.
	// Values: "skip" | "overwrite" | "fail".
	OnExists string `json:"on_exists,omitempty" yaml:"on_exists,omitempty"`

	// Dedup configures deduplication.
	Dedup DedupConfig `json:"dedup,omitempty" yaml:"dedup,omitempty"`

	// PathTemplate optionally maps source keys to target keys.
	PathTemplate string `json:"path_template,omitempty" yaml:"path_template,omitempty"`

	// Preflight configures permission checks and provider probes.
	Preflight PreflightConfig `json:"preflight,omitempty" yaml:"preflight,omitempty"`
}

// DedupConfig controls deduplication behavior.
type DedupConfig struct {
	Enabled  *bool  `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Strategy string `json:"strategy,omitempty" yaml:"strategy,omitempty"`
}

const (
	// DefaultTransferConcurrency is the default concurrent transfer workers.
	DefaultTransferConcurrency = 16

	// DefaultTransferMode is the default transfer mode.
	DefaultTransferMode = "copy"

	// DefaultOnExists is the default on-exists behavior.
	DefaultOnExists = "skip"

	// DefaultDedupEnabled is the default for dedup enabled.
	DefaultDedupEnabled = true

	// DefaultDedupStrategy is the default dedup strategy.
	DefaultDedupStrategy = "etag"
)

// ApplyDefaults fills in default values for optional fields.
func (m *TransferManifest) ApplyDefaults() {
	if m.Transfer.Concurrency == 0 {
		m.Transfer.Concurrency = DefaultTransferConcurrency
	}
	if m.Transfer.Mode == "" {
		m.Transfer.Mode = DefaultTransferMode
	}
	if m.Transfer.OnExists == "" {
		m.Transfer.OnExists = DefaultOnExists
	}
	if m.Transfer.Dedup.Enabled == nil {
		defaultEnabled := DefaultDedupEnabled
		m.Transfer.Dedup.Enabled = &defaultEnabled
	}
	if m.Transfer.Dedup.Strategy == "" {
		m.Transfer.Dedup.Strategy = DefaultDedupStrategy
	}
	if m.Transfer.Preflight.Mode == "" {
		m.Transfer.Preflight.Mode = DefaultPreflightMode
	}
	if m.Transfer.Preflight.ProbeStrategy == "" {
		m.Transfer.Preflight.ProbeStrategy = DefaultProbeStrategy
	}
	if m.Transfer.Preflight.ProbePrefix == "" {
		m.Transfer.Preflight.ProbePrefix = DefaultProbePrefix
	}

	if m.Output.Destination == "" {
		m.Output.Destination = DefaultDestination
	}
	if m.Output.Progress == nil {
		defaultProgress := DefaultProgress
		m.Output.Progress = &defaultProgress
	}
}

// DedupEnabled returns whether deduplication is enabled.
func (d DedupConfig) DedupEnabled() bool {
	if d.Enabled == nil {
		return DefaultDedupEnabled
	}
	return *d.Enabled
}

// ValidateTransferRaw checks raw JSON data against the transfer manifest schema.
func ValidateTransferRaw(jsonData []byte) error {
	v, err := getTransferValidator()
	if err != nil {
		return err
	}

	diags, err := v.ValidateJSON(jsonData)
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

	// Override unwrap target to preserve existing error behavior expectations.
	return transferValidationErrors(errs)
}

// ValidateTransfer validates a typed TransferManifest by round-tripping to JSON.
func ValidateTransfer(m *TransferManifest) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to serialize transfer manifest for validation: %w", err)
	}
	return ValidateTransferRaw(data)
}

// LoadTransferFromBytes parses and validates a transfer manifest from raw bytes.
func LoadTransferFromBytes(data []byte, path string) (*TransferManifest, error) {
	if len(data) == 0 {
		return nil, errors.New("transfer manifest file is empty")
	}

	jsonData, err := toJSON(data, path)
	if err != nil {
		return nil, err
	}

	if err := ValidateTransferRaw(jsonData); err != nil {
		return nil, err
	}

	manifest, err := parseTransferManifest(data, path)
	if err != nil {
		return nil, err
	}

	manifest.ApplyDefaults()
	return manifest, nil
}

func parseTransferManifest(data []byte, path string) (*TransferManifest, error) {
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".json":
		var m TransferManifest
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("invalid JSON in transfer manifest: %w", err)
		}
		return &m, nil
	case ".yaml", ".yml":
		var m TransferManifest
		if err := yaml.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("invalid YAML in transfer manifest: %w", err)
		}
		return &m, nil
	default:
		// Unknown extension: try YAML first (more permissive), then JSON.
		var m TransferManifest
		yamlErr := yaml.Unmarshal(data, &m)
		if yamlErr == nil {
			return &m, nil
		}
		jsonErr := json.Unmarshal(data, &m)
		if jsonErr == nil {
			return &m, nil
		}
		return nil, fmt.Errorf("failed to parse transfer manifest (tried YAML and JSON): %w", yamlErr)
	}
}

// getTransferValidator returns a cached validator compiled from the embedded transfer schema.
func getTransferValidator() (*schema.Validator, error) {
	transferValidatorOnce.Do(func() {
		if len(schemasassets.TransferManifestSchema) == 0 {
			transferValidatorErr = fmt.Errorf("%w: embedded transfer-manifest schema is empty", ErrTransferSchemaNotFound)
			return
		}
		transferValidator, transferValidatorErr = schema.NewValidator(schemasassets.TransferManifestSchema)
		if transferValidatorErr != nil {
			transferValidatorErr = fmt.Errorf("failed to compile transfer manifest schema: %w", transferValidatorErr)
		}
	})
	return transferValidator, transferValidatorErr
}

var (
	transferValidatorOnce sync.Once
	transferValidator     *schema.Validator
	transferValidatorErr  error
)

// transferValidationErrors wraps ValidationErrors with transfer-specific unwrap semantics.
type transferValidationErrors ValidationErrors

func (e transferValidationErrors) Error() string {
	return ValidationErrors(e).Error()
}

func (e transferValidationErrors) Unwrap() error {
	return ErrTransferValidationFailed
}
