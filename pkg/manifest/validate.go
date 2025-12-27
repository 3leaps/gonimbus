package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	schemasassets "github.com/3leaps/gonimbus/internal/assets/schemas"
	"github.com/fulmenhq/gofulmen/schema"
)

// SchemaID is the schema identifier for job manifests.
const SchemaID = "gonimbus/v1.0.0/job-manifest"

// Validation errors
var (
	// ErrSchemaNotFound indicates the schema file could not be located.
	ErrSchemaNotFound = errors.New("manifest schema not found")

	// ErrValidationFailed indicates the manifest failed schema validation.
	ErrValidationFailed = errors.New("manifest validation failed")
)

// Cached validator instance (compiled once from embedded schema)
var (
	validatorOnce sync.Once
	validator     *schema.Validator
	validatorErr  error
)

// ValidationError represents a single validation issue.
type ValidationError struct {
	// Path is the JSON pointer to the problematic field (e.g., "/match/includes").
	Path string

	// Message describes the validation failure.
	Message string
}

// Error implements error interface.
func (e ValidationError) Error() string {
	if e.Path == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Path, e.Message)
}

// ValidationErrors is a collection of validation errors.
type ValidationErrors []ValidationError

// Error implements error interface.
func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return "validation failed"
	}
	if len(e) == 1 {
		return e[0].Error()
	}

	var b strings.Builder
	b.WriteString("manifest validation failed with ")
	b.WriteString(fmt.Sprintf("%d errors:\n", len(e)))
	for i, err := range e {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString("  - ")
		b.WriteString(err.Error())
	}
	return b.String()
}

// Unwrap returns the underlying error type.
func (e ValidationErrors) Unwrap() error {
	return ErrValidationFailed
}

// Validate checks the manifest against the JSON schema.
//
// Returns nil if validation succeeds, or a ValidationErrors with details
// about all validation failures.
//
// Note: This validates the struct representation, which loses unknown fields.
// For strict validation including additionalProperties checks, use ValidateRaw
// on the original input data.
func Validate(m *Manifest) error {
	// Convert manifest to JSON for schema validation
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to serialize manifest for validation: %w", err)
	}

	return ValidateRaw(data)
}

// ValidateRaw checks raw JSON data against the manifest schema.
//
// This function should be used when strict validation is needed, including
// rejection of unknown fields (additionalProperties: false). The raw JSON
// preserves all fields from the original input.
//
// The schema is embedded at compile time, so validation works correctly
// in installed binaries and library consumers without requiring schema
// files to be present on disk.
//
// Returns nil if validation succeeds, or a ValidationErrors with details
// about all validation failures.
func ValidateRaw(jsonData []byte) error {
	// Get or compile the validator from embedded schema
	v, err := getValidator()
	if err != nil {
		return err
	}

	// Validate against schema
	diags, err := v.ValidateJSON(jsonData)
	if err != nil {
		return fmt.Errorf("schema validation error: %w", err)
	}

	// Convert diagnostics to validation errors
	if len(diags) == 0 {
		return nil
	}

	var errs ValidationErrors
	for _, d := range diags {
		// Only include errors, not warnings
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

// getValidator returns a cached validator compiled from the embedded schema.
//
// The validator is compiled once on first use and cached for subsequent calls.
// This is thread-safe via sync.Once.
func getValidator() (*schema.Validator, error) {
	validatorOnce.Do(func() {
		if len(schemasassets.JobManifestSchema) == 0 {
			validatorErr = fmt.Errorf("%w: embedded job-manifest schema is empty", ErrSchemaNotFound)
			return
		}
		validator, validatorErr = schema.NewValidator(schemasassets.JobManifestSchema)
		if validatorErr != nil {
			validatorErr = fmt.Errorf("failed to compile manifest schema: %w", validatorErr)
		}
	})
	return validator, validatorErr
}
