package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Load reads and validates a manifest from the given file path.
//
// The file format is determined by extension: .yaml/.yml for YAML, .json for JSON.
// If the extension is unrecognized, YAML is attempted first, then JSON.
//
// After loading, the manifest is validated against the JSON schema, and
// defaults are applied to optional fields.
//
// Returns an error if:
//   - The file cannot be read (not found, permission denied, etc.)
//   - The file content is not valid YAML or JSON
//   - The manifest fails schema validation
func Load(path string) (*Manifest, error) {
	// Read file contents
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("manifest file not found: %s", path)
		}
		if os.IsPermission(err) {
			return nil, fmt.Errorf("permission denied reading manifest: %s", path)
		}
		return nil, fmt.Errorf("failed to read manifest file: %w", err)
	}

	return LoadFromBytes(data, path)
}

// LoadFromBytes parses and validates a manifest from raw bytes.
//
// The path parameter is used for error messages and format detection.
// If path is empty, format detection falls back to trying YAML first.
//
// Validation is performed on the raw data (converted to JSON) before parsing
// into the typed struct. This ensures strict validation including rejection
// of unknown fields (additionalProperties: false in the schema).
func LoadFromBytes(data []byte, path string) (*Manifest, error) {
	if len(data) == 0 {
		return nil, errors.New("manifest file is empty")
	}

	// Convert to JSON for schema validation
	// This preserves all fields including unknown ones for additionalProperties check
	jsonData, err := toJSON(data, path)
	if err != nil {
		return nil, err
	}

	// Validate raw JSON against schema BEFORE parsing into struct
	// This catches unknown fields that would be silently ignored by struct unmarshaling
	if err := ValidateRaw(jsonData); err != nil {
		return nil, err
	}

	// Parse into typed struct
	manifest, err := parseManifest(data, path)
	if err != nil {
		return nil, err
	}

	// Apply defaults
	manifest.ApplyDefaults()

	return manifest, nil
}

// LoadFromReader reads and validates a manifest from an io.Reader.
//
// The path parameter is used for error messages and format detection.
// If path is empty, format detection falls back to trying YAML first.
func LoadFromReader(r io.Reader, path string) (*Manifest, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}
	return LoadFromBytes(data, path)
}

// parseManifest parses the manifest data based on file extension.
func parseManifest(data []byte, path string) (*Manifest, error) {
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".json":
		return parseJSON(data)
	case ".yaml", ".yml":
		return parseYAML(data)
	default:
		// Unknown extension: try YAML first (more permissive), then JSON
		manifest, yamlErr := parseYAML(data)
		if yamlErr == nil {
			return manifest, nil
		}
		manifest, jsonErr := parseJSON(data)
		if jsonErr == nil {
			return manifest, nil
		}
		// Both failed - return YAML error as it's the preferred format
		return nil, fmt.Errorf("failed to parse manifest (tried YAML and JSON): %w", yamlErr)
	}
}

// parseJSON parses manifest data as JSON.
func parseJSON(data []byte) (*Manifest, error) {
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("invalid JSON in manifest: %w", err)
	}
	return &manifest, nil
}

// parseYAML parses manifest data as YAML.
func parseYAML(data []byte) (*Manifest, error) {
	var manifest Manifest
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("invalid YAML in manifest: %w", err)
	}
	return &manifest, nil
}

// toJSON converts the input data to JSON format for schema validation.
// If the data is YAML, it's converted to JSON. If already JSON, it's returned as-is.
func toJSON(data []byte, path string) ([]byte, error) {
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".json":
		// Already JSON, but validate it's valid JSON
		var raw any
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, fmt.Errorf("invalid JSON in manifest: %w", err)
		}
		return data, nil

	case ".yaml", ".yml":
		return yamlToJSON(data)

	default:
		// Try YAML first (superset of JSON)
		jsonData, err := yamlToJSON(data)
		if err == nil {
			return jsonData, nil
		}
		// Try raw JSON
		var raw any
		if jsonErr := json.Unmarshal(data, &raw); jsonErr == nil {
			return data, nil
		}
		return nil, fmt.Errorf("failed to parse manifest (tried YAML and JSON): %w", err)
	}
}

// yamlToJSON converts YAML data to JSON.
func yamlToJSON(data []byte) ([]byte, error) {
	// Parse YAML into generic structure
	var raw any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid YAML in manifest: %w", err)
	}

	// Convert to JSON
	jsonData, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to convert manifest to JSON: %w", err)
	}

	return jsonData, nil
}
