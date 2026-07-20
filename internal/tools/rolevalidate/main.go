// Command rolevalidate checks the role prompts in config/agentic/roles against
// the vendored role-prompt schema.
//
// The role files declare a $schema URL, but nothing verified that they conform
// to it: a malformed or drifted role file reached agents unchecked. Validation
// runs against the vendored copy rather than the published URL so the gate
// cannot pass by silently skipping when the network is unreachable.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"gopkg.in/yaml.v3"
)

const (
	defaultRolesDir   = "config/agentic/roles"
	defaultSchemaPath = "schemas/agentic/v0/role-prompt.schema.json"
)

func main() {
	rolesDir := flag.String("roles", defaultRolesDir, "directory holding role prompt YAML files")
	schemaPath := flag.String("schema", defaultSchemaPath, "path to the vendored role-prompt schema")
	flag.Parse()

	if err := run(*rolesDir, *schemaPath); err != nil {
		fmt.Fprintln(os.Stderr, "validate-roles:", err)
		os.Exit(1)
	}
}

func run(rolesDir, schemaPath string) error {
	schema, err := compileSchema(schemaPath)
	if err != nil {
		return err
	}

	files, err := roleFiles(rolesDir)
	if err != nil {
		return err
	}
	// An empty set would pass vacuously, which is the failure mode this gate
	// exists to prevent: a validator that validates nothing reports success.
	if len(files) == 0 {
		return fmt.Errorf("no role files found under %s", rolesDir)
	}

	var failures []string
	for _, file := range files {
		if err := validateFile(schema, file); err != nil {
			failures = append(failures, err.Error())
			continue
		}
		fmt.Printf("  ok %s\n", filepath.Base(file))
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d role file(s) failed validation:\n  - %s",
			len(failures), strings.Join(failures, "\n  - "))
	}
	fmt.Printf("✅ %d role prompt(s) conform to %s\n", len(files), schemaPath)
	return nil
}

func compileSchema(schemaPath string) (*jsonschema.Schema, error) {
	raw, err := os.ReadFile(schemaPath) // #nosec G304 -- repo-relative schema path
	if err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}
	c := jsonschema.NewCompiler()
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse schema: %w", err)
	}
	if err := c.AddResource(schemaPath, strings.NewReader(string(raw))); err != nil {
		return nil, fmt.Errorf("add schema resource: %w", err)
	}
	schema, err := c.Compile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("compile schema: %w", err)
	}
	return schema, nil
}

func roleFiles(rolesDir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(rolesDir, "*.yaml"))
	if err != nil {
		return nil, fmt.Errorf("scan roles: %w", err)
	}
	sort.Strings(matches)
	return matches, nil
}

// validateFile converts one YAML role prompt into the generic form the schema
// validator consumes. YAML is decoded and re-encoded through JSON so that
// document types match what the schema expects (notably map keys as strings).
func validateFile(schema *jsonschema.Schema, path string) error {
	raw, err := os.ReadFile(path) // #nosec G304 -- repo-relative role path
	if err != nil {
		return fmt.Errorf("%s: read: %v", filepath.Base(path), err)
	}
	var parsed any
	if err := yaml.Unmarshal(raw, &parsed); err != nil {
		return fmt.Errorf("%s: parse yaml: %v", filepath.Base(path), err)
	}
	encoded, err := json.Marshal(parsed)
	if err != nil {
		return fmt.Errorf("%s: convert to json: %v", filepath.Base(path), err)
	}
	var doc any
	if err := json.Unmarshal(encoded, &doc); err != nil {
		return fmt.Errorf("%s: decode json: %v", filepath.Base(path), err)
	}
	if err := schema.Validate(doc); err != nil {
		return fmt.Errorf("%s: %v", filepath.Base(path), err)
	}
	return nil
}
