package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const repoSchema = "../../../schemas/agentic/v0/role-prompt.schema.json"

// validRole is the minimum a role prompt must carry to satisfy the schema.
const validRole = `slug: probe
name: Probe Role
description: Fixture role for validator tests
version: 1.0.0
status: review
scope:
  - Fixture scope
responsibilities:
  - Fixture responsibility
escalates_to:
  - target: human maintainers
    when: Fixture escalation
does_not:
  - Fixture prohibition
`

func writeRole(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// The committed role prompts must actually conform — this is the assertion the
// repository previously lacked entirely.
func TestRepositoryRolesConform(t *testing.T) {
	if err := run("../../../config/agentic/roles", repoSchema); err != nil {
		t.Fatalf("committed role prompts must validate: %v", err)
	}
}

// Negative controls: each mutation must fail. A validator never observed
// rejecting anything is configured, not proven.
func TestValidatorRejectsMalformedRoles(t *testing.T) {
	cases := map[string]string{
		"missing required field": strings.Replace(validRole, "version: 1.0.0\n", "", 1),
		"undeclared property":    validRole + "invented_field: nope\n",
		"status outside enum":    strings.Replace(validRole, "status: review", "status: blessed", 1),
		"version not semver":     strings.Replace(validRole, "version: 1.0.0", "version: 1.0", 1),
		"wrong type for scope":   strings.Replace(validRole, "scope:\n  - Fixture scope\n", "scope: a-string-not-a-list\n", 1),
		"malformed yaml":         "slug: probe\n  bad-indent: [",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeRole(t, dir, "probe.yaml", body)
			if err := run(dir, repoSchema); err == nil {
				t.Fatalf("expected %s to be rejected", name)
			}
		})
	}
}

// A valid fixture must pass, so the rejections above are attributable to the
// mutation rather than to the harness refusing everything.
func TestValidatorAcceptsValidRole(t *testing.T) {
	dir := t.TempDir()
	writeRole(t, dir, "probe.yaml", validRole)
	if err := run(dir, repoSchema); err != nil {
		t.Fatalf("valid role must pass: %v", err)
	}
}

// An empty directory must fail rather than pass vacuously: a gate that
// validates nothing and reports success is the defect this tool guards against.
func TestValidatorRefusesEmptyRoleSet(t *testing.T) {
	if err := run(t.TempDir(), repoSchema); err == nil {
		t.Fatal("expected an empty role directory to be refused")
	}
}

func TestValidatorRefusesMissingSchema(t *testing.T) {
	dir := t.TempDir()
	writeRole(t, dir, "probe.yaml", validRole)
	if err := run(dir, filepath.Join(dir, "absent.schema.json")); err == nil {
		t.Fatal("expected a missing schema to be refused")
	}
}
