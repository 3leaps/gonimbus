package cmd

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/scope"
	"github.com/spf13/cobra"
)

func TestRunIndexMigrateMatchScope_ConvertibleJSON(t *testing.T) {
	dir := t.TempDir()
	job := filepath.Join(dir, "job.yaml")
	content := `version: "1.0"
connection:
  provider: s3
  bucket: example-bucket
  base_uri: "s3://example-bucket/data/"
identity:
  storage_provider: aws_s3
  cloud_provider: aws
  region_kind: aws
  region: us-east-1
build:
  source: crawl
  match:
    includes:
      - "cohort-a/**"
`
	if err := os.WriteFile(job, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	// Reset package-level flags for isolation.
	indexMigrateMatchScopeJob = job
	indexMigrateMatchScopeEmit = ""
	indexMigrateMatchScopeJSON = true
	indexMigrateMatchScopeForce = false
	versionInfo.Version = "0.4.1-test"

	stdout, stderr, err := captureStdoutStderr(func() error {
		return runIndexMigrateMatchScope(&cobra.Command{}, nil)
	})
	if err != nil {
		t.Fatalf("err=%v stderr=%s", err, stderr)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %s", stderr)
	}

	var plan scope.MigrationPlan
	if err := json.Unmarshal([]byte(stdout), &plan); err != nil {
		t.Fatalf("json: %v\n%s", err, stdout)
	}
	if plan.Classification != scope.ClassificationConvertible {
		t.Fatalf("classification=%s reason=%s detail=%s", plan.Classification, plan.ReasonCode, plan.Detail)
	}
	if plan.LegacyPlanCount != 1 || plan.ProposedPlanCount != 1 {
		t.Fatalf("counts legacy=%d proposed=%d", plan.LegacyPlanCount, plan.ProposedPlanCount)
	}
	if plan.LegacyConfigIdentity == nil || plan.ProposedConfigIdentity == nil {
		t.Fatal("expected identity evidence")
	}
	if plan.LegacyConfigIdentity.IndexSetID == plan.ProposedConfigIdentity.IndexSetID {
		t.Fatal("expected identity change")
	}
}

func TestRunIndexMigrateMatchScope_RefuseBeforeEmit(t *testing.T) {
	dir := t.TempDir()
	job := filepath.Join(dir, "job.yaml")
	emit := filepath.Join(dir, "out.yaml")
	content := `version: "1.0"
connection:
  provider: s3
  bucket: example-bucket
  base_uri: "s3://example-bucket/data/"
identity:
  storage_provider: aws_s3
  cloud_provider: aws
  region_kind: aws
  region: us-east-1
build:
  source: crawl
  match:
    includes:
      - "**/*.parquet"
`
	if err := os.WriteFile(job, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	indexMigrateMatchScopeJob = job
	indexMigrateMatchScopeEmit = emit
	indexMigrateMatchScopeJSON = false
	indexMigrateMatchScopeForce = false
	versionInfo.Version = "0.4.1-test"

	err := runIndexMigrateMatchScope(&cobra.Command{}, nil)
	if err == nil {
		t.Fatal("expected error for non-convertible emit")
	}
	if _, statErr := os.Stat(emit); !os.IsNotExist(statErr) {
		t.Fatalf("emit path must not be created on refuse: %v", statErr)
	}
	if !strings.Contains(err.Error(), "non_prefix_include") && !strings.Contains(err.Error(), "classification=refused") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunIndexMigrateMatchScope_EmitExclusive(t *testing.T) {
	dir := t.TempDir()
	job := filepath.Join(dir, "job.yaml")
	emit := filepath.Join(dir, "out.yaml")
	content := `version: "1.0"
connection:
  provider: s3
  bucket: example-bucket
  base_uri: "s3://example-bucket/data/"
identity:
  storage_provider: aws_s3
  cloud_provider: aws
  region_kind: aws
  region: us-east-1
build:
  source: crawl
  match:
    includes:
      - "cohort-a/**"
`
	if err := os.WriteFile(job, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	indexMigrateMatchScopeJob = job
	indexMigrateMatchScopeEmit = emit
	indexMigrateMatchScopeJSON = true
	indexMigrateMatchScopeForce = false
	versionInfo.Version = "0.4.1-test"

	if err := runIndexMigrateMatchScope(&cobra.Command{}, nil); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(emit)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("prefix_list")) {
		t.Fatalf("emit missing scope: %s", data)
	}
	if !bytes.Contains(data, []byte(`"**"`)) && !bytes.Contains(data, []byte("- '**'")) && !bytes.Contains(data, []byte("- **")) {
		// YAML may render as - '**' or - "**"
		if !bytes.Contains(data, []byte("**")) {
			t.Fatalf("emit missing default includes: %s", data)
		}
	}

	// Second exclusive write without force must fail and leave original intact.
	indexMigrateMatchScopeForce = false
	err = runIndexMigrateMatchScope(&cobra.Command{}, nil)
	if err == nil {
		t.Fatal("expected exclusive create failure")
	}
	data2, _ := os.ReadFile(emit)
	if !bytes.Equal(data, data2) {
		t.Fatal("emit path mutated after exclusive failure")
	}
}

func captureStdoutStderr(fn func() error) (string, string, error) {
	oldOut, oldErr := os.Stdout, os.Stderr
	rOut, wOut, err := os.Pipe()
	if err != nil {
		return "", "", err
	}
	rErr, wErr, err := os.Pipe()
	if err != nil {
		return "", "", err
	}
	os.Stdout, os.Stderr = wOut, wErr
	runErr := fn()
	_ = wOut.Close()
	_ = wErr.Close()
	os.Stdout, os.Stderr = oldOut, oldErr

	var outBuf, errBuf bytes.Buffer
	_, _ = outBuf.ReadFrom(rOut)
	_, _ = errBuf.ReadFrom(rErr)
	_ = rOut.Close()
	_ = rErr.Close()
	return outBuf.String(), errBuf.String(), runErr
}
