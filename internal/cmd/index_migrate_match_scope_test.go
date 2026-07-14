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
	content := sampleConvertibleManifest()
	if err := os.WriteFile(job, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

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
	if err := os.WriteFile(job, []byte(content), 0o600); err != nil {
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

func TestWriteProposedManifestSafe_ExclusiveAndForce(t *testing.T) {
	dir := t.TempDir()
	job := filepath.Join(dir, "job.yaml")
	emit := filepath.Join(dir, "out.yaml")
	if err := os.WriteFile(job, []byte(sampleConvertibleManifest()), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := writeProposedManifestSafe(job, emit, "proposed: one\n", false); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(emit)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%o want 0600", info.Mode().Perm())
	}

	// Exclusive without force fails and leaves content intact.
	if err := writeProposedManifestSafe(job, emit, "proposed: two\n", false); err == nil {
		t.Fatal("expected exclusive failure")
	}
	data, _ := os.ReadFile(emit)
	if string(data) != "proposed: one\n" {
		t.Fatalf("content changed on exclusive fail: %q", data)
	}

	// Force replaces with 0600 even if prior mode was wider.
	if err := os.Chmod(emit, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeProposedManifestSafe(job, emit, "proposed: three\n", true); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(emit)
	if string(data) != "proposed: three\n" {
		t.Fatalf("force content=%q", data)
	}
	info, _ = os.Lstat(emit)
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("force mode=%o want 0600", info.Mode().Perm())
	}
}

func TestWriteProposedManifestSafe_NoForceNeverReplacesExisting(t *testing.T) {
	dir := t.TempDir()
	job := filepath.Join(dir, "job.yaml")
	emit := filepath.Join(dir, "out.yaml")
	if err := os.WriteFile(job, []byte(sampleConvertibleManifest()), 0o600); err != nil {
		t.Fatal(err)
	}
	// Pre-create destination with distinct content and permissive mode.
	if err := os.WriteFile(emit, []byte("original\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := writeProposedManifestSafe(job, emit, "should-not-land\n", false)
	if err == nil {
		t.Fatal("expected exclusive failure when destination already exists")
	}
	data, readErr := os.ReadFile(emit)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(data) != "original\n" {
		t.Fatalf("no-force path must not replace existing dest: got %q", data)
	}
	info, _ := os.Lstat(emit)
	if info.Mode().Perm() != 0o644 {
		// Mode should remain whatever was pre-created; exclusive path must not rewrite.
		t.Fatalf("pre-existing mode changed: %o", info.Mode().Perm())
	}
}

func TestWriteProposedManifestSafe_ExclusivePublishIsCompleteOrAbsent(t *testing.T) {
	dir := t.TempDir()
	job := filepath.Join(dir, "job.yaml")
	emit := filepath.Join(dir, "out.yaml")
	if err := os.WriteFile(job, []byte(sampleConvertibleManifest()), 0o600); err != nil {
		t.Fatal(err)
	}
	content := "complete-manifest-bytes\nline-two\n"
	if err := writeProposedManifestSafe(job, emit, content, false); err != nil {
		t.Fatal(err)
	}
	// Final path must be complete (hard-linked from fully written temp), not empty/partial.
	data, err := os.ReadFile(emit)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != content {
		t.Fatalf("final path incomplete or wrong: got %q want %q", data, content)
	}
	info, err := os.Lstat(emit)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("final path not regular: %v", info.Mode())
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("final mode=%o want 0600 (inherited from temp inode)", info.Mode().Perm())
	}
	// No temp leftovers in the emit directory.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".gonimbus-migrate-") {
			t.Fatalf("temp name left after exclusive publish: %s", e.Name())
		}
	}
	// Second exclusive attempt: dest remains complete and unchanged.
	before := data
	if err := writeProposedManifestSafe(job, emit, "other\n", false); err == nil {
		t.Fatal("expected second exclusive publish to fail")
	}
	after, _ := os.ReadFile(emit)
	if string(after) != string(before) {
		t.Fatalf("existing complete dest changed: %q", after)
	}
}

func TestWriteProposedManifestSafe_RefuseSourceAlias(t *testing.T) {
	dir := t.TempDir()
	job := filepath.Join(dir, "job.yaml")
	if err := os.WriteFile(job, []byte(sampleConvertibleManifest()), 0o600); err != nil {
		t.Fatal(err)
	}
	// Same path
	if err := writeProposedManifestSafe(job, job, "x\n", true); err == nil {
		t.Fatal("expected same-path refusal")
	}
	// Hard-link alias
	alias := filepath.Join(dir, "alias.yaml")
	if err := os.Link(job, alias); err != nil {
		t.Skipf("hard link not supported: %v", err)
	}
	if err := writeProposedManifestSafe(job, alias, "x\n", true); err == nil {
		t.Fatal("expected hard-link alias refusal")
	}
	// Symlink destination refused
	link := filepath.Join(dir, "link.yaml")
	target := filepath.Join(dir, "target.yaml")
	if err := os.WriteFile(target, []byte("old\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := writeProposedManifestSafe(job, link, "new\n", true); err == nil {
		t.Fatal("expected symlink refusal")
	}
	// Target must be untouched (no-follow).
	data, _ := os.ReadFile(target)
	if string(data) != "old\n" {
		t.Fatalf("symlink target mutated: %q", data)
	}
}

func TestRunIndexMigrateMatchScope_EmitExclusive(t *testing.T) {
	dir := t.TempDir()
	job := filepath.Join(dir, "job.yaml")
	emit := filepath.Join(dir, "out.yaml")
	if err := os.WriteFile(job, []byte(sampleConvertibleManifest()), 0o600); err != nil {
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
	if !bytes.Contains(data, []byte("**")) {
		t.Fatalf("emit missing default includes: %s", data)
	}

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

func sampleConvertibleManifest() string {
	return `version: "1.0"
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
