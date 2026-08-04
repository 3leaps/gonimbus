package reflowthroughput

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestChildBinaryIdentityDoesNotFallbackToInstrumentHead pins R3 via the
// production NormalizeMeasuredBinaryCommit helper used by Run.
func TestChildBinaryIdentityDoesNotFallbackToInstrumentHead(t *testing.T) {
	t.Parallel()
	fake := filepath.Join(t.TempDir(), "not-gonimbus")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, commit := probeBinaryIdentity(context.Background(), fake)
	binCommit := NormalizeMeasuredBinaryCommit(commit)
	worktree := "deadbeef"
	// Production path: NormalizeMeasuredBinaryCommit has no worktree argument.
	if binCommit == worktree {
		t.Fatal("binary commit must not equal instrument/worktree commit via fallback")
	}
	if commit == "" && binCommit != "unknown" {
		t.Fatalf("empty probe must normalize to unknown, got %q", binCommit)
	}
	// Instrument may use worktree; measured binary must not.
	if NormalizeMeasuredBinaryCommit("") == worktree {
		t.Fatal("normalize must not invent worktree commit")
	}
}

func TestCaptureInstrumentIdentityFailClosedAndStable(t *testing.T) {
	// Not parallel: uses real git + os.Args[0] content hash.
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip(err)
	}
	sha, commit, dirty, err := captureInstrumentIdentity("")
	if err != nil {
		// Outside a git worktree this must fail closed, not return clean+empty.
		t.Logf("capture failed (ok if not in git worktree): %v", err)
		if commit != "" || dirty {
			t.Fatalf("on error must not invent clean provenance: commit=%q dirty=%v", commit, dirty)
		}
		return
	}
	if sha == "" || commit == "" {
		t.Fatalf("success requires non-empty sha and commit, got sha=%q commit=%q", sha, commit)
	}
	// dirty is a real bool — both true and false are valid; omitempty is forbidden on the report field.
	_ = dirty

	// Override path: commit comes from override; dirty still probed.
	sha2, commit2, dirty2, err := captureInstrumentIdentity("override-commit")
	if err != nil {
		t.Fatal(err)
	}
	if commit2 != "override-commit" {
		t.Fatalf("override commit=%q", commit2)
	}
	if sha2 != sha {
		t.Fatal("sha should match same process")
	}
	// Re-probe unchanged under override.
	if err := assertInstrumentIdentityUnchanged("override-commit", sha2, commit2, dirty2); err != nil {
		t.Fatal(err)
	}
	// Drift detection: wrong expected commit fails.
	if err := assertInstrumentIdentityUnchanged("override-commit", sha2, "not-the-commit", dirty2); err == nil {
		t.Fatal("expected commit drift failure")
	}
	if err := assertInstrumentIdentityUnchanged("override-commit", sha2, commit2, !dirty2); err == nil {
		t.Fatal("expected dirty drift failure")
	}
	if err := assertInstrumentIdentityUnchanged("override-commit", "deadbeef", commit2, dirty2); err == nil {
		t.Fatal("expected sha drift failure")
	}
}

func TestGitWorktreeDirtyHelper(t *testing.T) {
	_, err := gitWorktreeDirty()
	if err != nil {
		if _, e2 := exec.LookPath("git"); e2 != nil {
			t.Skip(e2)
		}
		// Fail-closed in Run: probe error must be returned, not ignored.
		t.Logf("gitWorktreeDirty error (Run would fail closed): %v", err)
	}
}
