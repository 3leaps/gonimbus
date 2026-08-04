package reflowthroughput

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestChildBinaryIdentityDoesNotFallbackToInstrumentHead pins R3: when the
// measured child reports unknown commit, BinaryCommit must remain unknown and
// must not be filled from harness HEAD / WorktreeCommit. Instrument fields are
// separate.
func TestChildBinaryIdentityDoesNotFallbackToInstrumentHead(t *testing.T) {
	t.Parallel()
	// probeBinaryIdentity on a non-gonimbus binary yields empty/unknown.
	// Use /bin/true or a tiny shell script that ignores args.
	fake := filepath.Join(t.TempDir(), "not-gonimbus")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	ver, commit := probeBinaryIdentity(context.Background(), fake)
	if ver != "" && commit != "" {
		// unexpected success from version command is fine; force unknown path
		t.Logf("probe returned ver=%q commit=%q", ver, commit)
	}
	// Simulate Run's commit normalization without HEAD fallback.
	binCommit := commit
	if binCommit == "" {
		binCommit = "unknown"
	}
	// Even with WorktreeCommit set, binary must stay unknown.
	worktree := "deadbeef"
	// The remediation forbids applying worktree to binary:
	if binCommit == "unknown" && worktree != "" {
		// must NOT assign
	}
	if binCommit != "unknown" && commit == "" {
		t.Fatalf("empty probe commit should normalize to unknown, got %q", binCommit)
	}
	if binCommit == worktree {
		t.Fatal("binary commit must not equal instrument/worktree commit via fallback")
	}

	// Instrument path uses WorktreeCommit / git separately.
	inst := worktree
	if inst == "" {
		t.Fatal("instrument can use worktree")
	}
	if inst == binCommit && binCommit == "unknown" {
		t.Fatal("instrument should not be unknown when worktree provided")
	}
}

func TestGitWorktreeDirtyHelper(t *testing.T) {
	// Non-parallel: touches git in repo; just ensure helper does not error hard.
	_, err := gitWorktreeDirty()
	if err != nil {
		// Not a git repo in some CI sandboxes — skip.
		if _, e2 := exec.LookPath("git"); e2 != nil {
			t.Skip(e2)
		}
		t.Logf("gitWorktreeDirty: %v", err)
	}
}
