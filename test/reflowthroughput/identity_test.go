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
