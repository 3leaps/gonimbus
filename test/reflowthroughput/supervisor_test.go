package reflowthroughput

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// fakeFailBinary writes a tiny shell script that exits nonzero.
func writeFailScript(t *testing.T, dir, name string, code int) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("script helper is unix-focused")
	}
	path := filepath.Join(dir, name)
	body := "#!/bin/sh\nexit " + itoa(code) + "\n"
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestRunReflowOnlyNonzeroExit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bin := writeFailScript(t, dir, "fail-bin", 7)
	input := filepath.Join(dir, "in.jsonl")
	if err := os.WriteFile(input, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout := filepath.Join(dir, "out.jsonl")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pr, err := RunReflowOnly(ctx, StageRunOpts{
		Binary:         bin,
		InputPath:      input,
		DestURI:        "file://" + dir + "/d/",
		Parallel:       1,
		CheckpointPath: filepath.Join(dir, "c.db"),
		StdoutPath:     stdout,
		NoAdaptive:     true,
	})
	if err == nil {
		t.Fatal("expected nonzero exit error")
	}
	if pr.Stages["reflow"].ExitCode != 7 {
		t.Fatalf("exit=%d", pr.Stages["reflow"].ExitCode)
	}
}

func TestRunProbeDrainCancelOnTapMalformed(t *testing.T) {
	// Requires a real gonimbus for probe; skip if not building here.
	// Covered indirectly by tap unit tests + fullpipe smoke.
	t.Skip("integration path exercised via PROFILE=probe-saturation; unit tap covers malformed")
}

func TestChildEnvExtraOverrides(t *testing.T) {
	t.Parallel()
	got := ChildEnv([]string{"AWS_ACCESS_KEY_ID=old", "PATH=/bin"}, "", "AWS_ACCESS_KEY_ID=new")
	found := false
	for _, kv := range got {
		if kv == "AWS_ACCESS_KEY_ID=old" {
			t.Fatal("old key should be replaced")
		}
		if kv == "AWS_ACCESS_KEY_ID=new" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing override: %v", got)
	}
}

// Ensure we can exec a real binary path existence check used by harness.
func TestBuildBinaryProducesExecutable(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	if runtime.GOOS == "windows" {
		t.Skip("unix")
	}
	repo := repoRoot(t)
	out := filepath.Join(t.TempDir(), "gn")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	// Use ldflags like Make so version/commit are meaningful when available.
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, "./cmd/gonimbus")
	cmd.Dir = repo
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, b)
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatal(err)
	}
}
