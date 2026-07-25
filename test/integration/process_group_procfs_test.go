//go:build !windows

package integration

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// The procfs reader is the path CI takes, on a container image with no ps, while
// development hosts take the ps fallback. Parsing it against a fixture is what
// keeps the CI path from being the untested one.
func TestListProcessesProcfsParsesStatAndCmdline(t *testing.T) {
	root := t.TempDir()

	write := func(pid, stat, cmdline string) {
		t.Helper()
		dir := filepath.Join(root, pid)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("fixture %s: %v", pid, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(stat), 0o600); err != nil {
			t.Fatalf("fixture %s stat: %v", pid, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "cmdline"), []byte(cmdline), 0o600); err != nil {
			t.Fatalf("fixture %s cmdline: %v", pid, err)
		}
	}

	// An ordinary entry.
	write("42", "42 (worker) S 1 7 7 0 -1 4194304 0 0\n", "gonimbus\x00index\x00build\x00")
	// comm containing spaces and parentheses: counting fields from the left would
	// read the wrong column, so the reader must count from comm's final ')'.
	write("43", "43 (od (d) name) S 1 99 99 0 -1 4194304 0 0\n", "weird\x00--flag\x00")
	// Non-numeric entries in procfs are not processes.
	if err := os.MkdirAll(filepath.Join(root, "self"), 0o755); err != nil {
		t.Fatal(err)
	}

	entries, err := listProcessesProcfs(root)
	if err != nil {
		t.Fatalf("read fixture procfs: %v", err)
	}

	byPID := map[int]processEntry{}
	for _, entry := range entries {
		byPID[entry.pid] = entry
	}
	if len(byPID) != 2 {
		t.Fatalf("read %d processes, want 2: %+v", len(byPID), entries)
	}

	ordinary, ok := byPID[42]
	if !ok {
		t.Fatal("pid 42 missing")
	}
	if ordinary.pgid != 7 {
		t.Errorf("pid 42 pgid = %d, want 7", ordinary.pgid)
	}
	if got, want := len(ordinary.args), 3; got != want {
		t.Fatalf("pid 42 args = %v, want %d elements", ordinary.args, want)
	}
	if ordinary.args[1] != "index" {
		t.Errorf("pid 42 args = %v, want NUL-separated cmdline split", ordinary.args)
	}

	awkward, ok := byPID[43]
	if !ok {
		t.Fatal("pid 43 missing")
	}
	if awkward.pgid != 99 {
		t.Errorf("pid 43 pgid = %d, want 99; comm with spaces and parens shifted the field offset", awkward.pgid)
	}
}

// A procfs that yields nothing must be reported as an error rather than as an
// empty process table, or the callers that prove a negative would pass vacuously
// on a host where the read silently found nothing.
func TestListProcessesProcfsEmptyIsAnError(t *testing.T) {
	if _, err := listProcessesProcfs(t.TempDir()); err == nil {
		t.Fatal("an empty procfs must be an error, not an empty process table")
	}
}

// TestHasTerminatedTreatsAnUnreapedExitAsTerminated pins the distinction the CI
// container exposed and a developer machine hides.
//
// The child here is deliberately started outside the ownership primitive and
// left unreaped until after the assertion: ownedproc's whole purpose is to reap
// promptly, so it cannot produce the state under test. A killed-but-unreaped
// child is a zombie — it has stopped executing, yet a signal-zero probe still
// succeeds for it. Whether such a zombie lingers depends on whether pid 1 reaps
// orphans, which is an environment property, so termination must not be read
// from the probe alone.
func TestHasTerminatedTreatsAnUnreapedExitAsTerminated(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start fixture child: %v", err)
	}
	pid := cmd.Process.Pid
	// Reaped only after the assertions, so the zombie state exists while they run.
	t.Cleanup(func() { _ = cmd.Wait() })

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill fixture child: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if state, ok := processState(pid); ok && state == 'Z' {
			break
		}
		if time.Now().After(deadline) {
			t.Skip("the platform reaped the child before a zombie could be observed")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The premise: the probe the naive check relies on still reports this process.
	process, err := os.FindProcess(pid)
	if err != nil {
		t.Fatalf("find fixture child: %v", err)
	}
	if process.Signal(syscall.Signal(0)) != nil {
		t.Skip("this platform does not keep an unreaped exit addressable")
	}

	if !hasTerminated(pid) {
		t.Fatal("a killed but unreaped child has terminated; the check reported it as still running")
	}
}
