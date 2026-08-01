package jobregistry

import (
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// Portable signal-session child for E3 evidence. No bash/sleep dependency —
// re-exec of this test binary so Linux and Windows runners share one harness.

const (
	signalChildEnv       = "GONIMBUS_SIGNAL_CHILD"
	signalChildIgnoreEnv = "GONIMBUS_SIGNAL_CHILD_IGNORE_TERM"
	signalChildReadyEnv  = "GONIMBUS_SIGNAL_CHILD_READY"
)

// spawnSignalChild starts a long-lived re-exec helper. When ignoreTerm is true,
// the child ignores SIGTERM (Unix) so TERM→KILL escalation can be proven.
func spawnSignalChild(t *testing.T, ignoreTerm bool) *exec.Cmd {
	t.Helper()
	ready := filepath.Join(t.TempDir(), "child-ready")
	args := []string{"-test.run=TestEvidence_SignalChildHelper$", "-test.timeout=3m"}
	cmd := exec.Command(os.Args[0], args...) // #nosec G204
	env := append(os.Environ(),
		signalChildEnv+"=1",
		signalChildReadyEnv+"="+ready,
	)
	if ignoreTerm {
		env = append(env, signalChildIgnoreEnv+"=1")
	}
	cmd.Env = env
	if err := cmd.Start(); err != nil {
		t.Fatalf("start signal child: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
			t.Fatal("signal child never wrote ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cmd
}

// TestEvidence_SignalChildHelper is the re-exec entry only.
func TestEvidence_SignalChildHelper(t *testing.T) {
	if os.Getenv(signalChildEnv) != "1" {
		t.Skip("signal child helper entry only")
	}
	if os.Getenv(signalChildIgnoreEnv) == "1" {
		// Unix: ignore SIGTERM so DeliverTerm does not kill. Windows: TERM maps
		// differently; helper still parks until Kill from parent.
		if runtime.GOOS != "windows" {
			signal.Ignore(syscall.SIGTERM)
		}
	}
	if ready := os.Getenv(signalChildReadyEnv); ready != "" {
		if err := os.WriteFile(ready, []byte(runtime.GOOS+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// Park until SIGKILL / process kill from parent session transport.
	for {
		time.Sleep(time.Hour)
	}
}
