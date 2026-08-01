package procidentity

import (
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"
)

func TestObserveSelfIsProvenOnSupportedPlatforms(t *testing.T) {
	switch runtime.GOOS {
	case "linux", "darwin", "windows":
	default:
		t.Skip("no process birth identity on this platform")
	}
	id := Observe(os.Getpid())
	if !id.Proven {
		t.Fatalf("expected proven identity for self, got %#v", id)
	}
	if id.PID != os.Getpid() || id.StartTimeUnixMS == 0 {
		t.Fatalf("unexpected identity: %#v", id)
	}
	again := Observe(os.Getpid())
	if !Match(id, again) {
		t.Fatalf("self identity must be stable: %s vs %s", Format(id), Format(again))
	}
}

func TestObserveDeadPIDIsUnproven(t *testing.T) {
	id := Observe(1 << 30)
	if id.Proven {
		t.Fatalf("dead pid must not be proven: %#v", id)
	}
}

func TestMatchRequiresProvenBirthToken(t *testing.T) {
	a := Identity{PID: 1, StartTimeUnixMS: 10, Proven: true}
	b := Identity{PID: 1, StartTimeUnixMS: 10, Proven: false}
	if Match(a, b) {
		t.Fatal("unproven observed identity must not match")
	}
	c := Identity{PID: 1, StartTimeUnixMS: 11, Proven: true}
	if Match(a, c) {
		t.Fatal("mismatched start time must not match")
	}
}

func TestChildStartTimeDiffersFromParent(t *testing.T) {
	switch runtime.GOOS {
	case "linux", "darwin":
	default:
		t.Skip("child start-time check is unix-focused")
	}
	parent := Observe(os.Getpid())
	if !parent.Proven {
		t.Fatalf("parent unproven: %#v", parent)
	}
	// Sleep briefly so a new process cannot share the same start millisecond on
	// coarse clocks, then spawn a short-lived child and observe it while live.
	time.Sleep(20 * time.Millisecond)
	cmd := exec.Command("sleep", "5")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()
	child := Observe(cmd.Process.Pid)
	if !child.Proven {
		t.Fatalf("child unproven: %#v", child)
	}
	if Match(parent, child) {
		t.Fatalf("parent and child must not share birth identity: %s", Format(parent))
	}
	// Re-observe child: same instance.
	again := Observe(cmd.Process.Pid)
	if !Match(child, again) {
		t.Fatalf("child identity unstable: %s vs %s", Format(child), Format(again))
	}
}
