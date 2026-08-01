package procidentity

import (
	"errors"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"
)

func TestBindAndKillChild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("child signal test is unix-focused in this suite")
	}
	if runtime.GOOS == "darwin" {
		t.Skip("Darwin stalled-recovery refuses destructive Bind (no instance-stable signal primitive)")
	}
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()
	id := Observe(cmd.Process.Pid)
	if !id.Proven {
		t.Fatalf("unproven child: %#v", id)
	}
	target, err := Bind(id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = target.Close() }()
	if err := target.SignalKill(); err != nil {
		t.Fatal(err)
	}
	done, err := target.WaitTerminated(5*time.Second, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("child not terminated")
	}
	_, _ = cmd.Process.Wait()
}

func TestBindRefusesMismatchedBirthToken(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("Darwin refuses all Bind; identity-mismatch path not reached")
	}
	id := Observe(os.Getpid())
	if !id.Proven {
		t.Skip("self unproven")
	}
	bad := id
	// Corrupt the platform-authoritative native field (v1 Match ignores display MS).
	switch {
	case id.StartTicks != 0:
		bad.StartTicks = id.StartTicks + 1
	case id.Filetime != 0:
		bad.Filetime = id.Filetime + 1
	case id.StartSec != 0 || id.StartUsec != 0:
		bad.StartSec = id.StartSec + 1
	default:
		bad.StartTimeUnixMS = 1
	}
	if _, err := Bind(bad); err == nil {
		t.Fatal("expected bind to refuse mismatched token")
	}
}

func TestDarwinBindRefusesDestructive(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("darwin-only refuse path")
	}
	id := Observe(os.Getpid())
	if !id.Proven {
		t.Skip("self unproven")
	}
	_, err := Bind(id)
	if !errors.Is(err, ErrUnsupportedPlatform) {
		t.Fatalf("darwin Bind must refuse destructive recovery, got %v", err)
	}
}

func TestWaitTerminatedZeroIsImmediate(t *testing.T) {
	// D-R11-03: timeout==0 must not expand to the 30s default.
	if runtime.GOOS == "darwin" {
		// Use a live self identity only for Terminated check via a fake target —
		// Darwin cannot Bind; exercise the public zero path through a mock-like
		// approach by binding on platforms that support it, or skip transport.
		t.Skip("requires Bind; covered on linux/windows")
	}
	id := Observe(os.Getpid())
	if !id.Proven {
		t.Skip("self unproven")
	}
	target, err := Bind(id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = target.Close() }()
	start := time.Now()
	done, err := target.WaitTerminated(0, time.Second)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("self must not report terminated")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("WaitTerminated(0) must return immediately, took %v", elapsed)
	}
}
