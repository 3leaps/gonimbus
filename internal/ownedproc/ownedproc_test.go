package ownedproc

import (
	"os/exec"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// TestStopLeavesAnAlreadyExitedChildAlone is the normal-exit control: the body
// observes completion, teardown runs immediately afterwards, and teardown must
// neither signal nor wait. One wait, no kill.
//
// It also pins the publication order. Completion is observable only through
// Done, which closes after the result is stored, so a body that has seen an exit
// cannot leave teardown believing the child is still running.
func TestStopLeavesAnAlreadyExitedChildAlone(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("child-process controls are unix-focused")
	}
	child, err := Start(exec.Command("true"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := child.AwaitExit(20 * time.Second); !ok {
		t.Fatal("child did not exit")
	}
	if err := child.Stop(20 * time.Second); err != nil {
		t.Fatalf("stop after exit: %v", err)
	}
	if got := child.KillCalls(); got != 0 {
		t.Errorf("teardown signalled an already-exited child %d times", got)
	}
	if got := child.WaitCalls(); got != 1 {
		t.Errorf("wait ran %d times, want exactly one owner", got)
	}
}

// TestStopSignalsAndJoinsTheSoleWaiter is the teardown-before-completion
// control: the wait owner is still blocked when teardown runs. Teardown signals
// the retained process once and completes by joining that same waiter — never by
// waiting itself — and an unrelated process is untouched.
func TestStopSignalsAndJoinsTheSoleWaiter(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("child-process controls are unix-focused")
	}
	bystander, err := Start(exec.Command("sleep", "60"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bystander.Stop(20 * time.Second) }()

	child, err := Start(exec.Command("sleep", "60"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-child.Done():
		t.Fatal("the child must still be running when teardown starts")
	default:
	}

	if err := child.Stop(20 * time.Second); err != nil {
		t.Fatalf("stop a running child: %v", err)
	}
	select {
	case <-child.Done():
	default:
		t.Fatal("stop returned before the wait owner completed")
	}
	if got := child.WaitCalls(); got != 1 {
		t.Errorf("wait ran %d times, want exactly one owner", got)
	}
	if got := child.KillCalls(); got != 1 {
		t.Errorf("teardown signalled %d times, want exactly one", got)
	}
	select {
	case <-bystander.Done():
		t.Fatal("teardown must not reach a process it was not stopping")
	default:
	}
}

// TestSignalReachesTheRetainedProcess pins that signalling a child this package
// owns works while it is unreaped, which is what the interrupt controls rely on.
func TestSignalReachesTheRetainedProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("child-process controls are unix-focused")
	}
	child, err := Start(exec.Command("sleep", "60"))
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Signal(syscall.SIGINT); err != nil {
		t.Fatalf("signal retained process: %v", err)
	}
	if _, ok := child.AwaitExit(20 * time.Second); !ok {
		t.Fatal("signalled child did not exit")
	}
	if err := child.Stop(20 * time.Second); err != nil {
		t.Fatal(err)
	}
	if got := child.WaitCalls(); got != 1 {
		t.Errorf("wait ran %d times, want exactly one owner", got)
	}
	if got := child.KillCalls(); got != 0 {
		t.Errorf("teardown signalled an already-exited child %d times", got)
	}
}
