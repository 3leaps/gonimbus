//go:build unix

package jobregistry

import (
	"os"
	"syscall"

	"github.com/3leaps/gonimbus/internal/procstate"
)

// procfsRoot is the mount point consulted for process state. It is a variable so
// the liveness decision can be exercised against a fixture, including on a host
// that has no procfs of its own.
var procfsRoot = "/proc"

func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// Signal zero checks for existence without delivering a signal on Unix.
	if process.Signal(os.Signal(syscall.Signal(0))) != nil {
		return false
	}
	// Signal zero is not the whole question. It succeeds for a process that has
	// exited but whose PID is still allocated, which is the case while the parent
	// has yet to collect the status. Such a process has finished; reporting it as
	// alive is what lets a finished job hold a running state indefinitely.
	//
	// How long that window lasts is not a property of the process: it ends when
	// whichever parent owns the child collects it. Where the child has been
	// orphaned that parent is PID 1, which collects promptly on a developer
	// machine and may never do so in a minimal container.
	return !hasExited(pid)
}

// hasExited reports whether pid is known to have exited.
//
// Where the state cannot be read the answer is false, which preserves the
// signal-zero verdict rather than declaring a process dead on no evidence.
//
// That leaves a real residual rather than an empty one. An exited process stays
// addressable while any parent has yet to collect it — it need not be an orphan,
// and the parent need not be PID 1 — so a system without a procfs will read such
// a process as alive and has no second opinion to offer. The residual is
// accepted because the deployment that motivated this work — a minimal container
// whose PID 1 never collects an orphan — has a procfs.
//
// Reading the state through ps instead would fork once per probe, and this is
// called per job record on an enumeration path.
func hasExited(pid int) bool {
	state, ok := procstate.State(procfsRoot, pid)
	if !ok {
		return false
	}
	return procstate.IsTerminal(state)
}
