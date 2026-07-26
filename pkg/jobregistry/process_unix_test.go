//go:build unix

package jobregistry

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/procstate"
	"github.com/stretchr/testify/require"
)

// writeProcfsStat lays down a single process entry under a fixture procfs root.
// Parsing is covered in internal/procstate; what these tests need from it is a
// root that reports a chosen state for a real PID.
func writeProcfsStat(t *testing.T, root string, pid int, comm string, state byte) {
	t.Helper()
	dir := filepath.Join(root, strconv.Itoa(pid))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	line := strconv.Itoa(pid) + " (" + comm + ") " + string(state) + " 1 1 0 -1 4194304"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stat"), []byte(line), 0o600))
}

func withProcfsRoot(root string) func() {
	previous := procfsRoot
	procfsRoot = root
	return func() { procfsRoot = previous }
}

// startUnreapedChild starts a child, waits for it to exit, and deliberately does
// not collect it. The test process stays its parent, so the PID remains
// allocated and addressable until the cleanup reaps it.
func startUnreapedChild(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("sh", "-c", "exit 0")
	require.NoError(t, cmd.Start())
	pid := cmd.Process.Pid
	t.Cleanup(func() { _ = cmd.Wait() })

	// The exit is asynchronous, so wait for it rather than assuming it.
	// Addressability cannot be the signal here — that it lies is the whole
	// subject — so poll the state where it can be read, and fall back to a bounded
	// settle where it cannot.
	if runtime.GOOS == "linux" {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if state, ok := procstate.State("/proc", pid); ok && state == procstate.Zombie {
				return pid
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("child pid=%d did not become unreaped within the bound", pid)
	}
	time.Sleep(250 * time.Millisecond)
	return pid
}

// startLiveChild starts a process that stays alive until the test ends.
func startLiveChild(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("sleep", "30")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return cmd.Process.Pid
}

// This is the control for the fix: it pins the defect that motivated it. If
// signal zero ever stops reporting an unreaped child as present, the exited-state
// check is answering a question nobody is asking, and this says so.
func TestSignalZeroStillReportsAnUnreapedChildAsPresent(t *testing.T) {
	pid := startUnreapedChild(t)

	process, err := os.FindProcess(pid)
	require.NoError(t, err)
	require.NoError(t, process.Signal(syscall.Signal(0)),
		"an unreaped child must remain addressable, or this fix has no subject")
}

func TestIsProcessAliveReportsAnUnreapedChildAsDead(t *testing.T) {
	pid := startUnreapedChild(t)

	root := t.TempDir()
	writeProcfsStat(t, root, pid, "sh", 'Z')
	t.Cleanup(withProcfsRoot(root))

	require.False(t, isProcessAlive(pid),
		"a process that has exited and awaits a reap has finished, not survived")
}

func TestIsProcessAliveReportsARunningChildAsAlive(t *testing.T) {
	pid := startLiveChild(t)

	root := t.TempDir()
	writeProcfsStat(t, root, pid, "sleep", 'S')
	t.Cleanup(withProcfsRoot(root))

	require.True(t, isProcessAlive(pid), "a sleeping process has not exited")
}

// Each case uses one real, addressable PID, so signal zero succeeds throughout
// and the readable state is the only thing that moves the verdict.
//
// The alive rows are as load-bearing as the dead ones. A process that has been
// stopped has not exited, so widening the terminal set to "anything not running"
// would be a defect, and these rows are what says so. The historical letters are
// carried for the same reason the historical dead letter is: they cost nothing,
// and a reader should not have to wonder which kernel the table assumed.
func TestIsProcessAliveClassifiesProcfsStates(t *testing.T) {
	for _, tc := range []struct {
		state byte
		alive bool
		why   string
	}{
		{'Z', false, "exited, awaiting collection by its parent"},
		{'X', false, "dead"},
		{'x', false, "dead, as older kernels spelled it"},
		{'R', true, "running"},
		{'S', true, "sleeping"},
		{'D', true, "in uninterruptible sleep"},
		{'I', true, "idle"},
		{'T', true, "stopped, which is not exited"},
		{'t', true, "stopped by a tracer, which is not exited"},
		{'W', true, "paging or waking, depending on the kernel"},
		{'K', true, "wakekill, as some kernels spelled it"},
		{'P', true, "parked, as some kernels spelled it"},
	} {
		pid := startLiveChild(t)
		root := t.TempDir()
		writeProcfsStat(t, root, pid, "sleep", tc.state)
		restore := withProcfsRoot(root)

		got := isProcessAlive(pid)
		restore()

		require.Equal(t, tc.alive, got, "a process reported as %c is %s", tc.state, tc.why)
	}
}

// A readable state this code does not recognize leaves the process alive, which
// is the conservative direction and is stated as a contract rather than left to
// the shape of a switch. Kept separate from the table above so that flipping the
// default has exactly one test to answer to.
func TestIsProcessAliveTreatsAnUnrecognizedStateAsAlive(t *testing.T) {
	pid := startLiveChild(t)

	root := t.TempDir()
	writeProcfsStat(t, root, pid, "sleep", '?')
	t.Cleanup(withProcfsRoot(root))

	require.True(t, isProcessAlive(pid),
		"a state nobody has classified must not be read as exited")
}

// Where the state cannot be read the signal-zero verdict stands. This is the
// documented degradation, not an accident, so it is pinned.
func TestIsProcessAliveFallsBackToAddressabilityWithoutProcessState(t *testing.T) {
	pid := startUnreapedChild(t)
	t.Cleanup(withProcfsRoot(t.TempDir()))

	require.True(t, isProcessAlive(pid),
		"with no readable state the addressable PID is the only available answer")
}

func TestIsProcessAliveRejectsNonPositivePIDs(t *testing.T) {
	require.False(t, isProcessAlive(0))
	require.False(t, isProcessAlive(-1))
}

// The fixture tests above drive the decision from a procfs we wrote. This one
// drives it from the kernel's, which is what checks the fixtures are shaped like
// the real thing for the state they were built to represent.
func TestIsProcessAliveAgainstRealProcfs(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires a real procfs")
	}
	pid := startUnreapedChild(t)

	require.False(t, isProcessAlive(pid),
		"an unreaped child must read as dead through the real /proc")
	require.True(t, isProcessAlive(startLiveChild(t)),
		"an executing child must read as alive through the real /proc")
}
