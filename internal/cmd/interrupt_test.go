package cmd

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/ownedproc"
)

// Environment contract for the re-exec interrupt child.
const (
	envInterruptChild = "GONIMBUS_HELPER_INTERRUPT_CHILD"
	envInterruptReady = "GONIMBUS_HELPER_INTERRUPT_READY"
	envInterruptSeen  = "GONIMBUS_HELPER_INTERRUPT_SEEN"
	// envInterruptParentCancel makes the child cancel its own parent context
	// instead of waiting for a signal, so the parent-cancellation branch of the
	// bridge can be driven deterministically.
	envInterruptParentCancel = "GONIMBUS_HELPER_INTERRUPT_PARENT_CANCEL"
)

// TestWithInterruptCancel_FirstSignalCancels pins the property the completion
// path depends on: an interrupt becomes cancellation, so the command unwinds
// through its deferred cleanup instead of dying in place.
//
// The test registers its own receiver for the same signal first. That is not
// belt-and-braces: without it, a regression that failed to install the bridge
// would let the default disposition kill the test binary outright instead of
// failing this test.
func TestWithInterruptCancel_FirstSignalCancels(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no portable way to raise a console interrupt at this process on windows")
	}
	guard := make(chan os.Signal, 1)
	signal.Notify(guard, syscall.SIGTERM)
	t.Cleanup(func() { signal.Stop(guard) })

	ctx, stop := withInterruptCancel(context.Background())
	t.Cleanup(stop)
	require.NoError(t, ctx.Err(), "the context must be live before any signal")

	self, err := os.FindProcess(os.Getpid())
	require.NoError(t, err)
	require.NoError(t, self.Signal(syscall.SIGTERM))

	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("an interrupt must cancel the build context")
	}
}

// TestWithInterruptCancel_StopIsIdempotent pins that the returned stop function
// can run more than once — commands defer it, and a command may also return
// through a path that has already stopped signal handling.
func TestWithInterruptCancel_StopIsIdempotent(t *testing.T) {
	ctx, stop := withInterruptCancel(context.Background())
	stop()
	stop()
	require.Error(t, ctx.Err(), "stop cancels the derived context")
}

// TestWithInterruptCancel_SecondSignalTerminates proves the escape hatch: after
// the first interrupt is translated into cancellation, the handler is
// uninstalled, so a second interrupt terminates the process immediately rather
// than being absorbed while a slow shutdown drags on.
//
// This needs a real child because the observable outcome is process death. The
// child confirms it observed cancellation before the parent sends the second
// signal, and the bridge uninstalls the handler BEFORE cancelling, so that
// confirmation makes the ordering deterministic rather than racy.
func TestWithInterruptCancel_SecondSignalTerminates(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signalling another process is not portable on windows")
	}
	dir := t.TempDir()
	readyFile := dir + "/ready"
	seenFile := dir + "/seen"

	cmd := exec.Command(os.Args[0], "-test.run=TestInterruptHelperProcess", "-test.timeout=120s") // #nosec G204 -- os.Args[0] is this test binary
	cmd.Env = append(os.Environ(),
		envInterruptChild+"=1",
		envInterruptReady+"="+readyFile,
		envInterruptSeen+"="+seenFile,
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	child := startOwnedChild(t, cmd)

	waitForFile(t, readyFile, "child did not install the interrupt bridge")
	require.NoError(t, child.Signal(syscall.SIGINT))
	waitForFile(t, seenFile, "the first interrupt must reach the child as cancellation")

	// The child deliberately ignores cancellation and keeps running; only the
	// restored default disposition can end it.
	require.NoError(t, child.Signal(syscall.SIGINT))
	requireSignalledExit(t, child, syscall.SIGINT, "a repeated interrupt must not be absorbed by the bridge")
}

// TestWithInterruptCancel_ParentCancellationStillRestoresDefault covers the
// other way the bridge can finish: the parent context is cancelled rather than
// signalled. The default disposition must be restored on that branch too, so a
// later interrupt still terminates.
//
// The child cancels its own parent, waits for the bridge to quiesce (its stop
// function still deferred, exactly as a command's would be), and then stays
// alive; only the restored default disposition can end it.
func TestWithInterruptCancel_ParentCancellationStillRestoresDefault(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signalling another process is not portable on windows")
	}
	dir := t.TempDir()
	readyFile := dir + "/ready"
	seenFile := dir + "/seen"

	cmd := exec.Command(os.Args[0], "-test.run=TestInterruptHelperProcess", "-test.timeout=120s") // #nosec G204 -- os.Args[0] is this test binary
	cmd.Env = append(os.Environ(),
		envInterruptChild+"=1",
		envInterruptParentCancel+"=1",
		envInterruptReady+"="+readyFile,
		envInterruptSeen+"="+seenFile,
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	child := startOwnedChild(t, cmd)

	waitForFile(t, readyFile, "child did not install the interrupt bridge")
	// The child writes this only after the bridge has fully quiesced on the
	// parent-cancellation branch, so no sleep is needed to close the window.
	waitForFile(t, seenFile, "child did not observe parent cancellation quiescing the bridge")

	require.NoError(t, child.Signal(syscall.SIGINT))
	requireSignalledExit(t, child, syscall.SIGINT,
		"an interrupt after parent-cancellation quiescence must not be absorbed")
}

// TestInterruptHelperProcess is the re-exec entry point. Under a normal `go test`
// it is a no-op; only the spawned child installs the bridge and then ignores the
// cancellation it observes, so the parent can prove what a later signal does.
func TestInterruptHelperProcess(t *testing.T) {
	if os.Getenv(envInterruptChild) != "1" {
		t.Skip("helper process entry point; only runs as a spawned child")
	}
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	parentCancels := os.Getenv(envInterruptParentCancel) == "1"
	ctx, stop, quiesced := newInterruptBridge(parent)
	// stop stays deferred, exactly as a command's would: the bridge must restore
	// the default disposition on its own, not only when the command returns.
	defer stop()
	if err := os.WriteFile(os.Getenv(envInterruptReady), []byte("ready"), 0o600); err != nil {
		t.Fatalf("child could not write ready file: %v", err)
	}
	if parentCancels {
		cancelParent()
		<-quiesced
	} else {
		<-ctx.Done()
	}
	if err := os.WriteFile(os.Getenv(envInterruptSeen), []byte("cancelled"), 0o600); err != nil {
		t.Fatalf("child could not write seen file: %v", err)
	}
	// Ignore the cancellation on purpose and stay alive.
	<-time.After(110 * time.Second)
}

// startOwnedChild starts a helper child under single-wait ownership and
// registers its teardown.
func startOwnedChild(t *testing.T, cmd *exec.Cmd) *ownedproc.Child {
	t.Helper()
	child, err := ownedproc.Start(cmd)
	require.NoError(t, err)
	t.Cleanup(func() {
		if stopErr := child.Stop(15 * time.Second); stopErr != nil {
			t.Errorf("stop helper child: %v", stopErr)
		}
	})
	return child
}

// requireSignalledExit asserts the child died from sig rather than exiting or
// hanging. It observes the child's sole wait owner rather than waiting itself.
func requireSignalledExit(t *testing.T, child *ownedproc.Child, sig syscall.Signal, message string) {
	t.Helper()
	err, exited := child.AwaitExit(15 * time.Second)
	if !exited {
		t.Fatal(message)
	}
	require.Error(t, err, message)
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	status, ok := exit.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.True(t, status.Signaled(), "the process must die from the signal, not exit cleanly")
	require.Equal(t, sig, status.Signal())
}

func waitForFile(t *testing.T, path, message string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(message)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
