//go:build unix

package jobregistry

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// runningJobForPID writes a registry record that claims to be running under pid.
func runningJobForPID(t *testing.T, pid int) *Store {
	t.Helper()
	store := NewStore(t.TempDir())
	require.NoError(t, store.Write(&JobRecord{
		JobID:     testJobID1,
		Type:      JobTypeIndexBuild,
		State:     JobStateRunning,
		PID:       pid,
		CreatedAt: time.Now().UTC(),
	}))
	return store
}

// Get demotes a running job whose process is gone. The demotion was written as
// zombie detection and was defeated by an actual zombie: the PID stays
// addressable until it is reaped, so a finished job kept its running state for
// as long as nothing collected it, which in a container whose PID 1 does not
// reap is indefinitely.
func TestGetDemotesJobWhoseProcessHasExitedUnreaped(t *testing.T) {
	pid := startUnreapedChild(t)
	root := t.TempDir()
	writeProcfsStat(t, root, pid, "sh", 'Z')
	t.Cleanup(withProcfsRoot(root))

	store := runningJobForPID(t, pid)
	record, err := store.Get(testJobID1)
	require.NoError(t, err)
	require.Equal(t, JobStateUnknown, record.State,
		"a job whose process has exited is not still running")

	persisted, err := store.Get(testJobID1)
	require.NoError(t, err)
	require.Equal(t, JobStateUnknown, persisted.State, "the demotion must be recorded, not recomputed")
}

func TestGetLeavesJobRunningWhileItsProcessExecutes(t *testing.T) {
	pid := startLiveChild(t)
	root := t.TempDir()
	writeProcfsStat(t, root, pid, "sleep", 'S')
	t.Cleanup(withProcfsRoot(root))

	store := runningJobForPID(t, pid)
	record, err := store.Get(testJobID1)
	require.NoError(t, err)
	require.Equal(t, JobStateRunning, record.State, "an executing process must keep its job running")
}

// Stop polls liveness to decide whether a signalled job went down gracefully.
// Against an unreaped process the poll never observed an exit, so a job that had
// already finished absorbed the full wait timeout and was then reported as a
// forced kill. It is now refused up front as a job that is not running.
func TestStopRefusesJobWhoseProcessHasExitedUnreaped(t *testing.T) {
	pid := startUnreapedChild(t)
	root := t.TempDir()
	writeProcfsStat(t, root, pid, "sh", 'Z')
	t.Cleanup(withProcfsRoot(root))

	store := runningJobForPID(t, pid)

	start := time.Now()
	_, err := store.Stop(testJobID1, StopOptions{
		Signal:       "term",
		WaitTimeout:  2 * time.Second,
		PollInterval: 50 * time.Millisecond,
	})
	elapsed := time.Since(start)

	require.Error(t, err)
	require.True(t, errors.Is(err, ErrJobNotRunning),
		"a process that has exited is not a job to stop")
	require.Less(t, elapsed, time.Second,
		"the decision must not wait out the graceful-stop timeout")
}
