package jobregistry

import (
	"errors"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStoreStopRejectsNonRunningJob(t *testing.T) {
	store := NewStore(t.TempDir())
	rec := &JobRecord{
		JobID:     testJobID1,
		Type:      JobTypeIndexBuild,
		State:     JobStateSuccess,
		PID:       12345,
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, store.Write(rec))

	_, err := store.Stop(testJobID1, StopOptions{Signal: "term", WaitTimeout: time.Millisecond})

	require.Error(t, err)
	require.True(t, errors.Is(err, ErrJobNotRunning))
}

func TestStoreStopRejectsStoppingJob(t *testing.T) {
	store := NewStore(t.TempDir())
	rec := &JobRecord{
		JobID:     testJobID1,
		Type:      JobTypeIndexBuild,
		State:     JobStateStopping,
		PID:       12345,
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, store.Write(rec))

	_, err := store.Stop(testJobID1, StopOptions{Signal: "term", WaitTimeout: time.Millisecond})

	require.Error(t, err)
	require.True(t, errors.Is(err, ErrJobNotRunning))
}

func TestStoreStopRejectsMissingPID(t *testing.T) {
	store := NewStore(t.TempDir())
	rec := &JobRecord{
		JobID:     testJobID1,
		Type:      JobTypeIndexBuild,
		State:     JobStateRunning,
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, store.Write(rec))

	_, err := store.Stop(testJobID1, StopOptions{Signal: "term", WaitTimeout: time.Millisecond})

	require.Error(t, err)
	require.True(t, errors.Is(err, ErrJobNoPID))
}

func TestStoreStopKillsRunningJob(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			<-done
		}
	})

	store := NewStore(t.TempDir())
	rec := &JobRecord{
		JobID:     testJobID1,
		Type:      JobTypeIndexBuild,
		State:     JobStateRunning,
		PID:       cmd.Process.Pid,
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, store.Write(rec))

	result, err := store.Stop(testJobID1, StopOptions{Signal: "kill", WaitTimeout: time.Millisecond})

	require.NoError(t, err)
	require.Equal(t, testJobID1, result.JobID)
	require.Equal(t, "kill", result.Signal)
	require.True(t, result.ForcedKill)
	require.Equal(t, string(JobStateStopped), result.State)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("process did not exit after kill")
	}

	stored, err := store.Get(testJobID1)
	require.NoError(t, err)
	require.Equal(t, JobStateStopped, stored.State)
	require.NotNil(t, stored.EndedAt)
}

func TestStoreStopRejectsInvalidSignal(t *testing.T) {
	store := NewStore(t.TempDir())
	rec := &JobRecord{
		JobID:     testJobID1,
		Type:      JobTypeIndexBuild,
		State:     JobStateRunning,
		PID:       12345,
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, store.Write(rec))

	_, err := store.Stop(testJobID1, StopOptions{Signal: "hup", WaitTimeout: time.Millisecond})

	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidSignal))
}
