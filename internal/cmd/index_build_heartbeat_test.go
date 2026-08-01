package cmd

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

func TestManagedHeartbeatStopReturnsPromptly(t *testing.T) {
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	rec := &jobregistry.JobRecord{
		JobID:         "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		Type:          jobregistry.JobTypeIndexBuild,
		State:         jobregistry.JobStateRunning,
		PID:           1 << 28,
		CreatedAt:     now,
		StartedAt:     &now,
		LastHeartbeat: &now,
	}
	start := uint64(1)
	rec.ProcessStartTimeUnixMS = &start
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}

	callersCopy := *rec
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop, fatal := startManagedHeartbeat(ctx, cancel, store, &callersCopy, 0)

	done := make(chan struct{})
	go func() {
		stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("heartbeat stop must return promptly")
	}

	select {
	case <-fatal:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("fatal channel must close on stop")
	}

	stop()
	if callersCopy.State != rec.State {
		t.Fatalf("must not mutate caller record")
	}
}

func TestDualPersistFailureCancelsBuildContext(t *testing.T) {
	// Exercise the real dual-failure path via hooks (both Touch and
	// RecordHeartbeatPersistError fail), short cadence, require build cancel.
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	rec := &jobregistry.JobRecord{
		JobID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
		Type:  jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateRunning,
		PID: os.Getpid(), CreatedAt: now, StartedAt: &now, LastHeartbeat: &now,
	}
	start := uint64(2)
	rec.ProcessStartTimeUnixMS = &start
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		heartbeatTouchHook = nil
		heartbeatRecordErrorHook = nil
	})
	heartbeatTouchHook = func(store *jobregistry.Store, jobID string, pid int, startMS *uint64, bootID string) error {
		return fmt.Errorf("injected touch failure")
	}
	heartbeatRecordErrorHook = func(store *jobregistry.Store, jobID string, persistErr error) error {
		return fmt.Errorf("injected record-error failure")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop, fatal := startManagedHeartbeat(ctx, cancel, store, rec, 10*time.Millisecond)
	defer stop()

	select {
	case err := <-fatal:
		if err == nil {
			t.Fatal("expected non-nil fatal")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected fatal within 2s on dual persist failure")
	}

	select {
	case <-ctx.Done():
	default:
		t.Fatal("build context must be cancelled on dual persist failure")
	}

	// Marker should succeed (job dir is writable).
	if !heartbeatUnhealthy(store, rec.JobID) {
		t.Fatal("expected heartbeat_unhealthy marker after dual failure")
	}
}
