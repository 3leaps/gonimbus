package jobregistry

import (
	"os"
	"testing"
	"time"
)

func TestClaimQueuedCapturesProcessBirthIdentity(t *testing.T) {
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	rec := &JobRecord{
		JobID:     testJobID1,
		Type:      JobTypeIndexBuild,
		State:     JobStateQueued,
		CreatedAt: now,
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	pid := os.Getpid()
	claimed, err := store.ClaimQueued(testJobID1, pid, nil)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ProcessStartTimeUnixMS == nil || *claimed.ProcessStartTimeUnixMS == 0 {
		// Platforms without identity leave the field empty; plan treats that as
		// indeterminate. Supported CI platforms must capture a token.
		switch {
		case testing.Short():
			t.Skip("process identity unavailable in this environment")
		default:
			obs := ObserveProcessIdentity(pid)
			if obs.Proven {
				t.Fatalf("claim did not persist a proven identity: claimed=%#v observed=%#v", claimed, obs)
			}
			t.Skipf("process identity unproven on this host: %s", FormatProcessIdentity(obs))
		}
	}
	strict, err := store.GetReadOnlyStrict(testJobID1)
	if err != nil {
		t.Fatal(err)
	}
	if strict.ProcessStartTimeUnixMS == nil || *strict.ProcessStartTimeUnixMS != *claimed.ProcessStartTimeUnixMS {
		t.Fatalf("strict read lost birth identity: %#v", strict)
	}
	observed := ObserveProcessIdentity(pid)
	recorded := ProcessIdentityFromRecord(strict)
	if !ProcessIdentityMatch(recorded, observed) {
		t.Fatalf("recorded identity must match self: %s vs %s", FormatProcessIdentity(recorded), FormatProcessIdentity(observed))
	}
}

func TestGetReadOnlyStrictDoesNotDemoteDeadPID(t *testing.T) {
	store := NewStore(t.TempDir())
	rec := &JobRecord{JobID: testJobID1, State: JobStateRunning, PID: 1 << 30, CreatedAt: time.Now().UTC()}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(store.JobPath(testJobID1))
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.GetReadOnlyStrict(testJobID1)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != JobStateRunning {
		t.Fatalf("strict get demoted state: %s", got.State)
	}
	after, err := os.ReadFile(store.JobPath(testJobID1))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("GetReadOnlyStrict must be byte-preserving")
	}
}

func TestTouchHeartbeatRefusesNonRunning(t *testing.T) {
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	start := uint64(123)
	rec := &JobRecord{
		JobID:                  testJobID1,
		State:                  JobStateStopped,
		PID:                    os.Getpid(),
		ProcessStartTimeUnixMS: &start,
		CreatedAt:              now,
		LastHeartbeat:          &now,
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := store.TouchHeartbeat(testJobID1, rec.PID, &start, ""); err == nil {
		t.Fatal("expected touch to refuse non-running job")
	}
}
