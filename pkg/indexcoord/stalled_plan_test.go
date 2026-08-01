package indexcoord

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/internal/procidentity"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

func TestPlanManagedStalled_HealthyLiveJob(t *testing.T) {
	store, rec := writeRunningSelfJob(t, "idx_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	authority := writeHeldLease(t, rec.IndexSetID, "index-build-"+rec.JobID)

	plan, err := PlanManagedStalledRecovery(store, rec.JobID, StalledPlanOptions{
		AuthorityRoot: authority,
		Now:           time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Class != PlanHealthy {
		t.Fatalf("expected healthy, got %s (%s)", plan.Class, plan.Detail)
	}
	if plan.SignalCandidate {
		t.Fatal("healthy must not be a signal candidate")
	}
}

func TestPlanManagedStalled_SuspectHeartbeatOverdue(t *testing.T) {
	store, rec := writeRunningSelfJob(t, "idx_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	stale := time.Now().UTC().Add(-10 * time.Minute)
	rec.LastHeartbeat = &stale
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	authority := writeHeldLease(t, rec.IndexSetID, "index-build-"+rec.JobID)

	plan, err := PlanManagedStalledRecovery(store, rec.JobID, StalledPlanOptions{
		AuthorityRoot:  authority,
		Now:            time.Now().UTC(),
		HeartbeatGrace: time.Nanosecond, // raised to MinStalledHeartbeatGrace
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Class != PlanSuspectHeartbeatOverdue {
		t.Fatalf("expected suspect-heartbeat-overdue, got %s (%s)", plan.Class, plan.Detail)
	}
	if !plan.SignalCandidate {
		t.Fatal("suspect must be a signal candidate")
	}
}

func TestPlanManagedStalled_GraceFloor(t *testing.T) {
	if MinStalledHeartbeatGrace < 2*ManagedHeartbeatInterval {
		t.Fatalf("grace floor too low: %s", MinStalledHeartbeatGrace)
	}
	store, rec := writeRunningSelfJob(t, "idx_cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	// Heartbeat age between default floor and a would-be tiny grace.
	stale := time.Now().UTC().Add(-45 * time.Second)
	rec.LastHeartbeat = &stale
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	authority := writeHeldLease(t, rec.IndexSetID, "index-build-"+rec.JobID)

	plan, err := PlanManagedStalledRecovery(store, rec.JobID, StalledPlanOptions{
		AuthorityRoot:  authority,
		Now:            time.Now().UTC(),
		HeartbeatGrace: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	// 45s < MinGrace (60s) ⇒ still healthy under the floor.
	if plan.Class != PlanHealthy {
		t.Fatalf("expected grace floor to keep job healthy, got %s (%s)", plan.Class, plan.Detail)
	}
}

func TestPlanManagedStalled_TerminalContradictionNoSignal(t *testing.T) {
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	start := uint64(1)
	indexSetID := "idx_dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	rec := &jobregistry.JobRecord{
		JobID:                  "11111111-1111-1111-1111-111111111111",
		Type:                   jobregistry.JobTypeIndexBuild,
		State:                  jobregistry.JobStateRunning,
		PID:                    1 << 30,
		ProcessStartTimeUnixMS: &start,
		IndexSetID:             indexSetID,
		CreatedAt:              now,
		StartedAt:              &now,
		LastHeartbeat:          &now,
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	authority := writeUnheldLease(t, indexSetID, "index-build-"+rec.JobID)

	plan, err := PlanManagedStalledRecovery(store, rec.JobID, StalledPlanOptions{AuthorityRoot: authority})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Class != PlanTerminalContradiction {
		t.Fatalf("expected terminal-contradiction, got %s (%s)", plan.Class, plan.Detail)
	}
	if plan.SignalCandidate {
		t.Fatal("terminal contradiction must never be a signal candidate")
	}
	if !plan.MayReapUnheld {
		t.Fatal("unheld lease under dead pid should allow unheld reap")
	}
}

func TestPlanManagedStalled_IdentityMismatchBystander(t *testing.T) {
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	// Record a fake birth token that cannot match the live self process.
	fakeStart := uint64(1)
	indexSetID := "idx_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	rec := &jobregistry.JobRecord{
		JobID:                  "22222222-2222-2222-2222-222222222222",
		Type:                   jobregistry.JobTypeIndexBuild,
		State:                  jobregistry.JobStateRunning,
		PID:                    os.Getpid(),
		ProcessStartTimeUnixMS: &fakeStart,
		IndexSetID:             indexSetID,
		CreatedAt:              now,
		StartedAt:              &now,
		LastHeartbeat:          &now,
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	authority := writeHeldLease(t, indexSetID, "index-build-"+rec.JobID)

	plan, err := PlanManagedStalledRecovery(store, rec.JobID, StalledPlanOptions{AuthorityRoot: authority})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Class != PlanIdentityMismatch {
		// If self identity cannot be proven, indeterminate is the closed form.
		if plan.Class != PlanIndeterminate {
			t.Fatalf("expected identity-mismatch (or indeterminate), got %s (%s)", plan.Class, plan.Detail)
		}
		return
	}
	if plan.SignalCandidate {
		t.Fatal("identity mismatch must not be a signal candidate")
	}
}

func TestPlanManagedStalled_MissingBirthIdentityIndeterminate(t *testing.T) {
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	rec := &jobregistry.JobRecord{
		JobID:         "33333333-3333-3333-3333-333333333333",
		Type:          jobregistry.JobTypeIndexBuild,
		State:         jobregistry.JobStateRunning,
		PID:           os.Getpid(),
		IndexSetID:    "idx_ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		CreatedAt:     now,
		StartedAt:     &now,
		LastHeartbeat: &now,
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	plan, err := PlanManagedStalledRecovery(store, rec.JobID, StalledPlanOptions{
		AuthorityRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Class != PlanIndeterminate {
		t.Fatalf("expected indeterminate without birth token, got %s (%s)", plan.Class, plan.Detail)
	}
	if plan.SignalCandidate {
		t.Fatal("indeterminate must not be a signal candidate")
	}
}

func TestPlanManagedStalled_HeartbeatPersistErrorIndeterminate(t *testing.T) {
	store, rec := writeRunningSelfJob(t, "idx_9999999999999999999999999999999999999999999999999999999999999999")
	rec.HeartbeatPersistError = "disk full"
	stale := time.Now().UTC().Add(-10 * time.Minute)
	rec.LastHeartbeat = &stale
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	authority := writeHeldLease(t, rec.IndexSetID, "index-build-"+rec.JobID)
	plan, err := PlanManagedStalledRecovery(store, rec.JobID, StalledPlanOptions{AuthorityRoot: authority})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Class != PlanIndeterminate {
		t.Fatalf("expected indeterminate when heartbeat writes fail, got %s (%s)", plan.Class, plan.Detail)
	}
	if plan.SignalCandidate {
		t.Fatal("failed heartbeat writes must not authorize a signal candidate")
	}
}

func TestPlanManagedStalled_ParentlessnessIsNotAuthority(t *testing.T) {
	// A live matching job with a fresh heartbeat is healthy even if we cannot
	// observe a parent — parentlessness is never consulted by the planner.
	store, rec := writeRunningSelfJob(t, "idx_8888888888888888888888888888888888888888888888888888888888888888")
	authority := writeHeldLease(t, rec.IndexSetID, "index-build-"+rec.JobID)
	plan, err := PlanManagedStalledRecovery(store, rec.JobID, StalledPlanOptions{AuthorityRoot: authority})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Class != PlanHealthy {
		t.Fatalf("expected healthy (ppid is not a gate), got %s (%s)", plan.Class, plan.Detail)
	}
}

func writeRunningSelfJob(t *testing.T, indexSetID string) (*jobregistry.Store, *jobregistry.JobRecord) {
	t.Helper()
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	pid := os.Getpid()
	obs := jobregistry.ObserveProcessIdentity(pid)
	if !obs.Proven {
		t.Skipf("process identity unproven: %s", jobregistry.FormatProcessIdentity(obs))
	}
	rec := &jobregistry.JobRecord{
		JobID:         "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		Type:          jobregistry.JobTypeIndexBuild,
		State:         jobregistry.JobStateRunning,
		PID:           pid,
		IndexSetID:    indexSetID,
		CreatedAt:     now,
		StartedAt:     &now,
		LastHeartbeat: &now,
	}
	jobregistry.ApplyProcessIdentity(rec, obs)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	return store, rec
}

func writeHeldLease(t *testing.T, indexSetID, holder string) string {
	t.Helper()
	segmentRoot := t.TempDir()
	auth, err := indexsubstrate.AcquireSetAuthority(context.Background(), segmentRoot, indexSetID, holder)
	if err != nil {
		t.Fatalf("acquire set authority: %v", err)
	}
	t.Cleanup(func() { _ = auth.Release() })
	authorityRoot, err := AuthorityRoot(segmentRoot)
	if err != nil {
		t.Fatal(err)
	}
	return authorityRoot
}

// seedActiveFence writes a durable recovery fence without BeginStalledRecovery
// (Begin refuses on platforms without instance-stable destructive primitives).
func seedActiveFence(t *testing.T, store *jobregistry.Store, rec *jobregistry.JobRecord, owner, authorityRoot string) {
	t.Helper()
	now := time.Now().UTC()
	rec.State = jobregistry.JobStateStopping
	rec.RecoveryIntent = jobregistry.RecoveryIntentStalled
	rec.RecoveryOwner = owner
	rec.RecoverySignalOwner = ""
	rec.RecoveryStartedAt = &now
	rec.RecoveryGeneration = 1
	rec.RecoveryPhase = jobregistry.RecoveryPhaseFenced
	applyAuthorityIdentity(t, rec, authorityRoot)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
}

// applyAuthorityIdentity sets canonical authority root path + dir identity on rec.
func applyAuthorityIdentity(t *testing.T, rec *jobregistry.JobRecord, authorityRoot string) {
	t.Helper()
	if rec == nil {
		return
	}
	if strings.TrimSpace(authorityRoot) == "" {
		rec.RecoveryAuthorityRoot = ""
		rec.RecoveryAuthorityDev = 0
		rec.RecoveryAuthorityIno = 0
		return
	}
	if err := os.MkdirAll(authorityRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	abs, err := filepath.Abs(authorityRoot)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		t.Fatal(err)
	}
	rec.RecoveryAuthorityRoot = resolved
	rec.RecoveryAuthorityDev, rec.RecoveryAuthorityIno = procidentity.FileDevIno(resolved)
}

func writeUnheldLease(t *testing.T, indexSetID, holder string) string {
	t.Helper()
	// Missing lease under a real authority root is a valid observation for
	// MayReapUnheld under terminal contradiction.
	_ = holder
	_ = indexSetID
	segmentRoot := t.TempDir()
	authorityRoot, err := AuthorityRoot(segmentRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(authorityRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	return authorityRoot
}
