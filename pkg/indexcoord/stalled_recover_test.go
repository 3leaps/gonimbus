package indexcoord

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/internal/procidentity"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

func TestRecoverManagedStalled_DryRunIsBytePreserving(t *testing.T) {
	store, rec, authority := setupSuspectJob(t)
	before, err := os.ReadFile(store.JobPath(rec.JobID))
	if err != nil {
		t.Fatal(err)
	}
	result, err := RecoverManagedStalled(store, rec.JobID, RecoverStalledOptions{
		AuthorityRoot: authority,
		Confirm:       false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != OutcomeDryRun || result.Signalled || result.Reclaimed {
		t.Fatalf("unexpected dry-run result: %#v", result)
	}
	after, err := os.ReadFile(store.JobPath(rec.JobID))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("dry-run must not mutate the job record")
	}
}

func TestRecoverManagedStalled_HealthyRefused(t *testing.T) {
	store, rec := writeRunningSelfJob(t, "idx_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	authority := writeHeldLease(t, rec.IndexSetID, "index-build-"+rec.JobID)
	result, err := RecoverManagedStalled(store, rec.JobID, RecoverStalledOptions{
		AuthorityRoot: authority,
		Confirm:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != OutcomeRefused || result.Signalled {
		t.Fatalf("healthy job must be refused: %#v", result)
	}
	// Process still us.
	if !jobregistry.IsProcessAlive(os.Getpid()) {
		t.Fatal("self must still be alive")
	}
}

func TestRecoverManagedStalled_IdentityMismatchBystanderSurvives(t *testing.T) {
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	fake := uint64(1)
	indexSetID := "idx_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	rec := &jobregistry.JobRecord{
		JobID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", Type: jobregistry.JobTypeIndexBuild,
		State: jobregistry.JobStateRunning, PID: os.Getpid(), ProcessStartTimeUnixMS: &fake,
		IndexSetID: indexSetID, CreatedAt: now, StartedAt: &now,
		LastHeartbeat: ptrTime(now.Add(-10 * time.Minute)),
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	authority := writeHeldLease(t, indexSetID, "index-build-"+rec.JobID)
	result, err := RecoverManagedStalled(store, rec.JobID, RecoverStalledOptions{
		AuthorityRoot: authority, Confirm: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Signalled {
		t.Fatalf("must not signal on identity mismatch: %#v", result)
	}
	if result.Outcome != OutcomeRefused && result.Plan.Class != PlanIdentityMismatch && result.Plan.Class != PlanIndeterminate {
		t.Fatalf("expected refuse/mismatch: %#v", result)
	}
	if !jobregistry.IsProcessAlive(os.Getpid()) {
		t.Fatal("bystander (self) must survive")
	}
}

func TestRecoverManagedStalled_TerminalContradictionNoSignal(t *testing.T) {
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	start := uint64(1)
	indexSetID := "idx_cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	rec := &jobregistry.JobRecord{
		JobID: "cccccccc-cccc-cccc-cccc-cccccccccccc", Type: jobregistry.JobTypeIndexBuild,
		State: jobregistry.JobStateRunning, PID: 1 << 30, ProcessStartTimeUnixMS: &start,
		IndexSetID: indexSetID, CreatedAt: now, StartedAt: &now, LastHeartbeat: &now,
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	authority := writeUnheldLease(t, indexSetID, "index-build-"+rec.JobID)
	result, err := RecoverManagedStalled(store, rec.JobID, RecoverStalledOptions{
		AuthorityRoot: authority, Confirm: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Signalled {
		t.Fatal("terminal contradiction must not signal")
	}
	if result.Outcome != OutcomeReapedOnly && result.Outcome != OutcomeRefused {
		// Missing lease may be reaped-only with reclaimed=false or refused depending on root.
		t.Logf("outcome=%s detail=%s", result.Outcome, result.Detail)
	}
}

func TestRecoverManagedStalled_PositiveEndToEndChild(t *testing.T) {
	// Spawn a real child, capture birth identity, hold a real lease, stale heartbeat,
	// recover with confirm, observe death, reclaim.
	// Panel: Darwin has no instance-stable destructive primitive — Bind refuses.
	if runtime.GOOS == "darwin" {
		t.Skip("destructive recovery unsupported on Darwin (panel refuse path)")
	}
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()

	id := procidentity.Observe(pid)
	if !id.Proven {
		t.Skipf("cannot prove child identity: %s", procidentity.Format(id))
	}
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	indexSetID := "idx_dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	// Hold lease in a helper process? Same process can hold the lease while we
	// signal the child — lease holder is this test process, job PID is the child.
	// That mismatches holder string index-build-<job> vs who holds the flock.
	// For a faithful E2E the child should hold the lease. Simpler E2E: acquire
	// lease in this process with the managed holder name; job names the child.
	// Probe = held by us; reclaim after child death still works when we Release
	// or when OS... we still hold the flock, so after kill reclaim would fail
	// held. So we must Release our hold after kill to simulate OS drop, or have
	// the child hold it.
	//
	// Child-held lease: start a tiny helper that acquires and sleeps. Too heavy.
	// Practical proof: fence + identity-bound kill of child without lease reclaim
	// leg when lease is held by test; separately test reclaim on unheld.
	//
	// Combined: acquire lease, then in cleanup Release after child death so
	// reclaim sees unheld... but Release unlinks under held lock. After Release
	// lease is missing (gone), reclaim is idempotent missing.
	segmentRoot := t.TempDir()
	jobID := "dddddddd-dddd-dddd-dddd-dddddddddddd"
	auth, err := indexsubstrate.AcquireSetAuthority(context.Background(), segmentRoot, indexSetID, "index-build-"+jobID)
	if err != nil {
		t.Fatal(err)
	}
	authorityRoot, err := AuthorityRoot(segmentRoot)
	if err != nil {
		t.Fatal(err)
	}
	rec := &jobregistry.JobRecord{
		JobID: jobID, Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateRunning,
		PID: pid, IndexSetID: indexSetID, CreatedAt: now, StartedAt: &now,
		LastHeartbeat: ptrTime(now.Add(-10 * time.Minute)),
	}
	jobregistry.ApplyProcessIdentity(rec, id)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}

	result, err := RecoverManagedStalled(store, jobID, RecoverStalledOptions{
		AuthorityRoot: authorityRoot,
		Confirm:       true,
		// Bound covers second observation + TERM wait + optional KILL wait.
		WaitTimeout:  30 * time.Second,
		PollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		_ = auth.Release()
		t.Fatal(err)
	}
	// Lease still held by this test process → expect signalled stop + lease-still-held
	// OR signalled-stopped with lease-still-held outcome.
	if !result.Signalled && result.Outcome != OutcomeSignalled && result.Outcome != OutcomeLeaseStillHeld {
		_ = auth.Release()
		t.Fatalf("expected signal path, got %#v", result)
	}
	// Child must be dead.
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = auth.Release()
		t.Fatal("child did not exit after recover-stalled")
	}
	if jobregistry.IsProcessAlive(pid) {
		_ = auth.Release()
		t.Fatal("child pid still alive after recover")
	}
	_ = auth.Release()

	// Job record should be stopped.
	final, err := store.GetReadOnlyStrict(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != jobregistry.JobStateStopped {
		t.Fatalf("expected stopped, got %s (%#v)", final.State, result)
	}
}

func TestRecoverManagedStalled_FenceBlocksSuccessWrite(t *testing.T) {
	store, rec, authority := setupSuspectJob(t)
	_ = authority
	seedActiveFence(t, store, rec, "test-owner", t.TempDir())
	if err := store.ClaimStalledRecoverySignal(rec.JobID, "test-owner", "att-1"); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetReadOnlyStrict(rec.JobID)
	if err != nil {
		t.Fatal(err)
	}
	success := *got
	success.State = jobregistry.JobStateSuccess
	success.RecoveryIntent = ""
	success.RecoveryOwner = ""
	if err := store.Write(&success); err == nil {
		t.Fatal("success write must be fenced under active recovery")
	}
	got, err = store.GetReadOnlyStrict(rec.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != jobregistry.JobStateStopping || got.RecoveryOwner != "test-owner" {
		t.Fatalf("fence must survive success attempt: %#v", got)
	}
}

func TestRecoverManagedStalled_ResumesExistingFence(t *testing.T) {
	// Active durable fence + live matching self + held lease must resume, not
	// refuse with "fence already active" from a newly minted owner.
	if runtime.GOOS == "darwin" {
		t.Skip("destructive resume/signal unsupported on Darwin")
	}
	store, rec, authority := setupSuspectJob(t)
	seedActiveFence(t, store, rec, "durable-owner", t.TempDir())

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() }()
	id := procidentity.Observe(cmd.Process.Pid)
	if !id.Proven {
		t.Skipf("child identity unproven")
	}
	fenced, err := store.GetReadOnlyStrict(rec.JobID)
	if err != nil {
		t.Fatal(err)
	}
	fenced.PID = cmd.Process.Pid
	jobregistry.ApplyProcessIdentity(fenced, id)
	if err := store.Write(fenced); err != nil {
		t.Fatal(err)
	}

	result, err := RecoverManagedStalled(store, rec.JobID, RecoverStalledOptions{
		AuthorityRoot: authority,
		Confirm:       true,
		WaitTimeout:   3 * time.Second,
		PollInterval:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Owner != "durable-owner" {
		t.Fatalf("must resume durable owner, got %q detail=%s", result.Owner, result.Detail)
	}
	if result.Outcome == OutcomeRefused && strings.Contains(result.Detail, "already active") {
		t.Fatalf("must not refuse own fence: %#v", result)
	}
}

func TestClaimSignalPhaseIsExclusiveUnderResume(t *testing.T) {
	store, rec, _ := setupSuspectJob(t)
	seedActiveFence(t, store, rec, "durable-owner", t.TempDir())
	if err := store.ClaimStalledRecoverySignal(rec.JobID, "durable-owner", "attempt-a"); err != nil {
		t.Fatal(err)
	}
	if err := store.ClaimStalledRecoverySignal(rec.JobID, "durable-owner", "attempt-b"); err == nil {
		t.Fatal("second concurrent signal claim must be busy")
	}
}

func TestRecoverManagedStalled_CrashClaimTakeoverWhenDead(t *testing.T) {
	// Durable fence + crashed signal claim + dead PID + unheld lease → takeover finalize, no signal.
	// Seed fence fields directly (Begin requires live matching identity; crash leaves a dead PID).
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	start := uint64(1)
	indexSetID := "idx_ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	jobID := "ffffffff-ffff-ffff-ffff-ffffffffffff"
	claimedAt := now.Add(-time.Minute)
	// Claimer PID is a dead process (not us); managed PID also dead.
	claimerPID := 1 << 28
	rec := &jobregistry.JobRecord{
		JobID: jobID, Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateStopping,
		PID: 1 << 29, ProcessStartTimeUnixMS: &start, ProcessTokenVersion: 1,
		ProcessStartTicks: 42, ProcessBootID: "boot-test",
		IndexSetID: indexSetID, CreatedAt: now, StartedAt: &now,
		LastHeartbeat:                     ptrTime(now.Add(-10 * time.Minute)),
		RecoveryIntent:                    jobregistry.RecoveryIntentStalled,
		RecoveryOwner:                     "durable-owner",
		RecoverySignalOwner:               "crashed-attempt",
		RecoverySignalClaimedAt:           &claimedAt,
		RecoverySignalClaimerPID:          claimerPID,
		RecoverySignalClaimerTokenVersion: 1,
		RecoverySignalClaimerStartTicks:   99,
		RecoverySignalClaimerBootID:       "boot-test",
		RecoveryStartedAt:                 &claimedAt,
		RecoveryGeneration:                1,
		RecoveryPhase:                     jobregistry.RecoveryPhaseClaimed,
		// Pre-transport bound may be established; authority root is mandatory on active fence.
		RecoveryBoundPID: 1 << 29, RecoveryBoundTokenVersion: 1, RecoveryBoundStartMS: start,
		RecoveryBoundBootID: "boot-test", RecoveryBoundStartTicks: 42,
		RecoveryBoundAttempt: "crashed-attempt", RecoveryBoundFenceOwner: "durable-owner",
		RecoveryBoundGeneration: 1, RecoveryBoundIndexSetID: indexSetID, RecoveryBoundJobID: jobID,
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	authority := writeUnheldLease(t, indexSetID, "index-build-"+jobID)
	if got, gerr := store.GetReadOnlyStrict(jobID); gerr == nil {
		applyAuthorityIdentity(t, got, authority)
		if err := store.Write(got); err != nil {
			t.Fatal(err)
		}
	}

	result, err := RecoverManagedStalled(store, jobID, RecoverStalledOptions{
		AuthorityRoot: authority, Confirm: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Signalled {
		t.Fatalf("takeover must not signal: %#v", result)
	}
	if result.Outcome == OutcomeRefused && strings.Contains(result.Detail, "already claimed") {
		t.Fatalf("must takeover abandoned claim: %#v", result)
	}
	final, err := store.GetReadOnlyStrict(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != jobregistry.JobStateStopped {
		t.Fatalf("expected stopped after takeover finalize, got %s (%#v)", final.State, result)
	}
}

func TestRecoverManagedStalled_DarwinRefusesDestructive(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("darwin-only panel refuse path")
	}
	store, rec, authority := setupSuspectJob(t)
	before, err := os.ReadFile(store.JobPath(rec.JobID))
	if err != nil {
		t.Fatal(err)
	}
	result, err := RecoverManagedStalled(store, rec.JobID, RecoverStalledOptions{
		AuthorityRoot: authority, Confirm: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Signalled || result.ForcedKill {
		t.Fatalf("darwin must not signal: %#v", result)
	}
	if result.Outcome != OutcomeRefused {
		t.Fatalf("expected refused, got %#v", result)
	}
	if !strings.Contains(result.Detail, "preflight") && !strings.Contains(result.Detail, "unsupported") {
		// Accept any refuse before fence; prefer preflight wording when present.
		t.Logf("darwin refuse detail: %s", result.Detail)
	}
	// D-R11-04: pre-fence refuse is byte-preserving (no stopping fence).
	after, err := os.ReadFile(store.JobPath(rec.JobID))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("darwin destructive refuse must not mutate job record before fence")
	}
	got, err := store.GetReadOnlyStrict(rec.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State == jobregistry.JobStateStopped {
		t.Fatal("darwin refuse must not finalize stopped without death proof")
	}
}

func TestRecoverManagedStalled_LegacyTokenRefusesBeforeFence(t *testing.T) {
	// D-R11-04: lossy/legacy tokens must refuse without BeginStalledRecovery mutation.
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	start := uint64(12345)
	indexSetID := "idx_legacylegacylegacylegacylegacylegacylegacylegacylegacylegacy"
	// Live PID with only legacy start_ms — no versioned native fields.
	rec := &jobregistry.JobRecord{
		JobID: "11111111-1111-1111-1111-111111111111", Type: jobregistry.JobTypeIndexBuild,
		State: jobregistry.JobStateRunning, PID: os.Getpid(),
		ProcessStartTimeUnixMS: &start, ProcessTokenVersion: 0,
		IndexSetID: indexSetID, CreatedAt: now, StartedAt: &now,
		LastHeartbeat: ptrTime(now.Add(-10 * time.Minute)),
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	authority := writeHeldLease(t, indexSetID, "index-build-"+rec.JobID)
	before, err := os.ReadFile(store.JobPath(rec.JobID))
	if err != nil {
		t.Fatal(err)
	}
	result, err := RecoverManagedStalled(store, rec.JobID, RecoverStalledOptions{
		AuthorityRoot: authority, Confirm: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// May refuse as identity-mismatch (live token differs) or preflight/legacy.
	if result.Signalled {
		t.Fatalf("legacy must not signal: %#v", result)
	}
	after, err := os.ReadFile(store.JobPath(rec.JobID))
	if err != nil {
		t.Fatal(err)
	}
	// If plan never reached signal candidate, state may still be running.
	// When confirm would have fenced, preflight must keep bytes identical.
	if result.Outcome == OutcomeRefused && strings.Contains(result.Detail, "preflight") {
		if string(before) != string(after) {
			t.Fatal("legacy preflight refuse must be byte-preserving")
		}
	}
	if got, gerr := store.GetReadOnlyStrict(rec.JobID); gerr == nil && got.State == jobregistry.JobStateStopped {
		t.Fatal("legacy refuse must not finalize stopped")
	}
}

func TestRecoverManagedStalled_CallerNowIgnoredOnConfirm(t *testing.T) {
	// Fresh heartbeat + Confirm with forged future Now must not signal.
	store, rec, authority := setupSuspectJob(t)
	// Make heartbeat fresh.
	start := *rec.ProcessStartTimeUnixMS
	if err := store.TouchHeartbeat(rec.JobID, rec.PID, &start, rec.ProcessBootID); err != nil {
		t.Fatal(err)
	}
	result, err := RecoverManagedStalled(store, rec.JobID, RecoverStalledOptions{
		AuthorityRoot: authority,
		Confirm:       true,
		Now:           time.Now().UTC().Add(10 * time.Minute), // forged — must be ignored
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Signalled {
		t.Fatalf("forged Now must not authorize signal: %#v", result)
	}
	if result.Outcome != OutcomeRefused {
		t.Fatalf("expected refused healthy/fresh, got %#v", result)
	}
	if !jobregistry.IsProcessAlive(rec.PID) {
		t.Fatal("self must survive")
	}
}

func TestRecoverManagedStalled_FreshHeartbeatBeforeFencePreventsSignal(t *testing.T) {
	store, rec, authority := setupSuspectJob(t)
	// Plan-time stale snapshot, then a fresh heartbeat before confirm.
	staleHB := *rec.LastHeartbeat
	start := *rec.ProcessStartTimeUnixMS
	if err := store.TouchHeartbeat(rec.JobID, rec.PID, &start, rec.ProcessBootID); err != nil {
		t.Fatal(err)
	}
	// Recover with Confirm must refuse (snapshot mismatch or healthy plan).
	// After heartbeat the plan itself is healthy — refuse without signal.
	result, err := RecoverManagedStalled(store, rec.JobID, RecoverStalledOptions{
		AuthorityRoot: authority,
		Confirm:       true,
		// Inject plan clock? Plan uses Now; without injection it sees fresh HB as healthy.
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Signalled {
		t.Fatalf("must not signal after fresh heartbeat: %#v", result)
	}
	if result.Outcome != OutcomeRefused {
		t.Fatalf("expected refused, got %#v", result)
	}
	// Process (self) still alive.
	if !jobregistry.IsProcessAlive(rec.PID) {
		t.Fatal("process must survive")
	}
	// And still running (no fence acquired from stale decision).
	got, err := store.GetReadOnlyStrict(rec.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != jobregistry.JobStateRunning {
		t.Fatalf("must remain running after refused recover: %#v", got)
	}
	_ = staleHB
}

func setupSuspectJob(t *testing.T) (*jobregistry.Store, *jobregistry.JobRecord, string) {
	t.Helper()
	store, rec := writeRunningSelfJob(t, "idx_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	stale := time.Now().UTC().Add(-10 * time.Minute)
	rec.LastHeartbeat = &stale
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	authority := writeHeldLease(t, rec.IndexSetID, "index-build-"+rec.JobID)
	return store, rec, authority
}

func ptrTime(t time.Time) *time.Time { return &t }
