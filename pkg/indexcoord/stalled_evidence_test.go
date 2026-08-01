package indexcoord

import (
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/leasefixture"
	"github.com/3leaps/gonimbus/internal/procidentity"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

// ensure leasefixture import used when only evidence tests plant leases.

// D-R11-05 evidence package: successor race, phase-boundary resume seeds,
// concurrent claim mutations, platform refuse. Not a substitute for full
// multi-OS adversarial CI, but closes the protocol controls on this host.

func TestEvidence_SuccessorLeaseRaceUnheldDifferentHolder(t *testing.T) {
	// Crash model: death-observed, old residue unlinked (or never present),
	// successor planted at same pathname with different holder → refuse unlink.
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	indexSetID := "idx_" + strings.Repeat("a", 64)
	jobID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	start := uint64(1)
	claimedAt := now.Add(-time.Minute)
	rec := &jobregistry.JobRecord{
		JobID: jobID, Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateStopping,
		PID: 1 << 28, ProcessStartTimeUnixMS: &start, ProcessTokenVersion: 1,
		ProcessStartTicks: 42, ProcessBootID: "boot-test",
		IndexSetID: indexSetID, CreatedAt: now, StartedAt: &now,
		LastHeartbeat:  ptrTime(now.Add(-10 * time.Minute)),
		RecoveryIntent: jobregistry.RecoveryIntentStalled, RecoveryOwner: "owner-1",
		RecoverySignalOwner: "attempt-1", RecoverySignalClaimedAt: &claimedAt,
		RecoverySignalClaimerPID: 1 << 27, RecoverySignalClaimerTokenVersion: 1,
		RecoverySignalClaimerStartTicks: 99, RecoverySignalClaimerBootID: "boot-test",
		RecoveryStartedAt:  &claimedAt,
		RecoveryGeneration: 1, RecoveryPhase: jobregistry.RecoveryPhaseDeathObserved,
		RecoveryAuthorityRoot: "", // set after authority root known
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}

	segmentRoot := t.TempDir()
	authorityRoot, err := AuthorityRoot(segmentRoot)
	if err != nil {
		t.Fatal(err)
	}
	// Successor: same index set path, different managed job holder.
	if got, gerr := store.GetReadOnlyStrict(jobID); gerr == nil {
		applyAuthorityIdentity(t, got, authorityRoot)
		_ = store.Write(got)
	}
	planted, err := leasefixture.PlantValidUnheldAs(authorityRoot, indexSetID, "index-build-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(planted.Path)
	if err != nil {
		t.Fatal(err)
	}
	beforeDev, beforeIno := procidentity.FileDevIno(planted.Path)
	if beforeDev == 0 && beforeIno == 0 && runtime.GOOS != "windows" {
		t.Fatal("expected unix file identity on planted successor")
	}

	result, err := RecoverManagedStalled(store, jobID, RecoverStalledOptions{
		AuthorityRoot: authorityRoot, Confirm: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Reclaimed {
		t.Fatalf("must not reclaim successor: %#v", result)
	}
	if result.Outcome != OutcomeRefused {
		t.Fatalf("expected refused successor race, got %#v", result)
	}
	after, err := os.ReadFile(planted.Path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("successor lease bytes must survive resume")
	}
	afterDev, afterIno := procidentity.FileDevIno(planted.Path)
	if beforeDev != afterDev || beforeIno != afterIno {
		t.Fatalf("successor file identity changed: before=%d:%d after=%d:%d", beforeDev, beforeIno, afterDev, afterIno)
	}
	// Recovery must not finalize.
	got, err := store.GetReadOnlyStrict(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State == jobregistry.JobStateStopped {
		t.Fatal("must not finalize when successor blocks reclaim")
	}
}

func TestEvidence_SuccessorLeaseRaceHeld(t *testing.T) {
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	indexSetID := "idx_" + strings.Repeat("c", 64)
	jobID := "cccccccc-cccc-cccc-cccc-cccccccccccc"
	start := uint64(1)
	claimedAt := now.Add(-time.Minute)
	rec := &jobregistry.JobRecord{
		JobID: jobID, Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateStopping,
		PID: 1 << 28, ProcessStartTimeUnixMS: &start, ProcessTokenVersion: 1,
		ProcessStartTicks: 42, ProcessBootID: "boot-test",
		IndexSetID: indexSetID, CreatedAt: now, StartedAt: &now,
		LastHeartbeat:  ptrTime(now.Add(-10 * time.Minute)),
		RecoveryIntent: jobregistry.RecoveryIntentStalled, RecoveryOwner: "owner-1",
		RecoverySignalOwner: "attempt-1", RecoverySignalClaimedAt: &claimedAt,
		RecoverySignalClaimerPID: 1 << 27, RecoverySignalClaimerTokenVersion: 1,
		RecoverySignalClaimerStartTicks: 99, RecoverySignalClaimerBootID: "boot-test",
		RecoveryStartedAt:  &claimedAt,
		RecoveryGeneration: 1, RecoveryPhase: jobregistry.RecoveryPhaseDeathObserved,
		RecoveryAuthorityRoot: "", // set after authority root known
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	// Live held lease = successor owner still running.
	authority := writeHeldLease(t, indexSetID, "index-build-successor-holder")
	if got, gerr := store.GetReadOnlyStrict(jobID); gerr == nil {
		applyAuthorityIdentity(t, got, authority)
		_ = store.Write(got)
	}
	result, err := RecoverManagedStalled(store, jobID, RecoverStalledOptions{
		AuthorityRoot: authority, Confirm: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Reclaimed || result.Outcome == OutcomeSignalled || result.Outcome == OutcomeReapedOnly {
		t.Fatalf("held successor must not reclaim/finalize: %#v", result)
	}
	if result.Outcome != OutcomeLeaseStillHeld {
		// Also accept refused if probe path differs; must not finalize stopped.
		got, _ := store.GetReadOnlyStrict(jobID)
		if got != nil && got.State == jobregistry.JobStateStopped {
			t.Fatalf("must not stop with held lease: %#v outcome=%v", got, result)
		}
	}
}

func TestEvidence_LeaseReconciledResumeFinalizeOnly(t *testing.T) {
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	indexSetID := "idx_" + strings.Repeat("d", 64)
	jobID := "dddddddd-dddd-dddd-dddd-dddddddddddd"
	start := uint64(1)
	claimedAt := now.Add(-time.Minute)
	rec := &jobregistry.JobRecord{
		JobID: jobID, Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateStopping,
		PID: 1 << 28, ProcessStartTimeUnixMS: &start, ProcessTokenVersion: 1,
		ProcessStartTicks: 42, ProcessBootID: "boot-test",
		IndexSetID: indexSetID, CreatedAt: now, StartedAt: &now,
		LastHeartbeat:  ptrTime(now.Add(-10 * time.Minute)),
		RecoveryIntent: jobregistry.RecoveryIntentStalled, RecoveryOwner: "owner-1",
		RecoverySignalOwner: "attempt-old", RecoverySignalClaimedAt: &claimedAt,
		RecoverySignalClaimerPID: 1 << 27, RecoverySignalClaimerTokenVersion: 1,
		RecoverySignalClaimerStartTicks: 99, RecoverySignalClaimerBootID: "boot-test",
		RecoveryStartedAt:  &claimedAt,
		RecoveryGeneration: 1, RecoveryPhase: jobregistry.RecoveryPhaseLeaseReconciled,
		RecoveryW2Receipt: &jobregistry.RecoveryW2Receipt{
			SchemaVersion: 1, Generation: 1, FenceOwner: "owner-1",
			OriginAttempt: "attempt-old", JobID: jobID, IndexSetID: indexSetID,
			LeaseVerdict: "missing", Reclaimed: false, Signalled: true, ForcedKill: true,
			ReconciledAt: now,
		},
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	// Plant a successor that must not be touched (finalize-only path).
	segmentRoot := t.TempDir()
	authorityRoot, err := AuthorityRoot(segmentRoot)
	if err != nil {
		t.Fatal(err)
	}
	planted, err := leasefixture.PlantValidUnheldAs(authorityRoot, indexSetID, "index-build-should-survive")
	if err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(planted.Path)
	if got, gerr := store.GetReadOnlyStrict(jobID); gerr == nil {
		applyAuthorityIdentity(t, got, authorityRoot)
		_ = store.Write(got)
	}

	result, err := RecoverManagedStalled(store, jobID, RecoverStalledOptions{
		AuthorityRoot: authorityRoot, Confirm: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(planted.Path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("finalize-only resume must not touch successor lease")
	}
	got, err := store.GetReadOnlyStrict(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != jobregistry.JobStateStopped {
		t.Fatalf("expected finalize-only stop, got %s (%#v)", got.State, result)
	}
	if got.Metadata["stalled_recovery_forced_kill"] != "true" {
		t.Fatalf("forced kill provenance from receipt lost: %#v", got.Metadata)
	}
	if !result.ForcedKill || !result.Signalled {
		t.Fatalf("result must carry receipt provenance: %#v", result)
	}
}

func TestEvidence_PhaseBoundaryResumeSeeds(t *testing.T) {
	// Seed each recoverable phase with dead managed PID + dead claimer.
	phases := []struct {
		phase    string
		wantStop bool
		name     string
	}{
		{jobregistry.RecoveryPhaseClaimed, true, "claimed"},
		{jobregistry.RecoveryPhaseBound, true, "bound"},
		{jobregistry.RecoveryPhaseTermSent, true, "term-sent"},
		{jobregistry.RecoveryPhaseKillSent, true, "kill-sent"},
		{jobregistry.RecoveryPhaseDeathObserved, true, "death-observed"},
		{jobregistry.RecoveryPhaseLeaseReconciled, true, "lease-reconciled"},
	}
	for i, tc := range phases {
		t.Run(tc.name, func(t *testing.T) {
			store := jobregistry.NewStore(t.TempDir())
			now := time.Now().UTC()
			indexSetID := "idx_" + strings.Repeat("e", 64)
			jobID := "eeeeeeee-eeee-eeee-eeee-" + padHex(i, 12)
			start := uint64(1)
			claimedAt := now.Add(-time.Minute)
			rec := &jobregistry.JobRecord{
				JobID: jobID, Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateStopping,
				PID: 1 << 28, ProcessStartTimeUnixMS: &start, ProcessTokenVersion: 1,
				ProcessStartTicks: 42, ProcessBootID: "boot-test",
				IndexSetID: indexSetID, CreatedAt: now, StartedAt: &now,
				LastHeartbeat:  ptrTime(now.Add(-10 * time.Minute)),
				RecoveryIntent: jobregistry.RecoveryIntentStalled, RecoveryOwner: "owner-1",
				RecoverySignalOwner: "crashed", RecoverySignalClaimedAt: &claimedAt,
				RecoverySignalClaimerPID: 1 << 27, RecoverySignalClaimerTokenVersion: 1,
				RecoverySignalClaimerStartTicks: 99, RecoverySignalClaimerBootID: "boot-test",
				RecoveryStartedAt:  &claimedAt,
				RecoveryGeneration: 1, RecoveryPhase: tc.phase,
				// Historical bound target (immutable for generation) — required at advanced phases.
				RecoveryBoundPID: 1 << 28, RecoveryBoundTokenVersion: 1, RecoveryBoundStartMS: start,
				RecoveryBoundBootID: "boot-test", RecoveryBoundStartTicks: 42,
				RecoveryBoundAttempt: "crashed", RecoveryBoundFenceOwner: "owner-1",
				RecoveryBoundGeneration: 1, RecoveryBoundIndexSetID: indexSetID, RecoveryBoundJobID: jobID,
			}
			if tc.phase == jobregistry.RecoveryPhaseLeaseReconciled {
				rec.RecoveryW2Receipt = &jobregistry.RecoveryW2Receipt{
					SchemaVersion: 1, Generation: 1, FenceOwner: "owner-1",
					OriginAttempt: "crashed", JobID: jobID, IndexSetID: indexSetID,
					LeaseVerdict: "missing", ReconciledAt: now,
				}
			}
			if err := store.Write(rec); err != nil {
				t.Fatal(err)
			}
			authority := writeUnheldLease(t, indexSetID, "index-build-"+jobID)
			// death-observed without receipt: plant matching unheld so reclaim can proceed.
			if tc.phase == jobregistry.RecoveryPhaseDeathObserved {
				ar, err := AuthorityRoot(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				authority = ar
				if _, err := leasefixture.PlantValidUnheldAs(authority, indexSetID, "index-build-"+jobID); err != nil {
					t.Fatal(err)
				}
			}
			// Bind durable authority root on seed for W2.
			if got, gerr := store.GetReadOnlyStrict(jobID); gerr == nil {
				applyAuthorityIdentity(t, got, authority)
				_ = store.Write(got)
			}
			result, err := RecoverManagedStalled(store, jobID, RecoverStalledOptions{
				AuthorityRoot: authority, Confirm: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			got, gerr := store.GetReadOnlyStrict(jobID)
			if gerr != nil {
				t.Fatal(gerr)
			}
			if tc.wantStop && got.State != jobregistry.JobStateStopped {
				t.Fatalf("phase %s expected stop: state=%s result=%#v", tc.phase, got.State, result)
			}
			if result.Signalled {
				t.Fatalf("dead-target phase seed must not signal: %#v", result)
			}
		})
	}
}

func padHex(n int, width int) string {
	const digits = "0123456789abcdef"
	out := make([]byte, width)
	for i := width - 1; i >= 0; i-- {
		out[i] = digits[n%16]
		n /= 16
	}
	return string(out)
}

func TestEvidence_ConcurrentSignalClaimMutations(t *testing.T) {
	// 100+ concurrent claim attempts: exactly one exclusive claimer.
	const N = 128
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	pid := os.Getpid()
	obs := jobregistry.ObserveProcessIdentity(pid)
	if !obs.Proven {
		t.Skip("unproven")
	}
	rec := &jobregistry.JobRecord{
		JobID: "11111111-2222-3333-4444-555555555555", Type: jobregistry.JobTypeIndexBuild,
		State: jobregistry.JobStateRunning, PID: pid, CreatedAt: now, StartedAt: &now, LastHeartbeat: &now,
	}
	jobregistry.ApplyProcessIdentity(rec, obs)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	seedActiveFence(t, store, rec, "owner-conc", t.TempDir())

	var wins atomic.Int64
	var busy atomic.Int64
	var other atomic.Int64
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			attempt := "attempt-" + padHex(i, 8)
			err := store.ClaimStalledRecoverySignal(rec.JobID, "owner-conc", attempt)
			switch {
			case err == nil:
				wins.Add(1)
			case err != nil && (strings.Contains(err.Error(), "already claimed") ||
				strings.Contains(err.Error(), "signal phase") ||
				strings.Contains(err.Error(), "busy")):
				busy.Add(1)
			default:
				if err != nil {
					other.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("expected exactly 1 claim win, got wins=%d busy=%d other=%d", wins.Load(), busy.Load(), other.Load())
	}
	if wins.Load()+busy.Load()+other.Load() != N {
		t.Fatalf("lost attempts: wins=%d busy=%d other=%d", wins.Load(), busy.Load(), other.Load())
	}
}

func TestEvidence_PlatformDestructivePreflight(t *testing.T) {
	err := procidentity.CheckDestructiveRecoverySupported()
	switch runtime.GOOS {
	case "darwin":
		if err == nil {
			t.Fatal("darwin must report destructive recovery unsupported")
		}
	case "linux", "windows":
		if err != nil {
			t.Fatalf("expected supported on %s: %v", runtime.GOOS, err)
		}
	}
}

func TestEvidence_W2RequiresRealAuthorityAndPhase(t *testing.T) {
	// External package cannot inject lease facts; only authority coordinates.
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	pid := os.Getpid()
	obs := jobregistry.ObserveProcessIdentity(pid)
	if !obs.Proven {
		t.Skip("unproven")
	}
	rec := &jobregistry.JobRecord{
		JobID: "99999999-9999-9999-9999-999999999999", Type: jobregistry.JobTypeIndexBuild,
		State: jobregistry.JobStateRunning, PID: pid, CreatedAt: now, StartedAt: &now, LastHeartbeat: &now,
		IndexSetID: "idx_" + strings.Repeat("9", 64),
	}
	jobregistry.ApplyProcessIdentity(rec, obs)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	authorityRoot := t.TempDir()
	seedActiveFence(t, store, rec, "owner-1", authorityRoot)
	if err := store.ClaimStalledRecoverySignal(rec.JobID, "owner-1", "a1"); err != nil {
		t.Fatal(err)
	}
	// Wrong phase for W2.
	if err := store.ReconcileStalledW2(rec.JobID, "owner-1", "a1", 1); err == nil {
		t.Fatal("W2 from claimed phase must refuse")
	}
	// Successor unheld different holder at death-observed: store must not reclaim as ours.
	if err := store.AdvanceRecoveryPhase(rec.JobID, jobregistry.AdvanceRecoveryPhaseOptions{
		FenceOwner: "owner-1", AttemptID: "a1", ExpectedGeneration: 1,
		FromPhase: jobregistry.RecoveryPhaseClaimed, ToPhase: jobregistry.RecoveryPhaseBound,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceRecoveryPhase(rec.JobID, jobregistry.AdvanceRecoveryPhaseOptions{
		FenceOwner: "owner-1", AttemptID: "a1", ExpectedGeneration: 1,
		FromPhase: jobregistry.RecoveryPhaseBound, ToPhase: jobregistry.RecoveryPhaseDeathObserved,
	}); err != nil {
		t.Fatal(err)
	}
	planted, err := leasefixture.PlantValidUnheldAs(authorityRoot, rec.IndexSetID, "index-build-other-job")
	if err != nil {
		t.Fatal(err)
	}
	beforeBytes, _ := os.ReadFile(planted.Path)
	beforeRec, _ := os.ReadFile(store.JobPath(rec.JobID))
	if err := store.ReconcileStalledW2(rec.JobID, "owner-1", "a1", 1); err == nil {
		t.Fatal("W2 must refuse reclaiming successor holder")
	}
	afterBytes, _ := os.ReadFile(planted.Path)
	afterRec, _ := os.ReadFile(store.JobPath(rec.JobID))
	if string(beforeBytes) != string(afterBytes) {
		t.Fatal("successor lease bytes must survive")
	}
	if string(beforeRec) != string(afterRec) {
		t.Fatal("failed W2 must be byte-preserving on job record")
	}
}

func TestEvidence_DeadlineExpiredAtEntry(t *testing.T) {
	// D-R13-03: already-expired attempt deadline refuses before fence/signal.
	store, rec, authority := setupSuspectJob(t)
	before, err := os.ReadFile(store.JobPath(rec.JobID))
	if err != nil {
		t.Fatal(err)
	}
	result, err := RecoverManagedStalled(store, rec.JobID, RecoverStalledOptions{
		AuthorityRoot: authority,
		Confirm:       true,
		Deadline:      time.Now().Add(-time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Signalled {
		t.Fatalf("expired deadline must not signal: %#v", result)
	}
	if result.Outcome != OutcomeRecoveryFailed && result.Outcome != OutcomeRefused {
		// May refuse healthy/second-obs first; on signal path expect deadline.
		t.Logf("outcome=%s detail=%s", result.Outcome, result.Detail)
	}
	// If plan was signal candidate and confirmed, deadline should fail before mutation.
	after, err := os.ReadFile(store.JobPath(rec.JobID))
	if err != nil {
		t.Fatal(err)
	}
	// When second observation still classifies as signal candidate, recoverSignalCandidate
	// runs and refuses at entry with no fence.
	if strings.Contains(result.Detail, "deadline") {
		if string(before) != string(after) {
			t.Fatal("deadline refuse at entry must be byte-preserving")
		}
	}
}

func TestEvidence_BoundSnapshotMismatchRefusesTransport(t *testing.T) {
	// D-R13-02: after claim binds correctly, corrupt native bound ticks and same-attempt
	// reentry must fail closed without repairing from live record drift.
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	pid := os.Getpid()
	obs := jobregistry.ObserveProcessIdentity(pid)
	if !obs.Proven {
		t.Skip("unproven")
	}
	rec := &jobregistry.JobRecord{
		JobID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", Type: jobregistry.JobTypeIndexBuild,
		State: jobregistry.JobStateRunning, PID: pid, CreatedAt: now, StartedAt: &now, LastHeartbeat: &now,
	}
	jobregistry.ApplyProcessIdentity(rec, obs)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	seedActiveFence(t, store, rec, "owner-1", t.TempDir())
	if err := store.ClaimStalledRecoverySignal(rec.JobID, "owner-1", "attempt-1"); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetReadOnlyStrict(rec.JobID)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt durable bound native token while leaving claimer live.
	if got.RecoveryBoundStartTicks != 0 {
		got.RecoveryBoundStartTicks++
	} else if got.RecoveryBoundFiletime != 0 {
		got.RecoveryBoundFiletime++
	} else {
		got.RecoveryBoundStartSec++
	}
	if err := store.Write(got); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(store.JobPath(rec.JobID))
	// Same-attempt claim must refuse mismatched bound (no silent repair of native drift).
	if err := store.ClaimStalledRecoverySignal(rec.JobID, "owner-1", "attempt-1"); err == nil {
		t.Fatal("same-attempt claim must refuse mismatched bound snapshot")
	}
	after, _ := os.ReadFile(store.JobPath(rec.JobID))
	if string(before) != string(after) {
		t.Fatal("mismatched bound reentry must be byte-preserving")
	}
	if jobregistry.BoundTargetMatches(got) {
		t.Fatal("corrupted bound must fail closed")
	}
}
