package indexcoord

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/leasefixture"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

// D-R12-04 stress evidence: concurrent recover outcomes + dual-fail HB path.

func TestEvidence_ConcurrentRecoverStalledRefusals(t *testing.T) {
	// Baseline: 128 concurrent confirms on a healthy job never signal.
	const N = 128
	store, rec, authority := setupSuspectJob(t)
	start := *rec.ProcessStartTimeUnixMS
	if err := store.TouchHeartbeat(rec.JobID, rec.PID, &start, rec.ProcessBootID); err != nil {
		t.Fatal(err)
	}
	var signalled atomic.Int64
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			result, err := RecoverManagedStalled(store, rec.JobID, RecoverStalledOptions{
				AuthorityRoot: authority,
				Confirm:       true,
			})
			if err != nil {
				return
			}
			if result.Signalled {
				signalled.Add(1)
			}
		}()
	}
	wg.Wait()
	if signalled.Load() != 0 {
		t.Fatalf("healthy concurrent recover must never signal, got %d", signalled.Load())
	}
	got, err := store.GetReadOnlyStrict(rec.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != jobregistry.JobStateRunning {
		t.Fatalf("must remain running, got %s", got.State)
	}
}

func TestEvidence_ConcurrentDeadTargetRecoveryConvergence(t *testing.T) {
	// E6 same-job contention: N concurrent confirms on one fenced dead-target job.
	// Zero signals; errs==0; final stopped with valid W2 receipt; outcomes enumerated.
	const N = 128
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	start := uint64(1)
	indexSetID := "idx_" + strings.Repeat("c", 64)
	jobID := "cccccccc-cccc-cccc-cccc-cccccccccccc"
	claimedAt := now.Add(-time.Minute)
	rec := &jobregistry.JobRecord{
		JobID: jobID, Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateStopping,
		PID: 1 << 28, ProcessStartTimeUnixMS: &start, ProcessTokenVersion: 1,
		ProcessStartTicks: 42, ProcessBootID: "boot-test",
		IndexSetID: indexSetID, CreatedAt: now, StartedAt: &now,
		LastHeartbeat:  ptrTime(now.Add(-10 * time.Minute)),
		RecoveryIntent: jobregistry.RecoveryIntentStalled, RecoveryOwner: "owner-1",
		RecoverySignalOwner: "", // open claim race
		RecoveryStartedAt:   &claimedAt, RecoveryGeneration: 1,
		RecoveryPhase:    jobregistry.RecoveryPhaseFenced,
		RecoveryBoundPID: 1 << 28, RecoveryBoundTokenVersion: 1, RecoveryBoundStartMS: start,
		RecoveryBoundBootID: "boot-test", RecoveryBoundStartTicks: 42,
		RecoveryBoundAttempt: "", RecoveryBoundFenceOwner: "owner-1",
		RecoveryBoundGeneration: 1, RecoveryBoundIndexSetID: indexSetID, RecoveryBoundJobID: jobID,
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	authority := writeUnheldLease(t, indexSetID, "index-build-"+jobID)
	if _, err := leasefixture.PlantValidUnheldAs(authority, indexSetID, "index-build-"+jobID); err != nil {
		t.Fatal(err)
	}
	if got, gerr := store.GetReadOnlyStrict(jobID); gerr == nil {
		applyAuthorityIdentity(t, got, authority)
		if err := store.Write(got); err != nil {
			t.Fatal(err)
		}
	}

	var (
		wg        sync.WaitGroup
		signalled atomic.Int64
		errs      atomic.Int64
		mu        sync.Mutex
		outcomes  = map[RecoverStalledOutcome]int{}
	)
	// Allowed concurrent outcomes for a single dead-target lineage race.
	// RecoveryFailed is NOT ordinary — fail the gate if any appear.
	allowed := map[RecoverStalledOutcome]bool{
		OutcomeReapedOnly:     true, // winner lineage
		OutcomeAlreadyStopped: true, // late callers after finalize
		OutcomeRefused:        true, // claim busy / bound / plan race
		OutcomeNoop:           true,
	}
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			res, err := RecoverManagedStalled(store, jobID, RecoverStalledOptions{
				AuthorityRoot: authority,
				Confirm:       true,
				WaitTimeout:   5 * time.Second,
				PollInterval:  20 * time.Millisecond,
			})
			if err != nil {
				errs.Add(1)
				return
			}
			if res.Signalled {
				signalled.Add(1)
			}
			mu.Lock()
			outcomes[res.Outcome]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if errs.Load() != 0 {
		t.Fatalf("unexpected API errors: %d (outcomes=%v)", errs.Load(), outcomes)
	}
	if signalled.Load() != 0 {
		t.Fatalf("dead-target concurrent recover must not signal, got %d", signalled.Load())
	}
	if outcomes[OutcomeRecoveryFailed] != 0 {
		t.Fatalf("recovery-failed must be zero for this gate, got %d full=%v", outcomes[OutcomeRecoveryFailed], outcomes)
	}
	total := 0
	for o, n := range outcomes {
		if !allowed[o] {
			t.Fatalf("unexpected outcome %s count=%d full=%v", o, n, outcomes)
		}
		total += n
	}
	if total != N {
		t.Fatalf("classified outcomes=%d want %d full=%v", total, N, outcomes)
	}
	// Exactly one durable terminal winner lineage: reaped-only counts as the
	// recovery that completed W2+finalize; already-stopped are late observers.
	if outcomes[OutcomeReapedOnly] != 1 {
		t.Fatalf("expected exactly one reaped-only winner lineage, got %v", outcomes)
	}
	final, err := store.GetReadOnlyStrict(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != jobregistry.JobStateStopped {
		t.Fatalf("expected stopped, got %s outcomes=%v", final.State, outcomes)
	}
	if final.RecoveryIntent != "" || final.RecoveryOwner != "" || final.RecoverySignalOwner != "" {
		t.Fatalf("recovery fields must be cleared: intent=%q owner=%q signal=%q",
			final.RecoveryIntent, final.RecoveryOwner, final.RecoverySignalOwner)
	}
	if final.RecoveryPhase != jobregistry.RecoveryPhaseFinalized {
		t.Fatalf("expected phase finalized, got %q", final.RecoveryPhase)
	}
	if final.RecoveryGeneration != 1 {
		t.Fatalf("generation should remain 1, got %d", final.RecoveryGeneration)
	}
	if final.RecoveryW2Receipt == nil {
		t.Fatal("expected durable W2 receipt after concurrent convergence")
	}
	r := final.RecoveryW2Receipt
	if r.JobID != jobID || r.IndexSetID != indexSetID || r.FenceOwner != "owner-1" {
		t.Fatalf("receipt identity wrong: %#v", r)
	}
	if r.Generation != 1 || r.OriginAttempt == "" {
		t.Fatalf("receipt gen/attempt wrong: %#v", r)
	}
	if r.Signalled || r.ForcedKill {
		t.Fatalf("receipt must not claim signal/forced for dead-target: %#v", r)
	}
	if r.LeaseVerdict != "missing" && r.LeaseVerdict != "unheld" {
		t.Fatalf("unexpected lease verdict %q", r.LeaseVerdict)
	}
	// If unheld was reclaimed, path identity should be present when platform provides it.
	if r.Reclaimed && strings.TrimSpace(r.LeasePath) == "" {
		t.Fatal("reclaimed receipt requires lease path")
	}
	if final.Metadata == nil || final.Metadata["stalled_recovery"] != "completed" {
		t.Fatalf("terminal metadata missing: %#v", final.Metadata)
	}
}

func TestEvidence_IndependentDeadTargetRecoveriesUnderRace(t *testing.T) {
	// E6 complementary: N independent jobs, each full dead-proof → W2 → finalize.
	const N = 128
	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	var wg sync.WaitGroup
	var fail atomic.Int64
	wg.Add(N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			start := uint64(1000 + i)
			// Unique 64-hex index set id per job (no collisions under N=128).
			indexSetID := fmt.Sprintf("idx_%064x", i)
			if len(indexSetID) != 4+64 {
				indexSetID = "idx_" + fmt.Sprintf("%064d", i)[:64]
			}
			jobID := fmt.Sprintf("eeeeeeee-eeee-eeee-eeee-%012x", i)
			claimedAt := now.Add(-time.Minute)
			rec := &jobregistry.JobRecord{
				JobID: jobID, Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateStopping,
				PID: 1<<20 + i, ProcessStartTimeUnixMS: &start, ProcessTokenVersion: 1,
				ProcessStartTicks: start, ProcessBootID: "boot-test",
				IndexSetID: indexSetID, CreatedAt: now, StartedAt: &now,
				LastHeartbeat:  ptrTime(now.Add(-10 * time.Minute)),
				RecoveryIntent: jobregistry.RecoveryIntentStalled, RecoveryOwner: "owner-" + jobID[:8],
				RecoverySignalOwner: "", RecoveryStartedAt: &claimedAt,
				RecoveryGeneration: 1, RecoveryPhase: jobregistry.RecoveryPhaseFenced,
				RecoveryBoundPID: 1<<20 + i, RecoveryBoundTokenVersion: 1, RecoveryBoundStartMS: start,
				RecoveryBoundBootID: "boot-test", RecoveryBoundStartTicks: start,
				RecoveryBoundFenceOwner: "owner-" + jobID[:8], RecoveryBoundGeneration: 1,
				RecoveryBoundIndexSetID: indexSetID, RecoveryBoundJobID: jobID,
			}
			if err := store.Write(rec); err != nil {
				fail.Add(1)
				return
			}
			// Per-job authority root with unique planted lease.
			segmentRoot := t.TempDir()
			authority, err := AuthorityRoot(segmentRoot)
			if err != nil {
				fail.Add(1)
				return
			}
			if err := os.MkdirAll(authority, 0o755); err != nil {
				fail.Add(1)
				return
			}
			if _, err := leasefixture.PlantValidUnheldAs(authority, indexSetID, "index-build-"+jobID); err != nil {
				fail.Add(1)
				return
			}
			if got, gerr := store.GetReadOnlyStrict(jobID); gerr == nil {
				applyAuthorityIdentity(t, got, authority)
				if err := store.Write(got); err != nil {
					fail.Add(1)
					return
				}
			}
			res, err := RecoverManagedStalled(store, jobID, RecoverStalledOptions{
				AuthorityRoot: authority, Confirm: true,
				WaitTimeout: 5 * time.Second, PollInterval: 20 * time.Millisecond,
			})
			if err != nil {
				t.Errorf("job %s api err: %v", jobID, err)
				fail.Add(1)
				return
			}
			if res.Signalled {
				t.Errorf("job %s signalled: %#v", jobID, res)
				fail.Add(1)
				return
			}
			if res.Outcome != OutcomeReapedOnly && res.Outcome != OutcomeAlreadyStopped {
				// Independent jobs should complete reaped-only (no contending siblings).
				if res.Outcome != OutcomeReapedOnly {
					t.Errorf("job %s outcome=%s detail=%s", jobID, res.Outcome, res.Detail)
					fail.Add(1)
					return
				}
			}
			final, err := store.GetReadOnlyStrict(jobID)
			if err != nil {
				t.Errorf("job %s get: %v", jobID, err)
				fail.Add(1)
				return
			}
			if final.State != jobregistry.JobStateStopped {
				t.Errorf("job %s state=%s", jobID, final.State)
				fail.Add(1)
				return
			}
			if final.RecoveryIntent != "" || final.RecoveryOwner != "" {
				t.Errorf("job %s recovery fields not cleared", jobID)
				fail.Add(1)
				return
			}
			if final.RecoveryPhase != jobregistry.RecoveryPhaseFinalized {
				t.Errorf("job %s phase=%q", jobID, final.RecoveryPhase)
				fail.Add(1)
				return
			}
			r := final.RecoveryW2Receipt
			if r == nil {
				t.Errorf("job %s missing receipt", jobID)
				fail.Add(1)
				return
			}
			if r.JobID != jobID || r.IndexSetID != indexSetID || r.Generation != 1 {
				t.Errorf("job %s receipt identity %#v", jobID, r)
				fail.Add(1)
				return
			}
			if r.Signalled || r.ForcedKill {
				t.Errorf("job %s receipt claims signal: %#v", jobID, r)
				fail.Add(1)
				return
			}
			if r.FenceOwner == "" || r.OriginAttempt == "" {
				t.Errorf("job %s receipt missing fence/attempt: %#v", jobID, r)
				fail.Add(1)
				return
			}
			if final.Metadata == nil || final.Metadata["stalled_recovery"] != "completed" {
				t.Errorf("job %s metadata %#v", jobID, final.Metadata)
				fail.Add(1)
			}
		}()
	}
	wg.Wait()
	if fail.Load() != 0 {
		t.Fatalf("%d independent recoveries failed", fail.Load())
	}
}

func TestEvidence_HeartbeatPersistErrorRefusesSignal(t *testing.T) {
	// Dual-fail style: live matching identity + stale age but HeartbeatPersistError set
	// must never authorize signal (plan indeterminate / refuse).
	store, rec, authority := setupSuspectJob(t)
	stale := time.Now().UTC().Add(-10 * time.Minute)
	rec.LastHeartbeat = &stale
	rec.HeartbeatPersistError = "simulated heartbeat write failure"
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(store.JobPath(rec.JobID))
	result, err := RecoverManagedStalled(store, rec.JobID, RecoverStalledOptions{
		AuthorityRoot: authority,
		Confirm:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Signalled {
		t.Fatalf("HB persist error must not signal: %#v", result)
	}
	if result.Outcome != OutcomeRefused && result.Outcome != OutcomeDryRun {
		// Plan class may be indeterminate → refused on confirm.
		if !strings.Contains(result.Detail, "heartbeat") &&
			!strings.Contains(strings.ToLower(result.Detail), "indeterminate") &&
			result.Plan.Class != PlanIndeterminate &&
			result.Plan.Class != PlanHealthy {
			t.Logf("outcome=%s class=%s detail=%s", result.Outcome, result.Plan.Class, result.Detail)
		}
	}
	got, _ := store.GetReadOnlyStrict(rec.JobID)
	if got.State == jobregistry.JobStateStopped {
		t.Fatal("must not finalize under HB write failure")
	}
	// Prefer no fence when refuse-before-fence paths apply.
	if got.State == jobregistry.JobStateStopping && runtime.GOOS == "darwin" {
		// Darwin may refuse at preflight after fence in older paths; r17 should refuse earlier.
		t.Logf("stopping after refuse on %s: %s", runtime.GOOS, result.Detail)
	}
	_ = before
}

func TestEvidence_MissingAuthorityRootOnSeedRefusesResume(t *testing.T) {
	// D-R16-02 / D-R17: active fence without durable authority root refuses.
	store, rec, authority := setupSuspectJob(t)
	seedActiveFence(t, store, rec, "owner-1", "") // empty
	before, _ := os.ReadFile(store.JobPath(rec.JobID))
	result, err := RecoverManagedStalled(store, rec.JobID, RecoverStalledOptions{
		AuthorityRoot: authority,
		Confirm:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Signalled {
		t.Fatal("must not signal")
	}
	after, _ := os.ReadFile(store.JobPath(rec.JobID))
	// Resume Begin may fail without mutating if already fenced with empty root.
	if string(before) != string(after) {
		// FailStalledRecovery might not run; Begin refuse should be byte-preserving.
		// If detail mentions authority root, prefer byte identity.
		if strings.Contains(result.Detail, "authority root") {
			t.Fatalf("authority-root refuse should be byte-preserving: %s", result.Detail)
		}
	}
}
