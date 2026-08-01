package indexcoord

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/leasefixture"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

// E4: recoverer subprocess is SIGKILL'd *after* a durable recovery boundary is
// reached; parent asserts exact persisted snapshot, verifies SIGKILL exit, then
// a fresh recoverer resumes that generation without reseeding.
//
// Durable crash windows (distinct store phases):
//   claimed | death-observed | lease-reconciled
// pre-finalize is the same persisted phase as lease-reconciled and is not a
// separate matrix entry (devrev: one durable-state crash window).

func TestEvidence_RecovererSIGKILLThenResumeConverges(t *testing.T) {
	// Dead managed PID: no Bind required (Darwin-capable death/W2 path).
	for _, barrier := range []string{"claimed", "death-observed", "lease-reconciled"} {
		barrier := barrier
		t.Run(barrier, func(t *testing.T) {
			runRecovererSIGKILLAtBarrier(t, barrier)
		})
	}
}

// TestEvidence_ProductionPathIgnoresEvidenceEnv proves the library path cannot
// be delayed or write marker artifacts from environment alone. The park hook
// is only installed inside the re-exec helper process (not via env).
func TestEvidence_ProductionPathIgnoresEvidenceEnv(t *testing.T) {
	if recoveryEvidencePark != nil {
		t.Fatal("recoveryEvidencePark must be nil outside the re-exec helper process")
	}
	barrierDir := t.TempDir()
	t.Setenv("GONIMBUS_STALLED_EVIDENCE_HELPER", "1")
	t.Setenv("GONIMBUS_STALLED_EVIDENCE_BARRIER_DIR", barrierDir)
	t.Setenv("GONIMBUS_STALLED_EVIDENCE_PARK_AT", "all")

	store := jobregistry.NewStore(t.TempDir())
	now := time.Now().UTC()
	start := uint64(1)
	indexSetID := "idx_" + padHex64('e')
	jobID := "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
	claimedAt := now.Add(-time.Minute)
	rec := &jobregistry.JobRecord{
		JobID: jobID, Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateStopping,
		PID: 1 << 26, ProcessStartTimeUnixMS: &start, ProcessTokenVersion: 1,
		ProcessStartTicks: 3, ProcessBootID: "boot-test",
		IndexSetID: indexSetID, CreatedAt: now, StartedAt: &now,
		LastHeartbeat:  ptrTime(now.Add(-10 * time.Minute)),
		RecoveryIntent: jobregistry.RecoveryIntentStalled, RecoveryOwner: "owner-1",
		RecoverySignalOwner: "", RecoveryStartedAt: &claimedAt,
		RecoveryGeneration: 1, RecoveryPhase: jobregistry.RecoveryPhaseFenced,
		RecoveryBoundPID: 1 << 26, RecoveryBoundTokenVersion: 1, RecoveryBoundStartMS: start,
		RecoveryBoundBootID: "boot-test", RecoveryBoundStartTicks: 3,
		RecoveryBoundFenceOwner: "owner-1", RecoveryBoundGeneration: 1,
		RecoveryBoundIndexSetID: indexSetID, RecoveryBoundJobID: jobID,
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	authority := writeUnheldLease(t, indexSetID, "index-build-"+jobID)
	if _, err := leasefixture.PlantValidUnheldAs(authority, indexSetID, "index-build-"+jobID); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetReadOnlyStrict(jobID); err == nil {
		applyAuthorityIdentity(t, got, authority)
		_ = store.Write(got)
	}

	// Must complete quickly: a 60s park would fail this timeout window.
	done := make(chan struct{})
	var res RecoverStalledResult
	var callErr error
	go func() {
		defer close(done)
		res, callErr = RecoverManagedStalled(store, jobID, RecoverStalledOptions{
			AuthorityRoot: authority, Confirm: true,
			WaitTimeout: 5 * time.Second, PollInterval: 20 * time.Millisecond,
		})
	}()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("RecoverManagedStalled blocked under evidence env; production path must ignore env-only park")
	}
	if callErr != nil {
		t.Fatal(callErr)
	}
	if res.Outcome != OutcomeReapedOnly && res.Outcome != OutcomeAlreadyStopped {
		t.Fatalf("unexpected outcome under env-only evidence vars: %#v", res)
	}
	// No barrier marker files may appear.
	entries, err := os.ReadDir(barrierDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("env-only evidence vars must not create barrier artifacts, got %v", entries)
	}
}

func runRecovererSIGKILLAtBarrier(t *testing.T, parkAt string) {
	t.Helper()
	storeRoot := t.TempDir()
	store := jobregistry.NewStore(storeRoot)
	now := time.Now().UTC()
	start := uint64(1)
	indexSetID := "idx_" + padHex64('d')
	jobID := "dddddddd-dddd-dddd-dddd-dddddddddddd"
	claimedAt := now.Add(-time.Minute)
	fenceOwner := "owner-1"
	rec := &jobregistry.JobRecord{
		JobID: jobID, Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateStopping,
		PID: 1 << 27, ProcessStartTimeUnixMS: &start, ProcessTokenVersion: 1,
		ProcessStartTicks: 7, ProcessBootID: "boot-test",
		IndexSetID: indexSetID, CreatedAt: now, StartedAt: &now,
		LastHeartbeat:  ptrTime(now.Add(-10 * time.Minute)),
		RecoveryIntent: jobregistry.RecoveryIntentStalled, RecoveryOwner: fenceOwner,
		RecoverySignalOwner: "", RecoveryStartedAt: &claimedAt,
		RecoveryGeneration: 1, RecoveryPhase: jobregistry.RecoveryPhaseFenced,
		RecoveryBoundPID: 1 << 27, RecoveryBoundTokenVersion: 1, RecoveryBoundStartMS: start,
		RecoveryBoundBootID: "boot-test", RecoveryBoundStartTicks: 7,
		RecoveryBoundFenceOwner: fenceOwner, RecoveryBoundGeneration: 1,
		RecoveryBoundIndexSetID: indexSetID, RecoveryBoundJobID: jobID,
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	authority := writeUnheldLease(t, indexSetID, "index-build-"+jobID)
	if _, err := leasefixture.PlantValidUnheldAs(authority, indexSetID, "index-build-"+jobID); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetReadOnlyStrict(jobID); err == nil {
		applyAuthorityIdentity(t, got, authority)
		_ = store.Write(got)
	}

	barrierDir := t.TempDir()
	helper := exec.Command(os.Args[0], "-test.run=TestEvidence_RecovererHelperProcess$", "-test.timeout=45s") // #nosec G204
	helper.Env = append(os.Environ(),
		"GONIMBUS_STALLED_EVIDENCE_HELPER=1",
		"GONIMBUS_STALLED_EVIDENCE_STORE="+storeRoot,
		"GONIMBUS_STALLED_EVIDENCE_AUTH="+authority,
		"GONIMBUS_STALLED_EVIDENCE_JOB="+jobID,
		"GONIMBUS_STALLED_EVIDENCE_BARRIER_DIR="+barrierDir,
		"GONIMBUS_STALLED_EVIDENCE_PARK_AT="+parkAt,
	)
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}

	marker := filepath.Join(barrierDir, parkAt)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if helper.ProcessState != nil && helper.ProcessState.Exited() {
			t.Fatalf("helper exited before barrier %q", parkAt)
		}
		if time.Now().After(deadline) {
			_ = helper.Process.Kill()
			_, _ = helper.Process.Wait()
			t.Fatalf("helper never reached durable barrier %q", parkAt)
		}
		time.Sleep(10 * time.Millisecond)
	}

	preKill, err := store.GetReadOnlyStrict(jobID)
	if err != nil {
		_ = helper.Process.Kill()
		_, _ = helper.Process.Wait()
		t.Fatal(err)
	}
	assertExactMidRecoverySnapshot(t, preKill, parkAt, fenceOwner, jobID, indexSetID)

	if err := helper.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL recoverer: %v", err)
	}
	waitErr := helper.Wait()
	assertHelperKilledBySIGKILL(t, helper, waitErr)

	postKill, err := store.GetReadOnlyStrict(jobID)
	if err != nil {
		t.Fatal(err)
	}
	// No store mutation between pre-kill assert and post-kill except claimer
	// process liveness outside the record — record fields must match.
	assertExactMidRecoverySnapshot(t, postKill, parkAt, fenceOwner, jobID, indexSetID)
	if !recoveryRecordEqualForE4(preKill, postKill) {
		t.Fatalf("job record mutated between pre-kill and post-kill at barrier %q\npre=%#v\npost=%#v", parkAt, preKill, postKill)
	}

	// Fresh recoverer resumes same generation — no reseed of job identity.
	res, err := RecoverManagedStalled(store, jobID, RecoverStalledOptions{
		AuthorityRoot: authority,
		Confirm:       true,
		WaitTimeout:   5 * time.Second,
		PollInterval:  20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Signalled {
		t.Fatalf("dead-target resume must not signal: %#v", res)
	}
	final, err := store.GetReadOnlyStrict(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != jobregistry.JobStateStopped {
		t.Fatalf("after recoverer SIGKILL + resume want stopped, got %s (%#v)", final.State, res)
	}
	if final.RecoveryIntent != "" || final.RecoveryOwner != "" || final.RecoverySignalOwner != "" {
		t.Fatalf("recovery ownership not cleared: intent=%q owner=%q signal=%q",
			final.RecoveryIntent, final.RecoveryOwner, final.RecoverySignalOwner)
	}
	if final.RecoveryPhase != jobregistry.RecoveryPhaseFinalized {
		t.Fatalf("expected phase finalized, got %q", final.RecoveryPhase)
	}
	if final.RecoveryGeneration != 1 {
		t.Fatalf("generation should remain 1, got %d", final.RecoveryGeneration)
	}
	if final.RecoveryW2Receipt == nil {
		t.Fatal("expected durable W2 receipt after resume")
	}
	r := final.RecoveryW2Receipt
	if r.JobID != jobID || r.IndexSetID != indexSetID || r.FenceOwner != fenceOwner {
		t.Fatalf("receipt identity wrong: %#v", r)
	}
	if r.Generation != 1 || r.Signalled || r.ForcedKill {
		t.Fatalf("receipt gen/signal wrong: %#v", r)
	}
	if final.Metadata == nil || final.Metadata["stalled_recovery"] != "completed" {
		t.Fatalf("terminal metadata missing: %#v", final.Metadata)
	}
}

func assertExactMidRecoverySnapshot(t *testing.T, mid *jobregistry.JobRecord, parkAt, fenceOwner, jobID, indexSetID string) {
	t.Helper()
	if mid.State != jobregistry.JobStateStopping {
		t.Fatalf("barrier %q: state=%s want stopping", parkAt, mid.State)
	}
	if mid.RecoveryIntent != jobregistry.RecoveryIntentStalled {
		t.Fatalf("barrier %q: intent missing", parkAt)
	}
	if mid.RecoveryOwner != fenceOwner {
		t.Fatalf("barrier %q: owner=%q want %q", parkAt, mid.RecoveryOwner, fenceOwner)
	}
	if mid.RecoveryGeneration != 1 {
		t.Fatalf("barrier %q: gen=%d", parkAt, mid.RecoveryGeneration)
	}
	if mid.RecoverySignalOwner == "" {
		t.Fatalf("barrier %q: exclusive signal owner required", parkAt)
	}
	if !jobregistry.BoundTargetMatches(mid) {
		t.Fatalf("barrier %q: bound target missing/mismatched", parkAt)
	}
	if mid.RecoveryBoundJobID != jobID || mid.RecoveryBoundIndexSetID != indexSetID {
		t.Fatalf("barrier %q: bound identity job=%q index=%q", parkAt, mid.RecoveryBoundJobID, mid.RecoveryBoundIndexSetID)
	}
	switch parkAt {
	case "claimed":
		if mid.RecoveryPhase != jobregistry.RecoveryPhaseClaimed {
			t.Fatalf("claimed barrier: phase=%q want claimed", mid.RecoveryPhase)
		}
		if mid.RecoveryW2Receipt != nil {
			t.Fatal("claimed barrier: W2 receipt must be absent")
		}
	case "death-observed":
		if mid.RecoveryPhase != jobregistry.RecoveryPhaseDeathObserved {
			t.Fatalf("death-observed barrier: phase=%q want death-observed", mid.RecoveryPhase)
		}
		if mid.RecoveryW2Receipt != nil {
			t.Fatal("death-observed barrier: W2 receipt must be absent")
		}
	case "lease-reconciled":
		if mid.RecoveryPhase != jobregistry.RecoveryPhaseLeaseReconciled {
			t.Fatalf("lease-reconciled barrier: phase=%q want lease-reconciled", mid.RecoveryPhase)
		}
		if mid.RecoveryW2Receipt == nil {
			t.Fatal("lease-reconciled barrier requires durable W2 receipt")
		}
		if mid.RecoveryW2Receipt.JobID != jobID || mid.RecoveryW2Receipt.IndexSetID != indexSetID {
			t.Fatalf("lease-reconciled receipt identity %#v", mid.RecoveryW2Receipt)
		}
		if mid.RecoveryW2Receipt.Signalled || mid.RecoveryW2Receipt.ForcedKill {
			t.Fatalf("lease-reconciled receipt must not claim signal: %#v", mid.RecoveryW2Receipt)
		}
	default:
		t.Fatalf("unknown barrier %q", parkAt)
	}
}

// recoveryRecordEqualForE4 compares durable recovery fields that must not change
// between pre-kill assert and post-kill wait (claimer liveness is outside the record).
func recoveryRecordEqualForE4(a, b *jobregistry.JobRecord) bool {
	if a == nil || b == nil {
		return a == b
	}
	// Compare via a stable subset rather than full reflect on times/maps noise.
	type snap struct {
		State, Intent, Owner, SignalOwner, Phase string
		Gen                                      int64
		BoundPID                                 int
		BoundJob, BoundIndex                     string
		HasReceipt                               bool
	}
	sa := snap{
		State: string(a.State), Intent: a.RecoveryIntent, Owner: a.RecoveryOwner,
		SignalOwner: a.RecoverySignalOwner, Phase: a.RecoveryPhase, Gen: a.RecoveryGeneration,
		BoundPID: a.RecoveryBoundPID, BoundJob: a.RecoveryBoundJobID, BoundIndex: a.RecoveryBoundIndexSetID,
		HasReceipt: a.RecoveryW2Receipt != nil,
	}
	sb := snap{
		State: string(b.State), Intent: b.RecoveryIntent, Owner: b.RecoveryOwner,
		SignalOwner: b.RecoverySignalOwner, Phase: b.RecoveryPhase, Gen: b.RecoveryGeneration,
		BoundPID: b.RecoveryBoundPID, BoundJob: b.RecoveryBoundJobID, BoundIndex: b.RecoveryBoundIndexSetID,
		HasReceipt: b.RecoveryW2Receipt != nil,
	}
	return reflect.DeepEqual(sa, sb)
}

func assertHelperKilledBySIGKILL(t *testing.T, helper *exec.Cmd, waitErr error) {
	t.Helper()
	if helper.ProcessState == nil {
		t.Fatalf("helper ProcessState nil after Wait (err=%v)", waitErr)
	}
	ws, ok := helper.ProcessState.Sys().(syscall.WaitStatus)
	if !ok {
		if helper.ProcessState.Success() {
			t.Fatal("helper exited success; expected SIGKILL mid-recovery")
		}
		return
	}
	if !ws.Signaled() {
		t.Fatalf("helper not signalled (exit=%d err=%v); expected SIGKILL at barrier", ws.ExitStatus(), waitErr)
	}
	if ws.Signal() != syscall.SIGKILL {
		t.Fatalf("helper signal=%v want SIGKILL", ws.Signal())
	}
}

// installEvidencePark is the only place that arms recoveryEvidencePark. It runs
// solely inside the re-exec helper process.
func installEvidencePark() {
	recoveryEvidencePark = func(name string) {
		dir := os.Getenv("GONIMBUS_STALLED_EVIDENCE_BARRIER_DIR")
		if dir == "" || name == "" {
			return
		}
		if want := os.Getenv("GONIMBUS_STALLED_EVIDENCE_PARK_AT"); want != "" && want != "all" && want != name {
			return
		}
		_ = os.MkdirAll(dir, 0o755)
		marker := filepath.Join(dir, name)
		_ = os.WriteFile(marker, []byte(name+"\n"), 0o600)
		continuePath := filepath.Join(dir, "continue-"+name)
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(continuePath); err == nil {
				return
			}
			time.Sleep(15 * time.Millisecond)
		}
	}
}

// TestEvidence_RecovererHelperProcess is the re-exec recoverer. Only runs when
// GONIMBUS_STALLED_EVIDENCE_HELPER=1. Installs the park hook in-process only —
// production builds never include this test entry or arm the hook.
func TestEvidence_RecovererHelperProcess(t *testing.T) {
	if os.Getenv("GONIMBUS_STALLED_EVIDENCE_HELPER") != "1" {
		t.Skip("helper process entry only")
	}
	storeRoot := os.Getenv("GONIMBUS_STALLED_EVIDENCE_STORE")
	auth := os.Getenv("GONIMBUS_STALLED_EVIDENCE_AUTH")
	jobID := os.Getenv("GONIMBUS_STALLED_EVIDENCE_JOB")
	if storeRoot == "" || auth == "" || jobID == "" {
		t.Fatal("helper env incomplete")
	}
	// Arm park only in this helper process (not via env in library code).
	installEvidencePark()
	store := jobregistry.NewStore(storeRoot)
	_, _ = RecoverManagedStalled(store, jobID, RecoverStalledOptions{
		AuthorityRoot: auth,
		Confirm:       true,
		WaitTimeout:   30 * time.Second,
		PollInterval:  50 * time.Millisecond,
	})
}

func padHex64(seed byte) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 64)
	c := hex[int(seed)%16]
	for i := range out {
		out[i] = c
	}
	return string(out)
}
