package jobregistry

import (
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/internal/leasefixture"
)

func beginOpts(pid int, start uint64, boot string, hb *time.Time) BeginStalledRecoveryOptions {
	obs := ObserveProcessIdentity(pid)
	return BeginStalledRecoveryOptions{
		Owner: "owner-1", ExpectedPID: pid, ExpectedStartMS: start, ExpectedBootID: boot,
		ExpectedTokenVersion: obs.TokenVersion, ExpectedStartTicks: obs.StartTicks,
		ExpectedStartSec: obs.StartSec, ExpectedStartUsec: obs.StartUsec, ExpectedFiletime: obs.Filetime,
		MatchPlanSnapshot: true, ExpectedLastHeartbeat: hb,
	}
}

func requireBeginPlatform(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "darwin" {
		t.Skip("Begin refuses destructive fence on Darwin")
	}
}

func seedActiveFence(t *testing.T, store *Store, rec *JobRecord, owner, authorityRoot string) {
	t.Helper()
	now := time.Now().UTC()
	rec.State = JobStateStopping
	rec.RecoveryIntent = RecoveryIntentStalled
	rec.RecoveryOwner = owner
	rec.RecoverySignalOwner = ""
	rec.RecoveryStartedAt = &now
	rec.RecoveryGeneration = 1
	rec.RecoveryPhase = RecoveryPhaseFenced
	if authorityRoot != "" {
		canon, dev, ino, err := resolveAuthorityRootIdentity(authorityRoot)
		if err != nil {
			t.Fatal(err)
		}
		rec.RecoveryAuthorityRoot = canon
		rec.RecoveryAuthorityDev = dev
		rec.RecoveryAuthorityIno = ino
	} else {
		rec.RecoveryAuthorityRoot = ""
		rec.RecoveryAuthorityDev = 0
		rec.RecoveryAuthorityIno = 0
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
}

func writeSelfRunning(t *testing.T, store *Store, hb time.Time) (*JobRecord, ProcessIdentity) {
	t.Helper()
	pid := os.Getpid()
	obs := ObserveProcessIdentity(pid)
	if !obs.Proven {
		t.Skipf("unproven: %s", FormatProcessIdentity(obs))
	}
	now := time.Now().UTC()
	rec := &JobRecord{
		JobID: testJobID1, Type: JobTypeIndexBuild, State: JobStateRunning, PID: pid,
		IndexSetID: "idx_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		CreatedAt:  now, StartedAt: &now, LastHeartbeat: &hb,
	}
	ApplyProcessIdentity(rec, obs)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	return rec, obs
}

func TestBeginStalledRecoveryRefusesUnsupportedPlatform(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("darwin-only")
	}
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	rec, obs := writeSelfRunning(t, store, now)
	before, _ := os.ReadFile(store.JobPath(testJobID1))
	if _, err := store.BeginStalledRecovery(testJobID1, beginOpts(rec.PID, obs.StartTimeUnixMS, obs.BootID, &now)); err == nil {
		t.Fatal("must refuse")
	}
	after, _ := os.ReadFile(store.JobPath(testJobID1))
	if string(before) != string(after) {
		t.Fatal("byte-preserving")
	}
}

func TestReconcileStalledW2UsesRealLease(t *testing.T) {
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	rec, _ := writeSelfRunning(t, store, now)
	authorityRoot := t.TempDir()
	_ = os.MkdirAll(authorityRoot, 0o755)
	seedActiveFence(t, store, rec, "owner-1", authorityRoot)
	if err := store.ClaimStalledRecoverySignal(testJobID1, "owner-1", "a1"); err != nil {
		t.Fatal(err)
	}
	// Advance to death-observed with delivery via unexported path after intent.
	if err := store.AdvanceRecoveryPhase(testJobID1, AdvanceRecoveryPhaseOptions{
		FenceOwner: "owner-1", AttemptID: "a1", ExpectedGeneration: 1,
		FromPhase: RecoveryPhaseClaimed, ToPhase: RecoveryPhaseBound,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceRecoveryPhase(testJobID1, AdvanceRecoveryPhaseOptions{
		FenceOwner: "owner-1", AttemptID: "a1", ExpectedGeneration: 1,
		FromPhase: RecoveryPhaseBound, ToPhase: RecoveryPhaseTermIntent,
	}); err != nil {
		t.Fatal(err)
	}
	// Manually set delivery as if session delivered (same-package unexported).
	if err := store.recordAcceptedSignalAndAdvance(testJobID1, "owner-1", "a1", 1,
		RecoveryPhaseTermIntent, RecoveryPhaseTermSent, false); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceRecoveryPhase(testJobID1, AdvanceRecoveryPhaseOptions{
		FenceOwner: "owner-1", AttemptID: "a1", ExpectedGeneration: 1,
		FromPhase: RecoveryPhaseTermSent, ToPhase: RecoveryPhaseDeathObserved,
	}); err != nil {
		t.Fatal(err)
	}

	if err := store.ReconcileStalledW2(testJobID1, "owner-1", "a1", 1); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetReadOnlyStrict(testJobID1)
	if err != nil {
		t.Fatal(err)
	}
	if got.RecoveryPhase != RecoveryPhaseLeaseReconciled || got.RecoveryW2Receipt == nil {
		t.Fatalf("expected receipt: %#v", got)
	}
	if got.RecoveryW2Receipt.LeaseVerdict != string(indexsubstrate.LeaseMissing) {
		t.Fatalf("want missing, got %#v", got.RecoveryW2Receipt)
	}
	if !got.RecoveryW2Receipt.Signalled {
		t.Fatal("delivery provenance lost")
	}

	// Held lease cannot be forged as missing: plant unheld then hold is hard;
	// plant unheld with wrong holder refuses reclaim path.
	before, _ := os.ReadFile(store.JobPath(testJobID1))
	// Already reconciled — second W2 must refuse.
	if err := store.ReconcileStalledW2(testJobID1, "owner-1", "a1", 1); err == nil {
		t.Fatal("second W2 must refuse")
	}
	after, _ := os.ReadFile(store.JobPath(testJobID1))
	if string(before) != string(after) {
		t.Fatal("byte-preserving")
	}
}

func TestW2ReclaimRealUnheld(t *testing.T) {
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	rec, _ := writeSelfRunning(t, store, now)
	authorityRoot := t.TempDir()
	_ = os.MkdirAll(authorityRoot, 0o755)
	seedActiveFence(t, store, rec, "owner-1", authorityRoot)
	if err := store.ClaimStalledRecoverySignal(testJobID1, "owner-1", "a1"); err != nil {
		t.Fatal(err)
	}
	for _, s := range []struct{ from, to string }{
		{RecoveryPhaseClaimed, RecoveryPhaseBound},
		{RecoveryPhaseBound, RecoveryPhaseDeathObserved},
	} {
		if err := store.AdvanceRecoveryPhase(testJobID1, AdvanceRecoveryPhaseOptions{
			FenceOwner: "owner-1", AttemptID: "a1", ExpectedGeneration: 1,
			FromPhase: s.from, ToPhase: s.to,
		}); err != nil {
			t.Fatal(err)
		}
	}
	holder := "index-build-" + testJobID1
	if _, err := leasefixture.PlantValidUnheldAs(authorityRoot, rec.IndexSetID, holder); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileStalledW2(testJobID1, "owner-1", "a1", 1); err != nil {
		t.Fatal(err)
	}
	got, _ := store.GetReadOnlyStrict(testJobID1)
	if got.RecoveryW2Receipt == nil || !got.RecoveryW2Receipt.Reclaimed {
		t.Fatalf("expected reclaim receipt: %#v", got.RecoveryW2Receipt)
	}
}

func TestBoundImmutableOnAdvancedTakeover(t *testing.T) {
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	rec, _ := writeSelfRunning(t, store, now)
	seedActiveFence(t, store, rec, "owner-1", t.TempDir())
	if err := store.ClaimStalledRecoverySignal(testJobID1, "owner-1", "a1"); err != nil {
		t.Fatal(err)
	}
	// Advance to kill-sent with bound intact.
	if err := store.AdvanceRecoveryPhase(testJobID1, AdvanceRecoveryPhaseOptions{
		FenceOwner: "owner-1", AttemptID: "a1", ExpectedGeneration: 1,
		FromPhase: RecoveryPhaseClaimed, ToPhase: RecoveryPhaseBound,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceRecoveryPhase(testJobID1, AdvanceRecoveryPhaseOptions{
		FenceOwner: "owner-1", AttemptID: "a1", ExpectedGeneration: 1,
		FromPhase: RecoveryPhaseBound, ToPhase: RecoveryPhaseKillIntent,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.recordAcceptedSignalAndAdvance(testJobID1, "owner-1", "a1", 1,
		RecoveryPhaseKillIntent, RecoveryPhaseKillSent, true); err != nil {
		t.Fatal(err)
	}
	got, _ := store.GetReadOnlyStrict(testJobID1)
	origTicks := got.RecoveryBoundStartTicks
	origPID := got.RecoveryBoundPID
	// Clear bound — advanced takeover must refuse inventing target.
	got.RecoveryBoundPID = 0
	got.RecoveryBoundTokenVersion = 0
	got.RecoverySignalOwner = "crashed"
	got.RecoverySignalClaimerPID = 1 << 28
	got.RecoverySignalClaimerTokenVersion = 1
	got.RecoverySignalClaimerStartTicks = 1
	got.RecoverySignalClaimerBootID = "x"
	if err := store.Write(got); err != nil {
		t.Fatal(err)
	}
	if err := store.ClaimStalledRecoverySignalVerifiedTakeover(testJobID1, "owner-1", "a2"); err == nil {
		t.Fatal("takeover must refuse missing bound at advanced phase")
	}
	// Restore wrong-generation bound — refuse.
	got, _ = store.GetReadOnlyStrict(testJobID1)
	got.RecoveryBoundPID = origPID
	got.RecoveryBoundTokenVersion = ProcessTokenVersionV1
	got.RecoveryBoundStartTicks = origTicks + 1
	got.RecoveryBoundGeneration = 1
	got.RecoveryBoundAttempt = "crashed"
	got.RecoveryBoundFenceOwner = "owner-1"
	got.RecoveryBoundJobID = testJobID1
	got.RecoveryBoundIndexSetID = rec.IndexSetID
	if rec.ProcessStartTimeUnixMS != nil {
		got.RecoveryBoundStartMS = *rec.ProcessStartTimeUnixMS
	}
	got.RecoveryBoundBootID = rec.ProcessBootID
	if err := store.Write(got); err != nil {
		t.Fatal(err)
	}
	if err := store.ClaimStalledRecoverySignalVerifiedTakeover(testJobID1, "owner-1", "a3"); err == nil {
		t.Fatal("takeover must refuse mismatched bound ticks")
	}
	_ = errors.New
}

func TestClaimExclusiveAndBound(t *testing.T) {
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	rec, _ := writeSelfRunning(t, store, now)
	seedActiveFence(t, store, rec, "owner-1", t.TempDir())
	if err := store.ClaimStalledRecoverySignal(testJobID1, "owner-1", "a"); err != nil {
		t.Fatal(err)
	}
	if err := store.ClaimStalledRecoverySignal(testJobID1, "owner-1", "b"); !errors.Is(err, ErrRecoverySignalBusy) {
		t.Fatalf("busy: %v", err)
	}
	got, _ := store.GetReadOnlyStrict(testJobID1)
	if !BoundTargetMatches(got) {
		t.Fatal("bound required")
	}
}

func TestForeignFailureCannotEraseActiveRecoveryFence(t *testing.T) {
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	rec, _ := writeSelfRunning(t, store, now)
	seedActiveFence(t, store, rec, "owner-1", t.TempDir())
	failed := *rec
	failed.State = JobStateFailed
	failed.RecoveryIntent = ""
	if err := store.Write(&failed); err == nil {
		t.Fatal("must refuse")
	}
}

func TestAdvanceRejectsZeroGen(t *testing.T) {
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	rec, _ := writeSelfRunning(t, store, now)
	seedActiveFence(t, store, rec, "owner-1", t.TempDir())
	if err := store.ClaimStalledRecoverySignal(testJobID1, "owner-1", "a1"); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceRecoveryPhase(testJobID1, AdvanceRecoveryPhaseOptions{
		FenceOwner: "owner-1", AttemptID: "a1", ExpectedGeneration: 0,
		FromPhase: RecoveryPhaseClaimed, ToPhase: RecoveryPhaseBound,
	}); err == nil {
		t.Fatal("zero gen")
	}
}

func TestAuthorityRootMissingOnActiveFenceRefuses(t *testing.T) {
	// D-R16-02: missing durable authority root on active fence is not repaired.
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	rec, obs := writeSelfRunning(t, store, now)
	seedActiveFence(t, store, rec, "owner-1", "") // empty root
	before, _ := os.ReadFile(store.JobPath(testJobID1))
	if err := store.CheckRecoveryAuthorityRoot(testJobID1, "owner-1", t.TempDir()); err == nil {
		t.Fatal("missing authority root must refuse")
	}
	after, _ := os.ReadFile(store.JobPath(testJobID1))
	if string(before) != string(after) {
		t.Fatal("check must be byte-preserving")
	}
	// Resume Begin must refuse empty root (non-Darwin only).
	if runtime.GOOS == "darwin" {
		return
	}
	opts := beginOpts(rec.PID, obs.StartTimeUnixMS, obs.BootID, &now)
	opts.ExpectedIndexSetID = rec.IndexSetID
	opts.ExpectedAuthorityRoot = t.TempDir()
	if _, err := store.BeginStalledRecovery(testJobID1, opts); err == nil {
		t.Fatal("resume Begin must refuse missing durable authority root")
	}
}

func TestOpenSignalSessionRequiresLiveDeadline(t *testing.T) {
	// D-R17-01: zero / expired deadline refuses; no public clock override.
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	rec, _ := writeSelfRunning(t, store, now)
	auth := t.TempDir()
	seedActiveFence(t, store, rec, "owner-1", auth)
	if err := store.ClaimStalledRecoverySignal(testJobID1, "owner-1", "a1"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenSignalSession(testJobID1, "owner-1", "a1", 1, time.Time{}); err == nil {
		t.Fatal("zero deadline must refuse")
	}
	if _, err := store.OpenSignalSession(testJobID1, "owner-1", "a1", 1, time.Now().Add(-time.Second)); err == nil {
		t.Fatal("expired deadline must refuse")
	}
}

func TestDeadlineExpiresBetweenIntentAndSyscall(t *testing.T) {
	// D-R17-02: two-step package clock — intent persists, pre-syscall deadline fails.
	// Sacrificial child so we never signal self.
	if runtime.GOOS == "darwin" {
		t.Skip("destructive Bind unsupported on Darwin; Linux/Windows runner evidence residual")
	}
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() }()
	id := ObserveProcessIdentity(cmd.Process.Pid)
	if !id.Proven {
		t.Skipf("child unproven: %s", FormatProcessIdentity(id))
	}
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	auth := t.TempDir()
	rec := &JobRecord{
		JobID: testJobID1, Type: JobTypeIndexBuild, State: JobStateRunning,
		PID: cmd.Process.Pid, IndexSetID: "idx_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		CreatedAt: now, StartedAt: &now, LastHeartbeat: &now,
	}
	ApplyProcessIdentity(rec, id)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	seedActiveFence(t, store, rec, "owner-1", auth)
	if err := store.ClaimStalledRecoverySignal(testJobID1, "owner-1", "a1"); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceRecoveryPhase(testJobID1, AdvanceRecoveryPhaseOptions{
		FenceOwner: "owner-1", AttemptID: "a1", ExpectedGeneration: 1,
		FromPhase: RecoveryPhaseClaimed, ToPhase: RecoveryPhaseBound,
	}); err != nil {
		t.Fatal(err)
	}
	deadline := now.Add(time.Hour)
	// Package-private clock: first N reads are live (open + pre-intent + revalidates),
	// then expire before pre-syscall check.
	reads := 0
	sessionNowForTest = func() time.Time {
		reads++
		// OpenSignalSession: 1 check; DeliverTerm: pre-intent, then after intent pre-syscall.
		// Allow open + pre-intent path, expire at second Deliver check.
		if reads <= 2 {
			return now.Add(time.Minute)
		}
		return deadline.Add(time.Second)
	}
	defer func() { sessionNowForTest = nil }()

	sess, err := store.OpenSignalSession(testJobID1, "owner-1", "a1", 1, deadline)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	defer func() { _ = sess.Close() }()

	err = sess.DeliverTerm()
	if err == nil {
		t.Fatal("DeliverTerm must fail at post-intent pre-syscall deadline")
	}
	if !strings.Contains(err.Error(), "deadline expired") {
		t.Fatalf("want deadline error, got %v", err)
	}
	// Child must still be alive.
	if !IsProcessAlive(cmd.Process.Pid) {
		t.Fatal("child must survive refused deliver")
	}
	got, err := store.GetReadOnlyStrict(testJobID1)
	if err != nil {
		t.Fatal(err)
	}
	if got.RecoveryDeliverySignalled || got.RecoveryDeliveryForced {
		t.Fatalf("no delivery flags: %#v", got)
	}
	// Phase may be term-intent (persisted) but not term-sent.
	if got.RecoveryPhase == RecoveryPhaseTermSent || got.RecoveryPhase == RecoveryPhaseKillSent {
		t.Fatalf("must not advance to sent phase: %s", got.RecoveryPhase)
	}
}

func TestBeginRefusesNativeTokenDrift(t *testing.T) {
	requireBeginPlatform(t)
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	rec, obs := writeSelfRunning(t, store, now)
	opts := beginOpts(rec.PID, obs.StartTimeUnixMS, obs.BootID, &now)
	opts.ExpectedIndexSetID = rec.IndexSetID
	if rec.ProcessStartTicks != 0 {
		opts.ExpectedStartTicks++
	} else if rec.ProcessFiletime != 0 {
		opts.ExpectedFiletime++
	} else {
		opts.ExpectedStartSec++
	}
	before, _ := os.ReadFile(store.JobPath(testJobID1))
	if _, err := store.BeginStalledRecovery(testJobID1, opts); err == nil {
		t.Fatal("drift refuse")
	}
	after, _ := os.ReadFile(store.JobPath(testJobID1))
	if string(before) != string(after) {
		t.Fatal("byte-preserving")
	}
}
