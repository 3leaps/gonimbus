package jobregistry

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

// D-R12-04 / D-R17 deadline evidence matrix (session-owned transport).
// Uses package-private sessionNowForTest only (not public API).
// Children are re-exec of this test binary (no bash/sleep dependency) so Linux
// and native Windows runners share one harness.

func requireLinuxOrWindowsBind(t *testing.T) {
	t.Helper()
	switch runtime.GOOS {
	case "linux", "windows":
	case "darwin":
		t.Skip("destructive Bind unsupported on Darwin; live Linux/Windows runner residual")
	default:
		t.Skip("platform unsupported for Bind")
	}
}

type childSession struct {
	store    *Store
	cmd      *exec.Cmd
	deadline time.Time
	base     time.Time
	sess     *SignalSession
}

func setupChildSignalSession(t *testing.T) *childSession {
	t.Helper()
	requireLinuxOrWindowsBind(t)
	cmd := spawnSignalChild(t, false)
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
	cs := &childSession{store: store, cmd: cmd, deadline: deadline, base: now}
	sessionNowForTest = func() time.Time { return now.Add(time.Minute) }
	t.Cleanup(func() { sessionNowForTest = nil })
	sess, err := store.OpenSignalSession(testJobID1, "owner-1", "a1", 1, deadline)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	cs.sess = sess
	return cs
}

func assertNoDeliverySent(t *testing.T, store *Store) {
	t.Helper()
	got, err := store.GetReadOnlyStrict(testJobID1)
	if err != nil {
		t.Fatal(err)
	}
	if got.RecoveryDeliverySignalled || got.RecoveryDeliveryForced {
		t.Fatalf("delivery flags must be clear: signalled=%v forced=%v", got.RecoveryDeliverySignalled, got.RecoveryDeliveryForced)
	}
}

func assertExactPhase(t *testing.T, store *Store, want string) {
	t.Helper()
	got, err := store.GetReadOnlyStrict(testJobID1)
	if err != nil {
		t.Fatal(err)
	}
	if got.RecoveryPhase != want {
		t.Fatalf("phase=%q want %q", got.RecoveryPhase, want)
	}
}

func assertChildAlive(t *testing.T, pid int) {
	t.Helper()
	if !IsProcessAlive(pid) {
		t.Fatal("child must remain live")
	}
}

// expireAfterNClockReads makes the first n sessionClockNow reads live, then expired.
func expireAfterNClockReads(t *testing.T, base, deadline time.Time, n int) {
	t.Helper()
	reads := 0
	sessionNowForTest = func() time.Time {
		reads++
		if reads <= n {
			return base.Add(time.Minute)
		}
		return deadline.Add(time.Second)
	}
	t.Cleanup(func() { sessionNowForTest = nil })
}

func TestEvidence_DeadlineMatrix_TermPostIntent(t *testing.T) {
	// Open (1) + Deliver pre-intent (2) live; pre-syscall (3) expired.
	// Exact postcondition: phase=term-intent, delivery flags false, child alive.
	cs := setupChildSignalSession(t)
	_ = cs.sess.Close()
	expireAfterNClockReads(t, cs.base, cs.deadline, 2)
	sess, err := cs.store.OpenSignalSession(testJobID1, "owner-1", "a1", 1, cs.deadline)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.Close() }()
	err = sess.DeliverTerm()
	if err == nil || !strings.Contains(err.Error(), "deadline expired") {
		t.Fatalf("want deadline error, got %v", err)
	}
	assertChildAlive(t, cs.cmd.Process.Pid)
	assertNoDeliverySent(t, cs.store)
	assertExactPhase(t, cs.store, RecoveryPhaseTermIntent)
}

func TestEvidence_DeadlineMatrix_KillPostIntent(t *testing.T) {
	cs := setupChildSignalSession(t)
	_ = cs.sess.Close()
	expireAfterNClockReads(t, cs.base, cs.deadline, 2)
	sess, err := cs.store.OpenSignalSession(testJobID1, "owner-1", "a1", 1, cs.deadline)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.Close() }()
	err = sess.DeliverKill()
	if err == nil || !strings.Contains(err.Error(), "deadline expired") {
		t.Fatalf("want deadline error, got %v", err)
	}
	assertChildAlive(t, cs.cmd.Process.Pid)
	assertNoDeliverySent(t, cs.store)
	assertExactPhase(t, cs.store, RecoveryPhaseKillIntent)
}

func TestEvidence_DeadlineMatrix_KillSentReplayPreSyscall(t *testing.T) {
	// Seed kill-sent + delivery flags without OS kill; expire before replay syscall.
	requireLinuxOrWindowsBind(t)
	cmd := spawnSignalChild(t, false)
	id := ObserveProcessIdentity(cmd.Process.Pid)
	if !id.Proven {
		t.Skip("unproven")
	}
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	auth := t.TempDir()
	rec := &JobRecord{
		JobID: testJobID1, Type: JobTypeIndexBuild, State: JobStateRunning,
		PID: cmd.Process.Pid, CreatedAt: now, StartedAt: &now, LastHeartbeat: &now,
		IndexSetID: "idx_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	ApplyProcessIdentity(rec, id)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	seedActiveFence(t, store, rec, "owner-1", auth)
	if err := store.ClaimStalledRecoverySignal(testJobID1, "owner-1", "a1"); err != nil {
		t.Fatal(err)
	}
	for _, s := range []struct{ from, to string }{
		{RecoveryPhaseClaimed, RecoveryPhaseBound},
		{RecoveryPhaseBound, RecoveryPhaseKillIntent},
	} {
		if err := store.AdvanceRecoveryPhase(testJobID1, AdvanceRecoveryPhaseOptions{
			FenceOwner: "owner-1", AttemptID: "a1", ExpectedGeneration: 1,
			FromPhase: s.from, ToPhase: s.to,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.recordAcceptedSignalAndAdvance(testJobID1, "owner-1", "a1", 1,
		RecoveryPhaseKillIntent, RecoveryPhaseKillSent, true); err != nil {
		t.Fatal(err)
	}
	deadline := now.Add(time.Hour)
	expireAfterNClockReads(t, now, deadline, 2)
	sess, err := store.OpenSignalSession(testJobID1, "owner-1", "a1", 1, deadline)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.Close() }()
	err = sess.DeliverKill()
	if err == nil || !strings.Contains(err.Error(), "deadline expired") {
		t.Fatalf("want deadline on kill replay, got %v", err)
	}
	assertChildAlive(t, cmd.Process.Pid)
	got, _ := store.GetReadOnlyStrict(testJobID1)
	if got.RecoveryPhase != RecoveryPhaseKillSent {
		t.Fatalf("phase should remain kill-sent, got %s", got.RecoveryPhase)
	}
	if !got.RecoveryDeliverySignalled || !got.RecoveryDeliveryForced {
		t.Fatalf("kill-sent provenance must remain: signalled=%v forced=%v", got.RecoveryDeliverySignalled, got.RecoveryDeliveryForced)
	}
}

func TestEvidence_DeadlineMatrix_TermSentEscalationPreSyscall(t *testing.T) {
	// Seed term-sent, DeliverKill escalation expires after kill-intent is written.
	// Exact: phase=kill-intent, TERM delivery retained, forced=false, child alive.
	requireLinuxOrWindowsBind(t)
	cmd := spawnSignalChild(t, true)
	id := ObserveProcessIdentity(cmd.Process.Pid)
	if !id.Proven {
		t.Skip("unproven")
	}
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	auth := t.TempDir()
	rec := &JobRecord{
		JobID: testJobID1, Type: JobTypeIndexBuild, State: JobStateRunning,
		PID: cmd.Process.Pid, CreatedAt: now, StartedAt: &now, LastHeartbeat: &now,
		IndexSetID: "idx_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	ApplyProcessIdentity(rec, id)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	seedActiveFence(t, store, rec, "owner-1", auth)
	if err := store.ClaimStalledRecoverySignal(testJobID1, "owner-1", "a1"); err != nil {
		t.Fatal(err)
	}
	for _, s := range []struct{ from, to string }{
		{RecoveryPhaseClaimed, RecoveryPhaseBound},
		{RecoveryPhaseBound, RecoveryPhaseTermIntent},
	} {
		if err := store.AdvanceRecoveryPhase(testJobID1, AdvanceRecoveryPhaseOptions{
			FenceOwner: "owner-1", AttemptID: "a1", ExpectedGeneration: 1,
			FromPhase: s.from, ToPhase: s.to,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.recordAcceptedSignalAndAdvance(testJobID1, "owner-1", "a1", 1,
		RecoveryPhaseTermIntent, RecoveryPhaseTermSent, false); err != nil {
		t.Fatal(err)
	}
	deadline := now.Add(time.Hour)
	// Open (1) live; DeliverKill: requireDeadline (2) live, ensureIntent→kill-intent,
	// requireDeadline pre-syscall (3) expired.
	expireAfterNClockReads(t, now, deadline, 2)
	sess, err := store.OpenSignalSession(testJobID1, "owner-1", "a1", 1, deadline)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.Close() }()
	err = sess.DeliverKill()
	if err == nil || !strings.Contains(err.Error(), "deadline expired") {
		t.Fatalf("want deadline on term-sent escalation, got %v", err)
	}
	assertChildAlive(t, cmd.Process.Pid)
	got, _ := store.GetReadOnlyStrict(testJobID1)
	if got.RecoveryPhase != RecoveryPhaseKillIntent {
		t.Fatalf("want phase kill-intent after pre-syscall expiry, got %s", got.RecoveryPhase)
	}
	if !got.RecoveryDeliverySignalled {
		t.Fatal("prior TERM delivery provenance must be retained")
	}
	if got.RecoveryDeliveryForced {
		t.Fatal("forced delivery must not be set without accepted kill syscall")
	}
}

func TestEvidence_DeadlineMatrix_PreIntentExpiryBytePreserving(t *testing.T) {
	cs := setupChildSignalSession(t)
	_ = cs.sess.Close()
	expireAfterNClockReads(t, cs.base, cs.deadline, 1) // only Open live
	sess, err := cs.store.OpenSignalSession(testJobID1, "owner-1", "a1", 1, cs.deadline)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.Close() }()
	before, _ := os.ReadFile(cs.store.JobPath(testJobID1))
	err = sess.DeliverTerm()
	if err == nil || !strings.Contains(err.Error(), "deadline expired") {
		t.Fatalf("want deadline, got %v", err)
	}
	after, _ := os.ReadFile(cs.store.JobPath(testJobID1))
	if string(before) != string(after) {
		t.Fatal("pre-intent deadline refuse must be byte-preserving")
	}
	assertChildAlive(t, cs.cmd.Process.Pid)
	assertNoDeliverySent(t, cs.store)
}

// Live TERM→KILL on TERM-ignoring portable child (Bind platforms).
func TestEvidence_DeadlineMatrix_TermThenKillLiveConvergence(t *testing.T) {
	requireLinuxOrWindowsBind(t)
	cmd := spawnSignalChild(t, true)
	id := ObserveProcessIdentity(cmd.Process.Pid)
	if !id.Proven {
		t.Skipf("child unproven: %s", FormatProcessIdentity(id))
	}
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	auth := t.TempDir()
	rec := &JobRecord{
		JobID: testJobID1, Type: JobTypeIndexBuild, State: JobStateRunning,
		PID: cmd.Process.Pid, CreatedAt: now, StartedAt: &now, LastHeartbeat: &now,
		IndexSetID: "idx_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
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
	sessionNowForTest = func() time.Time { return now.Add(time.Minute) }
	t.Cleanup(func() { sessionNowForTest = nil })
	sess, err := store.OpenSignalSession(testJobID1, "owner-1", "a1", 1, deadline)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	defer func() { _ = sess.Close() }()

	if err := sess.DeliverTerm(); err != nil {
		t.Fatalf("DeliverTerm: %v", err)
	}
	assertExactPhase(t, store, RecoveryPhaseTermSent)
	got, _ := store.GetReadOnlyStrict(testJobID1)
	if !got.RecoveryDeliverySignalled {
		t.Fatalf("after TERM: signalled must be true, forced=%v", got.RecoveryDeliveryForced)
	}
	// Windows: DeliverTerm is hard-stop (TerminateProcess); child is already dead.
	// Unix TERM-ignore child must stay live for real escalation proof.
	if sess.HardStopOnly() {
		done, err := sess.WaitTerminated(3*time.Second, 25*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if !done {
			t.Fatal("hard-stop DeliverTerm must terminate the child")
		}
		return
	}
	done, err := sess.WaitTerminated(300*time.Millisecond, 25*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("TERM-ignore child must still be live after DeliverTerm")
	}
	assertChildAlive(t, cmd.Process.Pid)

	if err := sess.DeliverKill(); err != nil {
		t.Fatalf("DeliverKill escalation: %v", err)
	}
	assertExactPhase(t, store, RecoveryPhaseKillSent)
	got, _ = store.GetReadOnlyStrict(testJobID1)
	if !got.RecoveryDeliveryForced {
		t.Fatal("after KILL forced delivery must be set")
	}
	done, err = sess.WaitTerminated(3*time.Second, 25*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("child must terminate after DeliverKill")
	}
}

func TestEvidence_DeadlineMatrix_TermSentResumeKillEscalation(t *testing.T) {
	requireLinuxOrWindowsBind(t)
	cmd := spawnSignalChild(t, true)
	id := ObserveProcessIdentity(cmd.Process.Pid)
	if !id.Proven {
		t.Skip("unproven")
	}
	store := NewStore(t.TempDir())
	now := time.Now().UTC()
	auth := t.TempDir()
	rec := &JobRecord{
		JobID: testJobID1, Type: JobTypeIndexBuild, State: JobStateRunning,
		PID: cmd.Process.Pid, CreatedAt: now, StartedAt: &now, LastHeartbeat: &now,
		IndexSetID: "idx_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	ApplyProcessIdentity(rec, id)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	seedActiveFence(t, store, rec, "owner-1", auth)
	if err := store.ClaimStalledRecoverySignal(testJobID1, "owner-1", "a1"); err != nil {
		t.Fatal(err)
	}
	for _, s := range []struct{ from, to string }{
		{RecoveryPhaseClaimed, RecoveryPhaseBound},
		{RecoveryPhaseBound, RecoveryPhaseTermIntent},
	} {
		if err := store.AdvanceRecoveryPhase(testJobID1, AdvanceRecoveryPhaseOptions{
			FenceOwner: "owner-1", AttemptID: "a1", ExpectedGeneration: 1,
			FromPhase: s.from, ToPhase: s.to,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.recordAcceptedSignalAndAdvance(testJobID1, "owner-1", "a1", 1,
		RecoveryPhaseTermIntent, RecoveryPhaseTermSent, false); err != nil {
		t.Fatal(err)
	}
	deadline := now.Add(time.Hour)
	sessionNowForTest = func() time.Time { return now.Add(time.Minute) }
	t.Cleanup(func() { sessionNowForTest = nil })
	sess, err := store.OpenSignalSession(testJobID1, "owner-1", "a1", 1, deadline)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.Close() }()
	if err := sess.DeliverKill(); err != nil {
		t.Fatalf("resume DeliverKill: %v", err)
	}
	assertExactPhase(t, store, RecoveryPhaseKillSent)
	done, err := sess.WaitTerminated(3*time.Second, 25*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("child must die after resume kill escalation")
	}
}
