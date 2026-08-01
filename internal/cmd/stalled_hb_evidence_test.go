package cmd

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

// E7: re-exec child owns the set-authority lease and runs production
// startManagedHeartbeat; dual-fail injects only Touch + RecordError. Parent
// plan/recover refuses while the child is still alive and lease remains Held.

const (
	hbChildEnv      = "GONIMBUS_STALLED_HB_CHILD"
	hbChildStoreEnv = "GONIMBUS_STALLED_HB_STORE"
	hbChildSegEnv   = "GONIMBUS_STALLED_HB_SEGMENT"
	hbChildJobEnv   = "GONIMBUS_STALLED_HB_JOB"
	hbChildIdxEnv   = "GONIMBUS_STALLED_HB_INDEX"
	hbChildReadyEnv = "GONIMBUS_STALLED_HB_READY"
	hbChildBarrier  = "GONIMBUS_STALLED_HB_BARRIER" // dual-fail reached
)

func TestEvidence_DualFailManagedHB_RefusesSignal_ChildLease(t *testing.T) {
	storeRoot := t.TempDir()
	segmentRoot := t.TempDir()
	jobID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	indexSetID := "idx_" + strings.Repeat("b", 64)
	readyPath := filepath.Join(t.TempDir(), "ready")
	barrierPath := filepath.Join(t.TempDir(), "dual-fail")

	helper := exec.Command(os.Args[0], "-test.run=TestEvidence_DualFailHBChildHelper$", "-test.timeout=45s") // #nosec G204
	helper.Env = append(os.Environ(),
		hbChildEnv+"=1",
		hbChildStoreEnv+"="+storeRoot,
		hbChildSegEnv+"="+segmentRoot,
		hbChildJobEnv+"="+jobID,
		hbChildIdxEnv+"="+indexSetID,
		hbChildReadyEnv+"="+readyPath,
		hbChildBarrier+"="+barrierPath,
	)
	helper.Stdout = os.Stdout
	helper.Stderr = os.Stderr
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}
	// Always reap child.
	t.Cleanup(func() {
		_ = helper.Process.Kill()
		_, _ = helper.Process.Wait()
	})

	// Wait until child has written job + acquired lease.
	waitFile(t, readyPath, 10*time.Second)
	store := jobregistry.NewStore(storeRoot)
	childRec, err := store.GetReadOnlyStrict(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if childRec.PID != helper.Process.Pid {
		t.Fatalf("job PID=%d want child %d", childRec.PID, helper.Process.Pid)
	}
	if childRec.State != jobregistry.JobStateRunning {
		t.Fatalf("child job state=%s", childRec.State)
	}
	id := jobregistry.ProcessIdentityFromRecord(childRec)
	if !id.Proven {
		t.Fatalf("child identity unproven: %s", jobregistry.FormatProcessIdentity(id))
	}

	authorityRoot, err := indexsubstrate.SetAuthorityRootForSegmentSet(segmentRoot)
	if err != nil {
		t.Fatal(err)
	}
	holder := "index-build-" + jobID
	beforeLease, err := indexcoord.ProbeLease(authorityRoot, indexSetID, nil)
	if err != nil && beforeLease.Verdict == "" {
		t.Fatal(err)
	}
	if beforeLease.Verdict != indexcoord.LeaseHeld {
		t.Fatalf("pre dual-fail lease want held, got %s", beforeLease.Verdict)
	}
	if strings.TrimSpace(beforeLease.Holder) != holder {
		t.Fatalf("pre dual-fail holder=%q want %q", beforeLease.Holder, holder)
	}
	leasePath := beforeLease.Path

	// Wait for dual-fail barrier while child still alive.
	waitFile(t, barrierPath, 10*time.Second)
	if !jobregistry.IsProcessAlive(helper.Process.Pid) {
		t.Fatal("child must remain alive after dual-fail barrier")
	}
	if !heartbeatUnhealthy(store, jobID) {
		t.Fatal("expected heartbeat_unhealthy marker from child HB loop")
	}

	// Parent plan/recover while child holds lease.
	before, err := os.ReadFile(store.JobPath(jobID))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := indexcoord.PlanManagedStalledRecovery(store, jobID, indexcoord.StalledPlanOptions{
		AuthorityRoot: authorityRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Class != indexcoord.PlanIndeterminate {
		t.Fatalf("want PlanIndeterminate, got %s (%s)", plan.Class, plan.Detail)
	}
	if plan.SignalCandidate {
		t.Fatal("SignalCandidate must be false under unhealthy marker")
	}
	if !strings.Contains(plan.Detail, "unhealthy") && !strings.Contains(plan.Detail, "heartbeat") {
		t.Fatalf("plan detail should cite HB unhealthy: %s", plan.Detail)
	}

	result, err := indexcoord.RecoverManagedStalled(store, jobID, indexcoord.RecoverStalledOptions{
		AuthorityRoot: authorityRoot,
		Confirm:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Signalled {
		t.Fatalf("must not signal: %#v", result)
	}
	if result.Outcome != indexcoord.OutcomeRefused {
		t.Fatalf("want OutcomeRefused, got %s detail=%s", result.Outcome, result.Detail)
	}

	after, err := os.ReadFile(store.JobPath(jobID))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("job record must be byte-identical after refuse (no fence/owner/phase mutation)")
	}
	final, err := store.GetReadOnlyStrict(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != jobregistry.JobStateRunning {
		t.Fatalf("state must remain running, got %s", final.State)
	}
	if final.RecoveryIntent != "" || final.RecoveryOwner != "" || final.RecoveryPhase != "" {
		t.Fatalf("recovery fields must stay empty: intent=%q owner=%q phase=%q",
			final.RecoveryIntent, final.RecoveryOwner, final.RecoveryPhase)
	}

	afterLease, err := indexcoord.ProbeLease(authorityRoot, indexSetID, nil)
	if err != nil && afterLease.Verdict == "" {
		t.Fatal(err)
	}
	if afterLease.Verdict != indexcoord.LeaseHeld {
		t.Fatalf("lease must remain held after refuse, got %s", afterLease.Verdict)
	}
	if strings.TrimSpace(afterLease.Holder) != holder {
		t.Fatalf("holder drift: %q want %q", afterLease.Holder, holder)
	}
	if leasePath != "" && afterLease.Path != "" && afterLease.Path != leasePath {
		t.Fatalf("lease path changed: %q -> %q", leasePath, afterLease.Path)
	}

	// Reap child: no orderly Release — residue must not stay live-Held by the dead child.
	_ = helper.Process.Kill()
	waitState, waitErr := helper.Process.Wait()
	_ = waitState
	_ = waitErr
	// Ensure the OS has reaped before lease probe (no false live-held).
	deadline := time.Now().Add(3 * time.Second)
	for jobregistry.IsProcessAlive(helper.Process.Pid) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if jobregistry.IsProcessAlive(helper.Process.Pid) {
		t.Fatal("child still alive after Kill+Wait")
	}

	post, err := indexcoord.ProbeLease(authorityRoot, indexSetID, nil)
	if err != nil && post.Verdict == "" {
		t.Fatal(err)
	}
	// Pin AC: reaped child must not classify as live Held by that holder.
	if post.Verdict == indexcoord.LeaseHeld {
		t.Fatalf("after reaped child, lease must not be live Held (got held holder=%q path=%q)", post.Holder, post.Path)
	}
	switch post.Verdict {
	case indexcoord.LeaseUnheld:
		// Expected residue: same holder attribution when substrate retains doc.
		if h := strings.TrimSpace(post.Holder); h != "" && h != holder {
			t.Fatalf("unheld residue holder=%q want %q or empty", h, holder)
		}
		if leasePath != "" && post.Path != "" && post.Path != leasePath {
			t.Fatalf("unheld residue path changed: %q -> %q", leasePath, post.Path)
		}
	case indexcoord.LeaseMissing:
		// Acceptable if platform unlinked on death; no reclaim needed.
	default:
		t.Fatalf("unexpected post-exit lease verdict %q (want unheld or missing)", post.Verdict)
	}

	// Separate authorized cleanup when residue is unheld.
	if post.Verdict == indexcoord.LeaseUnheld {
		rr, rerr := indexcoord.ReclaimUnheldLease(authorityRoot, indexSetID)
		if rerr != nil {
			t.Fatalf("authorized reclaim of unheld residue: %v", rerr)
		}
		if !rr.Reclaimed && rr.Verdict != indexcoord.LeaseMissing {
			t.Fatalf("expected reclaim success or already-missing, got %#v", rr)
		}
		// After reclaim, lease must be missing.
		finalLease, ferr := indexcoord.ProbeLease(authorityRoot, indexSetID, nil)
		if ferr != nil && finalLease.Verdict == "" {
			t.Fatal(ferr)
		}
		if finalLease.Verdict != indexcoord.LeaseMissing {
			t.Fatalf("after reclaim want missing, got %s", finalLease.Verdict)
		}
	}
}

// TestEvidence_DualFailHBChildHelper: owns lease + runs dual-fail HB; parks after barrier.
func TestEvidence_DualFailHBChildHelper(t *testing.T) {
	if os.Getenv(hbChildEnv) != "1" {
		t.Skip("HB dual-fail child helper only")
	}
	storeRoot := os.Getenv(hbChildStoreEnv)
	segmentRoot := os.Getenv(hbChildSegEnv)
	jobID := os.Getenv(hbChildJobEnv)
	indexSetID := os.Getenv(hbChildIdxEnv)
	readyPath := os.Getenv(hbChildReadyEnv)
	barrierPath := os.Getenv(hbChildBarrier)
	if storeRoot == "" || segmentRoot == "" || jobID == "" || indexSetID == "" || readyPath == "" || barrierPath == "" {
		t.Fatal("helper env incomplete")
	}

	store := jobregistry.NewStore(storeRoot)
	now := time.Now().UTC()
	pid := os.Getpid()
	id := jobregistry.ObserveProcessIdentity(pid)
	if !id.Proven {
		t.Fatalf("self unproven: %s", jobregistry.FormatProcessIdentity(id))
	}
	// Stale age so only dual-fail/marker blocks signal authorization.
	stale := now.Add(-30 * time.Minute)
	rec := &jobregistry.JobRecord{
		JobID: jobID, Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateRunning,
		PID: pid, IndexSetID: indexSetID,
		CreatedAt: now, StartedAt: &now, LastHeartbeat: &stale,
	}
	jobregistry.ApplyProcessIdentity(rec, id)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}

	holder := "index-build-" + jobID
	lease, err := indexsubstrate.AcquireSetAuthority(context.Background(), segmentRoot, indexSetID, holder)
	if err != nil {
		t.Fatalf("child acquire lease: %v", err)
	}
	// Keep lease held until process death (no Release) so parent sees Held.
	_ = lease

	if err := os.WriteFile(readyPath, []byte(fmt.Sprintf("pid=%d goos=%s\n", pid, runtime.GOOS)), 0o600); err != nil {
		t.Fatal(err)
	}

	// Dual-fail only the two write paths; HB loop + marker + cancel are production.
	heartbeatTouchHook = func(store *jobregistry.Store, jobID string, pid int, startMS *uint64, bootID string) error {
		return fmt.Errorf("injected primary heartbeat write failure")
	}
	heartbeatRecordErrorHook = func(store *jobregistry.Store, jobID string, persistErr error) error {
		return fmt.Errorf("injected heartbeat-error record failure")
	}

	ctx, buildCancel := context.WithCancel(context.Background())
	stop, fatal := startManagedHeartbeat(ctx, buildCancel, store, rec, 15*time.Millisecond)
	defer stop()

	select {
	case err := <-fatal:
		if err == nil {
			t.Fatal("expected dual-fail fatal")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dual-fail not observed")
	}

	if err := os.WriteFile(barrierPath, []byte("dual-fail\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Park while holding lease so parent can plan/recover against live child.
	for {
		time.Sleep(time.Hour)
	}
}

func waitFile(t *testing.T, path string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", path)
		}
		time.Sleep(15 * time.Millisecond)
	}
}
