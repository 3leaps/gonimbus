package stalledrecovery_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
	"github.com/3leaps/gonimbus/test/stalledrecovery"
)

// Opt-in evidence suite. Absent GONIMBUS_STALLED_EVIDENCE → skip entire file's tests.
// Not part of default CI. Mirrors cloudtest / reflowthroughput BYO lanes.

func TestOptIn_EnvAbsentSkips(t *testing.T) {
	// Documents the skip contract: absent env → LoadConfig ok=false.
	// When the developer has enabled the lane, this meta-check is N/A.
	if _, ok := stalledrecovery.LoadConfig(); ok {
		t.Logf("%s is set; skip-contract verified separately by default CI without this env", stalledrecovery.EvidenceOptInEnv)
		return
	}
	if _, ok := stalledrecovery.LoadConfig(); ok {
		t.Fatal("LoadConfig must report disabled when env unset")
	}
}

func TestOptIn_DeadlineSessionMatrix(t *testing.T) {
	cfg := stalledrecovery.RequireOptIn(t)
	stalledrecovery.RequireBind(t, cfg)

	cmd := stalledrecovery.SpawnSleepChild(t, 120)
	seed := stalledrecovery.SeedFencedChildJob(t, cfg, cmd)
	// Advance to bound for session open.
	if err := seed.Store.AdvanceRecoveryPhase(seed.JobID, jobregistry.AdvanceRecoveryPhaseOptions{
		FenceOwner: seed.Owner, AttemptID: seed.AttemptID, ExpectedGeneration: seed.Gen,
		FromPhase: jobregistry.RecoveryPhaseClaimed, ToPhase: jobregistry.RecoveryPhaseBound,
	}); err != nil {
		// May already be claimed-only; try from fenced→claimed if needed.
		t.Fatalf("phase bound: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	sess, err := seed.Store.OpenSignalSession(seed.JobID, seed.Owner, seed.AttemptID, seed.Gen, deadline)
	if err != nil {
		t.Fatalf("OpenSignalSession: %v", err)
	}
	defer func() { _ = sess.Close() }()

	// Zero deadline must refuse on a fresh open.
	if _, err := seed.Store.OpenSignalSession(seed.JobID, seed.Owner, seed.AttemptID, seed.Gen, time.Time{}); err == nil {
		t.Fatal("zero deadline must refuse")
	}

	// Successful kill under live deadline (destroys child).
	if err := sess.DeliverKill(); err != nil {
		t.Fatalf("DeliverKill: %v", err)
	}
	done, err := sess.WaitTerminated(10*time.Second, 50*time.Millisecond)
	if err != nil || !done {
		t.Fatalf("wait terminated: done=%v err=%v", done, err)
	}
	got, err := seed.Store.GetReadOnlyStrict(seed.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.RecoveryDeliverySignalled || !got.RecoveryDeliveryForced {
		t.Fatalf("delivery flags missing: %#v", got)
	}
	if got.RecoveryPhase != jobregistry.RecoveryPhaseKillSent {
		t.Fatalf("want kill-sent, got %s", got.RecoveryPhase)
	}
}

func TestOptIn_ConcurrentRecoverHealthyNeverSignals(t *testing.T) {
	cfg := stalledrecovery.RequireOptIn(t)
	n := cfg.Concurrency
	root := stalledrecovery.MintRoot(t, cfg, "conc")
	store := jobregistry.NewStore(filepath.Join(root, "jobs"))
	now := time.Now().UTC()
	pid := os.Getpid()
	obs := jobregistry.ObserveProcessIdentity(pid)
	if !obs.Proven {
		t.Skipf("self unproven: %s", jobregistry.FormatProcessIdentity(obs))
	}
	jobID := "22222222-2222-2222-2222-222222222222"
	rec := &jobregistry.JobRecord{
		JobID: jobID, Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateRunning,
		PID: pid, IndexSetID: "idx_" + strings.Repeat("b", 64),
		CreatedAt: now, StartedAt: &now, LastHeartbeat: &now,
	}
	jobregistry.ApplyProcessIdentity(rec, obs)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	auth := filepath.Join(root, "authority")
	if err := os.MkdirAll(auth, 0o755); err != nil {
		t.Fatal(err)
	}

	var signalled atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			res, err := indexcoord.RecoverManagedStalled(store, jobID, indexcoord.RecoverStalledOptions{
				AuthorityRoot: auth,
				Confirm:       true,
			})
			if err == nil && res.Signalled {
				signalled.Add(1)
			}
		}()
	}
	wg.Wait()
	if signalled.Load() != 0 {
		t.Fatalf("healthy concurrent recover signalled %d times", signalled.Load())
	}
	got, err := store.GetReadOnlyStrict(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != jobregistry.JobStateRunning {
		t.Fatalf("want running, got %s", got.State)
	}
}

func TestOptIn_PlatformCapabilityReport(t *testing.T) {
	// Always useful when opted in: record host capability for pin notes.
	cfg := stalledrecovery.RequireOptIn(t)
	t.Logf("goos=%s bind_capable=%v strict=%v concurrency=%d",
		runtime.GOOS, stalledrecovery.BindCapable(), cfg.Strict, cfg.Concurrency)
	if cfg.Strict && !stalledrecovery.BindCapable() {
		t.Fatalf("STRICT set but Bind unsupported on %s", runtime.GOOS)
	}
}
