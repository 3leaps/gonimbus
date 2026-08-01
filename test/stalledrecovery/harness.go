package stalledrecovery

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/procidentity"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

// BindCapable reports whether this GOOS supports instance-stable destructive Bind.
func BindCapable() bool {
	switch runtime.GOOS {
	case "linux", "windows":
		return true
	default:
		return false
	}
}

// RequireBind skips or fails when destructive Bind is unavailable.
func RequireBind(t *testing.T, cfg Config) {
	t.Helper()
	if BindCapable() {
		return
	}
	msg := "destructive Bind unsupported on " + runtime.GOOS + " (use Linux/Windows runner for full transport evidence)"
	if cfg.Strict {
		t.Fatal(msg)
	}
	t.Skip(msg)
}

// SeededChild is a live child process with a proven birth token written onto a
// job record under a durable recovery fence (pre-signal).
type SeededChild struct {
	Store     *jobregistry.Store
	JobID     string
	Cmd       *exec.Cmd
	AuthRoot  string
	Owner     string
	AttemptID string
	Gen       int64
}

// SpawnSleepChild starts a long-lived child for signal/deadline evidence.
// Portable: re-execs this package's test binary (no Unix sleep/bash dependency),
// so Linux and native Windows runners share one harness.
func SpawnSleepChild(t *testing.T, seconds int) *exec.Cmd {
	t.Helper()
	if seconds < 1 {
		seconds = 60
	}
	ready := filepath.Join(t.TempDir(), "sleep-ready")
	// -test.run must match only the helper; park duration is soft (parent Kill ends it).
	cmd := exec.Command(os.Args[0], "-test.run=TestSpawnSleepChildHelper$", "-test.timeout=5m") // #nosec G204
	cmd.Env = append(os.Environ(),
		"GONIMBUS_STALLED_SLEEP_HELPER=1",
		"GONIMBUS_STALLED_SLEEP_READY="+ready,
		"GONIMBUS_STALLED_SLEEP_SECONDS="+itoa(seconds),
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start portable sleep child: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
			t.Fatal("portable sleep child never became ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cmd
}

// SeedFencedChildJob writes a running→fenced managed job for the child PID with
// full v1 identity, claimer claim, and canonical authority root.
func SeedFencedChildJob(t *testing.T, cfg Config, cmd *exec.Cmd) *SeededChild {
	t.Helper()
	id := jobregistry.ObserveProcessIdentity(cmd.Process.Pid)
	if !id.Proven {
		t.Skipf("child identity unproven: %s", jobregistry.FormatProcessIdentity(id))
	}
	root := MintRoot(t, cfg, "job")
	store := jobregistry.NewStore(filepath.Join(root, "jobs"))
	authRoot := filepath.Join(root, "authority")
	if err := os.MkdirAll(authRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	jobID := "11111111-1111-1111-1111-111111111111"
	rec := &jobregistry.JobRecord{
		JobID: jobID, Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateRunning,
		PID: cmd.Process.Pid, IndexSetID: "idx_" + strings.Repeat("a", 64),
		CreatedAt: now, StartedAt: &now, LastHeartbeat: ptrTime(now.Add(-10 * time.Minute)),
	}
	jobregistry.ApplyProcessIdentity(rec, id)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	// Use Begin when platform allows fence; otherwise seed fence fields for
	// death-side / W2-only evidence (still needs authority root identity).
	owner := "evidence-owner"
	opts := jobregistry.BeginStalledRecoveryOptions{
		Owner: owner, ExpectedPID: rec.PID, ExpectedStartMS: id.StartTimeUnixMS,
		ExpectedBootID: id.BootID, ExpectedTokenVersion: id.TokenVersion,
		ExpectedStartTicks: id.StartTicks, ExpectedStartSec: id.StartSec,
		ExpectedStartUsec: id.StartUsec, ExpectedFiletime: id.Filetime,
		ExpectedIndexSetID: rec.IndexSetID, MatchPlanSnapshot: true,
		ExpectedLastHeartbeat: rec.LastHeartbeat, ExpectedAuthorityRoot: authRoot,
	}
	fence, err := store.BeginStalledRecovery(jobID, opts)
	if err != nil {
		// Darwin refuse is expected — seed durable fence for non-transport paths.
		if BindCapable() {
			t.Fatalf("BeginStalledRecovery: %v", err)
		}
		seedFenceWithoutBegin(t, store, rec, owner, authRoot)
	} else {
		owner = fence.Owner
	}
	if err := store.ClaimStalledRecoverySignal(jobID, owner, "evidence-attempt-1"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	return &SeededChild{
		Store: store, JobID: jobID, Cmd: cmd, AuthRoot: authRoot,
		Owner: owner, AttemptID: "evidence-attempt-1", Gen: 1,
	}
}

func seedFenceWithoutBegin(t *testing.T, store *jobregistry.Store, rec *jobregistry.JobRecord, owner, authRoot string) {
	t.Helper()
	now := time.Now().UTC()
	rec.State = jobregistry.JobStateStopping
	rec.RecoveryIntent = jobregistry.RecoveryIntentStalled
	rec.RecoveryOwner = owner
	rec.RecoveryGeneration = 1
	rec.RecoveryPhase = jobregistry.RecoveryPhaseFenced
	rec.RecoveryStartedAt = &now
	// Canonical authority identity via Abs+EvalSymlinks+dev/ino is applied by
	// Begin in production; for seed path mirror the store helper contract.
	abs, err := filepath.Abs(authRoot)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		t.Fatal(err)
	}
	rec.RecoveryAuthorityRoot = resolved
	rec.RecoveryAuthorityDev, rec.RecoveryAuthorityIno = procidentity.FileDevIno(resolved)
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
}

func ptrTime(t time.Time) *time.Time { return &t }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
