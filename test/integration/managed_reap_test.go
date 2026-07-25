package integration

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/3leaps/gonimbus/internal/ownedproc"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

// TestReapManagedJobLeavesBystanderAlive is the first safety control for
// teardown reaping. A job record can outlive its child and keep naming a process
// id that now belongs to something else; teardown must not touch it.
func TestReapManagedJobLeavesBystanderAlive(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("managed-child reaping is unix-focused")
	}
	dataRoot := filepath.Join(t.TempDir(), "data")
	group := newManagedChildGroup(t)
	bystander := startBystander(t)

	jobID := uuid.NewString()
	writeStaleRunningJobRecord(t, dataRoot, jobID, bystander.PID())

	if reapManagedJobNow(t, dataRoot, jobID, group) {
		t.Fatal("a group with no member naming this job must never be signalled")
	}
	if !jobregistry.IsProcessAlive(bystander.PID()) {
		t.Fatal("the bystander process must still be alive")
	}
}

// TestReapManagedJobSignalsOnlyTheAnchoredGroup covers the window between
// proving a managed child is running and signalling it: the verified worker
// leaves, and the record comes to name something else.
//
// The worker is released to finish its own work and exit rather than being
// signalled by process id — a test that reached for a recyclable id here would
// carry the hazard it exists to rule out. Its absence is confirmed inside the
// hook, before any signal can be sent, so the ordering this control claims is the
// ordering it asserts.
func TestReapManagedJobSignalsOnlyTheAnchoredGroup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("managed-child reaping is unix-focused")
	}
	binary := buildManagedTestBinary(t)
	dataRoot := filepath.Join(t.TempDir(), "data")
	server, entered, release := fakeBarrierListServer(t)
	manifestPath := writeFakeS3Manifest(t, server.URL)

	group := newManagedChildGroup(t)
	start := group.launcher(binary, "index", "build", "--job", manifestPath, "--background", "--name", "reap-window")
	start.Env = managedTestEnv(dataRoot)
	out, err := start.CombinedOutput()
	if err != nil {
		t.Fatalf("start background: %v\n%s", err, out)
	}
	jobID := strings.TrimSpace(string(out))

	// The worker is parked in its listing call and is a member of the owned group.
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("managed worker never reached the listing call")
	}
	if members := managedJobGroupMembers(t, group.pgid, jobID); len(members) == 0 {
		t.Fatal("managed worker is not a member of the group this test owns")
	}
	if !group.alive() {
		t.Fatal("the group anchor must be holding the group id before the reap")
	}
	bystander := startBystander(t)

	anchorAliveInsideHook := false
	workerGoneBeforeSignal := false
	oldHook := afterReapIdentityCheck
	afterReapIdentityCheck = func() {
		release()
		deadline := time.Now().Add(30 * time.Second)
		for {
			if len(managedJobGroupMembers(t, group.pgid, jobID)) == 0 {
				workerGoneBeforeSignal = true
				break
			}
			if time.Now().After(deadline) {
				t.Error("the verified worker must be gone before the group is signalled")
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		// The record now names the bystander.
		writeStaleRunningJobRecord(t, dataRoot, jobID, bystander.PID())
		anchorAliveInsideHook = group.alive()
	}
	t.Cleanup(func() { afterReapIdentityCheck = oldHook })

	reapManagedJobNow(t, dataRoot, jobID, group)

	if !workerGoneBeforeSignal {
		t.Fatal("the control did not establish that the verified worker left before the signal")
	}
	if !anchorAliveInsideHook {
		t.Fatal("the anchor must still hold the group id at the moment of signalling")
	}
	if !jobregistry.IsProcessAlive(bystander.PID()) {
		t.Fatal("the bystander must never be signalled, even after it becomes the record's pid")
	}
}

// TestManagedChildGroupCleanupReapsWholeGroup pins the harness's final safety
// net: a test that returns before reaching an explicit reap — a t.Fatal, a
// timeout — must still leave nothing of its own running. Group ownership is what
// makes that possible without naming any process id.
func TestManagedChildGroupCleanupReapsWholeGroup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("managed-child reaping is unix-focused")
	}
	bystander := startBystander(t)
	reaped := make(chan struct{})

	t.Run("scoped", func(t *testing.T) {
		group := newManagedChildGroup(t)
		member, err := ownedproc.Start(group.launcher("sleep", "60"))
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			<-member.Done()
			close(reaped)
		}()
		// Return without any job-aware reap.
	})

	select {
	case <-reaped:
	case <-time.After(20 * time.Second):
		t.Fatal("group-owner cleanup must terminate every member of the group it owns")
	}
	if !jobregistry.IsProcessAlive(bystander.PID()) {
		t.Fatal("group-owner cleanup must not reach outside its own group")
	}
}

// TestReapManagedJobTerminatesRealLeakedChild is the effectiveness control: a
// genuine managed child, still running, is terminated and observed dead. Without
// it, the safety controls above could be satisfied by a reap that never kills
// anything.
func TestReapManagedJobTerminatesRealLeakedChild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("managed-child reaping is unix-focused")
	}
	binary := buildManagedTestBinary(t)
	dataRoot := filepath.Join(t.TempDir(), "data")
	// A slow LIST keeps the managed child running while the reap is exercised.
	server := fakeListServer(t, 30*time.Second)
	manifestPath := writeFakeS3Manifest(t, server.URL)

	group := newManagedChildGroup(t)
	start := group.launcher(binary, "index", "build", "--job", manifestPath, "--background", "--name", "reap-target")
	start.Env = managedTestEnv(dataRoot)
	out, err := start.CombinedOutput()
	if err != nil {
		t.Fatalf("start background: %v\n%s", err, out)
	}
	jobID := strings.TrimSpace(string(out))

	pid := waitForRunningManagedChild(t, dataRoot, jobID)
	if members := managedJobGroupMembers(t, group.pgid, jobID); len(members) == 0 {
		t.Fatalf("managed child pid=%d is not a member of the group this test owns", pid)
	}

	if !reapManagedJobNow(t, dataRoot, jobID, group) {
		t.Fatal("a live managed child must be reaped")
	}
	requireTerminatedWithin(t, pid, 15*time.Second)
}

// TestCommandLineNamesJobRequiresTheFlagValuePair pins that identity is the flag
// and its value adjacent, not the two strings appearing somewhere in a command
// line. A log path or a job id mentioned in an unrelated argument must not read
// as ownership.
func TestCommandLineNamesJobRequiresTheFlagValuePair(t *testing.T) {
	jobID := uuid.NewString()
	other := uuid.NewString()
	cases := []struct {
		name string
		args []string
		want bool
	}{
		{"adjacent pair", []string{"index", "build", managedJobIDFlag, jobID}, true},
		{"equals form", []string{"index", "build", managedJobIDFlag + "=" + jobID}, true},
		{"flag with a different job", []string{"index", "build", managedJobIDFlag, other, "--log", jobID}, false},
		{"both strings, never as a pair", []string{"tail", "-f", "/logs/" + jobID + ".log", "--note", managedJobIDFlag}, false},
		{"flag with no value", []string{"index", "build", managedJobIDFlag}, false},
	}
	for _, tc := range cases {
		if got := commandLineNamesJob(tc.args, jobID); got != tc.want {
			t.Errorf("%s: commandLineNamesJob = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// startBystander runs an unrelated process in its own group, standing in for
// anything else on the machine — in CI, that includes the runner's own
// processes. Like every child this harness starts directly, its wait has exactly
// one owner.
func startBystander(t *testing.T) *ownedproc.Child {
	t.Helper()
	cmd := exec.Command("sleep", "30")
	setOwnProcessGroup(cmd)
	bystander, err := ownedproc.Start(cmd)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if stopErr := bystander.Stop(15 * time.Second); stopErr != nil {
			t.Errorf("stop bystander: %v", stopErr)
		}
	})
	return bystander
}

// writeStaleRunningJobRecord plants a record that claims a live process id is
// still its running child — the exact shape a child that died before persisting
// a terminal state leaves behind.
func writeStaleRunningJobRecord(t *testing.T, dataRoot, jobID string, pid int) {
	t.Helper()
	started := time.Now().UTC()
	record := jobregistry.JobRecord{
		JobID:        jobID,
		Type:         jobregistry.JobTypeIndexBuild,
		State:        jobregistry.JobStateRunning,
		ManifestPath: filepath.Join(dataRoot, "index.yaml"),
		PID:          pid,
		CreatedAt:    started,
		StartedAt:    &started,
	}
	dir := filepath.Join(dataRoot, "jobs", "index-build", jobID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "job.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
}

// waitForRunningManagedChild blocks until the job record carries a live pid.
func waitForRunningManagedChild(t *testing.T, dataRoot, jobID string) int {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		record, err := readManagedJobRecord(dataRoot, jobID)
		if err == nil && record.PID > 0 && record.State == jobregistry.JobStateRunning && jobregistry.IsProcessAlive(record.PID) {
			return record.PID
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("managed child never reached a running state with a live pid")
	return 0
}
