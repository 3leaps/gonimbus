package jobregistry

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testJobID1 = "11111111-1111-4111-8111-111111111111"
	testJobID2 = "22222222-2222-4222-8222-222222222222"
)

func TestStore_WriteGetRoundTrip(t *testing.T) {
	root := t.TempDir()
	s := NewStore(root)

	now := time.Date(2026, 1, 19, 12, 0, 0, 0, time.UTC)
	rec := &JobRecord{
		JobID:        testJobID1,
		Name:         "demo",
		State:        JobStateRunning,
		ManifestPath: "/tmp/manifest.yaml",
		CreatedAt:    now,
		StartedAt:    &now,
		Identity: &EffectiveIdentity{
			StorageProvider: "aws_s3",
			CloudProvider:   "aws",
			RegionKind:      "aws",
			Region:          "us-east-1",
		},
	}

	if err := s.Write(rec); err != nil {
		t.Fatalf("Write() error: %v", err)
	}

	got, err := s.Get(testJobID1)
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if got.JobID != rec.JobID {
		t.Fatalf("job_id mismatch: got=%q want=%q", got.JobID, rec.JobID)
	}
	if got.State != rec.State {
		t.Fatalf("state mismatch: got=%q want=%q", got.State, rec.State)
	}
	if got.Identity == nil || got.Identity.StorageProvider != "aws_s3" {
		t.Fatalf("identity not persisted")
	}
}

func TestStore_ListSortsNewestFirst(t *testing.T) {
	root := t.TempDir()
	s := NewStore(root)

	t1 := time.Date(2026, 1, 19, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 19, 13, 0, 0, 0, time.UTC)

	if err := s.Write(&JobRecord{JobID: testJobID1, State: JobStateRunning, ManifestPath: "/tmp/a", CreatedAt: t1, StartedAt: &t1}); err != nil {
		t.Fatalf("Write job-1: %v", err)
	}
	if err := s.Write(&JobRecord{JobID: testJobID2, State: JobStateRunning, ManifestPath: "/tmp/b", CreatedAt: t2, StartedAt: &t2}); err != nil {
		t.Fatalf("Write job-2: %v", err)
	}

	got, err := s.List()
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("unexpected job count: %d", len(got))
	}
	if got[0].JobID != testJobID2 {
		t.Fatalf("expected newest first, got[0]=%q", got[0].JobID)
	}
}

func TestListReadOnlyStrictDoesNotPromoteDeadPID(t *testing.T) {
	store := NewStore(t.TempDir())
	rec := &JobRecord{JobID: testJobID1, State: JobStateRunning, PID: 1 << 30, CreatedAt: time.Now().UTC()}
	if err := store.Write(rec); err != nil {
		t.Fatalf("Write() error: %v", err)
	}
	before, err := os.ReadFile(store.JobPath(testJobID1))
	if err != nil {
		t.Fatalf("read before snapshot: %v", err)
	}
	jobs, err := store.ListReadOnlyStrict()
	if err != nil {
		t.Fatalf("ListReadOnlyStrict() error: %v", err)
	}
	if len(jobs) != 1 || jobs[0].State != JobStateRunning {
		t.Fatalf("unexpected strict snapshot: %#v", jobs)
	}
	after, err := os.ReadFile(store.JobPath(testJobID1))
	if err != nil {
		t.Fatalf("read after snapshot: %v", err)
	}
	if string(before) != string(after) {
		t.Fatal("strict snapshot rewrote zombie job state")
	}
}

func TestListReadOnlyStrictRejectsMalformedAndSymlinkEntries(t *testing.T) {
	t.Run("malformed record", func(t *testing.T) {
		root := t.TempDir()
		jobDir := filepath.Join(root, testJobID1)
		if err := os.MkdirAll(jobDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(jobDir, "job.json"), []byte("{broken\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := NewStore(root).ListReadOnlyStrict(); err == nil || !strings.Contains(err.Error(), "parse job.json") {
			t.Fatalf("expected strict malformed-record error, got %v", err)
		}
	})

	t.Run("symlinked canonical job directory", func(t *testing.T) {
		root := t.TempDir()
		if err := os.Symlink(t.TempDir(), filepath.Join(root, testJobID1)); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		if _, err := NewStore(root).ListReadOnlyStrict(); err == nil || !strings.Contains(err.Error(), "unrecognized job registry entry") {
			t.Fatalf("expected strict symlink-entry error, got %v", err)
		}
	})
}

func TestValidatePersistedJobStateCompleteEnum(t *testing.T) {
	known := []JobState{
		JobStateQueued,
		JobStateRunning,
		JobStateStopping,
		JobStateStopped,
		JobStateSuccess,
		JobStatePartial,
		JobStateFailed,
		JobStateUnknown,
	}
	for _, state := range known {
		if err := validatePersistedJobState(state); err != nil {
			t.Fatalf("known state %q rejected: %v", state, err)
		}
	}
	for _, state := range []JobState{"", "runnnig", "future-state"} {
		if err := validatePersistedJobState(state); err == nil {
			t.Fatalf("unrecognized state %q accepted", state)
		}
	}
}

func TestStoreRejectsInvalidMismatchAndSymlinkPaths(t *testing.T) {
	root := t.TempDir()
	s := NewStore(root)
	requireInvalid := func(err error) {
		t.Helper()
		if err == nil {
			t.Fatal("expected invalid job id error")
		}
	}
	requireInvalid(s.Write(&JobRecord{JobID: "../escape", CreatedAt: time.Now().UTC()}))
	_, err := s.Get("../escape")
	requireInvalid(err)

	jobDir := filepath.Join(s.RootDir(), testJobID1)
	if err := os.MkdirAll(jobDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(jobDir, "job.json"), []byte(`{"job_id":"`+testJobID2+`","created_at":"2026-01-19T12:00:00Z"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(testJobID1); err == nil {
		t.Fatal("expected stored id mismatch")
	}

	if err := os.RemoveAll(jobDir); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, jobDir); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := s.Write(&JobRecord{JobID: testJobID1, CreatedAt: time.Now().UTC()}); err == nil {
		t.Fatal("expected symlink job directory rejection")
	}
}

func TestStoreClaimQueuedAllowsExactlyOneClaimant(t *testing.T) {
	s := NewStore(t.TempDir())
	rec := &JobRecord{JobID: testJobID1, State: JobStateQueued, CreatedAt: time.Now().UTC()}
	if err := s.Write(rec); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for pid := 1001; pid <= 1002; pid++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			<-start
			_, err := s.ClaimQueued(testJobID1, pid, nil)
			errs <- err
		}(pid)
	}
	close(start)
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful claims = %d, want 1", successes)
	}
}

func TestEmptyStoreRootFailsWithoutFilesystemMutation(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	store := NewStore(" ")
	if store.RootDir() != "" {
		t.Fatalf("empty root canonicalized to %q", store.RootDir())
	}
	rec := &JobRecord{JobID: testJobID1, CreatedAt: time.Now().UTC()}
	if err := store.Write(rec); err == nil {
		t.Fatal("expected empty-root write rejection")
	}
	if _, err := store.Get(testJobID1); err == nil {
		t.Fatal("expected empty-root read rejection")
	}
	if _, err := store.List(); err == nil {
		t.Fatal("expected empty-root list rejection")
	}
	entries, err := os.ReadDir(cwd)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("empty-root operations mutated cwd: %v", entries)
	}
}

func TestStoreOpenLogReadRejectsSwappedSymlink(t *testing.T) {
	store := NewStore(t.TempDir())
	rec := &JobRecord{JobID: testJobID1, CreatedAt: time.Now().UTC()}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	logFile, err := store.OpenLog(testJobID1, "stdout.log", true)
	if err != nil {
		t.Fatal(err)
	}
	_ = logFile.Close()
	logPath := filepath.Join(store.JobDir(testJobID1), "stdout.log")
	if err := os.Remove(logPath); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside.log")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, logPath); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := store.OpenLogRead(testJobID1, "stdout.log"); err == nil {
		t.Fatal("expected swapped log symlink rejection")
	}
}

func TestStoreGetAndLogReadRejectSymlinkedJobDirectory(t *testing.T) {
	store := NewStore(t.TempDir())
	outside := t.TempDir()
	record := `{"job_id":"` + testJobID1 + `","created_at":"2026-01-19T12:00:00Z"}`
	if err := os.WriteFile(filepath.Join(outside, "job.json"), []byte(record), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "stdout.log"), []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(store.RootDir(), testJobID1)); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := store.Get(testJobID1); err == nil {
		t.Fatal("expected intermediate job-directory symlink rejection")
	}
	if _, err := store.OpenLogRead(testJobID1, "stdout.log"); err == nil {
		t.Fatal("expected log read through job-directory symlink rejection")
	}
}

func TestStoreWriteRejectsJobDirectorySwapAtMutationBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		install func(func())
	}{
		{name: "before temp create", install: func(hook func()) { afterJobDirBoundBeforeTempCreate = hook }},
		{name: "before record replace", install: func(hook func()) { afterRecordTempCreateBeforeReplace = hook }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewStore(t.TempDir())
			rec := &JobRecord{JobID: testJobID1, Name: "before", CreatedAt: time.Now().UTC()}
			if err := store.Write(rec); err != nil {
				t.Fatal(err)
			}
			jobDir := store.JobDir(testJobID1)
			parked := jobDir + ".parked"
			outside := t.TempDir()
			outsideRecord := filepath.Join(outside, "job.json")
			if err := os.WriteFile(outsideRecord, []byte("outside-marker"), 0o600); err != nil {
				t.Fatal(err)
			}
			var swapErr error
			hook := func() {
				if swapErr = os.Rename(jobDir, parked); swapErr != nil {
					return
				}
				swapErr = os.Symlink(outside, jobDir)
			}
			oldTempHook := afterJobDirBoundBeforeTempCreate
			oldReplaceHook := afterRecordTempCreateBeforeReplace
			tt.install(hook)
			t.Cleanup(func() {
				afterJobDirBoundBeforeTempCreate = oldTempHook
				afterRecordTempCreateBeforeReplace = oldReplaceHook
			})
			rec.Name = "after"
			err := store.Write(rec)
			if swapErr != nil {
				t.Skipf("directory swap fixture unavailable: %v", swapErr)
			}
			if err == nil {
				t.Fatal("expected changed job-directory binding rejection")
			}
			outsideBytes, readErr := os.ReadFile(outsideRecord)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if string(outsideBytes) != "outside-marker" {
				t.Fatalf("outside record was modified: %q", outsideBytes)
			}
			outsideEntries, readErr := os.ReadDir(outside)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(outsideEntries) != 1 {
				t.Fatalf("temp record escaped into outside directory: %v", outsideEntries)
			}
		})
	}
}
