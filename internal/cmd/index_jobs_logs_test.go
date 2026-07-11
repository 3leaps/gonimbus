package cmd

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
	"github.com/spf13/cobra"
)

func TestRenderJobLogRejectsForgedPersistedPath(t *testing.T) {
	store := jobregistry.NewStore(t.TempDir())
	const jobID = "11111111-1111-4111-8111-111111111111"
	rec := &jobregistry.JobRecord{
		JobID:      jobID,
		CreatedAt:  time.Now().UTC(),
		StdoutPath: filepath.Join(t.TempDir(), "outside.log"),
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := renderJobLog(store, rec, "stdout", 10, false, false); err == nil {
		t.Fatal("expected forged persisted log path rejection")
	}
}

func TestIndexJobsStatusAndLogsRejectSymlinkedJobDirectory(t *testing.T) {
	dataRoot := t.TempDir()
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	jobsRoot := filepath.Join(dataRoot, "jobs", "index-build")
	if err := os.MkdirAll(jobsRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	const jobID = "11111111-1111-4111-8111-111111111111"
	outside := t.TempDir()
	record := `{"job_id":"` + jobID + `","created_at":"2026-01-19T12:00:00Z","stdout_path":"` + filepath.Join(jobsRoot, jobID, "stdout.log") + `"}`
	if err := os.WriteFile(filepath.Join(outside, "job.json"), []byte(record), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "stdout.log"), []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(jobsRoot, jobID)); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	statusCmd := &cobra.Command{}
	statusCmd.Flags().Bool("json", false, "")
	if err := runIndexJobsStatus(statusCmd, []string{jobID}); err == nil {
		t.Fatal("expected status through symlinked job directory to fail")
	}
	logsCmd := &cobra.Command{}
	logsCmd.Flags().String("stream", "stdout", "")
	logsCmd.Flags().Int("tail", 10, "")
	logsCmd.Flags().Bool("follow", false, "")
	logsCmd.Flags().Bool("json", false, "")
	if err := runIndexJobsLogs(logsCmd, []string{jobID}); err == nil {
		t.Fatal("expected logs through symlinked job directory to fail")
	}
}
