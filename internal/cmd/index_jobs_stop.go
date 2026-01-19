package cmd

import (
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

func runIndexJobsStop(cmd *cobra.Command, args []string) error {
	jobID := strings.TrimSpace(args[0])
	if jobID == "" {
		return fmt.Errorf("job_id is required")
	}

	sigStr, _ := cmd.Flags().GetString("signal")
	sigStr = strings.TrimSpace(strings.ToLower(sigStr))
	if sigStr == "" {
		sigStr = "term"
	}

	root, err := indexJobsRootDir()
	if err != nil {
		return err
	}
	store := jobregistry.NewStore(root)

	resolvedID, err := resolveJobID(store, jobID)
	if err != nil {
		return err
	}

	rec, err := store.Get(resolvedID)
	if err != nil {
		return err
	}
	if rec.PID <= 0 {
		return fmt.Errorf("job has no pid recorded")
	}
	if rec.State != jobregistry.JobStateRunning {
		return fmt.Errorf("job is not running (state=%s)", rec.State)
	}

	proc, err := os.FindProcess(rec.PID)
	if err != nil {
		return fmt.Errorf("find process: %w", err)
	}

	sig := syscall.SIGTERM
	if sigStr == "kill" {
		sig = syscall.SIGKILL
	}

	now := time.Now().UTC()
	rec.State = jobregistry.JobStateStopping
	rec.LastHeartbeat = &now
	_ = store.Write(rec)

	if err := proc.Signal(sig); err != nil {
		return fmt.Errorf("signal %s: %w", sigStr, err)
	}

	// Optimistic; child should mark terminal state on exit.
	_, _ = fmt.Fprintf(os.Stdout, "sent=%s\n", sigStr)
	return nil
}
