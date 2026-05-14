package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

type jobsStopResult struct {
	JobID         string `json:"job_id"`
	ResolvedJobID string `json:"resolved_job_id"`
	Signal        string `json:"signal"`
	ForcedKill    bool   `json:"forced_kill"`
	State         string `json:"state"`
}

func runIndexJobsStop(cmd *cobra.Command, args []string) error {
	jobID := strings.TrimSpace(args[0])
	if jobID == "" {
		return fmt.Errorf("job_id is required")
	}

	jsonOutput, _ := cmd.Flags().GetBool("json")

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

	result, err := store.Stop(resolvedID, jobregistry.StopOptions{Signal: sigStr})
	if err != nil {
		return err
	}

	if jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(jobsStopResult{JobID: jobID, ResolvedJobID: result.JobID, Signal: result.Signal, ForcedKill: result.ForcedKill, State: result.State})
	}
	if result.Signal == "term" && result.ForcedKill {
		_, _ = fmt.Fprintf(os.Stdout, "sent=term;forced=kill\n")
		return nil
	}
	_, _ = fmt.Fprintf(os.Stdout, "sent=%s\n", result.Signal)
	return nil
}
