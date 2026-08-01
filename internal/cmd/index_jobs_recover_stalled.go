package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

var indexJobsRecoverStalledCmd = &cobra.Command{
	Use:   "recover-stalled <job_id>",
	Short: "Recover a stalled managed index-build job (confirm-gated)",
	Long: `Recover one managed (--background) index-build job that is proven stalled.

This is a separate authority from 'index lease reap' and from 'index jobs stop'.
It acts only on a durable managed job identity (index-build-<jobID>) after a
read-only plan says the process birth token matches, the set-authority lease is
held, and the heartbeat is past grace — or, for a terminal contradiction (dead
PID under a running record), reclaims only an already-unheld lease without
signalling.

Dry-run by default (plan only, byte-preserving). Pass --confirm to mutate.
--force is equivalent to --confirm for opt-in only; it never bypasses identity,
lease, stalled-state, or termination gates.

Parentlessness is not a criterion. Foreground holders without a job record are
outside this command.`,
	Args: cobra.ExactArgs(1),
	RunE: runIndexJobsRecoverStalled,
}

func init() {
	indexJobsCmd.AddCommand(indexJobsRecoverStalledCmd)
	indexJobsRecoverStalledCmd.Flags().Bool("json", false, "Output as JSON")
	indexJobsRecoverStalledCmd.Flags().Bool("confirm", false, "Perform recovery (without this, only the plan runs)")
	indexJobsRecoverStalledCmd.Flags().Bool("force", false, "Non-interactive opt-in (equivalent to --confirm; safety gates always apply)")
}

func runIndexJobsRecoverStalled(cmd *cobra.Command, args []string) error {
	jobID := strings.TrimSpace(args[0])
	if jobID == "" {
		return fmt.Errorf("job_id is required")
	}
	jsonOutput, _ := cmd.Flags().GetBool("json")
	confirm, _ := cmd.Flags().GetBool("confirm")
	force, _ := cmd.Flags().GetBool("force")
	if force {
		confirm = true
	}

	root, err := indexJobsRootDir()
	if err != nil {
		return err
	}
	store := jobregistry.NewStore(root)
	resolvedID, err := resolveJobIDStrict(store, jobID)
	if err != nil {
		return err
	}
	authorityRoot, err := leaseAuthorityRoot()
	if err != nil {
		return err
	}

	result, err := indexcoord.RecoverManagedStalled(store, resolvedID, indexcoord.RecoverStalledOptions{
		AuthorityRoot: authorityRoot,
		Confirm:       confirm,
	})
	if err != nil {
		return err
	}

	if jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	_, _ = fmt.Fprintf(os.Stdout, "job_id=%s\n", result.JobID)
	_, _ = fmt.Fprintf(os.Stdout, "outcome=%s\n", result.Outcome)
	if result.Detail != "" {
		_, _ = fmt.Fprintf(os.Stdout, "detail=%s\n", result.Detail)
	}
	_, _ = fmt.Fprintf(os.Stdout, "plan_class=%s signal_candidate=%t\n", result.Plan.Class, result.Plan.SignalCandidate)
	_, _ = fmt.Fprintf(os.Stdout, "dry_run=%t signalled=%t forced_kill=%t reclaimed=%t\n", result.DryRun, result.Signalled, result.ForcedKill, result.Reclaimed)
	if result.Owner != "" {
		_, _ = fmt.Fprintf(os.Stdout, "recovery_owner=%s\n", result.Owner)
	}
	if result.JobState != "" {
		_, _ = fmt.Fprintf(os.Stdout, "job_state=%s\n", result.JobState)
	}
	if result.LeaseAfter != "" {
		_, _ = fmt.Fprintf(os.Stdout, "lease_verdict_after=%s\n", result.LeaseAfter)
	}
	// Non-zero exit when mutation was requested and did not succeed cleanly.
	if confirm {
		switch result.Outcome {
		case indexcoord.OutcomeSignalled, indexcoord.OutcomeReapedOnly, indexcoord.OutcomeAlreadyStopped, indexcoord.OutcomeNoop:
			return nil
		case indexcoord.OutcomeDryRun:
			return nil
		default:
			return fmt.Errorf("recover-stalled outcome=%s", result.Outcome)
		}
	}
	return nil
}
