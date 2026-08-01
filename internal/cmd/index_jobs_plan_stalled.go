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

// index jobs plan-stalled is the plan-stalled read-only microscope. It never signals.
var indexJobsPlanStalledCmd = &cobra.Command{
	Use:   "plan-stalled <job_id>",
	Short: "Read-only stalled-recovery plan for a managed index job",
	Long: `Classify one managed index-build job for stalled recovery without
mutating the job registry or any set-authority lease.

Outcomes are typed (healthy, suspect-heartbeat-overdue, terminal-contradiction,
identity-mismatch, lease-not-held, invalid, indeterminate, …). A
suspect-heartbeat-overdue result is only a candidate for a later confirm-gated
recover; this command never sends a signal or reaps a lease.

Parentlessness is not a criterion. Foreground holders without a durable job
record are outside this surface.`,
	Args: cobra.ExactArgs(1),
	RunE: runIndexJobsPlanStalled,
}

func init() {
	indexJobsCmd.AddCommand(indexJobsPlanStalledCmd)
	indexJobsPlanStalledCmd.Flags().Bool("json", false, "Output as JSON")
}

func runIndexJobsPlanStalled(cmd *cobra.Command, args []string) error {
	jobID := strings.TrimSpace(args[0])
	if jobID == "" {
		return fmt.Errorf("job_id is required")
	}
	jsonOutput, _ := cmd.Flags().GetBool("json")

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

	plan, err := indexcoord.PlanManagedStalledRecovery(store, resolvedID, indexcoord.StalledPlanOptions{
		AuthorityRoot: authorityRoot,
	})
	if err != nil {
		return err
	}

	if jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(plan)
	}

	_, _ = fmt.Fprintf(os.Stdout, "job_id=%s\n", plan.JobID)
	_, _ = fmt.Fprintf(os.Stdout, "class=%s\n", plan.Class)
	if plan.Detail != "" {
		_, _ = fmt.Fprintf(os.Stdout, "detail=%s\n", plan.Detail)
	}
	_, _ = fmt.Fprintf(os.Stdout, "job_state=%s\n", plan.JobState)
	_, _ = fmt.Fprintf(os.Stdout, "pid=%d process_alive=%t\n", plan.PID, plan.ProcessAlive)
	if plan.RecordedIdentity != "" {
		_, _ = fmt.Fprintf(os.Stdout, "recorded_identity=%s\n", plan.RecordedIdentity)
	}
	if plan.ObservedIdentity != "" {
		_, _ = fmt.Fprintf(os.Stdout, "observed_identity=%s\n", plan.ObservedIdentity)
	}
	if plan.IndexSetID != "" {
		_, _ = fmt.Fprintf(os.Stdout, "index_set_id=%s\n", plan.IndexSetID)
	}
	if plan.LeaseVerdict != "" {
		_, _ = fmt.Fprintf(os.Stdout, "lease_verdict=%s\n", plan.LeaseVerdict)
	}
	if plan.LeaseHolder != "" {
		_, _ = fmt.Fprintf(os.Stdout, "lease_holder=%s\n", plan.LeaseHolder)
	}
	if plan.HeartbeatAge != "" {
		_, _ = fmt.Fprintf(os.Stdout, "heartbeat_age=%s grace=%s\n", plan.HeartbeatAge, plan.HeartbeatGrace)
	}
	_, _ = fmt.Fprintf(os.Stdout, "signal_candidate=%t may_reap_unheld=%t\n", plan.SignalCandidate, plan.MayReapUnheld)
	return nil
}
