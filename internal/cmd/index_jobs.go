package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

var indexJobsCmd = &cobra.Command{
	Use:   "jobs",
	Short: "Manage index build jobs",
	Long: `Manage job records for long-running index builds.

This command group is designed to be agent-friendly:

- stable job ids
- predictable on-disk locations
- optional JSON output for machine parsing

Note: v0.1.4 supports managed background jobs via 'index build --background'.`,
}

var indexJobsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List index build jobs",
	RunE:  runIndexJobsList,
}

var indexJobsStatusCmd = &cobra.Command{
	Use:   "status <job_id>",
	Short: "Show status for a job",
	Args:  cobra.ExactArgs(1),
	RunE:  runIndexJobsStatus,
}

var indexJobsStopCmd = &cobra.Command{
	Use:   "stop <job_id>",
	Short: "Stop a running job",
	Args:  cobra.ExactArgs(1),
	RunE:  runIndexJobsStop,
}

var indexJobsLogsCmd = &cobra.Command{
	Use:   "logs <job_id>",
	Short: "Show logs for a job",
	Args:  cobra.ExactArgs(1),
	RunE:  runIndexJobsLogs,
}

var indexJobsGCCmd = &cobra.Command{
	Use:   "gc",
	Short: "Garbage collect old job records",
	RunE:  runIndexJobsGC,
}

func init() {
	indexCmd.AddCommand(indexJobsCmd)
	indexJobsCmd.AddCommand(indexJobsListCmd)
	indexJobsCmd.AddCommand(indexJobsStatusCmd)
	indexJobsCmd.AddCommand(indexJobsStopCmd)
	indexJobsCmd.AddCommand(indexJobsLogsCmd)
	indexJobsCmd.AddCommand(indexJobsGCCmd)

	indexJobsListCmd.Flags().Bool("json", false, "Output as JSON")
	indexJobsStatusCmd.Flags().Bool("json", false, "Output as JSON")
	indexJobsStopCmd.Flags().String("signal", "term", "Signal to send: term or kill")
	indexJobsLogsCmd.Flags().String("stream", "stdout", "Log stream: stdout, stderr, or both")
	indexJobsLogsCmd.Flags().Int("tail", 200, "Show last N lines (0 = no tail)")
	indexJobsLogsCmd.Flags().Bool("follow", false, "Follow log output")
	indexJobsGCCmd.Flags().String("max-age", "168h", "Delete completed jobs older than this duration")
	indexJobsGCCmd.Flags().Bool("dry-run", false, "Show how many jobs would be deleted")
}

func indexJobsRootDir() (string, error) {
	dataDir, err := indexDataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dataDir, "jobs", "index-build"), nil
}

func runIndexJobsList(cmd *cobra.Command, _ []string) error {
	jsonOutput, _ := cmd.Flags().GetBool("json")

	root, err := indexJobsRootDir()
	if err != nil {
		return err
	}
	store := jobregistry.NewStore(root)

	jobs, err := store.List()
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		_, _ = fmt.Fprintln(os.Stdout, "No jobs found")
		return nil
	}

	if jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(jobs)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer func() { _ = w.Flush() }()

	_, _ = fmt.Fprintln(w, "JOB ID\tNAME\tSTATE\tSTARTED\tENDED\tINDEX SET\tRUN\tMANIFEST")
	for _, j := range jobs {
		started := formatOptionalTime(j.StartedAt)
		ended := formatOptionalTime(j.EndedAt)
		name := j.Name
		if name == "" {
			name = "-"
		}
		indexSet := j.IndexSetID
		if indexSet == "" {
			indexSet = "-"
		}
		runID := j.RunID
		if runID == "" {
			runID = "-"
		}
		manifest := j.ManifestPath
		if manifest == "" {
			manifest = "-"
		}

		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			shortJobID(j.JobID),
			name,
			j.State,
			started,
			ended,
			shortIndexSetID(indexSet),
			runID,
			manifest,
		)
	}

	return nil
}

func runIndexJobsStatus(cmd *cobra.Command, args []string) error {
	jsonOutput, _ := cmd.Flags().GetBool("json")
	jobID := strings.TrimSpace(args[0])
	if jobID == "" {
		return fmt.Errorf("job_id is required")
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

	if jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(rec)
	}

	_, _ = fmt.Fprintf(os.Stdout, "job_id=%s\n", rec.JobID)
	if rec.Name != "" {
		_, _ = fmt.Fprintf(os.Stdout, "name=%s\n", rec.Name)
	}
	_, _ = fmt.Fprintf(os.Stdout, "state=%s\n", rec.State)
	_, _ = fmt.Fprintf(os.Stdout, "manifest_path=%s\n", rec.ManifestPath)
	if rec.IndexDir != "" {
		_, _ = fmt.Fprintf(os.Stdout, "index_dir=%s\n", rec.IndexDir)
	}
	if rec.IndexSetID != "" {
		_, _ = fmt.Fprintf(os.Stdout, "index_set_id=%s\n", rec.IndexSetID)
	}
	if rec.RunID != "" {
		_, _ = fmt.Fprintf(os.Stdout, "run_id=%s\n", rec.RunID)
	}
	if rec.StartedAt != nil {
		_, _ = fmt.Fprintf(os.Stdout, "started_at=%s\n", rec.StartedAt.UTC().Format(time.RFC3339))
	}
	if rec.EndedAt != nil {
		_, _ = fmt.Fprintf(os.Stdout, "ended_at=%s\n", rec.EndedAt.UTC().Format(time.RFC3339))
	}

	return nil
}

func shortJobID(jobID string) string {
	jobID = strings.TrimSpace(jobID)
	if len(jobID) <= 12 {
		return jobID
	}
	return jobID[:12]
}

func shortIndexSetID(indexSetID string) string {
	indexSetID = strings.TrimSpace(indexSetID)
	if strings.HasPrefix(indexSetID, "idx_") && len(indexSetID) > len("idx_")+8 {
		return "idx_" + indexSetID[len("idx_"):len("idx_")+8]
	}
	if indexSetID == "" {
		return "-"
	}
	return indexSetID
}

func formatOptionalTime(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func resolveJobID(store *jobregistry.Store, input string) (string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", fmt.Errorf("job_id is required")
	}

	// Exact match first.
	if store != nil {
		if _, err := store.Get(input); err == nil {
			return input, nil
		}
	}

	// Prefix match (allows table-friendly short IDs).
	jobs, err := store.List()
	if err != nil {
		return "", err
	}
	matches := make([]string, 0, 2)
	for _, j := range jobs {
		if strings.HasPrefix(j.JobID, input) {
			matches = append(matches, j.JobID)
		}
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("job not found: %s", input)
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("job id prefix is ambiguous (%d matches); use full job_id or --json", len(matches))
	}
	return matches[0], nil
}
