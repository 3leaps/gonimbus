package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

var indexGCCmd = &cobra.Command{
	Use:   "gc",
	Short: "Garbage collect old indexes",
	Long: `Remove old indexes based on age and retention policies.

The cleanup strategy:
1. If --max-age is specified: remove indexes older than this duration
2. If --keep-last is specified: keep at least N indexes per base URI

--keep-last takes precedence: even if an index is older than --max-age,
it won't be removed if it's within the --keep-last threshold.

Examples:
  # Preview what would be deleted (dry run)
  gonimbus index gc --max-age 30d --dry-run

  # Plan indexes older than 30 days, keeping at least 3 per base URI
  gonimbus index gc --max-age 30d --keep-last 3 --dry-run

  # Plan all but the 2 most recent indexes per base URI
  gonimbus index gc --keep-last 2 --dry-run

Execution reacquires set authority, revalidates the immutable plan immediately
before mutation, and records an outside-target recovery receipt.`,
	RunE: runIndexGC,
}

func init() {
	indexCmd.AddCommand(indexGCCmd)
	indexGCCmd.Flags().String("max-age", "", "Remove indexes older than this duration (e.g., 30d, 720h)")
	indexGCCmd.Flags().Int("keep-last", 0, "Keep at least N indexes per base URI")
	indexGCCmd.Flags().Bool("dry-run", false, "Preview what would be deleted without deleting")
	indexGCCmd.Flags().Bool("json", false, "Output as JSON")
}

func runIndexGC(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	maxAgeStr, _ := cmd.Flags().GetString("max-age")
	keepLast, _ := cmd.Flags().GetInt("keep-last")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	// Validate that at least one retention policy is specified
	if maxAgeStr == "" && keepLast == 0 {
		return fmt.Errorf("at least one of --max-age or --keep-last must be specified")
	}
	if keepLast < 0 {
		return fmt.Errorf("--keep-last must be greater than or equal to zero")
	}

	// Parse max age
	var maxAge time.Duration
	if maxAgeStr != "" {
		var err error
		maxAge, err = parseDuration(maxAgeStr)
		if err != nil {
			return fmt.Errorf("invalid --max-age: %w", err)
		}
		if maxAge <= 0 {
			return fmt.Errorf("invalid --max-age: duration must be greater than zero")
		}
	}

	now := time.Now().UTC()
	if dryRun {
		plan, err := buildIndexGCPlan(ctx, maxAge, maxAgeStr, keepLast, now)
		if err != nil {
			return fmt.Errorf("build index GC plan: %w", err)
		}
		return printIndexGCPlan(plan, jsonOutput)
	}

	control, err := acquireIndexOperationLease(ctx, operationIndexGCControl, "global", "index-gc-control")
	if err != nil {
		return fmt.Errorf("acquire global index GC lease: %w", err)
	}
	defer func() { _ = control.Release() }()
	ctx = control.Context()
	store, err := openDefaultOperationCheckpointStore(ctx)
	if err != nil {
		return err
	}
	recovered, err := recoverIndexGCDeletes(ctx, store, indexGCTestExecutionHooks)
	if err != nil {
		return fmt.Errorf("recover interrupted index GC: %w", err)
	}
	for _, result := range recovered {
		_, _ = fmt.Fprintf(os.Stderr, "Recovered index GC transaction %s (%d set(s), %s)\n", result.TransactionID, result.IndexSetsRemoved, formatBytes(result.RemovedBytes))
	}

	now = time.Now().UTC()
	plan, err := buildIndexGCPlan(ctx, maxAge, maxAgeStr, keepLast, now)
	if err != nil {
		return fmt.Errorf("build index GC plan: %w", err)
	}
	if len(plan.Candidates) == 0 {
		if len(recovered) > 0 && jsonOutput {
			return printGCResultJSON(recovered[len(recovered)-1])
		}
		return printIndexGCPlan(plan, jsonOutput)
	}
	result, err := executeIndexGCPlan(ctx, store, plan, maxAge, maxAgeStr, keepLast, now, indexGCTestExecutionHooks)
	if err != nil {
		return fmt.Errorf("execute index GC plan: %w", err)
	}
	if jsonOutput {
		return printGCResultJSON(result)
	}
	return printGCResultTable(result)
}

func printIndexGCPlan(plan *indexGCPlan, jsonOutput bool) error {
	if len(plan.Candidates) == 0 && len(plan.Warnings) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "No indexes found")
		return nil
	}
	for _, warning := range plan.Warnings {
		if warning.IndexSetID != "" {
			_, _ = fmt.Fprintf(os.Stderr, "warning: retain %s (%s): %s\n", warning.IndexSetID, warning.Path, warning.Reason)
		} else {
			_, _ = fmt.Fprintf(os.Stderr, "warning: %s: %s\n", warning.Path, warning.Reason)
		}
	}

	if jsonOutput {
		return printGCPlanJSON(plan)
	}

	return printGCPlanTable(plan)
}

func printGCResultTable(result indexGCExecutionResult) error {
	if result.IndexSetsRemoved == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "No indexes to remove")
		return nil
	}
	_, _ = fmt.Fprintf(os.Stderr, "Removed %d index(es)\n", result.IndexSetsRemoved)
	_, _ = fmt.Fprintf(os.Stderr, "Objects: %d\n", result.ObjectsRemoved)
	_, _ = fmt.Fprintf(os.Stderr, "Verified artifact bytes: %s\n", formatBytes(result.RemovedBytes))
	_, _ = fmt.Fprintf(os.Stderr, "Plan: %s\n", result.PlanSHA256)
	_, _ = fmt.Fprintf(os.Stdout, "%s\n", result.TransactionID)
	return nil
}

func printGCResultJSON(result indexGCExecutionResult) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func printGCPlanTable(plan *indexGCPlan) error {
	if len(plan.Candidates) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "No indexes to remove")
		return nil
	}

	_, _ = fmt.Fprintln(os.Stderr, "DRY RUN - immutable plan only; no changes made")
	_, _ = fmt.Fprintf(os.Stderr, "Plan: %s\n", plan.PlanSHA256)
	_, _ = fmt.Fprintln(os.Stderr)

	// Summary to stderr (status), table to stdout (data)
	_, _ = fmt.Fprintf(os.Stderr, "Would remove %d index(es)\n", plan.IndexSetsRemoved)
	_, _ = fmt.Fprintf(os.Stderr, "Objects: %d\n", plan.ObjectsRemoved)
	_, _ = fmt.Fprintf(os.Stderr, "Planned artifact bytes: %s\n", formatBytes(plan.PlannedSizeBytes))
	_, _ = fmt.Fprintln(os.Stderr)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "INDEX SET\tFORMATS\tBASE URI\tOBJECTS\tARTIFACT BYTES\tCREATED")
	for _, c := range plan.Candidates {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s\n",
			c.Info.IndexSetID,
			strings.Join(c.Formats, ","),
			c.Info.BaseURI,
			c.Info.ObjectCount,
			formatBytes(c.PlanSize),
			c.Info.CreatedAt.Format("2006-01-02"),
		)
	}
	_ = w.Flush()

	return nil
}

func printGCPlanJSON(plan *indexGCPlan) error {
	type jsonCandidate struct {
		IndexSetID      string          `json:"index_set_id"`
		BaseURI         string          `json:"base_uri"`
		Provider        string          `json:"provider"`
		CreatedAt       string          `json:"created_at"`
		ObjectCount     int64           `json:"object_count"`
		PlannedSize     int64           `json:"planned_size_bytes"`
		Formats         []string        `json:"formats"`
		DeletionTargets []indexGCTarget `json:"deletion_targets"`
	}

	type jsonOutput struct {
		Type             string           `json:"type"`
		DryRun           bool             `json:"dry_run"`
		PlanSHA256       string           `json:"plan_sha256"`
		MaxAge           string           `json:"max_age,omitempty"`
		KeepLast         int              `json:"keep_last,omitempty"`
		IndexSetsRemoved int              `json:"index_sets_removed"`
		ObjectsRemoved   int64            `json:"objects_removed"`
		PlannedSizeBytes int64            `json:"planned_size_bytes"`
		Candidates       []jsonCandidate  `json:"candidates"`
		Warnings         []indexGCWarning `json:"warnings,omitempty"`
	}

	out := jsonOutput{
		Type:             plan.Type,
		DryRun:           true,
		PlanSHA256:       plan.PlanSHA256,
		MaxAge:           plan.MaxAge,
		KeepLast:         plan.KeepLast,
		IndexSetsRemoved: plan.IndexSetsRemoved,
		ObjectsRemoved:   plan.ObjectsRemoved,
		PlannedSizeBytes: plan.PlannedSizeBytes,
		Candidates:       make([]jsonCandidate, len(plan.Candidates)),
		Warnings:         plan.Warnings,
	}

	for i, c := range plan.Candidates {
		out.Candidates[i] = jsonCandidate{
			IndexSetID:      c.Info.IndexSetID,
			BaseURI:         c.Info.BaseURI,
			Provider:        c.Info.Provider,
			CreatedAt:       c.Info.CreatedAt.Format(time.RFC3339),
			ObjectCount:     c.Info.ObjectCount,
			PlannedSize:     c.PlanSize,
			Formats:         c.Formats,
			DeletionTargets: c.Targets,
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// parseDuration parses a duration string that may include day suffix (e.g., "30d").
func parseDuration(s string) (time.Duration, error) {
	// Check for day suffix
	if len(s) > 0 && s[len(s)-1] == 'd' {
		// Parse days
		var days int
		if _, err := fmt.Sscanf(s, "%dd", &days); err != nil {
			return 0, fmt.Errorf("invalid duration: %s", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}

	// Fall back to standard duration parsing
	return time.ParseDuration(s)
}
