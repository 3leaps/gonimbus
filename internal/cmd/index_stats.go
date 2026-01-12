package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

var indexStatsCmd = &cobra.Command{
	Use:   "stats <base-uri>",
	Short: "Show index statistics",
	Long: `Display detailed statistics for an index.

Shows object counts, size distribution, run history, and prefix breakdown.

Examples:
  # Show stats for an index
  gonimbus index stats s3://bucket/prefix/

  # Show with JSON output
  gonimbus index stats s3://bucket/prefix/ --json

  # Show prefix breakdown
  gonimbus index stats s3://bucket/prefix/ --prefixes`,
	Args: cobra.ExactArgs(1),
	RunE: runIndexStats,
}

func init() {
	indexCmd.AddCommand(indexStatsCmd)
	indexStatsCmd.Flags().Bool("json", false, "Output as JSON")
	indexStatsCmd.Flags().Bool("prefixes", false, "Include prefix breakdown")
	indexStatsCmd.Flags().Bool("runs", false, "Include run history")
}

func runIndexStats(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	baseURI := normalizeQueryBaseURI(args[0])
	jsonOutput, _ := cmd.Flags().GetBool("json")
	showPrefixes, _ := cmd.Flags().GetBool("prefixes")
	showRuns, _ := cmd.Flags().GetBool("runs")

	// Open index database
	db, err := openQueryIndexDB(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Find index set by base URI
	indexSet, err := indexstore.GetIndexSetByBaseURI(ctx, db, baseURI)
	if err != nil {
		return fmt.Errorf("lookup index set: %w", err)
	}
	if indexSet == nil {
		return fmt.Errorf("no index found for base URI: %s", baseURI)
	}

	// Get summary statistics
	summary, err := indexstore.GetIndexSetSummary(ctx, db, indexSet.IndexSetID)
	if err != nil {
		return fmt.Errorf("get summary: %w", err)
	}

	// Get prefix stats if requested
	var prefixStats []indexstore.PrefixStatRow
	if showPrefixes {
		prefixStats, err = indexstore.GetLatestPrefixStats(ctx, db, indexSet.IndexSetID)
		if err != nil {
			return fmt.Errorf("get prefix stats: %w", err)
		}
	}

	// Get run history if requested
	var runs []indexstore.IndexRun
	if showRuns {
		runs, err = indexstore.ListIndexRuns(ctx, db, indexSet.IndexSetID)
		if err != nil {
			return fmt.Errorf("list runs: %w", err)
		}
	}

	if jsonOutput {
		return printStatsJSON(summary, prefixStats, runs, indexSet)
	}

	return printStatsTable(summary, prefixStats, runs, indexSet)
}

func printStatsTable(summary *indexstore.IndexSetSummary, prefixStats []indexstore.PrefixStatRow, runs []indexstore.IndexRun, indexSet *indexstore.IndexSet) error {
	// Header
	_, _ = fmt.Fprintf(os.Stdout, "Index: %s\n", summary.BaseURI)
	_, _ = fmt.Fprintf(os.Stdout, "Provider: %s\n", summary.Provider)
	if indexSet.StorageProvider != "" {
		_, _ = fmt.Fprintf(os.Stdout, "Storage Provider: %s\n", indexSet.StorageProvider)
	}
	if indexSet.Region != "" {
		_, _ = fmt.Fprintf(os.Stdout, "Region: %s\n", indexSet.Region)
	}
	_, _ = fmt.Fprintf(os.Stdout, "Created: %s\n", summary.CreatedAt.Format(time.RFC3339))
	_, _ = fmt.Fprintln(os.Stdout)

	// Object statistics
	_, _ = fmt.Fprintln(os.Stdout, "Objects:")
	_, _ = fmt.Fprintf(os.Stdout, "  Active:   %d\n", summary.ActiveObjects)
	_, _ = fmt.Fprintf(os.Stdout, "  Deleted:  %d\n", summary.DeletedObjects)
	_, _ = fmt.Fprintf(os.Stdout, "  Total:    %d\n", summary.TotalObjects)
	_, _ = fmt.Fprintf(os.Stdout, "  Size:     %s\n", formatBytes(summary.TotalSizeBytes))
	_, _ = fmt.Fprintln(os.Stdout)

	// Run statistics
	_, _ = fmt.Fprintln(os.Stdout, "Runs:")
	_, _ = fmt.Fprintf(os.Stdout, "  Total:      %d\n", summary.TotalRuns)
	_, _ = fmt.Fprintf(os.Stdout, "  Successful: %d\n", summary.SuccessfulRuns)
	_, _ = fmt.Fprintf(os.Stdout, "  Partial:    %d\n", summary.PartialRuns)
	_, _ = fmt.Fprintf(os.Stdout, "  Failed:     %d\n", summary.FailedRuns)

	if summary.LatestRun != nil {
		_, _ = fmt.Fprintf(os.Stdout, "  Latest:     %s (%s)\n",
			summary.LatestRun.StartedAt.Format(time.RFC3339),
			summary.LatestRun.Status)
	}
	_, _ = fmt.Fprintln(os.Stdout)

	// Prefix breakdown if requested
	if len(prefixStats) > 0 {
		_, _ = fmt.Fprintln(os.Stdout, "Prefixes (from latest run):")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(w, "  PREFIX\tOBJECTS\tSIZE\tSUBPREFIXES\tTRUNCATED")
		for _, ps := range prefixStats {
			truncated := "-"
			if ps.Truncated {
				truncated = "yes"
				if ps.TruncatedReason != "" {
					truncated = ps.TruncatedReason
				}
			}
			_, _ = fmt.Fprintf(w, "  %s\t%d\t%s\t%d\t%s\n",
				displayPrefix(ps.Prefix),
				ps.ObjectsDirect,
				formatBytes(ps.BytesDirect),
				ps.CommonPrefixes,
				truncated,
			)
		}
		_ = w.Flush()
		_, _ = fmt.Fprintln(os.Stdout)
	}

	// Run history if requested
	if len(runs) > 0 {
		_, _ = fmt.Fprintln(os.Stdout, "Run History:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(w, "  STARTED\tSTATUS\tSOURCE\tDURATION")
		for _, r := range runs {
			duration := "-"
			if r.EndedAt != nil {
				duration = r.EndedAt.Sub(r.StartedAt).Round(time.Second).String()
			}
			_, _ = fmt.Fprintf(w, "  %s\t%s\t%s\t%s\n",
				r.StartedAt.Format("2006-01-02 15:04:05"),
				r.Status,
				r.SourceType,
				duration,
			)
		}
		_ = w.Flush()
	}

	return nil
}

func displayPrefix(prefix string) string {
	if prefix == "" {
		return "(root)"
	}
	return prefix
}

func printStatsJSON(summary *indexstore.IndexSetSummary, prefixStats []indexstore.PrefixStatRow, runs []indexstore.IndexRun, indexSet *indexstore.IndexSet) error {
	type jsonRun struct {
		RunID      string  `json:"run_id"`
		StartedAt  string  `json:"started_at"`
		EndedAt    *string `json:"ended_at,omitempty"`
		SourceType string  `json:"source_type"`
		Status     string  `json:"status"`
	}

	type jsonPrefix struct {
		Prefix          string `json:"prefix"`
		Depth           int    `json:"depth"`
		ObjectsDirect   int64  `json:"objects_direct"`
		BytesDirect     int64  `json:"bytes_direct"`
		CommonPrefixes  int64  `json:"common_prefixes"`
		Truncated       bool   `json:"truncated"`
		TruncatedReason string `json:"truncated_reason,omitempty"`
	}

	type jsonOutput struct {
		IndexSetID      string `json:"index_set_id"`
		BaseURI         string `json:"base_uri"`
		Provider        string `json:"provider"`
		StorageProvider string `json:"storage_provider,omitempty"`
		CloudProvider   string `json:"cloud_provider,omitempty"`
		Region          string `json:"region,omitempty"`
		CreatedAt       string `json:"created_at"`

		Objects struct {
			Active         int64 `json:"active"`
			Deleted        int64 `json:"deleted"`
			Total          int64 `json:"total"`
			TotalSizeBytes int64 `json:"total_size_bytes"`
		} `json:"objects"`

		Runs struct {
			Total      int `json:"total"`
			Successful int `json:"successful"`
			Partial    int `json:"partial"`
			Failed     int `json:"failed"`
		} `json:"runs"`

		LatestRun  *jsonRun     `json:"latest_run,omitempty"`
		Prefixes   []jsonPrefix `json:"prefixes,omitempty"`
		RunHistory []jsonRun    `json:"run_history,omitempty"`
	}

	out := jsonOutput{
		IndexSetID:      summary.IndexSetID,
		BaseURI:         summary.BaseURI,
		Provider:        summary.Provider,
		StorageProvider: indexSet.StorageProvider,
		CloudProvider:   indexSet.CloudProvider,
		Region:          indexSet.Region,
		CreatedAt:       summary.CreatedAt.Format(time.RFC3339),
	}

	out.Objects.Active = summary.ActiveObjects
	out.Objects.Deleted = summary.DeletedObjects
	out.Objects.Total = summary.TotalObjects
	out.Objects.TotalSizeBytes = summary.TotalSizeBytes

	out.Runs.Total = summary.TotalRuns
	out.Runs.Successful = summary.SuccessfulRuns
	out.Runs.Partial = summary.PartialRuns
	out.Runs.Failed = summary.FailedRuns

	if summary.LatestRun != nil {
		r := summary.LatestRun
		out.LatestRun = &jsonRun{
			RunID:      r.RunID,
			StartedAt:  r.StartedAt.Format(time.RFC3339),
			SourceType: r.SourceType,
			Status:     string(r.Status),
		}
		if r.EndedAt != nil {
			ts := r.EndedAt.Format(time.RFC3339)
			out.LatestRun.EndedAt = &ts
		}
	}

	if len(prefixStats) > 0 {
		out.Prefixes = make([]jsonPrefix, len(prefixStats))
		for i, ps := range prefixStats {
			out.Prefixes[i] = jsonPrefix{
				Prefix:          ps.Prefix,
				Depth:           ps.Depth,
				ObjectsDirect:   ps.ObjectsDirect,
				BytesDirect:     ps.BytesDirect,
				CommonPrefixes:  ps.CommonPrefixes,
				Truncated:       ps.Truncated,
				TruncatedReason: ps.TruncatedReason,
			}
		}
	}

	if len(runs) > 0 {
		out.RunHistory = make([]jsonRun, len(runs))
		for i, r := range runs {
			out.RunHistory[i] = jsonRun{
				RunID:      r.RunID,
				StartedAt:  r.StartedAt.Format(time.RFC3339),
				SourceType: r.SourceType,
				Status:     string(r.Status),
			}
			if r.EndedAt != nil {
				ts := r.EndedAt.Format(time.RFC3339)
				out.RunHistory[i].EndedAt = &ts
			}
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}
