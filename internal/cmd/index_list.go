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

var indexListCmd = &cobra.Command{
	Use:   "list",
	Short: "List local indexes",
	Long: `List all indexes in the local index database.

Displays base URI, provider, object count, size, and run status for each index.

Examples:
  # List all indexes
  gonimbus index list

  # List with JSON output
  gonimbus index list --json`,
	RunE: runIndexList,
}

func init() {
	indexCmd.AddCommand(indexListCmd)
	indexListCmd.Flags().Bool("json", false, "Output as JSON")
}

func runIndexList(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	jsonOutput, _ := cmd.Flags().GetBool("json")

	// Open index database
	db, err := openQueryIndexDB(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Get all index sets with stats
	entries, err := indexstore.ListIndexSetsWithStats(ctx, db)
	if err != nil {
		return fmt.Errorf("list indexes: %w", err)
	}

	if len(entries) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "No indexes found")
		return nil
	}

	if jsonOutput {
		return printIndexListJSON(entries)
	}

	return printIndexListTable(entries)
}

func printIndexListTable(entries []indexstore.IndexListEntry) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer func() { _ = w.Flush() }()

	// Header
	_, _ = fmt.Fprintln(w, "BASE URI\tPROVIDER\tOBJECTS\tSIZE\tRUNS\tLATEST\tSTATUS")

	for _, e := range entries {
		sizeStr := formatBytes(e.TotalSizeBytes)

		latestStr := "-"
		if e.LatestRunAt != nil {
			latestStr = formatRelativeTime(*e.LatestRunAt)
		}

		status := "-"
		if e.LatestStatus != "" {
			status = e.LatestStatus
		}

		_, _ = fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%d\t%s\t%s\n",
			e.BaseURI,
			e.Provider,
			e.ObjectCount,
			sizeStr,
			e.RunCount,
			latestStr,
			status,
		)
	}

	return nil
}

func printIndexListJSON(entries []indexstore.IndexListEntry) error {
	type jsonEntry struct {
		IndexSetID     string  `json:"index_set_id"`
		BaseURI        string  `json:"base_uri"`
		Provider       string  `json:"provider"`
		CreatedAt      string  `json:"created_at"`
		ObjectCount    int64   `json:"object_count"`
		TotalSizeBytes int64   `json:"total_size_bytes"`
		RunCount       int     `json:"run_count"`
		LatestRunAt    *string `json:"latest_run_at,omitempty"`
		LatestStatus   string  `json:"latest_status,omitempty"`
	}

	out := make([]jsonEntry, len(entries))
	for i, e := range entries {
		out[i] = jsonEntry{
			IndexSetID:     e.IndexSetID,
			BaseURI:        e.BaseURI,
			Provider:       e.Provider,
			CreatedAt:      e.CreatedAt.Format(time.RFC3339),
			ObjectCount:    e.ObjectCount,
			TotalSizeBytes: e.TotalSizeBytes,
			RunCount:       e.RunCount,
			LatestStatus:   e.LatestStatus,
		}
		if e.LatestRunAt != nil {
			ts := e.LatestRunAt.Format(time.RFC3339)
			out[i].LatestRunAt = &ts
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// formatBytes formats bytes as human-readable string.
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// formatRelativeTime formats a time as relative to now.
func formatRelativeTime(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	case d < 7*24*time.Hour:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	default:
		return t.Format("2006-01-02")
	}
}
