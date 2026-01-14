package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/indexstore"
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

  # Delete indexes older than 30 days, keeping at least 3 per base URI
  gonimbus index gc --max-age 30d --keep-last 3

  # Delete all but the 2 most recent indexes per base URI
  gonimbus index gc --keep-last 2`,
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

	// Parse max age
	var maxAge time.Duration
	if maxAgeStr != "" {
		var err error
		maxAge, err = parseDuration(maxAgeStr)
		if err != nil {
			return fmt.Errorf("invalid --max-age: %w", err)
		}
	}

	entries, err := loadIndexEntriesWithPaths(ctx)
	if err != nil {
		return fmt.Errorf("list indexes: %w", err)
	}
	if len(entries) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "No indexes found")
		return nil
	}

	// Build GC params
	params := indexstore.GCParams{
		MaxAge:   maxAge,
		KeepLast: keepLast,
		DryRun:   dryRun,
	}

	result, candidates := buildGCCandidates(entries, params)
	if !dryRun {
		for _, candidate := range candidates {
			if err := os.RemoveAll(candidate.Dir); err != nil {
				return fmt.Errorf("remove index directory %s: %w", candidate.Dir, err)
			}
		}
	}

	if jsonOutput {
		return printGCResultJSON(result, dryRun)
	}

	return printGCResultTable(result, dryRun)
}

func printGCResultTable(result *indexstore.GCResult, dryRun bool) error {
	if len(result.Candidates) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "No indexes to remove")
		return nil
	}

	action := "Removed"
	if dryRun {
		action = "Would remove"
		_, _ = fmt.Fprintln(os.Stderr, "DRY RUN - no changes made")
		_, _ = fmt.Fprintln(os.Stderr)
	}

	// Summary to stderr (status), table to stdout (data)
	_, _ = fmt.Fprintf(os.Stderr, "%s %d index(es)\n", action, result.IndexSetsRemoved)
	_, _ = fmt.Fprintf(os.Stderr, "Objects: %d\n", result.ObjectsRemoved)
	_, _ = fmt.Fprintf(os.Stderr, "Space freed: %s\n", formatBytes(result.BytesFreed))
	_, _ = fmt.Fprintln(os.Stderr)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "BASE URI\tPROVIDER\tOBJECTS\tSIZE\tCREATED")
	for _, c := range result.Candidates {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\n",
			c.BaseURI,
			c.Provider,
			c.ObjectCount,
			formatBytes(c.TotalSizeBytes),
			c.CreatedAt.Format("2006-01-02"),
		)
	}
	_ = w.Flush()

	return nil
}

func printGCResultJSON(result *indexstore.GCResult, dryRun bool) error {
	type jsonCandidate struct {
		IndexSetID     string `json:"index_set_id"`
		BaseURI        string `json:"base_uri"`
		Provider       string `json:"provider"`
		CreatedAt      string `json:"created_at"`
		ObjectCount    int64  `json:"object_count"`
		TotalSizeBytes int64  `json:"total_size_bytes"`
	}

	type jsonOutput struct {
		DryRun           bool            `json:"dry_run"`
		IndexSetsRemoved int             `json:"index_sets_removed"`
		ObjectsRemoved   int64           `json:"objects_removed"`
		BytesFreed       int64           `json:"bytes_freed"`
		Candidates       []jsonCandidate `json:"candidates"`
	}

	out := jsonOutput{
		DryRun:           dryRun,
		IndexSetsRemoved: result.IndexSetsRemoved,
		ObjectsRemoved:   result.ObjectsRemoved,
		BytesFreed:       result.BytesFreed,
		Candidates:       make([]jsonCandidate, len(result.Candidates)),
	}

	for i, c := range result.Candidates {
		out.Candidates[i] = jsonCandidate{
			IndexSetID:     c.IndexSetID,
			BaseURI:        c.BaseURI,
			Provider:       c.Provider,
			CreatedAt:      c.CreatedAt.Format(time.RFC3339),
			ObjectCount:    c.ObjectCount,
			TotalSizeBytes: c.TotalSizeBytes,
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func buildGCCandidates(entries []indexDBEntry, params indexstore.GCParams) (*indexstore.GCResult, []indexDBEntry) {
	result := &indexstore.GCResult{}
	if len(entries) == 0 {
		return result, nil
	}

	byBaseURI := make(map[string][]indexDBEntry)
	for _, entry := range entries {
		byBaseURI[entry.Info.BaseURI] = append(byBaseURI[entry.Info.BaseURI], entry)
	}

	now := time.Now().UTC()
	cutoff := time.Time{}
	if params.MaxAge > 0 {
		cutoff = now.Add(-params.MaxAge)
	}

	var candidates []indexDBEntry
	for _, grouped := range byBaseURI {
		sort.Slice(grouped, func(i, j int) bool {
			return grouped[i].Info.CreatedAt.After(grouped[j].Info.CreatedAt)
		})

		toCheck := grouped
		if params.KeepLast > 0 && len(grouped) > params.KeepLast {
			toCheck = grouped[params.KeepLast:]
		} else if params.KeepLast > 0 {
			continue
		}

		for _, entry := range toCheck {
			if params.MaxAge > 0 && entry.Info.CreatedAt.After(cutoff) {
				continue
			}
			candidates = append(candidates, entry)
			result.Candidates = append(result.Candidates, entry.Info)
			result.BytesFreed += entry.Info.TotalSizeBytes
			result.ObjectsRemoved += entry.Info.ObjectCount
		}
	}

	result.IndexSetsRemoved = len(candidates)
	return result, candidates
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
