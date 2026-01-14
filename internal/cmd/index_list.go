package cmd

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

var indexListCmd = &cobra.Command{
	Use:   "list",
	Short: "List local indexes",
	Long: `List all indexes in the local index directory.

Indexes are stored one-per-directory under the app data dir:
  indexes/idx_<hashprefix>/index.db

A companion identity file is written alongside the DB:
  indexes/idx_<hashprefix>/identity.json

The list output includes an IDENTITY status to help interpret hash-based directories:
  ok        identity.json matches DB index_set_id
  missing   no identity.json found
  invalid   identity.json is unreadable or invalid JSON
  mismatch  identity.json hash disagrees with the DB or directory name

For deeper diagnosis (including base_uri/provider mismatches), use:
  gonimbus index doctor

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

type indexListDisplayEntry struct {
	DBPath         string
	Dir            string
	DirName        string
	IdentityPath   string
	IdentityStatus string // ok, missing, invalid, mismatch, error
	Info           indexstore.IndexListEntry
}

func runIndexList(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	jsonOutput, _ := cmd.Flags().GetBool("json")

	rawEntries, err := loadIndexEntriesWithPaths(ctx)
	if err != nil {
		return fmt.Errorf("list indexes: %w", err)
	}

	if len(rawEntries) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "No indexes found")
		return nil
	}

	entries := make([]indexListDisplayEntry, 0, len(rawEntries))
	for _, e := range rawEntries {
		dirName := filepath.Base(e.Dir)
		identityPath := filepath.Join(e.Dir, "identity.json")
		entries = append(entries, indexListDisplayEntry{
			DBPath:         e.Path,
			Dir:            e.Dir,
			DirName:        dirName,
			IdentityPath:   identityPath,
			IdentityStatus: computeIndexIdentityStatus(identityPath, dirName, e.Info.IndexSetID),
			Info:           e.Info,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Info.CreatedAt.After(entries[j].Info.CreatedAt)
	})

	if jsonOutput {
		return printIndexListJSON(entries)
	}

	return printIndexListTable(entries)
}

func computeIndexIdentityStatus(identityPath, dirName, indexSetID string) string {
	b, err := os.ReadFile(identityPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "missing"
		}
		return "error"
	}

	trimmed := strings.TrimSpace(string(b))
	if trimmed == "" {
		return "invalid"
	}
	if !json.Valid([]byte(trimmed)) {
		return "invalid"
	}

	sha := sha256.Sum256([]byte(trimmed))
	shaHex := hex.EncodeToString(sha[:])
	if len(shaHex) < 16 {
		return "invalid"
	}

	expectedID := "idx_" + shaHex
	expectedDir := "idx_" + shaHex[:16]

	if indexSetID != "" && expectedID != indexSetID {
		return "mismatch"
	}
	if dirName != "" && expectedDir != dirName {
		return "mismatch"
	}
	return "ok"
}

func printIndexListTable(entries []indexListDisplayEntry) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer func() { _ = w.Flush() }()

	// Header
	_, _ = fmt.Fprintln(w, "DIR\tBASE URI\tPROVIDER\tOBJECTS\tSIZE\tRUNS\tLATEST\tSTATUS\tIDENTITY")

	for _, e := range entries {
		info := e.Info
		sizeStr := formatBytes(info.TotalSizeBytes)

		latestStr := "-"
		if info.LatestRunAt != nil {
			latestStr = formatRelativeTime(*info.LatestRunAt)
		}

		status := "-"
		if info.LatestStatus != "" {
			status = info.LatestStatus
		}

		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%d\t%s\t%s\t%s\n",
			e.DirName,
			info.BaseURI,
			info.Provider,
			info.ObjectCount,
			sizeStr,
			info.RunCount,
			latestStr,
			status,
			e.IdentityStatus,
		)
	}

	return nil
}

func printIndexListJSON(entries []indexListDisplayEntry) error {
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

		DirName        string `json:"dir_name"`
		DBPath         string `json:"db_path"`
		IdentityPath   string `json:"identity_path"`
		IdentityStatus string `json:"identity_status"`
	}

	out := make([]jsonEntry, len(entries))
	for i, e := range entries {
		info := e.Info
		out[i] = jsonEntry{
			IndexSetID:     info.IndexSetID,
			BaseURI:        info.BaseURI,
			Provider:       info.Provider,
			CreatedAt:      info.CreatedAt.Format(time.RFC3339),
			ObjectCount:    info.ObjectCount,
			TotalSizeBytes: info.TotalSizeBytes,
			RunCount:       info.RunCount,
			LatestStatus:   info.LatestStatus,
			DirName:        e.DirName,
			DBPath:         e.DBPath,
			IdentityPath:   e.IdentityPath,
			IdentityStatus: e.IdentityStatus,
		}
		if info.LatestRunAt != nil {
			ts := info.LatestRunAt.Format(time.RFC3339)
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
