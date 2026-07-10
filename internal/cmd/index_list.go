package cmd

import (
	"context"
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

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
)

var indexListCmd = &cobra.Command{
	Use:   "list",
	Short: "List local indexes",
	Long: `List all indexes in the local index directory.

Indexes are stored under the app data dir. SQLite compatibility indexes use:
  indexes/idx_<hashprefix>/index.db
  indexes/idx_<hashprefix>/identity.json

Durable-default builds publish under the segment cache and may have identity
without index.db:
  indexes/idx_<hashprefix>/identity.json
  cache/segments/<index_set_id>/latest.json

list is format-aware (sqlite-v1 and durable-v2). When both formats exist for the
same index set (format both), the SQLite entry is preferred for run metadata.
Durable-only sets are always listed.

IDENTITY status helps interpret hash-based directories:
  ok        identity.json matches index_set_id
  missing   no identity.json found
  invalid   identity.json is unreadable or invalid JSON
  mismatch  identity.json hash disagrees with the set id or directory name

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
	Format         string // sqlite-v1 | durable-v2
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

	opts, err := indexReaderResolveOptions()
	if err != nil {
		return fmt.Errorf("list indexes: %w", err)
	}
	listed, err := indexreader.ListIndexReaders(ctx, opts)
	if err != nil {
		return fmt.Errorf("list indexes: %w", err)
	}
	listed = preferListedIndexes(listed)
	if len(listed) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "No indexes found")
		return nil
	}

	entries := make([]indexListDisplayEntry, 0, len(listed))
	for _, item := range listed {
		entry, err := loadIndexListDisplayEntry(ctx, opts, item)
		if err != nil {
			// Surface discovery without aborting the full list.
			warnIndexDB("skip index %s (%s): %v", item.Meta.IndexSetID, item.Meta.Format, err)
			continue
		}
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "No indexes found")
		return nil
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Info.CreatedAt.After(entries[j].Info.CreatedAt)
	})

	if jsonOutput {
		return printIndexListJSON(entries)
	}
	return printIndexListTable(entries)
}

func loadIndexListDisplayEntry(ctx context.Context, opts indexreader.ResolveOptions, item indexreader.ListedIndex) (indexListDisplayEntry, error) {
	meta := item.Meta
	dir := meta.IdentityDir
	if dir == "" && meta.SourcePath != "" {
		// Durable SourcePath is latest.json under segment set root.
		if meta.Format == indexreader.FormatDurableV2 {
			dir = filepath.Dir(meta.SourcePath)
		} else {
			dir = filepath.Dir(meta.SourcePath)
		}
	}
	dirName := filepath.Base(dir)
	identityPath := ""
	if meta.IdentityDir != "" {
		identityPath = filepath.Join(meta.IdentityDir, "identity.json")
		dirName = filepath.Base(meta.IdentityDir)
		dir = meta.IdentityDir
	}

	switch meta.Format {
	case indexreader.FormatSQLiteV1:
		return loadSQLiteListDisplayEntry(ctx, meta, dir, dirName, identityPath)
	case indexreader.FormatDurableV2:
		return loadDurableListDisplayEntry(opts, meta, dir, dirName, identityPath)
	default:
		return indexListDisplayEntry{}, fmt.Errorf("unsupported index format %q", meta.Format)
	}
}

func loadSQLiteListDisplayEntry(ctx context.Context, meta indexreader.Meta, dir, dirName, identityPath string) (indexListDisplayEntry, error) {
	db, err := openIndexDB(ctx, meta.SourcePath)
	if err != nil {
		return indexListDisplayEntry{}, err
	}
	defer func() { _ = db.Close() }()

	info, err := indexstore.ListIndexSetsWithStats(ctx, db)
	if err != nil {
		return indexListDisplayEntry{}, err
	}
	if len(info) == 0 {
		return indexListDisplayEntry{}, fmt.Errorf("no index sets")
	}
	if len(info) > 1 {
		return indexListDisplayEntry{}, fmt.Errorf("multiple index sets found")
	}
	entryInfo := info[0]
	if meta.IndexSetID != "" {
		entryInfo.IndexSetID = meta.IndexSetID
	}
	if meta.BaseURI != "" {
		entryInfo.BaseURI = meta.BaseURI
	}
	if meta.Provider != "" {
		entryInfo.Provider = meta.Provider
	}
	return indexListDisplayEntry{
		Format:         formatLabel(meta.Format),
		DBPath:         meta.SourcePath,
		Dir:            dir,
		DirName:        dirName,
		IdentityPath:   identityPath,
		IdentityStatus: computeIndexIdentityStatus(identityPath, dirName, entryInfo.IndexSetID),
		Info:           entryInfo,
	}, nil
}

func loadDurableListDisplayEntry(opts indexreader.ResolveOptions, meta indexreader.Meta, dir, dirName, identityPath string) (indexListDisplayEntry, error) {
	snap, err := indexsubstrate.OpenLatestPublishedSnapshotBounded(meta.SourcePath, opts.MaxMarkerBytes, opts.MaxManifestBytes)
	if err != nil {
		return indexListDisplayEntry{}, err
	}
	// Prefer durable publication/complete time. Do not invent wall-clock now when
	// artifact times are absent (identical artifacts must render identically).
	var listTime time.Time
	if ts, parseErr := time.Parse(time.RFC3339Nano, snap.Complete.CompletedAt); parseErr == nil {
		listTime = ts
	} else if !snap.Manifest.CreatedAt.IsZero() {
		listTime = snap.Manifest.CreatedAt
	}
	runCount, err := countDurablePublishedRuns(filepath.Dir(meta.SourcePath), opts.MaxMarkerBytes)
	if err != nil {
		// Still list the latest snapshot; note run count as 1.
		runCount = 1
	}
	if runCount < 1 {
		runCount = 1
	}
	status := string(indexstore.RunStatusSuccess)
	info := indexstore.IndexListEntry{
		IndexSetID:     snap.Manifest.IndexSetID,
		BaseURI:        meta.BaseURI,
		Provider:       meta.Provider,
		CreatedAt:      listTime, // durable: publication/manifest time when known; zero if absent
		ObjectCount:    int64(snap.Manifest.Counts.ActiveRows),
		TotalSizeBytes: durableSegmentTotalSize(snap.Manifest),
		RunCount:       runCount,
		LatestRunID:    snap.Manifest.RunID,
		LatestStatus:   status,
	}
	if !listTime.IsZero() {
		t := listTime
		info.LatestRunAt = &t
	}
	if info.IndexSetID == "" {
		info.IndexSetID = meta.IndexSetID
	}
	return indexListDisplayEntry{
		Format:         formatLabel(meta.Format),
		DBPath:         "", // no index.db
		Dir:            dir,
		DirName:        dirName,
		IdentityPath:   identityPath,
		IdentityStatus: computeIndexIdentityStatus(identityPath, dirName, info.IndexSetID),
		Info:           info,
	}, nil
}

func durableSegmentTotalSize(manifest indexsubstrate.InternalManifest) int64 {
	var total int64
	for _, seg := range manifest.Segments {
		total += seg.SizeBytes
	}
	return total
}

// countDurablePublishedRuns counts runs/<id>/complete.json that open as valid
// complete markers. Bare run directories without complete are ignored.
func countDurablePublishedRuns(segmentSetRoot string, maxMarkerBytes int64) (int, error) {
	runsDir := filepath.Join(segmentSetRoot, "runs")
	entries, err := os.ReadDir(runsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if maxMarkerBytes <= 0 {
		maxMarkerBytes = indexsubstrate.DefaultMaxPublishedMarkerBytes
	}
	n := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		completePath := filepath.Join(runsDir, entry.Name(), "complete.json")
		if _, err := os.Stat(completePath); err != nil {
			continue
		}
		// Trust-check only that a complete marker is parseable/openable; full
		// manifest verify is owned by open/list snapshot paths.
		if _, err := indexsubstrate.OpenPublishedRunSnapshotBounded(completePath, "", entry.Name(), maxMarkerBytes, indexsubstrate.DefaultMaxPublishedManifestBytes); err != nil {
			continue
		}
		n++
	}
	return n, nil
}

func computeIndexIdentityStatus(identityPath, dirName, indexSetID string) string {
	if strings.TrimSpace(identityPath) == "" {
		return "missing"
	}
	// Bounded single-open read (same posture as durable marker discovery).
	b, err := indexreader.ReadBoundedFile(identityPath, int64(maxHubMarkerBytes))
	if err != nil {
		if os.IsNotExist(err) {
			return "missing"
		}
		if strings.Contains(err.Error(), "exceeds limit") {
			return "invalid"
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
	if dirName != "" && strings.HasPrefix(dirName, "idx_") && expectedDir != dirName {
		// Durable segment root dirs use full idx_<64hex>; short identity dirs use 16-hex.
		if len(dirName) == len(expectedDir) && dirName != expectedDir {
			return "mismatch"
		}
		if len(dirName) > len(expectedDir) {
			// Full set id directory under segment cache is not the identity dir name.
			if !strings.HasPrefix(strings.TrimPrefix(dirName, "idx_"), shaHex[:16]) && dirName != expectedID {
				// Only flag when it looks like an identity-style dir mismatch.
				if len(strings.TrimPrefix(dirName, "idx_")) == 16 && dirName != expectedDir {
					return "mismatch"
				}
			}
		}
	}
	return "ok"
}

func printIndexListTable(entries []indexListDisplayEntry) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer func() { _ = w.Flush() }()

	_, _ = fmt.Fprintln(w, "DIR\tFORMAT\tBASE URI\tPROVIDER\tOBJECTS\tSIZE\tRUNS\tLATEST\tSTATUS\tRUN ID\tRESUME\tIDENTITY")

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

		runID := "-"
		if info.LatestRunID != "" {
			runID = info.LatestRunID
		}

		resume := "-"
		if cmd := resumeCommandForIndexRun(info.LatestStatus, info.LatestSourceType, info.LatestRunID); cmd != "" {
			resume = cmd
		}

		dirName := e.DirName
		if dirName == "" {
			dirName = "-"
		}

		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
			dirName,
			e.Format,
			info.BaseURI,
			info.Provider,
			info.ObjectCount,
			sizeStr,
			info.RunCount,
			latestStr,
			status,
			runID,
			resume,
			e.IdentityStatus,
		)
	}

	return nil
}

func printIndexListJSON(entries []indexListDisplayEntry) error {
	type jsonEntry struct {
		IndexSetID     string  `json:"index_set_id"`
		Format         string  `json:"format"`
		BaseURI        string  `json:"base_uri"`
		Provider       string  `json:"provider"`
		CreatedAt      string  `json:"created_at"`
		ObjectCount    int64   `json:"object_count"`
		TotalSizeBytes int64   `json:"total_size_bytes"`
		RunCount       int     `json:"run_count"`
		LatestRunID    string  `json:"latest_run_id,omitempty"`
		LatestRunAt    *string `json:"latest_run_at,omitempty"`
		LatestStatus   string  `json:"latest_status,omitempty"`
		ResumeCommand  string  `json:"resume_command,omitempty"`

		DirName        string `json:"dir_name"`
		DBPath         string `json:"db_path,omitempty"`
		IdentityPath   string `json:"identity_path,omitempty"`
		IdentityStatus string `json:"identity_status"`
	}

	out := make([]jsonEntry, len(entries))
	for i, e := range entries {
		info := e.Info
		out[i] = jsonEntry{
			IndexSetID:     info.IndexSetID,
			Format:         e.Format,
			BaseURI:        info.BaseURI,
			Provider:       info.Provider,
			CreatedAt:      info.CreatedAt.Format(time.RFC3339),
			ObjectCount:    info.ObjectCount,
			TotalSizeBytes: info.TotalSizeBytes,
			RunCount:       info.RunCount,
			LatestRunID:    info.LatestRunID,
			LatestStatus:   info.LatestStatus,
			ResumeCommand:  resumeCommandForIndexRun(info.LatestStatus, info.LatestSourceType, info.LatestRunID),
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

func resumeCommandForIndexRun(status string, sourceType string, runID string) string {
	if status != string(indexstore.RunStatusFailedResumable) || runID == "" {
		return ""
	}
	operation := operationIndexBuild
	if sourceType == enrichHeadSourceType {
		operation = operationIndexEnrichWithHead
	}
	cmd, err := opcheckpoint.ResumeCommand(operation, runID)
	if err != nil {
		return ""
	}
	return cmd
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
