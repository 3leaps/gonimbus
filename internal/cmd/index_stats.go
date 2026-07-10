package cmd

import (
	"context"
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
)

var indexStatsCmd = &cobra.Command{
	Use:   "stats <base-uri>",
	Short: "Show index statistics",
	Long: `Display detailed statistics for an index.

Shows object counts, size distribution, run history, and prefix breakdown.
Format-aware: resolves sqlite-v1 or durable-v2 via the local index reader seam.

Durable-v2 notes:
  - Object counts come from the published manifest (active/tombstone/total rows).
  - Size is the sum of published segment sizes (not per-object SUM(size_bytes)).
  - --prefixes is sqlite-only (prefix_stats table).
  - --runs lists published durable complete markers only (not SQLite run lifecycle).

Examples:
  # Show stats for an index
  gonimbus index stats s3://bucket/prefix/

  # Show with JSON output
  gonimbus index stats s3://bucket/prefix/ --json

  # Show prefix breakdown (sqlite-v1 / both)
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

	reader, err := openIndexReader(ctx, baseURI, "", "")
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()
	meta := reader.Meta()

	switch meta.Format {
	case indexreader.FormatSQLiteV1:
		return runIndexStatsSQLite(ctx, meta, jsonOutput, showPrefixes, showRuns)
	case indexreader.FormatDurableV2:
		if showPrefixes {
			return errUnsupportedOnDurable("--prefixes")
		}
		return runIndexStatsDurable(meta, jsonOutput, showRuns)
	default:
		return fmt.Errorf("unsupported index format %q", meta.Format)
	}
}

func runIndexStatsSQLite(ctx context.Context, meta indexreader.Meta, jsonOutput, showPrefixes, showRuns bool) error {
	db, err := openMigratedIndexDB(ctx, meta.SourcePath)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	sets, err := indexstore.ListIndexSets(ctx, db, "")
	if err != nil {
		return fmt.Errorf("list index sets: %w", err)
	}
	if len(sets) == 0 {
		return fmt.Errorf("no index sets in database")
	}
	indexSet := &sets[0]
	for i := range sets {
		if sets[i].IndexSetID == meta.IndexSetID || (meta.BaseURI != "" && sets[i].BaseURI == meta.BaseURI) {
			indexSet = &sets[i]
			break
		}
	}

	summary, err := indexstore.GetIndexSetSummary(ctx, db, indexSet.IndexSetID)
	if err != nil {
		return fmt.Errorf("get summary: %w", err)
	}

	var prefixStats []indexstore.PrefixStatRow
	if showPrefixes {
		prefixStats, err = indexstore.GetLatestPrefixStats(ctx, db, indexSet.IndexSetID)
		if err != nil {
			return fmt.Errorf("get prefix stats: %w", err)
		}
	}

	var runs []indexstore.IndexRun
	if showRuns {
		runs, err = indexstore.ListIndexRuns(ctx, db, indexSet.IndexSetID)
		if err != nil {
			return fmt.Errorf("list runs: %w", err)
		}
	}

	if jsonOutput {
		return printStatsJSON(summary, prefixStats, runs, indexSet, string(indexreader.FormatSQLiteV1))
	}
	return printStatsTable(summary, prefixStats, runs, indexSet, string(indexreader.FormatSQLiteV1))
}

// durablePublishedRun is publication history for durable-v2. It deliberately
// does not populate SQLite RunStartedAt semantics — durable markers carry
// publication/completion times, not exact crawl run-start provenance.
type durablePublishedRun struct {
	RunID             string
	PublishedAt       *time.Time // complete.completed_at when parseable
	ManifestCreatedAt *time.Time // manifest.created_at when non-zero
	Status            string
}

func runIndexStatsDurable(meta indexreader.Meta, jsonOutput, showRuns bool) error {
	opts, err := indexReaderResolveOptions()
	if err != nil {
		return err
	}
	snap, err := indexsubstrate.OpenLatestPublishedSnapshotBounded(meta.SourcePath, opts.MaxMarkerBytes, opts.MaxManifestBytes)
	if err != nil {
		return fmt.Errorf("open durable snapshot: %w", err)
	}

	indexSetID := firstNonEmpty(snap.Manifest.IndexSetID, meta.IndexSetID)
	// Artifact times only — never invent wall-clock now for missing timestamps.
	var manifestCreated *time.Time
	if !snap.Manifest.CreatedAt.IsZero() {
		t := snap.Manifest.CreatedAt
		manifestCreated = &t
	}
	var publishedAt *time.Time
	if ts, parseErr := time.Parse(time.RFC3339Nano, snap.Complete.CompletedAt); parseErr == nil {
		publishedAt = &ts
	}

	// Marker-authoritative latest: the verified latest.json snapshot only.
	// Do not re-derive "latest" from timestamp-sorted complete markers — a
	// newer complete without latest advance (or intentional pointer rollback)
	// must not replace the selected run.
	latest := durablePublishedRun{
		RunID:             firstNonEmpty(snap.Manifest.RunID, snap.Complete.RunID),
		PublishedAt:       publishedAt,
		ManifestCreatedAt: manifestCreated,
		Status:            string(indexstore.RunStatusSuccess),
	}

	segmentRoot := filepath.Dir(meta.SourcePath)
	published, err := listDurablePublishedRuns(segmentRoot, opts.MaxMarkerBytes, opts.MaxManifestBytes)
	if err != nil {
		published = nil
	}
	// History/count may include non-latest completes; never substitute for Latest.
	if len(published) == 0 {
		published = []durablePublishedRun{latest}
	}

	indexSet := &indexstore.IndexSet{
		IndexSetID: indexSetID,
		BaseURI:    meta.BaseURI,
		Provider:   meta.Provider,
	}
	if manifestCreated != nil {
		indexSet.CreatedAt = *manifestCreated
	}
	// Best-effort identity extras via bounded reader-seam identity loader.
	if meta.IdentityDir != "" {
		if idFile, idErr := indexreader.ReadLocalIdentityFile(filepath.Join(meta.IdentityDir, "identity.json"), opts.MaxMarkerBytes); idErr == nil {
			indexSet.StorageProvider = idFile.Payload.StorageProvider
			indexSet.CloudProvider = idFile.Payload.CloudProvider
			indexSet.Region = idFile.Payload.Region
			indexSet.RegionKind = idFile.Payload.RegionKind
			indexSet.EndpointHost = idFile.Payload.EndpointHost
			if indexSet.BaseURI == "" {
				indexSet.BaseURI = idFile.Payload.BaseURI
			}
			if indexSet.Provider == "" {
				indexSet.Provider = idFile.Payload.Provider
			}
		}
	}

	view := durableStatsView{
		IndexSetID:      indexSetID,
		BaseURI:         indexSet.BaseURI,
		Provider:        indexSet.Provider,
		StorageProvider: indexSet.StorageProvider,
		CloudProvider:   indexSet.CloudProvider,
		Region:          indexSet.Region,
		ActiveObjects:   int64(snap.Manifest.Counts.ActiveRows),
		DeletedObjects:  int64(snap.Manifest.Counts.Tombstones),
		TotalObjects:    int64(snap.Manifest.Counts.Rows),
		TotalSizeBytes:  durableSegmentTotalSize(snap.Manifest),
		PublishedRuns:   len(published),
		Latest:          latest,
		History:         nil,
	}
	if showRuns {
		view.History = published
	}
	if jsonOutput {
		return printDurableStatsJSON(view)
	}
	return printDurableStatsTable(view)
}

func listDurablePublishedRuns(segmentSetRoot string, maxMarker, maxManifest int64) ([]durablePublishedRun, error) {
	runsDir := filepath.Join(segmentSetRoot, "runs")
	entries, err := os.ReadDir(runsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var runs []durablePublishedRun
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		completePath := filepath.Join(runsDir, entry.Name(), "complete.json")
		snap, err := indexsubstrate.OpenPublishedRunSnapshotBounded(completePath, "", entry.Name(), maxMarker, maxManifest)
		if err != nil {
			continue
		}
		var publishedAt *time.Time
		if ts, parseErr := time.Parse(time.RFC3339Nano, snap.Complete.CompletedAt); parseErr == nil {
			publishedAt = &ts
		}
		var manifestCreated *time.Time
		if !snap.Manifest.CreatedAt.IsZero() {
			t := snap.Manifest.CreatedAt
			manifestCreated = &t
		}
		runs = append(runs, durablePublishedRun{
			RunID:             snap.Manifest.RunID,
			PublishedAt:       publishedAt,
			ManifestCreatedAt: manifestCreated,
			Status:            string(indexstore.RunStatusSuccess),
		})
	}
	sort.Slice(runs, func(i, j int) bool {
		// Prefer published_at; fall back to manifest created_at; stable by run id.
		ti, tj := durableSortTime(runs[i]), durableSortTime(runs[j])
		if !ti.Equal(tj) {
			return ti.After(tj)
		}
		return runs[i].RunID > runs[j].RunID
	})
	return runs, nil
}

func durableSortTime(r durablePublishedRun) time.Time {
	if r.PublishedAt != nil {
		return *r.PublishedAt
	}
	if r.ManifestCreatedAt != nil {
		return *r.ManifestCreatedAt
	}
	return time.Time{}
}

type durableStatsView struct {
	IndexSetID      string
	BaseURI         string
	Provider        string
	StorageProvider string
	CloudProvider   string
	Region          string
	ActiveObjects   int64
	DeletedObjects  int64
	TotalObjects    int64
	TotalSizeBytes  int64
	PublishedRuns   int
	Latest          durablePublishedRun
	History         []durablePublishedRun
}

func printDurableStatsTable(view durableStatsView) error {
	_, _ = fmt.Fprintf(os.Stdout, "Index: %s\n", view.BaseURI)
	_, _ = fmt.Fprintf(os.Stdout, "Format: %s\n", indexreader.FormatDurableV2)
	_, _ = fmt.Fprintf(os.Stdout, "Provider: %s\n", view.Provider)
	if view.StorageProvider != "" {
		_, _ = fmt.Fprintf(os.Stdout, "Storage Provider: %s\n", view.StorageProvider)
	}
	if view.Region != "" {
		_, _ = fmt.Fprintf(os.Stdout, "Region: %s\n", view.Region)
	}
	_, _ = fmt.Fprintln(os.Stdout)

	_, _ = fmt.Fprintln(os.Stdout, "Objects:")
	_, _ = fmt.Fprintf(os.Stdout, "  Active:   %d\n", view.ActiveObjects)
	_, _ = fmt.Fprintf(os.Stdout, "  Deleted:  %d\n", view.DeletedObjects)
	_, _ = fmt.Fprintf(os.Stdout, "  Total:    %d\n", view.TotalObjects)
	_, _ = fmt.Fprintf(os.Stdout, "  Size:     %s\n", formatBytes(view.TotalSizeBytes))
	_, _ = fmt.Fprintln(os.Stdout, "  note: size is sum of published segment file sizes")
	_, _ = fmt.Fprintln(os.Stdout)

	_, _ = fmt.Fprintln(os.Stdout, "Published runs (durable complete markers):")
	_, _ = fmt.Fprintf(os.Stdout, "  Total:      %d\n", view.PublishedRuns)
	_, _ = fmt.Fprintf(os.Stdout, "  Latest run: %s\n", valueOrDash(view.Latest.RunID))
	_, _ = fmt.Fprintf(os.Stdout, "  Published:  %s\n", formatOptionalRFC3339(view.Latest.PublishedAt))
	if view.Latest.ManifestCreatedAt != nil {
		_, _ = fmt.Fprintf(os.Stdout, "  Manifest:   %s\n", formatOptionalRFC3339(view.Latest.ManifestCreatedAt))
	}
	_, _ = fmt.Fprintln(os.Stdout, "  note: durable markers carry publication times, not crawl run-start")
	_, _ = fmt.Fprintln(os.Stdout)

	if len(view.History) > 0 {
		_, _ = fmt.Fprintln(os.Stdout, "Publication history:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(w, "  RUN ID\tPUBLISHED\tMANIFEST_CREATED\tSTATUS")
		for _, r := range view.History {
			_, _ = fmt.Fprintf(w, "  %s\t%s\t%s\t%s\n",
				r.RunID,
				formatOptionalRFC3339(r.PublishedAt),
				formatOptionalRFC3339(r.ManifestCreatedAt),
				r.Status,
			)
		}
		_ = w.Flush()
	}
	return nil
}

func printDurableStatsJSON(view durableStatsView) error {
	type jsonPubRun struct {
		RunID             string  `json:"run_id"`
		PublishedAt       *string `json:"published_at,omitempty"`
		ManifestCreatedAt *string `json:"manifest_created_at,omitempty"`
		Status            string  `json:"status"`
	}
	type jsonOut struct {
		IndexSetID      string `json:"index_set_id"`
		Format          string `json:"format"`
		BaseURI         string `json:"base_uri"`
		Provider        string `json:"provider"`
		StorageProvider string `json:"storage_provider,omitempty"`
		CloudProvider   string `json:"cloud_provider,omitempty"`
		Region          string `json:"region,omitempty"`
		Objects         struct {
			Active         int64  `json:"active"`
			Deleted        int64  `json:"deleted"`
			Total          int64  `json:"total"`
			TotalSizeBytes int64  `json:"total_size_bytes"`
			SizeSemantics  string `json:"size_semantics"`
		} `json:"objects"`
		PublishedRuns struct {
			Total  int         `json:"total"`
			Latest *jsonPubRun `json:"latest,omitempty"`
		} `json:"published_runs"`
		PublicationHistory []jsonPubRun `json:"publication_history,omitempty"`
		TimeSemantics      string       `json:"time_semantics"`
	}
	toJSON := func(r durablePublishedRun) jsonPubRun {
		out := jsonPubRun{RunID: r.RunID, Status: r.Status}
		if r.PublishedAt != nil {
			s := r.PublishedAt.Format(time.RFC3339Nano)
			out.PublishedAt = &s
		}
		if r.ManifestCreatedAt != nil {
			s := r.ManifestCreatedAt.Format(time.RFC3339Nano)
			out.ManifestCreatedAt = &s
		}
		return out
	}
	out := jsonOut{
		IndexSetID:      view.IndexSetID,
		Format:          string(indexreader.FormatDurableV2),
		BaseURI:         view.BaseURI,
		Provider:        view.Provider,
		StorageProvider: view.StorageProvider,
		CloudProvider:   view.CloudProvider,
		Region:          view.Region,
		TimeSemantics:   "durable publication times (completed_at/created_at); not crawl RunStartedAt",
	}
	out.Objects.Active = view.ActiveObjects
	out.Objects.Deleted = view.DeletedObjects
	out.Objects.Total = view.TotalObjects
	out.Objects.TotalSizeBytes = view.TotalSizeBytes
	out.Objects.SizeSemantics = "segment_file_bytes"
	out.PublishedRuns.Total = view.PublishedRuns
	latest := toJSON(view.Latest)
	out.PublishedRuns.Latest = &latest
	if len(view.History) > 0 {
		out.PublicationHistory = make([]jsonPubRun, len(view.History))
		for i, r := range view.History {
			out.PublicationHistory[i] = toJSON(r)
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func formatOptionalRFC3339(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "-"
	}
	return t.Format(time.RFC3339)
}

func valueOrDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func printStatsTable(summary *indexstore.IndexSetSummary, prefixStats []indexstore.PrefixStatRow, runs []indexstore.IndexRun, indexSet *indexstore.IndexSet, format string) error {
	_, _ = fmt.Fprintf(os.Stdout, "Index: %s\n", summary.BaseURI)
	_, _ = fmt.Fprintf(os.Stdout, "Format: %s\n", format)
	_, _ = fmt.Fprintf(os.Stdout, "Provider: %s\n", summary.Provider)
	if indexSet.StorageProvider != "" {
		_, _ = fmt.Fprintf(os.Stdout, "Storage Provider: %s\n", indexSet.StorageProvider)
	}
	if indexSet.Region != "" {
		_, _ = fmt.Fprintf(os.Stdout, "Region: %s\n", indexSet.Region)
	}
	_, _ = fmt.Fprintf(os.Stdout, "Created: %s\n", summary.CreatedAt.Format(time.RFC3339))
	_, _ = fmt.Fprintln(os.Stdout)

	_, _ = fmt.Fprintln(os.Stdout, "Objects:")
	_, _ = fmt.Fprintf(os.Stdout, "  Active:   %d\n", summary.ActiveObjects)
	_, _ = fmt.Fprintf(os.Stdout, "  Deleted:  %d\n", summary.DeletedObjects)
	_, _ = fmt.Fprintf(os.Stdout, "  Total:    %d\n", summary.TotalObjects)
	_, _ = fmt.Fprintf(os.Stdout, "  Size:     %s\n", formatBytes(summary.TotalSizeBytes))
	if format == string(indexreader.FormatDurableV2) {
		_, _ = fmt.Fprintln(os.Stdout, "  note: size is sum of published segment file sizes")
	}
	_, _ = fmt.Fprintln(os.Stdout)

	_, _ = fmt.Fprintln(os.Stdout, "Runs:")
	_, _ = fmt.Fprintf(os.Stdout, "  Total:      %d\n", summary.TotalRuns)
	_, _ = fmt.Fprintf(os.Stdout, "  Successful: %d\n", summary.SuccessfulRuns)
	_, _ = fmt.Fprintf(os.Stdout, "  Partial:    %d\n", summary.PartialRuns)
	_, _ = fmt.Fprintf(os.Stdout, "  Failed:     %d\n", summary.FailedRuns)
	_, _ = fmt.Fprintf(os.Stdout, "  Resumable:  %d\n", summary.FailedResumableRuns)

	if summary.LatestRun != nil {
		_, _ = fmt.Fprintf(os.Stdout, "  Latest:     %s (%s, %s)\n",
			summary.LatestRun.StartedAt.Format(time.RFC3339),
			summary.LatestRun.RunID,
			summary.LatestRun.Status)
		if cmd := resumeCommandForIndexRun(string(summary.LatestRun.Status), summary.LatestRun.SourceType, summary.LatestRun.RunID); cmd != "" {
			_, _ = fmt.Fprintf(os.Stdout, "  Resume:     %s\n", cmd)
		}
	}
	_, _ = fmt.Fprintln(os.Stdout)

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

	if len(runs) > 0 {
		_, _ = fmt.Fprintln(os.Stdout, "Run History:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(w, "  RUN ID\tSTARTED\tSTATUS\tSOURCE\tDURATION\tRESUME")
		for _, r := range runs {
			duration := "-"
			if r.EndedAt != nil {
				duration = r.EndedAt.Sub(r.StartedAt).Round(time.Second).String()
			}
			resume := "-"
			if cmd := resumeCommandForIndexRun(string(r.Status), r.SourceType, r.RunID); cmd != "" {
				resume = cmd
			}
			_, _ = fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%s\t%s\n",
				r.RunID,
				r.StartedAt.Format("2006-01-02 15:04:05"),
				r.Status,
				r.SourceType,
				duration,
				resume,
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

func printStatsJSON(summary *indexstore.IndexSetSummary, prefixStats []indexstore.PrefixStatRow, runs []indexstore.IndexRun, indexSet *indexstore.IndexSet, format string) error {
	type jsonRun struct {
		RunID         string  `json:"run_id"`
		StartedAt     string  `json:"started_at"`
		EndedAt       *string `json:"ended_at,omitempty"`
		SourceType    string  `json:"source_type"`
		Status        string  `json:"status"`
		ResumeCommand string  `json:"resume_command,omitempty"`
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
		Format          string `json:"format"`
		BaseURI         string `json:"base_uri"`
		Provider        string `json:"provider"`
		StorageProvider string `json:"storage_provider,omitempty"`
		CloudProvider   string `json:"cloud_provider,omitempty"`
		Region          string `json:"region,omitempty"`
		CreatedAt       string `json:"created_at"`

		Objects struct {
			Active         int64  `json:"active"`
			Deleted        int64  `json:"deleted"`
			Total          int64  `json:"total"`
			TotalSizeBytes int64  `json:"total_size_bytes"`
			SizeSemantics  string `json:"size_semantics,omitempty"`
		} `json:"objects"`

		Runs struct {
			Total           int `json:"total"`
			Successful      int `json:"successful"`
			Partial         int `json:"partial"`
			Failed          int `json:"failed"`
			FailedResumable int `json:"failed_resumable"`
		} `json:"runs"`

		LatestRun  *jsonRun     `json:"latest_run,omitempty"`
		Prefixes   []jsonPrefix `json:"prefixes,omitempty"`
		RunHistory []jsonRun    `json:"run_history,omitempty"`
	}

	out := jsonOutput{
		IndexSetID:      summary.IndexSetID,
		Format:          format,
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
	if format == string(indexreader.FormatDurableV2) {
		out.Objects.SizeSemantics = "segment_file_bytes"
	}

	out.Runs.Total = summary.TotalRuns
	out.Runs.Successful = summary.SuccessfulRuns
	out.Runs.Partial = summary.PartialRuns
	out.Runs.Failed = summary.FailedRuns
	out.Runs.FailedResumable = summary.FailedResumableRuns

	if summary.LatestRun != nil {
		r := summary.LatestRun
		out.LatestRun = &jsonRun{
			RunID:         r.RunID,
			StartedAt:     r.StartedAt.Format(time.RFC3339),
			SourceType:    r.SourceType,
			Status:        string(r.Status),
			ResumeCommand: resumeCommandForIndexRun(string(r.Status), r.SourceType, r.RunID),
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
				RunID:         r.RunID,
				StartedAt:     r.StartedAt.Format(time.RFC3339),
				SourceType:    r.SourceType,
				Status:        string(r.Status),
				ResumeCommand: resumeCommandForIndexRun(string(r.Status), r.SourceType, r.RunID),
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
