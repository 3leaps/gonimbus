package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/spf13/cobra"
)

var indexHubCmd = &cobra.Command{
	Use:   "hub",
	Short: "Manage index hubs",
	Long: `Manage index hub lifecycle: initialize, browse, and maintain hub contents.

An index hub is a root URI containing versioned index artifacts and pointer files.
Supported hub roots: s3://bucket/prefix/ and file:///path/`,
}

// --- init ---

var indexHubInitCmd = &cobra.Command{
	Use:   "init [hub-uri]",
	Args:  validateHubURIArgs,
	Short: "Initialize a new index hub",
	Long: `Create hub.json marker at the hub root.

The marker identifies the hub layout version and records creation metadata.
This is optional — export works without it — but provides clear provenance.

Examples:
  gonimbus index hub init file:///tmp/gonimbus-hub/
  gonimbus index hub init --hub s3://my-bucket/ops/index-hub/ --hub-profile my-profile
  gonimbus index hub init file:///tmp/gonimbus-hub/ --description "Production indexes"`,
	RunE: runIndexHubInit,
}

// --- ls ---

var indexHubLsCmd = &cobra.Command{
	Use:   "ls [hub-uri]",
	Args:  validateHubURIArgs,
	Short: "List index sets in a hub",
	Long: `List index sets available in the hub, showing latest run info when available.

Examples:
  gonimbus index hub ls file:///tmp/gonimbus-hub/
  gonimbus index hub ls --hub s3://my-bucket/ops/index-hub/ --hub-profile my-profile`,
	RunE: runIndexHubLs,
}

// --- show ---

var indexHubShowCmd = &cobra.Command{
	Use:   "show [hub-uri]",
	Args:  validateHubURIArgs,
	Short: "Show details for an index set in a hub",
	Long: `Show details for an index set including available runs and the latest pointer.

Examples:
  gonimbus index hub show file:///tmp/gonimbus-hub/ --index-set idx_da038d8171b4a9ba...
  gonimbus index hub show --hub s3://my-bucket/ops/index-hub/ --index-set idx_da038d8171b4a9ba... --hub-profile my-profile`,
	RunE: runIndexHubShow,
}

// --- set-latest ---

var indexHubSetLatestCmd = &cobra.Command{
	Use:   "set-latest [hub-uri]",
	Args:  validateHubURIArgs,
	Short: "Repoint latest.json to a specific run",
	Long: `Update the latest.json pointer for an index set to a specific run.

The target run must have a valid complete.json commit marker.

Examples:
  gonimbus index hub set-latest file:///tmp/gonimbus-hub/ \
    --index-set idx_da038d8171b4a9ba... --run-id run_1709654400000000000`,
	RunE: runIndexHubSetLatest,
}

// --- rm-run ---

var indexHubRmRunCmd = &cobra.Command{
	Use:   "rm-run [hub-uri]",
	Args:  validateHubURIArgs,
	Short: "Remove a run from the hub",
	Long: `Remove a run's artifacts (index.db, identity.json, complete.json) from the hub.

Refuses to remove the current latest run unless --force is specified.

Examples:
  gonimbus index hub rm-run file:///tmp/gonimbus-hub/ \
    --index-set idx_da038d8171b4a9ba... --run-id run_1709654400000000000`,
	RunE: runIndexHubRmRun,
}

// --- gc ---

var indexHubGCCmd = &cobra.Command{
	Use:   "gc [hub-uri]",
	Args:  validateHubURIArgs,
	Short: "Garbage collect old runs from the hub",
	Long: `Prune old runs from the hub. Keeps the latest-pointed run and applies
the specified retention policy.

Retention modes (one required):
  --keep N       Keep the N most recent committed runs
  --before DATE  Remove committed runs older than DATE (RFC 3339 or YYYY-MM-DD)

Examples:
  gonimbus index hub gc file:///tmp/gonimbus-hub/ --keep 3
  gonimbus index hub gc --hub s3://my-bucket/ops/index-hub/ --index-set idx_da038d... --before 2026-01-01
  gonimbus index hub gc file:///tmp/gonimbus-hub/ --keep 2 --dry-run`,
	RunE: runIndexHubGC,
}

func init() {
	indexCmd.AddCommand(indexHubCmd)
	indexHubCmd.AddCommand(indexHubInitCmd)
	indexHubCmd.AddCommand(indexHubLsCmd)
	indexHubCmd.AddCommand(indexHubShowCmd)
	indexHubCmd.AddCommand(indexHubSetLatestCmd)
	indexHubCmd.AddCommand(indexHubRmRunCmd)
	indexHubCmd.AddCommand(indexHubGCCmd)

	// Common hub flags on all subcommands
	for _, cmd := range []*cobra.Command{indexHubInitCmd, indexHubLsCmd, indexHubShowCmd, indexHubSetLatestCmd, indexHubRmRunCmd, indexHubGCCmd} {
		cmd.Flags().String("hub", "", "Hub root URI (alternative to positional hub-uri)")
		cmd.Flags().String("hub-profile", "", "AWS profile for hub")
		cmd.Flags().String("hub-region", "", "AWS region for hub")
		cmd.Flags().String("hub-endpoint", "", "Custom endpoint for hub")
	}

	// init
	indexHubInitCmd.Flags().String("description", "", "Optional description for the hub")

	// ls
	indexHubLsCmd.Flags().Bool("json", false, "Output as JSON")

	// show
	indexHubShowCmd.Flags().String("index-set", "", "Index set ID (required)")
	indexHubShowCmd.Flags().Bool("json", false, "Output as JSON")
	_ = indexHubShowCmd.MarkFlagRequired("index-set")

	// set-latest
	indexHubSetLatestCmd.Flags().String("index-set", "", "Index set ID (required)")
	indexHubSetLatestCmd.Flags().String("run-id", "", "Run ID to set as latest (required)")
	_ = indexHubSetLatestCmd.MarkFlagRequired("index-set")
	_ = indexHubSetLatestCmd.MarkFlagRequired("run-id")

	// rm-run
	indexHubRmRunCmd.Flags().String("index-set", "", "Index set ID (required)")
	indexHubRmRunCmd.Flags().String("run-id", "", "Run ID to remove (required)")
	indexHubRmRunCmd.Flags().Bool("force", false, "Remove even if this is the current latest run")
	_ = indexHubRmRunCmd.MarkFlagRequired("index-set")
	_ = indexHubRmRunCmd.MarkFlagRequired("run-id")

	// gc
	indexHubGCCmd.Flags().String("index-set", "", "Scope to a specific index set (optional; default: all)")
	indexHubGCCmd.Flags().Int("keep", 0, "Keep the N most recent committed runs per index set")
	indexHubGCCmd.Flags().String("before", "", "Remove committed runs older than this date (RFC 3339 or YYYY-MM-DD)")
	indexHubGCCmd.Flags().Bool("dry-run", false, "Show what would be removed without deleting")
	indexHubGCCmd.Flags().Bool("json", false, "Output as JSON")
}

// validateHubURIArgs enforces the shared hub URI positional argument policy.
func validateHubURIArgs(_ *cobra.Command, args []string) error {
	if len(args) > 1 {
		return fmt.Errorf("at most one positional hub-uri is allowed")
	}
	return nil
}

func resolveHubURI(cmd *cobra.Command, args []string) (string, error) {
	hubURI, _ := cmd.Flags().GetString("hub")
	if len(args) > 1 {
		return "", fmt.Errorf("at most one positional hub-uri is allowed")
	}
	positionalHubURI := ""
	if len(args) == 1 {
		positionalHubURI = strings.TrimSpace(args[0])
	}
	if hubURI != "" && positionalHubURI != "" {
		return "", fmt.Errorf("--hub and positional hub-uri are mutually exclusive")
	}
	if hubURI != "" {
		return hubURI, nil
	}
	if positionalHubURI != "" {
		return positionalHubURI, nil
	}
	return "", fmt.Errorf("hub URI is required; provide positional hub-uri or --hub")
}

// parseHubFlags extracts common hub flags and returns a configured hubDestSpec.
func parseHubFlags(cmd *cobra.Command, args []string) (*hubDestSpec, error) {
	hubURI, err := resolveHubURI(cmd, args)
	if err != nil {
		return nil, err
	}
	hub, err := parseHubURI(hubURI)
	if err != nil {
		return nil, err
	}
	hub.Profile, _ = cmd.Flags().GetString("hub-profile")
	hub.Region, _ = cmd.Flags().GetString("hub-region")
	hub.Endpoint, _ = cmd.Flags().GetString("hub-endpoint")
	if hub.Endpoint != "" {
		hub.ForcePathStyle = true
	}
	return hub, nil
}

// --- init implementation ---

func runIndexHubInit(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	description, _ := cmd.Flags().GetString("description")

	hub, err := parseHubFlags(cmd, args)
	if err != nil {
		return err
	}

	// Check if hub.json already exists
	getter, err := newHubGetter(ctx, hub)
	if err != nil {
		return fmt.Errorf("hub provider: %w", err)
	}
	if closer, ok := getter.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	hubKey := hubArtifactKey(hub, "hub.json")
	if _, getErr := downloadBytes(ctx, getter, hubKey); getErr == nil {
		return fmt.Errorf("hub already initialized: %s", hubKey)
	} else if !provider.IsNotFound(getErr) {
		return fmt.Errorf("check existing hub.json: %w", getErr)
	}

	// Build hub.json
	type hubDoc struct {
		Version     string `json:"version"`
		CreatedAt   string `json:"created_at"`
		CreatedBy   string `json:"created_by"`
		Description string `json:"description,omitempty"`
	}
	doc := hubDoc{
		Version:     "1.0",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		CreatedBy:   exportedByString(),
		Description: description,
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal hub.json: %w", err)
	}

	// Write hub.json
	putter, err := newHubProvider(ctx, hub)
	if err != nil {
		return fmt.Errorf("hub provider: %w", err)
	}
	if closer, ok := putter.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	if err := uploadBytes(ctx, putter, hubKey, data); err != nil {
		return fmt.Errorf("write hub.json: %w", err)
	}

	_, _ = fmt.Fprintf(os.Stderr, "Hub initialized: %s\n", hubKey)
	return nil
}

// --- ls implementation ---

// hubIndexSetInfo holds summary info about an index set in the hub.
type hubIndexSetInfo struct {
	IndexSetID string `json:"index_set_id"`
	LatestRun  string `json:"latest_run,omitempty"`
	RunCount   int    `json:"run_count"`
}

func runIndexHubLs(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	jsonOutput, _ := cmd.Flags().GetBool("json")

	hub, err := parseHubFlags(cmd, args)
	if err != nil {
		return err
	}

	getter, err := newHubGetter(ctx, hub)
	if err != nil {
		return fmt.Errorf("hub provider: %w", err)
	}
	if closer, ok := getter.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	// Use provider.Provider (List) to enumerate index sets
	lister, ok := getter.(provider.Provider)
	if !ok {
		return fmt.Errorf("hub provider does not support listing")
	}

	indexSetsPrefix := hubArtifactKey(hub, "index-sets/")
	indexSetIDs, err := discoverIndexSets(ctx, lister, indexSetsPrefix)
	if err != nil {
		return err
	}

	if len(indexSetIDs) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "No index sets found in hub")
		return nil
	}

	// Enrich with latest.json and run count
	infos := make([]hubIndexSetInfo, 0, len(indexSetIDs))
	for _, setID := range indexSetIDs {
		info := hubIndexSetInfo{IndexSetID: setID}

		// Try to read latest.json
		latestKey := hubArtifactKey(hub, "index-sets", setID, "latest.json")
		if data, getErr := downloadBytes(ctx, getter, latestKey); getErr == nil {
			var latest struct {
				RunID string `json:"run_id"`
			}
			if json.Unmarshal(data, &latest) == nil {
				info.LatestRun = latest.RunID
			}
		}

		// Count runs
		runsPrefix := hubArtifactKey(hub, "index-sets", setID, "runs/")
		runs, _ := discoverRuns(ctx, lister, runsPrefix)
		info.RunCount = len(runs)

		infos = append(infos, info)
	}

	if jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(infos)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "INDEX SET\tLATEST RUN\tRUNS")
	for _, info := range infos {
		latest := info.LatestRun
		if latest == "" {
			latest = "-"
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%d\n", info.IndexSetID, latest, info.RunCount)
	}
	return w.Flush()
}

// discoverIndexSets lists unique index set IDs under the index-sets/ prefix.
func discoverIndexSets(ctx context.Context, lister provider.Provider, prefix string) ([]string, error) {
	seen := make(map[string]bool)
	token := ""
	for {
		result, err := lister.List(ctx, provider.ListOptions{
			Prefix:            prefix,
			ContinuationToken: token,
			MaxKeys:           1000,
		})
		if err != nil {
			return nil, fmt.Errorf("list index sets: %w", err)
		}

		for _, obj := range result.Objects {
			// Key looks like: <prefix>index-sets/<index_set_id>/...
			rel := strings.TrimPrefix(obj.Key, prefix)
			if idx := strings.Index(rel, "/"); idx > 0 {
				setID := rel[:idx]
				if strings.HasPrefix(setID, "idx_") {
					seen[setID] = true
				}
			}
		}

		if !result.IsTruncated || result.ContinuationToken == "" {
			break
		}
		token = result.ContinuationToken
	}

	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

// discoverRuns lists unique run IDs under an index set's runs/ prefix.
func discoverRuns(ctx context.Context, lister provider.Provider, runsPrefix string) ([]string, error) {
	seen := make(map[string]bool)
	token := ""
	for {
		result, err := lister.List(ctx, provider.ListOptions{
			Prefix:            runsPrefix,
			ContinuationToken: token,
			MaxKeys:           1000,
		})
		if err != nil {
			return nil, fmt.Errorf("list runs: %w", err)
		}

		for _, obj := range result.Objects {
			rel := strings.TrimPrefix(obj.Key, runsPrefix)
			if idx := strings.Index(rel, "/"); idx > 0 {
				runID := rel[:idx]
				if strings.HasPrefix(runID, "run_") {
					seen[runID] = true
				}
			}
		}

		if !result.IsTruncated || result.ContinuationToken == "" {
			break
		}
		token = result.ContinuationToken
	}

	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

// --- show implementation ---

// hubRunInfo holds details about a specific run in the hub.
type hubRunInfo struct {
	RunID       string          `json:"run_id"`
	IsLatest    bool            `json:"is_latest"`
	IsCommitted bool            `json:"is_committed"`
	Complete    json.RawMessage `json:"complete,omitempty"`
}

func runIndexHubShow(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	indexSetFlag, _ := cmd.Flags().GetString("index-set")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	if err := validateFullIndexSetID(indexSetFlag); err != nil {
		return err
	}

	hub, err := parseHubFlags(cmd, args)
	if err != nil {
		return err
	}

	getter, err := newHubGetter(ctx, hub)
	if err != nil {
		return fmt.Errorf("hub provider: %w", err)
	}
	if closer, ok := getter.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	lister, ok := getter.(provider.Provider)
	if !ok {
		return fmt.Errorf("hub provider does not support listing")
	}

	// Read latest.json
	var latestRunID string
	latestKey := hubArtifactKey(hub, "index-sets", indexSetFlag, "latest.json")
	if data, getErr := downloadBytes(ctx, getter, latestKey); getErr == nil {
		var latest struct {
			RunID string `json:"run_id"`
		}
		if json.Unmarshal(data, &latest) == nil {
			latestRunID = latest.RunID
		}
	}

	// Discover runs
	runsPrefix := hubArtifactKey(hub, "index-sets", indexSetFlag, "runs/")
	runIDs, err := discoverRuns(ctx, lister, runsPrefix)
	if err != nil {
		return err
	}

	if len(runIDs) == 0 {
		_, _ = fmt.Fprintf(os.Stderr, "No runs found for index set %s\n", indexSetFlag)
		return nil
	}

	// Enrich each run
	runs := make([]hubRunInfo, 0, len(runIDs))
	for _, runID := range runIDs {
		info := hubRunInfo{
			RunID:    runID,
			IsLatest: runID == latestRunID,
		}

		completeKey := hubArtifactKey(hub, "index-sets", indexSetFlag, "runs", runID, "complete.json")
		if data, getErr := downloadBytes(ctx, getter, completeKey); getErr == nil {
			info.IsCommitted = true
			info.Complete = data
		}

		runs = append(runs, info)
	}

	if jsonOutput {
		type showResult struct {
			IndexSetID string       `json:"index_set_id"`
			LatestRun  string       `json:"latest_run,omitempty"`
			Runs       []hubRunInfo `json:"runs"`
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(showResult{
			IndexSetID: indexSetFlag,
			LatestRun:  latestRunID,
			Runs:       runs,
		})
	}

	_, _ = fmt.Fprintf(os.Stdout, "Index Set: %s\n", indexSetFlag)
	if latestRunID != "" {
		_, _ = fmt.Fprintf(os.Stdout, "Latest:    %s\n", latestRunID)
	} else {
		_, _ = fmt.Fprintln(os.Stdout, "Latest:    (not set)")
	}
	_, _ = fmt.Fprintf(os.Stdout, "Runs:      %d\n\n", len(runs))

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "RUN ID\tCOMMITTED\tLATEST")
	for _, r := range runs {
		committed := "no"
		if r.IsCommitted {
			committed = "yes"
		}
		latest := ""
		if r.IsLatest {
			latest = "<--"
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", r.RunID, committed, latest)
	}
	return w.Flush()
}

// --- set-latest implementation ---

func runIndexHubSetLatest(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	indexSetFlag, _ := cmd.Flags().GetString("index-set")
	runIDFlag, _ := cmd.Flags().GetString("run-id")

	if err := validateFullIndexSetID(indexSetFlag); err != nil {
		return err
	}
	if err := validateRunID(runIDFlag); err != nil {
		return err
	}

	hub, err := parseHubFlags(cmd, args)
	if err != nil {
		return err
	}

	// Verify the run is committed (has complete.json)
	getter, err := newHubGetter(ctx, hub)
	if err != nil {
		return fmt.Errorf("hub provider: %w", err)
	}
	if closer, ok := getter.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	completeKey := hubArtifactKey(hub, "index-sets", indexSetFlag, "runs", runIDFlag, "complete.json")
	if _, getErr := downloadBytes(ctx, getter, completeKey); getErr != nil {
		if provider.IsNotFound(getErr) {
			return fmt.Errorf("run %s is not committed (complete.json not found); cannot set as latest", runIDFlag)
		}
		return fmt.Errorf("verify complete.json: %w", getErr)
	}

	// Build and upload latest.json
	type latestDoc struct {
		Version    string `json:"version"`
		IndexSetID string `json:"index_set_id"`
		RunID      string `json:"run_id"`
		UpdatedAt  string `json:"updated_at"`
		UpdatedBy  string `json:"updated_by"`
	}
	doc := latestDoc{
		Version:    "1.0",
		IndexSetID: indexSetFlag,
		RunID:      runIDFlag,
		UpdatedAt:  time.Now().UTC().Format(time.RFC3339),
		UpdatedBy:  exportedByString(),
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal latest.json: %w", err)
	}

	putter, err := newHubProvider(ctx, hub)
	if err != nil {
		return fmt.Errorf("hub provider: %w", err)
	}
	if closer, ok := putter.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	// Best-effort pointer advance: plain PutObject, last-writer-wins.
	// CAS / fail-closed semantics are tracked for v0.2.x.
	latestKey := hubArtifactKey(hub, "index-sets", indexSetFlag, "latest.json")
	if err := uploadBytes(ctx, putter, latestKey, data); err != nil {
		return fmt.Errorf("write latest.json: %w", err)
	}

	_, _ = fmt.Fprintf(os.Stderr, "Set latest for %s to %s\n", indexSetFlag, runIDFlag)
	return nil
}

// --- rm-run implementation ---

func runIndexHubRmRun(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	indexSetFlag, _ := cmd.Flags().GetString("index-set")
	runIDFlag, _ := cmd.Flags().GetString("run-id")
	force, _ := cmd.Flags().GetBool("force")

	if err := validateFullIndexSetID(indexSetFlag); err != nil {
		return err
	}
	if err := validateRunID(runIDFlag); err != nil {
		return err
	}

	hub, err := parseHubFlags(cmd, args)
	if err != nil {
		return err
	}

	getter, err := newHubGetter(ctx, hub)
	if err != nil {
		return fmt.Errorf("hub provider: %w", err)
	}
	if closer, ok := getter.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	// Check if this is the latest run
	if !force {
		latestKey := hubArtifactKey(hub, "index-sets", indexSetFlag, "latest.json")
		if data, getErr := downloadBytes(ctx, getter, latestKey); getErr == nil {
			var latest struct {
				RunID string `json:"run_id"`
			}
			if json.Unmarshal(data, &latest) == nil && latest.RunID == runIDFlag {
				return fmt.Errorf("run %s is the current latest; use --force to remove it", runIDFlag)
			}
		}
	}

	// List all artifacts in the run prefix
	lister, ok := getter.(provider.Provider)
	if !ok {
		return fmt.Errorf("hub provider does not support listing")
	}

	runPrefix := hubArtifactKey(hub, "index-sets", indexSetFlag, "runs", runIDFlag) + "/"
	keys, err := listAllKeys(ctx, lister, runPrefix)
	if err != nil {
		return fmt.Errorf("list run artifacts: %w", err)
	}

	if len(keys) == 0 {
		return fmt.Errorf("no artifacts found for run %s", runIDFlag)
	}

	// Delete artifacts
	deleter, ok := getter.(provider.ObjectDeleter)
	if !ok {
		// Try the putter path
		putter, putErr := newHubProvider(ctx, hub)
		if putErr != nil {
			return fmt.Errorf("hub provider: %w", putErr)
		}
		if d, dok := putter.(provider.ObjectDeleter); dok {
			deleter = d
			if closer, cok := putter.(io.Closer); cok {
				defer func() { _ = closer.Close() }()
			}
		} else {
			return fmt.Errorf("hub provider does not support deletion")
		}
	}

	for _, key := range keys {
		if err := deleter.DeleteObject(ctx, key); err != nil {
			return fmt.Errorf("delete %s: %w", path.Base(key), err)
		}
		_, _ = fmt.Fprintf(os.Stderr, "  deleted %s\n", path.Base(key))
	}

	_, _ = fmt.Fprintf(os.Stderr, "Removed run %s (%d artifacts)\n", runIDFlag, len(keys))
	return nil
}

// listAllKeys lists all object keys under a prefix.
func listAllKeys(ctx context.Context, lister provider.Provider, prefix string) ([]string, error) {
	var keys []string
	token := ""
	for {
		result, err := lister.List(ctx, provider.ListOptions{
			Prefix:            prefix,
			ContinuationToken: token,
			MaxKeys:           1000,
		})
		if err != nil {
			return nil, err
		}
		for _, obj := range result.Objects {
			keys = append(keys, obj.Key)
		}
		if !result.IsTruncated || result.ContinuationToken == "" {
			break
		}
		token = result.ContinuationToken
	}
	return keys, nil
}

// --- gc implementation ---

// gcRunCandidate represents a run being evaluated for garbage collection.
// In dry-run output, only the IndexSetID/RunID/IsLatest/CompletedAt fields are
// populated. In real-run output, Artifacts and Error are also populated to
// reflect the per-run deletion outcome.
type gcRunCandidate struct {
	IndexSetID  string `json:"index_set_id"`
	RunID       string `json:"run_id"`
	IsLatest    bool   `json:"is_latest"`
	CompletedAt string `json:"completed_at,omitempty"`
	Artifacts   int    `json:"artifacts,omitempty"` // count of objects deleted (real-run)
	Error       string `json:"error,omitempty"`     // populated if list/delete failed (real-run)
}

// gcResult is the JSON envelope emitted by `gonimbus index hub gc --json`.
// In dry-run mode, Removed lists candidates that *would* be removed.
// In real-run mode, Removed lists candidates that were *attempted*; each
// entry carries an Error if its deletion failed, and Errors is the count of
// such failures.
type gcResult struct {
	DryRun  bool             `json:"dry_run"`
	Removed []gcRunCandidate `json:"removed"`
	Errors  int              `json:"errors,omitempty"`
}

func runIndexHubGC(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	indexSetFlag, _ := cmd.Flags().GetString("index-set")
	keep, _ := cmd.Flags().GetInt("keep")
	beforeStr, _ := cmd.Flags().GetString("before")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	if keep == 0 && beforeStr == "" {
		return fmt.Errorf("one of --keep or --before is required")
	}
	if keep > 0 && beforeStr != "" {
		return fmt.Errorf("--keep and --before are mutually exclusive")
	}
	if keep < 0 {
		return fmt.Errorf("--keep must be positive")
	}

	var beforeTime time.Time
	if beforeStr != "" {
		var parseErr error
		beforeTime, parseErr = parseFlexibleTime(beforeStr)
		if parseErr != nil {
			return fmt.Errorf("invalid --before date: %w", parseErr)
		}
	}

	if indexSetFlag != "" {
		if err := validateFullIndexSetID(indexSetFlag); err != nil {
			return err
		}
	}

	hub, err := parseHubFlags(cmd, args)
	if err != nil {
		return err
	}

	getter, err := newHubGetter(ctx, hub)
	if err != nil {
		return fmt.Errorf("hub provider: %w", err)
	}
	if closer, ok := getter.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	lister, ok := getter.(provider.Provider)
	if !ok {
		return fmt.Errorf("hub provider does not support listing")
	}

	// Determine which index sets to process
	var indexSetIDs []string
	if indexSetFlag != "" {
		indexSetIDs = []string{indexSetFlag}
	} else {
		indexSetsPrefix := hubArtifactKey(hub, "index-sets/")
		indexSetIDs, err = discoverIndexSets(ctx, lister, indexSetsPrefix)
		if err != nil {
			return err
		}
	}

	var toRemove []gcRunCandidate
	for _, setID := range indexSetIDs {
		// Read latest pointer
		var latestRunID string
		latestKey := hubArtifactKey(hub, "index-sets", setID, "latest.json")
		if data, getErr := downloadBytes(ctx, getter, latestKey); getErr == nil {
			var latest struct {
				RunID string `json:"run_id"`
			}
			if json.Unmarshal(data, &latest) == nil {
				latestRunID = latest.RunID
			}
		}

		// Discover runs
		runsPrefix := hubArtifactKey(hub, "index-sets", setID, "runs/")
		runIDs, discoverErr := discoverRuns(ctx, lister, runsPrefix)
		if discoverErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: could not list runs for %s: %v\n", setID, discoverErr)
			continue
		}

		// Build candidates with completion times
		type runWithTime struct {
			runID       string
			isLatest    bool
			completedAt time.Time
			hasComplete bool
		}
		candidates := make([]runWithTime, 0, len(runIDs))
		for _, runID := range runIDs {
			r := runWithTime{runID: runID, isLatest: runID == latestRunID}
			completeKey := hubArtifactKey(hub, "index-sets", setID, "runs", runID, "complete.json")
			if data, getErr := downloadBytes(ctx, getter, completeKey); getErr == nil {
				r.hasComplete = true
				var complete struct {
					CompletedAt string `json:"completed_at"`
				}
				if json.Unmarshal(data, &complete) == nil && complete.CompletedAt != "" {
					if t, tErr := time.Parse(time.RFC3339, complete.CompletedAt); tErr == nil {
						r.completedAt = t
					}
				}
			}
			candidates = append(candidates, r)
		}

		// Sort by completion time (newest first) for --keep
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].completedAt.After(candidates[j].completedAt)
		})

		// Apply retention policy
		//
		// --keep N means "keep the N most recent committed runs total".
		// Latest is always kept (never removed) and counts toward N.
		// If latest is stale (not in the top N by time), it still occupies
		// one slot, so only N-1 non-latest runs are retained.
		if keep > 0 {
			// Reserve a slot for latest if it exists as a committed run
			hasLatest := false
			for _, r := range candidates {
				if r.isLatest && r.hasComplete {
					hasLatest = true
					break
				}
			}
			nonLatestSlots := keep
			if hasLatest {
				nonLatestSlots = keep - 1
				if nonLatestSlots < 0 {
					nonLatestSlots = 0
				}
			}

			kept := 0
			for _, r := range candidates {
				if !r.hasComplete {
					toRemove = append(toRemove, gcRunCandidate{
						IndexSetID: setID,
						RunID:      r.runID,
						IsLatest:   false,
					})
					continue
				}
				if r.isLatest {
					continue // always kept, slot already reserved
				}
				kept++
				if kept <= nonLatestSlots {
					continue
				}
				toRemove = append(toRemove, gcRunCandidate{
					IndexSetID:  setID,
					RunID:       r.runID,
					CompletedAt: r.completedAt.Format(time.RFC3339),
				})
			}
		} else {
			// --before mode
			for _, r := range candidates {
				if r.isLatest {
					continue
				}
				if !r.hasComplete {
					toRemove = append(toRemove, gcRunCandidate{
						IndexSetID: setID,
						RunID:      r.runID,
					})
					continue
				}
				if !r.completedAt.IsZero() && r.completedAt.Before(beforeTime) {
					toRemove = append(toRemove, gcRunCandidate{
						IndexSetID:  setID,
						RunID:       r.runID,
						CompletedAt: r.completedAt.Format(time.RFC3339),
					})
				}
			}
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	// Empty case
	if len(toRemove) == 0 {
		if jsonOutput {
			return enc.Encode(gcResult{DryRun: dryRun, Removed: []gcRunCandidate{}})
		}
		_, _ = fmt.Fprintln(os.Stderr, "Nothing to remove")
		return nil
	}

	// Dry-run case: report candidates and stop. No deletion.
	if dryRun {
		if jsonOutput {
			return enc.Encode(gcResult{DryRun: true, Removed: toRemove})
		}
		_, _ = fmt.Fprintf(os.Stderr, "Would remove %d run(s):\n", len(toRemove))
		for _, r := range toRemove {
			_, _ = fmt.Fprintf(os.Stderr, "  %s / %s\n", r.IndexSetID, r.RunID)
		}
		return nil
	}

	// Real run: resolve a deleter and execute deletions.
	var deleter provider.ObjectDeleter
	if d, dok := getter.(provider.ObjectDeleter); dok {
		deleter = d
	} else {
		putter, putErr := newHubProvider(ctx, hub)
		if putErr != nil {
			return fmt.Errorf("hub provider: %w", putErr)
		}
		if d, dok := putter.(provider.ObjectDeleter); dok {
			deleter = d
			if closer, cok := putter.(io.Closer); cok {
				defer func() { _ = closer.Close() }()
			}
		} else {
			return fmt.Errorf("hub provider does not support deletion")
		}
	}

	errors := 0
	successCount := 0
	for i, r := range toRemove {
		runPrefix := hubArtifactKey(hub, "index-sets", r.IndexSetID, "runs", r.RunID) + "/"
		keys, listErr := listAllKeys(ctx, lister, runPrefix)
		if listErr != nil {
			toRemove[i].Error = fmt.Sprintf("list artifacts: %v", listErr)
			errors++
			if !jsonOutput {
				_, _ = fmt.Fprintf(os.Stderr, "warning: could not list artifacts for %s/%s: %v\n", r.IndexSetID, r.RunID, listErr)
			}
			continue
		}

		var firstDelErr error
		for _, key := range keys {
			if delErr := deleter.DeleteObject(ctx, key); delErr != nil {
				if firstDelErr == nil {
					firstDelErr = delErr
				}
				if !jsonOutput {
					_, _ = fmt.Fprintf(os.Stderr, "warning: failed to delete %s: %v\n", key, delErr)
				}
			}
		}
		toRemove[i].Artifacts = len(keys)
		if firstDelErr != nil {
			toRemove[i].Error = fmt.Sprintf("delete: %v", firstDelErr)
			errors++
			continue
		}
		successCount++
		if !jsonOutput {
			_, _ = fmt.Fprintf(os.Stderr, "  removed %s / %s (%d artifacts)\n", r.IndexSetID, r.RunID, len(keys))
		}
	}

	if jsonOutput {
		return enc.Encode(gcResult{DryRun: false, Removed: toRemove, Errors: errors})
	}
	_, _ = fmt.Fprintf(os.Stderr, "GC complete: removed %d run(s)\n", successCount)
	if errors > 0 {
		_, _ = fmt.Fprintf(os.Stderr, "  with %d error(s)\n", errors)
	}
	return nil
}

// parseFlexibleTime parses time as RFC 3339 or YYYY-MM-DD.
func parseFlexibleTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("expected RFC 3339 or YYYY-MM-DD, got %q", s)
}
