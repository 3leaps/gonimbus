package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/atlas"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

type atlasBuildProvider interface {
	provider.Provider
	provider.ObjectGetter
}

var newAtlasBuildProvider = func(ctx context.Context, src *uri.ObjectURI, opts providerdispatch.SourceOptions) (atlasBuildProvider, error) {
	p, err := providerdispatch.NewSource(ctx, src, opts)
	if err != nil {
		return nil, err
	}
	return providerdispatch.RequireCapability[atlasBuildProvider](p, "atlas build", src.Provider, "ObjectGetter")
}

var atlasCmd = &cobra.Command{
	Use:   "atlas",
	Short: "Build and inspect content-addressed atlas artifacts",
	Long: `Build and inspect content-addressed atlas artifacts.

Phase A supports local filesystem atlas artifacts built as an immutable
post-pass over one completed IndexSet run.`,
}

var atlasBuildCmd = &cobra.Command{
	Use:   "build",
	Short: "Build a local atlas artifact from an index run",
	Long: `Build a local atlas artifact from one completed index run.

The builder reads object bytes from the source provider, computes SHA256
content hashes, extracts configured typed dimensions, and writes per-shard
JSONL plus an atlas header. Hub export, lower-trust export transforms, drift,
and atlas-assisted reflow are deferred beyond Phase A.`,
	Args: cobra.NoArgs,
	RunE: runAtlasBuild,
}

var atlasStatsCmd = &cobra.Command{
	Use:   "stats <atlas-dir>",
	Short: "Show atlas artifact statistics",
	Args:  cobra.ExactArgs(1),
	RunE:  runAtlasStats,
}

func init() {
	rootCmd.AddCommand(atlasCmd)
	atlasCmd.AddCommand(atlasBuildCmd)
	atlasCmd.AddCommand(atlasStatsCmd)

	atlasBuildCmd.Flags().String("from-index", "", "Source index set ID (idx_<hash> or unique prefix)")
	atlasBuildCmd.Flags().String("run", "", "Source index run ID")
	atlasBuildCmd.Flags().String("recipe", "", "Atlas recipe file (YAML or JSON)")
	atlasBuildCmd.Flags().String("output", "", "Local output directory for the atlas artifact")
	atlasBuildCmd.Flags().StringP("region", "r", "", "AWS region override")
	atlasBuildCmd.Flags().StringP("profile", "p", "", "AWS profile")
	atlasBuildCmd.Flags().String("endpoint", "", "Custom S3 endpoint")
	atlasBuildCmd.Flags().String("gcp-project", "", "GCP project hint for GCS")
	atlasBuildCmd.Flags().Bool("json", false, "Output build summary as JSON")
	_ = atlasBuildCmd.MarkFlagRequired("from-index")
	_ = atlasBuildCmd.MarkFlagRequired("run")
	_ = atlasBuildCmd.MarkFlagRequired("recipe")
	_ = atlasBuildCmd.MarkFlagRequired("output")

	atlasStatsCmd.Flags().Bool("json", false, "Output as JSON")
}

func runAtlasBuild(cmd *cobra.Command, args []string) (err error) {
	ctx := cmd.Context()
	indexID, _ := cmd.Flags().GetString("from-index")
	runID, _ := cmd.Flags().GetString("run")
	recipePath, _ := cmd.Flags().GetString("recipe")
	outputDir, _ := cmd.Flags().GetString("output")
	region, _ := cmd.Flags().GetString("region")
	profile, _ := cmd.Flags().GetString("profile")
	endpoint, _ := cmd.Flags().GetString("endpoint")
	gcpProject, _ := cmd.Flags().GetString("gcp-project")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	reader, err := openIndexReader(ctx, "", strings.TrimSpace(indexID), "")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	if reader.Meta().Format != indexreader.FormatSQLiteV1 || reader.SQLiteDB() == nil {
		return fmt.Errorf("atlas build requires a sqlite-v1 index")
	}
	db := reader.SQLiteDB()
	sets, err := indexstore.ListIndexSets(ctx, db, "")
	if err != nil {
		return fmt.Errorf("list index sets: %w", err)
	}
	var indexSet *indexstore.IndexSet
	for i := range sets {
		if sets[i].IndexSetID == reader.Meta().IndexSetID {
			indexSet = &sets[i]
			break
		}
	}
	if indexSet == nil {
		return fmt.Errorf("resolved SQLite index set is missing")
	}

	run, err := indexstore.GetIndexRun(ctx, db, strings.TrimSpace(runID))
	if err != nil {
		return err
	}
	if run.IndexSetID != indexSet.IndexSetID {
		return fmt.Errorf("run %s belongs to %s, not %s", run.RunID, run.IndexSetID, indexSet.IndexSetID)
	}
	if run.Status != indexstore.RunStatusSuccess && run.Status != indexstore.RunStatusPartial {
		return fmt.Errorf("atlas build requires a completed successful or partial run, got %s", run.Status)
	}
	runs, err := indexstore.ListIndexRuns(ctx, db, indexSet.IndexSetID)
	if err != nil {
		return fmt.Errorf("list index runs: %w", err)
	}
	if len(runs) == 0 || runs[0].RunID != run.RunID {
		return fmt.Errorf("atlas Phase A requires the selected run to be the latest local index run; local indexes do not retain historical object snapshots")
	}

	recipe, err := atlas.LoadRecipeFile(recipePath)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid atlas recipe", err)
	}

	objects, err := indexstore.ListObjectsForRun(ctx, db, indexSet.IndexSetID, run.RunID)
	if err != nil {
		return err
	}
	sourceObjects := make([]atlas.SourceObject, 0, len(objects))
	for _, obj := range objects {
		sourceObjects = append(sourceObjects, atlas.SourceObject{
			RelKey:        obj.RelKey,
			SizeBytes:     obj.SizeBytes,
			ETag:          obj.ETag,
			LastSeenRunID: obj.LastSeenRunID,
			LastSeenAt:    obj.LastSeenAt,
		})
	}

	parsed, err := uri.ParseURI(indexSet.BaseURI)
	if err != nil {
		return fmt.Errorf("parse index base_uri: %w", err)
	}
	if region == "" {
		region = indexSet.Region
	}
	if endpoint == "" {
		endpoint = indexSet.Endpoint
	}
	prov, err := newAtlasBuildProvider(ctx, parsed, providerdispatch.SourceOptions{
		Command: "atlas build",
		S3: providerdispatch.S3Options{
			Region:         region,
			Profile:        profile,
			Endpoint:       endpoint,
			ForcePathStyle: endpoint != "",
		},
		GCS: providerdispatch.GCSOptions{
			Project: strings.TrimSpace(gcpProject),
		},
	})
	if err != nil {
		return fmt.Errorf("source provider: %w", err)
	}
	defer func() { _ = prov.Close() }()

	scopeDigest := readIndexScopeDigest(strings.TrimSpace(indexID))
	result, err := atlas.Build(ctx, atlas.BuildOptions{
		Source: atlas.SourceRun{
			IndexSetID:  indexSet.IndexSetID,
			RunID:       run.RunID,
			BaseURI:     indexSet.BaseURI,
			ScopeDigest: scopeDigest,
			Coverage:    recipe.Coverage,
			Objects:     sourceObjects,
		},
		Recipe:    *recipe,
		Reader:    prov,
		OutputDir: outputDir,
	})
	if err != nil {
		return err
	}

	if jsonOutput {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(result.Header)
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Atlas: %s\n", result.Header.AtlasID)
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Output: %s\n", result.OutputDir)
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Rows: %d\n", result.Header.Counts.RowsWritten)
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Diagnostics: %d\n", result.Header.Counts.Diagnostics)
	return nil
}

func runAtlasStats(cmd *cobra.Command, args []string) error {
	jsonOutput, _ := cmd.Flags().GetBool("json")
	stats, err := atlas.ComputeStats(args[0])
	if err != nil {
		return err
	}
	if jsonOutput {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(stats)
	}
	out := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(out, "Atlas: %s\n", stats.Header.AtlasID)
	_, _ = fmt.Fprintf(out, "Schema: %s\n", stats.Header.SchemaVersion)
	_, _ = fmt.Fprintf(out, "Source index: %s\n", stats.Header.SourceIndexSetID)
	_, _ = fmt.Fprintf(out, "Source run: %s\n", stats.Header.SourceRunID)
	_, _ = fmt.Fprintf(out, "Coverage: %s\n", stats.Header.Coverage)
	_, _ = fmt.Fprintf(out, "Rows: %d\n", stats.Tier1Keys)
	_, _ = fmt.Fprintf(out, "Distinct content: %d\n", stats.Tier2Content)
	_, _ = fmt.Fprintf(out, "Distinct shard content: %d\n", stats.Tier3Shards)
	_, _ = fmt.Fprintf(out, "Diagnostics: %d\n", stats.Diagnostics)
	return nil
}

func readIndexScopeDigest(indexID string) string {
	rootDir, err := indexRootDir()
	if err != nil {
		return ""
	}
	match, err := resolveIndexDirInRoot(rootDir, indexID)
	if err != nil {
		return ""
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(match.DBPath), "identity.json"))
	if err != nil {
		return ""
	}
	var doc struct {
		Build struct {
			ScopeHash string `json:"scope_hash"`
		} `json:"build"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return ""
	}
	return strings.TrimSpace(doc.Build.ScopeHash)
}
