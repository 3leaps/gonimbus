package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	gfconfig "github.com/fulmenhq/gofulmen/config"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/crawler"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
)

var indexBuildCmd = &cobra.Command{
	Use:   "build",
	Short: "Build an index from crawl results",
	Long: `Build a local index by crawling a cloud storage location.

The index build process:
1. Loads and validates the index manifest
2. Creates or finds an existing IndexSet based on identity + build params
3. Creates a new IndexRun to track this execution
4. Runs a crawl and ingests object/prefix records into the index
5. Handles partial runs (throttling, access denied) with structured events
6. Marks objects not seen in this run as soft-deleted

Example:
  gonimbus index build --job index.yaml
  gonimbus index build --job index.yaml --storage-provider wasabi --region us-east-1`,
	RunE: runIndexBuild,
}

// Index build flags.
var (
	indexBuildJobPath      string
	indexBuildDBPath       string
	indexBuildDryRun       bool
	indexBuildStorageProv  string
	indexBuildCloudProv    string
	indexBuildRegionKind   string
	indexBuildRegion       string
	indexBuildEndpointHost string
)

func init() {
	indexCmd.AddCommand(indexBuildCmd)

	// Required
	indexBuildCmd.Flags().StringVarP(&indexBuildJobPath, "job", "j", "", "Path to index manifest (required)")
	_ = indexBuildCmd.MarkFlagRequired("job")

	// Optional
	indexBuildCmd.Flags().StringVar(&indexBuildDBPath, "db", "", "Index database path or libsql DSN (default is XDG data dir)")
	indexBuildCmd.Flags().BoolVar(&indexBuildDryRun, "dry-run", false, "Validate manifest and show plan without building")

	// Provider identity overrides (ENTARCH: explicit, never inferred)
	indexBuildCmd.Flags().StringVar(&indexBuildStorageProv, "storage-provider", "", "Storage provider (aws_s3, cloudflare_r2, wasabi, gcs, azure_blob, generic_s3)")
	indexBuildCmd.Flags().StringVar(&indexBuildCloudProv, "cloud-provider", "", "Cloud provider (aws, gcp, azure, cloudflare, other)")
	indexBuildCmd.Flags().StringVar(&indexBuildRegionKind, "region-kind", "", "Region naming scheme (aws, gcp, azure)")
	indexBuildCmd.Flags().StringVar(&indexBuildRegion, "region", "", "Region name")
	indexBuildCmd.Flags().StringVar(&indexBuildEndpointHost, "endpoint-host", "", "Endpoint host (host[:port])")
}

func runIndexBuild(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	// Load and validate manifest
	m, err := manifest.LoadIndexManifest(indexBuildJobPath)
	if err != nil {
		return fmt.Errorf("load index manifest: %w", err)
	}

	// Validate base_uri ends with /
	if !strings.HasSuffix(m.Connection.BaseURI, "/") {
		return fmt.Errorf("connection.base_uri must end with '/': %s", m.Connection.BaseURI)
	}

	// Validate endpoint is a valid URL if set
	if m.Connection.Endpoint != "" {
		if err := validateEndpointURL(m.Connection.Endpoint); err != nil {
			return fmt.Errorf("connection.endpoint: %w", err)
		}
	}

	// Build effective identity (manifest + CLI overrides)
	identity := buildEffectiveIdentity(m)

	// Validate explicit identity requirements (ENTARCH: no inference)
	if err := validateIdentity(m, identity); err != nil {
		return err
	}

	// Disallow build-time metadata filters in v0.1.3 (apply filters at query time)
	if err := validateNoMetadataFilters(m); err != nil {
		return err
	}

	// Show plan in dry-run mode
	if indexBuildDryRun {
		return showIndexBuildPlan(cmd, m, identity)
	}

	// Open index database
	dbPath, err := resolveIndexDBPath(indexBuildDBPath)
	if err != nil {
		return err
	}

	cfg := indexstore.Config{}
	if strings.HasPrefix(dbPath, "libsql://") || strings.HasPrefix(dbPath, "https://") {
		cfg.URL = dbPath
	} else {
		cfg.Path = dbPath
	}

	db, err := indexstore.Open(ctx, cfg)
	if err != nil {
		return fmt.Errorf("open index database: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Ensure schema is migrated
	if err := indexstore.Migrate(ctx, db); err != nil {
		return fmt.Errorf("migrate index schema: %w", err)
	}

	// Build IndexSetParams
	params := buildIndexSetParams(m, identity)

	// Find or create IndexSet
	indexSet, created, err := indexstore.FindOrCreateIndexSet(ctx, db, params)
	if err != nil {
		return fmt.Errorf("find or create index set: %w", err)
	}

	if created {
		_, _ = fmt.Fprintf(os.Stderr, "Created new IndexSet: %s\n", indexSet.IndexSetID)
	} else {
		_, _ = fmt.Fprintf(os.Stderr, "Using existing IndexSet: %s\n", indexSet.IndexSetID)
	}

	// Create IndexRun
	sourceType := m.Build.Source
	if sourceType == "" {
		sourceType = "crawl"
	}

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, sourceType)
	if err != nil {
		return fmt.Errorf("create index run: %w", err)
	}

	_, _ = fmt.Fprintf(os.Stderr, "Started IndexRun: %s\n", run.RunID)
	_, _ = fmt.Fprintf(os.Stderr, "  base_uri: %s\n", indexSet.BaseURI)
	_, _ = fmt.Fprintf(os.Stderr, "  source_type: %s\n", sourceType)

	// Run crawl and ingest records (streaming - memory-bounded)
	result, crawlErr := runCrawlForIndex(ctx, m, db, indexSet.IndexSetID, run)
	if crawlErr != nil {
		// ENTARCH: Distinguish context cancellation from fatal errors.
		// - Cancellation (Ctrl-C, timeout): record as "partial" (data was flushed, just incomplete)
		// - Other errors: record as "failed" (something actually broke)
		if errors.Is(crawlErr, context.Canceled) || errors.Is(crawlErr, context.DeadlineExceeded) {
			// Context cancellation - mark as partial, skip soft-delete
			_ = indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusPartial, nil)
			_, _ = fmt.Fprintf(os.Stderr, "\nIndex build interrupted\n")
			_, _ = fmt.Fprintf(os.Stderr, "  run_id: %s\n", run.RunID)
			_, _ = fmt.Fprintf(os.Stderr, "  status: %s\n", indexstore.RunStatusPartial)
			_, _ = fmt.Fprintf(os.Stderr, "  objects_ingested: %d\n", result.ObjectsIngested)
			_, _ = fmt.Fprintf(os.Stderr, "  prefixes_ingested: %d\n", result.PrefixesIngested)
			_, _ = fmt.Fprintf(os.Stderr, "  note: run cancelled, data flushed but incomplete\n")
			return fmt.Errorf("index build cancelled: %w", crawlErr)
		}
		// Fatal error - mark as failed
		_ = indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusFailed, nil)
		return fmt.Errorf("index build failed: %w", crawlErr)
	}

	// Finalize run status based on collected errors (soft-delete only on success)
	if err := finalizeIndexRun(ctx, db, indexSet.IndexSetID, run, result); err != nil {
		return fmt.Errorf("finalize index run: %w", err)
	}

	// Report results
	_, _ = fmt.Fprintf(os.Stderr, "\nIndex build completed\n")
	_, _ = fmt.Fprintf(os.Stderr, "  run_id: %s\n", run.RunID)
	_, _ = fmt.Fprintf(os.Stderr, "  index_set_id: %s\n", indexSet.IndexSetID)
	_, _ = fmt.Fprintf(os.Stderr, "  status: %s\n", result.FinalStatus)
	_, _ = fmt.Fprintf(os.Stderr, "  objects_ingested: %d\n", result.ObjectsIngested)
	_, _ = fmt.Fprintf(os.Stderr, "  prefixes_ingested: %d\n", result.PrefixesIngested)
	if result.ObjectsDeleted > 0 {
		_, _ = fmt.Fprintf(os.Stderr, "  objects_soft_deleted: %d\n", result.ObjectsDeleted)
	}
	if result.FinalStatus == indexstore.RunStatusPartial {
		_, _ = fmt.Fprintf(os.Stderr, "  note: partial run (some errors encountered)\n")
	}

	return nil
}

// validateEndpointURL validates that the endpoint is a parseable URL.
func validateEndpointURL(endpoint string) error {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	// Warn if bare hostname (url.Parse won't populate Host for "example.com")
	if parsed.Scheme == "" {
		return fmt.Errorf("missing scheme (expected http:// or https://): %s", endpoint)
	}

	if parsed.Host == "" {
		return fmt.Errorf("missing host: %s", endpoint)
	}

	return nil
}

// effectiveIdentity holds the merged identity from manifest + CLI overrides.
type effectiveIdentity struct {
	StorageProvider string
	CloudProvider   string
	RegionKind      string
	Region          string
	EndpointHost    string
}

// buildEffectiveIdentity merges manifest identity with CLI flag overrides.
func buildEffectiveIdentity(m *manifest.IndexManifest) effectiveIdentity {
	ident := effectiveIdentity{}

	// Start with manifest values
	if m.Identity != nil {
		ident.StorageProvider = m.Identity.StorageProvider
		ident.CloudProvider = m.Identity.CloudProvider
		ident.RegionKind = m.Identity.RegionKind
		ident.Region = m.Identity.Region
		ident.EndpointHost = m.Identity.EndpointHost
	}

	// Apply CLI overrides
	if indexBuildStorageProv != "" {
		ident.StorageProvider = indexBuildStorageProv
	}
	if indexBuildCloudProv != "" {
		ident.CloudProvider = indexBuildCloudProv
	}
	if indexBuildRegionKind != "" {
		ident.RegionKind = indexBuildRegionKind
	}
	if indexBuildRegion != "" {
		ident.Region = indexBuildRegion
	}
	if indexBuildEndpointHost != "" {
		ident.EndpointHost = indexBuildEndpointHost
	}

	// ENTARCH: No fallback to connection.region - identity must be explicit

	return ident
}

// buildIndexSetParams constructs IndexSetParams from manifest and effective identity.
func buildIndexSetParams(m *manifest.IndexManifest, ident effectiveIdentity) indexstore.IndexSetParams {
	// Build params for hash
	bp := indexstore.BuildParams{
		SourceType:      m.Build.Source,
		SchemaVersion:   indexstore.SchemaVersion,
		GonimbusVersion: versionInfo.Version,
	}

	// Match params
	if m.Build.Match != nil {
		bp.Includes = m.Build.Match.Includes
		bp.Excludes = m.Build.Match.Excludes
		bp.IncludeHidden = m.Build.Match.IncludeHidden
		// TODO: compute FiltersHash from m.Build.Match.Filters
	}

	// Path date extraction
	if m.PathDate != nil {
		bp.PathDateExtraction = &indexstore.PathDateExtraction{
			Method:       m.PathDate.Method,
			Regex:        m.PathDate.Regex,
			SegmentIndex: m.PathDate.SegmentIndex,
		}
	}

	return indexstore.IndexSetParams{
		BaseURI:         m.Connection.BaseURI,
		Provider:        m.Connection.Provider,
		StorageProvider: ident.StorageProvider,
		CloudProvider:   ident.CloudProvider,
		RegionKind:      ident.RegionKind,
		Region:          ident.Region,
		Endpoint:        m.Connection.Endpoint,
		EndpointHost:    ident.EndpointHost, // Explicit, not derived
		BuildParams:     bp,
	}
}

// showIndexBuildPlan displays the build plan without executing.
func showIndexBuildPlan(cmd *cobra.Command, m *manifest.IndexManifest, ident effectiveIdentity) error {
	_, _ = fmt.Fprintln(os.Stdout, "Index Build Plan (dry-run)")
	_, _ = fmt.Fprintln(os.Stdout, "==========================")
	_, _ = fmt.Fprintln(os.Stdout)

	_, _ = fmt.Fprintln(os.Stdout, "Connection:")
	_, _ = fmt.Fprintf(os.Stdout, "  provider: %s\n", m.Connection.Provider)
	_, _ = fmt.Fprintf(os.Stdout, "  bucket: %s\n", m.Connection.Bucket)
	_, _ = fmt.Fprintf(os.Stdout, "  base_uri: %s\n", m.Connection.BaseURI)
	if m.Connection.Region != "" {
		_, _ = fmt.Fprintf(os.Stdout, "  region: %s\n", m.Connection.Region)
	}
	if m.Connection.Endpoint != "" {
		_, _ = fmt.Fprintf(os.Stdout, "  endpoint: %s\n", m.Connection.Endpoint)
	}
	if m.Connection.Profile != "" {
		_, _ = fmt.Fprintf(os.Stdout, "  profile: %s\n", m.Connection.Profile)
	}

	_, _ = fmt.Fprintln(os.Stdout)
	_, _ = fmt.Fprintln(os.Stdout, "Identity (effective):")
	_, _ = fmt.Fprintf(os.Stdout, "  storage_provider: %s\n", valueOrDefault(ident.StorageProvider, "(not set)"))
	_, _ = fmt.Fprintf(os.Stdout, "  cloud_provider: %s\n", valueOrDefault(ident.CloudProvider, "(not set)"))
	_, _ = fmt.Fprintf(os.Stdout, "  region_kind: %s\n", valueOrDefault(ident.RegionKind, "(not set)"))
	_, _ = fmt.Fprintf(os.Stdout, "  region: %s\n", valueOrDefault(ident.Region, "(not set)"))
	_, _ = fmt.Fprintf(os.Stdout, "  endpoint_host: %s\n", valueOrDefault(ident.EndpointHost, "(not set)"))

	_, _ = fmt.Fprintln(os.Stdout)
	_, _ = fmt.Fprintln(os.Stdout, "Build:")
	_, _ = fmt.Fprintf(os.Stdout, "  source: %s\n", m.Build.Source)
	if m.Build.Match != nil {
		_, _ = fmt.Fprintf(os.Stdout, "  includes: %v\n", m.Build.Match.Includes)
		if len(m.Build.Match.Excludes) > 0 {
			_, _ = fmt.Fprintf(os.Stdout, "  excludes: %v\n", m.Build.Match.Excludes)
		}
		_, _ = fmt.Fprintf(os.Stdout, "  include_hidden: %v\n", m.Build.Match.IncludeHidden)
	}
	if m.Build.Crawl != nil {
		_, _ = fmt.Fprintf(os.Stdout, "  concurrency: %d\n", m.Build.Crawl.Concurrency)
		_, _ = fmt.Fprintf(os.Stdout, "  rate_limit: %.1f\n", m.Build.Crawl.RateLimit)
	}

	if m.PathDate != nil {
		_, _ = fmt.Fprintln(os.Stdout)
		_, _ = fmt.Fprintln(os.Stdout, "Path Date Extraction:")
		_, _ = fmt.Fprintf(os.Stdout, "  method: %s\n", m.PathDate.Method)
		if m.PathDate.Regex != "" {
			_, _ = fmt.Fprintf(os.Stdout, "  regex: %s\n", m.PathDate.Regex)
		}
		if m.PathDate.SegmentIndex > 0 {
			_, _ = fmt.Fprintf(os.Stdout, "  segment_index: %d\n", m.PathDate.SegmentIndex)
		}
	}

	return nil
}

// resolveIndexDBPath resolves the index database path.
func resolveIndexDBPath(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}

	identity := GetAppIdentity()
	if identity == nil || strings.TrimSpace(identity.ConfigName) == "" {
		return "", fmt.Errorf("app identity is not available to derive default index path")
	}

	dataDir := gfconfig.GetAppDataDir(identity.ConfigName)
	return filepath.Join(dataDir, "indexes", "gonimbus-index.db"), nil
}

// valueOrDefault returns the value or a default if empty.
func valueOrDefault(value, def string) string {
	if value == "" {
		return def
	}
	return value
}

// validateIdentity enforces explicit identity requirements.
//
// ENTARCH: Identity must be explicit, never inferred. This means:
// - If endpoint is set, endpoint_host must also be explicitly set
// - Region must be explicit when region_kind is set
func validateIdentity(m *manifest.IndexManifest, ident effectiveIdentity) error {
	// If endpoint is configured, endpoint_host must be explicit
	if m.Connection.Endpoint != "" && ident.EndpointHost == "" {
		return fmt.Errorf("identity.endpoint_host (or --endpoint-host) is required when connection.endpoint is set")
	}

	// If region_kind is set, region must be explicit
	if ident.RegionKind != "" && ident.Region == "" {
		return fmt.Errorf("identity.region (or --region) is required when identity.region_kind is set")
	}

	return nil
}

// validateNoMetadataFilters rejects build-time metadata filters in v0.1.3.
//
// Policy: Index everything, filter at query time. This keeps IndexSet identity
// stable and avoids the complexity of build-time filter semantics.
func validateNoMetadataFilters(m *manifest.IndexManifest) error {
	if m.Build == nil || m.Build.Match == nil || m.Build.Match.Filters == nil {
		return nil
	}

	filters := m.Build.Match.Filters

	// Check if any metadata filters are set
	if filters.Size != nil && (filters.Size.Min != "" || filters.Size.Max != "") {
		return fmt.Errorf("build-time size filters not supported in v0.1.3; use 'gonimbus index query' with --min-size/--max-size")
	}
	if filters.Modified != nil && (filters.Modified.After != "" || filters.Modified.Before != "") {
		return fmt.Errorf("build-time date filters not supported in v0.1.3; use 'gonimbus index query' with --after/--before")
	}
	if filters.KeyRegex != "" {
		return fmt.Errorf("build-time key_regex filter not supported in v0.1.3; use 'gonimbus index query' with --key-regex")
	}

	return nil
}

// indexBuildResult holds the outcome of crawl-to-index ingestion.
type indexBuildResult struct {
	FinalStatus      indexstore.RunStatus
	ObjectsIngested  int64
	PrefixesIngested int64
	ObjectsDeleted   int64
}

// runCrawlForIndex executes the crawl with streaming ingestion.
//
// Records are ingested in batches as they arrive, keeping memory usage
// bounded regardless of bucket size. This is critical for 1M+ object buckets.
func runCrawlForIndex(
	ctx context.Context,
	m *manifest.IndexManifest,
	db *sql.DB,
	indexSetID string,
	run *indexstore.IndexRun,
) (*indexBuildResult, error) {
	// Create provider
	cfg := s3.Config{
		Bucket:         m.Connection.Bucket,
		Region:         m.Connection.Region,
		Endpoint:       m.Connection.Endpoint,
		Profile:        m.Connection.Profile,
		ForcePathStyle: m.Connection.Endpoint != "",
	}
	prov, err := s3.New(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create provider: %w", err)
	}
	defer func() { _ = prov.Close() }()

	// Create matcher from build config with nil-guard
	matchCfg := match.Config{
		IncludeHidden: false,
	}
	if m.Build != nil && m.Build.Match != nil {
		matchCfg.Includes = m.Build.Match.Includes
		matchCfg.Excludes = m.Build.Match.Excludes
		matchCfg.IncludeHidden = m.Build.Match.IncludeHidden
	}
	// Default to include everything if no patterns specified
	if len(matchCfg.Includes) == 0 {
		matchCfg.Includes = []string{"**"}
	}

	matcher, err := match.New(matchCfg)
	if err != nil {
		return nil, fmt.Errorf("create matcher: %w", err)
	}

	// Create streaming ingest writer
	writer := newIndexIngestWriter(db, indexSetID, run, m.Connection.BaseURI, indexIngestWriterConfig{
		ObjectBatchSize: DefaultObjectBatchSize,
		PrefixBatchSize: DefaultPrefixBatchSize,
	})

	// Create crawler config with nil-guard
	crawlCfg := crawler.Config{
		Concurrency:   4,    // default
		RateLimit:     0,    // unlimited
		ProgressEvery: 1000, // default
	}
	if m.Build != nil && m.Build.Crawl != nil {
		if m.Build.Crawl.Concurrency > 0 {
			crawlCfg.Concurrency = m.Build.Crawl.Concurrency
		}
		crawlCfg.RateLimit = m.Build.Crawl.RateLimit
		if m.Build.Crawl.ProgressEvery > 0 {
			crawlCfg.ProgressEvery = m.Build.Crawl.ProgressEvery
		}
	}

	// Run crawler with streaming writer
	jobID := uuid.New().String()
	c := crawler.New(prov, matcher, writer, jobID, crawlCfg)
	_, crawlErr := c.Run(ctx)

	// Always close writer to flush remaining batches, even on error.
	// This persists what we've seen so far.
	closeErr := writer.Close()

	// Get result from writer state
	result := writer.Result()

	// ENTARCH: Context cancellation (Ctrl-C, timeout) must NOT look like success.
	// If context was cancelled, mark as partial and return the context error.
	// This prevents soft-delete from running on incomplete traversals.
	if ctx.Err() != nil {
		result.FinalStatus = indexstore.RunStatusPartial
		return result, ctx.Err()
	}

	// Handle fatal crawl errors (not context cancellation)
	if crawlErr != nil {
		return result, fmt.Errorf("crawl failed: %w", crawlErr)
	}

	// Handle flush errors
	if closeErr != nil {
		return result, fmt.Errorf("flush final batch: %w", closeErr)
	}

	return result, nil
}

// finalizeIndexRun updates run status and handles soft-deletes.
//
// IMPORTANT (ENTARCH): Soft-delete is ONLY performed for successful runs.
// For partial runs, missing objects may be due to incomplete traversal
// (throttling, access denied), not actual deletions.
func finalizeIndexRun(
	ctx context.Context,
	db *sql.DB,
	indexSetID string,
	run *indexstore.IndexRun,
	result *indexBuildResult,
) error {
	// Update run status
	if err := indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, result.FinalStatus, nil); err != nil {
		return fmt.Errorf("update run status: %w", err)
	}

	// ENTARCH: Only soft-delete for successful runs
	// Per softdelete.go policy: partial runs may have missing objects due to
	// incomplete traversal, not actual deletions.
	if result.FinalStatus == indexstore.RunStatusSuccess {
		deleted, err := indexstore.MarkObjectsDeletedNotSeenInRun(ctx, db, indexSetID, run.RunID, run.StartedAt)
		if err != nil {
			return fmt.Errorf("mark deleted objects: %w", err)
		}
		result.ObjectsDeleted = deleted
	}

	return nil
}
