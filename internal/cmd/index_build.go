package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	gfconfig "github.com/fulmenhq/gofulmen/config"
	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/manifest"
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

	// TODO: Checkpoint 2b - Run crawl and ingest records
	// TODO: Checkpoint 2c - Handle partial runs and finalize status

	_, _ = fmt.Fprintf(os.Stderr, "\nIndex build initiated (ingestion not yet implemented)\n")
	_, _ = fmt.Fprintf(os.Stderr, "run_id=%s\n", run.RunID)
	_, _ = fmt.Fprintf(os.Stderr, "index_set_id=%s\n", indexSet.IndexSetID)

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
