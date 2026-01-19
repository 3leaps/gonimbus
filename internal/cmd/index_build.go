package cmd

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	gfconfig "github.com/fulmenhq/gofulmen/config"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/crawler"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/scope"
)

var indexBuildCmd = &cobra.Command{
	Use:   "build",
	Short: "Build an index from crawl results",
	Long: `Build a local index by crawling a cloud storage location.

By default, index builds use a per-index directory under the app data dir:
  indexes/idx_<hashprefix>/index.db

An identity file is written alongside the DB for interpretability:
  indexes/idx_<hashprefix>/identity.json

The index build process:
1. Loads and validates the index manifest
2. Creates or finds an existing IndexSet based on identity + build params
3. Creates a new IndexRun to track this execution
4. Runs a crawl and ingests object/prefix records into the index
5. Handles partial runs (throttling, access denied) with structured events
6. Marks objects not seen in this run as soft-deleted

Tip: use 'gonimbus index list' to see IDENTITY status, and 'gonimbus index doctor' to explain a specific idx_<hashprefix> directory.

Example:
  gonimbus index build --job index.yaml
  gonimbus index build --job index.yaml --storage-provider wasabi --region us-east-1`,
	RunE: runIndexBuild,
}

// Index build flags.
var (
	indexBuildJobPath         string
	indexBuildDBPath          string
	indexBuildDryRun          bool
	indexBuildBackground      bool
	indexBuildManagedJobID    string
	indexBuildStorageProv     string
	indexBuildCloudProv       string
	indexBuildRegionKind      string
	indexBuildRegion          string
	indexBuildEndpointHost    string
	indexBuildScopeWarnPrefix int
	indexBuildScopeMaxPrefix  int
	indexBuildName            string
)

func init() {
	indexCmd.AddCommand(indexBuildCmd)

	// Required
	indexBuildCmd.Flags().StringVarP(&indexBuildJobPath, "job", "j", "", "Path to index manifest (required)")
	_ = indexBuildCmd.MarkFlagRequired("job")

	// Optional
	indexBuildCmd.Flags().StringVar(&indexBuildDBPath, "db", "", "Index database path or libsql DSN (default is per-index under data dir)")
	indexBuildCmd.Flags().BoolVar(&indexBuildDryRun, "dry-run", false, "Validate manifest and show plan without building")
	indexBuildCmd.Flags().BoolVar(&indexBuildBackground, "background", false, "Run index build as a managed background job")
	indexBuildCmd.Flags().StringVar(&indexBuildManagedJobID, "_managed-job-id", "", "(internal) Managed job id")
	_ = indexBuildCmd.Flags().MarkHidden("_managed-job-id")
	indexBuildCmd.Flags().StringVar(&indexBuildName, "name", "", "Optional job name (recorded in job registry)")
	indexBuildCmd.Flags().IntVar(&indexBuildScopeWarnPrefix, "scope-warn-prefixes", 10000, "Warn if build.scope expands to more than N prefixes (0 disables)")
	indexBuildCmd.Flags().IntVar(&indexBuildScopeMaxPrefix, "scope-max-prefixes", 50000, "Fail build if build.scope expands beyond N prefixes (0 disables)")

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

	// Managed mode: translate SIGTERM into context cancellation.
	if strings.TrimSpace(indexBuildManagedJobID) != "" {
		managedCtx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
		defer cancel()
		ctx = managedCtx
	}

	// Background mode: start a managed child process and return.
	if indexBuildBackground {
		if indexBuildDryRun {
			return fmt.Errorf("--background is not compatible with --dry-run")
		}
		execRoot, err := indexJobsRootDir()
		if err != nil {
			return err
		}
		exec := jobregistry.NewExecutor(execRoot)
		job, err := exec.StartIndexBuildBackground(indexBuildJobPath, strings.TrimSpace(indexBuildName))
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(os.Stdout, "%s\n", job.JobID)
		return nil
	}

	var store *jobregistry.Store
	var job *jobregistry.JobRecord

	if !indexBuildDryRun {
		jobsRoot, err := indexJobsRootDir()
		if err != nil {
			return err
		}
		store = jobregistry.NewStore(jobsRoot)

		absManifestPath, err := filepath.Abs(indexBuildJobPath)
		if err != nil {
			return fmt.Errorf("resolve manifest path: %w", err)
		}

		now := time.Now().UTC()
		jobID := uuid.New().String()
		if strings.TrimSpace(indexBuildManagedJobID) != "" {
			jobID = strings.TrimSpace(indexBuildManagedJobID)
		}
		job = &jobregistry.JobRecord{
			JobID:        jobID,
			Name:         strings.TrimSpace(indexBuildName),
			State:        jobregistry.JobStateRunning,
			ManifestPath: absManifestPath,
			StdoutPath:   filepath.Join(store.JobDir(jobID), "stdout.log"),
			StderrPath:   filepath.Join(store.JobDir(jobID), "stderr.log"),
			CreatedAt:    now,
			StartedAt:    &now,
		}

		// In managed mode, stdout/stderr are redirected by the parent.
		// In foreground mode, we don't capture logs yet, but we still expose
		// the expected paths for consistency.
		if strings.TrimSpace(indexBuildManagedJobID) == "" {
			_ = os.MkdirAll(store.JobDir(jobID), 0755)
			if f, err := os.OpenFile(job.StdoutPath, os.O_CREATE, 0644); err == nil {
				_ = f.Close()
			}
			if f, err := os.OpenFile(job.StderrPath, os.O_CREATE, 0644); err == nil {
				_ = f.Close()
			}
		}

		if err := store.Write(job); err != nil {
			return fmt.Errorf("write job record: %w", err)
		}
	}

	// Load and validate manifest
	m, err := manifest.LoadIndexManifest(indexBuildJobPath)
	if err != nil {
		if store != nil && job != nil {
			job.State = jobregistry.JobStateFailed
			ended := time.Now().UTC()
			job.EndedAt = &ended
			_ = store.Write(job)
		}
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
	if store != nil && job != nil {
		job.Identity = &jobregistry.EffectiveIdentity{
			StorageProvider: identity.StorageProvider,
			CloudProvider:   identity.CloudProvider,
			RegionKind:      identity.RegionKind,
			Region:          identity.Region,
			EndpointHost:    identity.EndpointHost,
		}
		_ = store.Write(job)
	}

	// Validate explicit identity requirements (ENTARCH: no inference)
	if err := validateIdentity(m, identity); err != nil {
		return err
	}

	buildFilters, err := computeIndexBuildFilters(m)
	if err != nil {
		return err
	}

	scopeHash, err := computeScopeHash(m)
	if err != nil {
		return err
	}

	// Show plan in dry-run mode
	if indexBuildDryRun {
		return showIndexBuildPlan(ctx, cmd, m, identity, buildFilters)
	}

	// Build IndexSetParams
	params := buildIndexSetParams(m, identity, buildFilters.FiltersHash, scopeHash)

	identityResult, err := indexstore.ComputeIndexSetID(params)
	if err != nil {
		return fmt.Errorf("compute index identity: %w", err)
	}

	// Open index database
	resolvedDB, err := resolveIndexDBPath(indexBuildDBPath, identityResult)
	if err != nil {
		if store != nil && job != nil {
			job.State = jobregistry.JobStateFailed
			ended := time.Now().UTC()
			job.EndedAt = &ended
			_ = store.Write(job)
		}
		return err

	}

	if store != nil && job != nil {
		job.IndexDir = resolvedDB.IdentityDir
		job.IndexSetID = identityResult.IndexSetID
		_ = store.Write(job)
	}

	cfg := indexstore.Config{}
	if strings.HasPrefix(resolvedDB.Path, "libsql://") || strings.HasPrefix(resolvedDB.Path, "https://") {
		cfg.URL = resolvedDB.Path
	} else {
		cfg.Path = resolvedDB.Path
	}

	if resolvedDB.WriteIdentity {
		if err := writeIndexIdentityFile(resolvedDB.IdentityDir, identityResult); err != nil {
			return err
		}
		if err := writeIndexManifestFile(resolvedDB.IdentityDir, m); err != nil {
			return err
		}
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
		if store != nil && job != nil {
			job.State = jobregistry.JobStateFailed
			ended := time.Now().UTC()
			job.EndedAt = &ended
			_ = store.Write(job)
		}
		return fmt.Errorf("create index run: %w", err)
	}

	var stopHeartbeat func()
	if store != nil && job != nil {
		job.RunID = run.RunID
		job.PID = os.Getpid()
		_ = store.Write(job)
		if strings.TrimSpace(indexBuildManagedJobID) != "" {
			stopHeartbeat = startManagedHeartbeat(ctx, store, job)
		}
	}
	defer func() {
		if stopHeartbeat != nil {
			stopHeartbeat()
		}
	}()

	_, _ = fmt.Fprintf(os.Stderr, "Started IndexRun: %s\n", run.RunID)
	_, _ = fmt.Fprintf(os.Stderr, "  base_uri: %s\n", indexSet.BaseURI)
	_, _ = fmt.Fprintf(os.Stderr, "  source_type: %s\n", sourceType)

	// Run crawl and ingest records (streaming - memory-bounded)
	result, crawlErr := runCrawlForIndex(ctx, m, db, indexSet.IndexSetID, run, buildFilters.Filter)
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

			if store != nil && job != nil {
				job.State = jobregistry.JobStatePartial
				ended := time.Now().UTC()
				job.EndedAt = &ended
				_ = store.Write(job)
			}
			return fmt.Errorf("index build cancelled: %w", crawlErr)
		}
		// Fatal error - mark as failed
		_ = indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusFailed, nil)

		if store != nil && job != nil {
			job.State = jobregistry.JobStateFailed
			ended := time.Now().UTC()
			job.EndedAt = &ended
			_ = store.Write(job)
		}
		return fmt.Errorf("index build failed: %w", crawlErr)
	}

	// Finalize run status based on collected errors (soft-delete only on success)
	allowSoftDelete := m.Build == nil || m.Build.Scope == nil
	if err := finalizeIndexRun(ctx, db, indexSet.IndexSetID, run, result, allowSoftDelete); err != nil {
		if store != nil && job != nil {
			job.State = jobregistry.JobStateFailed
			ended := time.Now().UTC()
			job.EndedAt = &ended
			_ = store.Write(job)
		}
		return fmt.Errorf("finalize index run: %w", err)
	}

	// Report results
	_, _ = fmt.Fprintf(os.Stderr, "\nIndex build completed\n")
	_, _ = fmt.Fprintf(os.Stderr, "  run_id: %s\n", run.RunID)
	_, _ = fmt.Fprintf(os.Stderr, "  index_set_id: %s\n", indexSet.IndexSetID)
	_, _ = fmt.Fprintf(os.Stderr, "  status: %s\n", result.FinalStatus)
	_, _ = fmt.Fprintf(os.Stderr, "  objects_ingested: %d\n", result.ObjectsIngested)
	_, _ = fmt.Fprintf(os.Stderr, "  prefixes_ingested: %d\n", result.PrefixesIngested)

	// Persist job completion.
	if store != nil && job != nil {
		switch result.FinalStatus {
		case indexstore.RunStatusSuccess:
			job.State = jobregistry.JobStateSuccess
		case indexstore.RunStatusPartial:
			job.State = jobregistry.JobStatePartial
		default:
			job.State = jobregistry.JobStateFailed
		}
		ended := time.Now().UTC()
		job.EndedAt = &ended
		_ = store.Write(job)
	}
	if result.ObjectsDeleted > 0 {
		_, _ = fmt.Fprintf(os.Stderr, "  objects_soft_deleted: %d\n", result.ObjectsDeleted)
	}
	if result.FinalStatus == indexstore.RunStatusSuccess && !allowSoftDelete {
		_, _ = fmt.Fprintf(os.Stderr, "  note: soft-delete skipped for scoped build (not full coverage by default)\n")
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

func parseS3BaseURI(baseURI string) (bucket string, prefix string, err error) {
	parsed, err := url.Parse(baseURI)
	if err != nil {
		return "", "", err
	}
	if parsed.Scheme != "s3" {
		return "", "", fmt.Errorf("expected s3:// URI, got scheme %q", parsed.Scheme)
	}
	bucket = parsed.Host
	prefix = strings.TrimPrefix(parsed.Path, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		return "", "", fmt.Errorf("base uri path must end with '/': %s", baseURI)
	}
	return bucket, prefix, nil
}

type indexBuildFilters struct {
	Filter      *match.CompositeFilter
	FiltersHash string
}

func computeScopeHash(m *manifest.IndexManifest) (string, error) {
	if m == nil || m.Build == nil || m.Build.Scope == nil {
		return "", nil
	}

	hash, err := scope.HashConfig(m.Build.Scope)
	if err != nil {
		return "", fmt.Errorf("build.scope: %w", err)
	}
	return hash, nil
}

func scopePlanWarning(prefixes []string, warnLimit int) string {
	if warnLimit <= 0 {
		return ""
	}
	if len(prefixes) <= warnLimit {
		return ""
	}
	return fmt.Sprintf("build.scope expands to %d prefixes (warn %d)", len(prefixes), warnLimit)
}

func validateScopePlan(prefixes []string, maxLimit int) error {
	if maxLimit <= 0 {
		return nil
	}
	if len(prefixes) <= maxLimit {
		return nil
	}
	return fmt.Errorf("build.scope expands to %d prefixes (max %d); rerun with --scope-max-prefixes to override", len(prefixes), maxLimit)
}

func computeIndexBuildFilters(m *manifest.IndexManifest) (*indexBuildFilters, error) {
	if m == nil || m.Build == nil || m.Build.Match == nil || m.Build.Match.Filters == nil {
		return &indexBuildFilters{}, nil
	}

	mf := m.Build.Match.Filters
	cfg := &match.FilterConfig{}

	// Size
	if mf.Size != nil {
		cfg.Size = &match.SizeFilterConfig{Min: strings.TrimSpace(mf.Size.Min), Max: strings.TrimSpace(mf.Size.Max)}
	}

	// Modified
	if mf.Modified != nil {
		cfg.Modified = &match.DateFilterConfig{After: strings.TrimSpace(mf.Modified.After), Before: strings.TrimSpace(mf.Modified.Before)}
	}

	// Regex
	cfg.KeyRegex = strings.TrimSpace(mf.KeyRegex)

	filter, err := match.NewFilterFromConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build.match.filters: %w", err)
	}
	if filter == nil {
		return &indexBuildFilters{}, nil
	}

	hash, err := computeFiltersHashFromConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build.match.filters: %w", err)
	}

	return &indexBuildFilters{Filter: filter, FiltersHash: hash}, nil
}

type filtersHashPayload struct {
	SizeMinBytes   *int64     `json:"size_min_bytes,omitempty"`
	SizeMaxBytes   *int64     `json:"size_max_bytes,omitempty"`
	ModifiedAfter  *time.Time `json:"modified_after,omitempty"`
	ModifiedBefore *time.Time `json:"modified_before,omitempty"`
	KeyRegex       string     `json:"key_regex,omitempty"`
}

func computeFiltersHashFromConfig(cfg *match.FilterConfig) (string, error) {
	if cfg == nil {
		return "", nil
	}

	payload := filtersHashPayload{}

	if cfg.Size != nil {
		if s := strings.TrimSpace(cfg.Size.Min); s != "" {
			b, err := match.ParseSize(s)
			if err != nil {
				return "", err
			}
			payload.SizeMinBytes = &b
		}
		if s := strings.TrimSpace(cfg.Size.Max); s != "" {
			b, err := match.ParseSize(s)
			if err != nil {
				return "", err
			}
			payload.SizeMaxBytes = &b
		}
		if payload.SizeMinBytes != nil && payload.SizeMaxBytes != nil && *payload.SizeMinBytes > *payload.SizeMaxBytes {
			return "", fmt.Errorf("%w: min (%d) > max (%d)", match.ErrInvalidSize, *payload.SizeMinBytes, *payload.SizeMaxBytes)
		}
	}

	if cfg.Modified != nil {
		if s := strings.TrimSpace(cfg.Modified.After); s != "" {
			t, err := match.ParseDate(s)
			if err != nil {
				return "", err
			}
			t = t.UTC()
			payload.ModifiedAfter = &t
		}
		if s := strings.TrimSpace(cfg.Modified.Before); s != "" {
			t, err := match.ParseDate(s)
			if err != nil {
				return "", err
			}
			t = t.UTC()
			payload.ModifiedBefore = &t
		}
		if payload.ModifiedAfter != nil && payload.ModifiedBefore != nil && !payload.ModifiedAfter.Before(*payload.ModifiedBefore) {
			return "", fmt.Errorf("%w: after (%s) >= before (%s)", match.ErrInvalidDate, payload.ModifiedAfter.Format(time.RFC3339Nano), payload.ModifiedBefore.Format(time.RFC3339Nano))
		}
	}

	payload.KeyRegex = strings.TrimSpace(cfg.KeyRegex)

	// If nothing is set, no hash.
	if payload.SizeMinBytes == nil && payload.SizeMaxBytes == nil && payload.ModifiedAfter == nil && payload.ModifiedBefore == nil && payload.KeyRegex == "" {
		return "", nil
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sha := sha256.Sum256(b)
	return hex.EncodeToString(sha[:]), nil
}

func deriveCrawlPrefixesForPlan(m *manifest.IndexManifest) ([]string, error) {
	if m == nil {
		return nil, fmt.Errorf("nil manifest")
	}
	_, basePrefix, err := parseS3BaseURI(m.Connection.BaseURI)
	if err != nil {
		return nil, err
	}

	matchCfg := match.Config{IncludeHidden: false}
	if m.Build != nil && m.Build.Match != nil {
		matchCfg.Includes = prefixPatterns(basePrefix, m.Build.Match.Includes)
		matchCfg.Excludes = prefixPatterns(basePrefix, m.Build.Match.Excludes)
		matchCfg.IncludeHidden = m.Build.Match.IncludeHidden
	}
	if len(matchCfg.Includes) == 0 {
		matchCfg.Includes = []string{basePrefix + "**"}
	}

	matcher, err := match.New(matchCfg)
	if err != nil {
		return nil, err
	}
	return matcher.Prefixes(), nil
}

func compileScopePlan(ctx context.Context, m *manifest.IndexManifest) (*scope.Plan, error) {
	if m == nil || m.Build == nil || m.Build.Scope == nil {
		return nil, nil
	}
	if m.Connection.Provider != "s3" {
		return nil, fmt.Errorf("scope plan not supported for provider %q", m.Connection.Provider)
	}

	_, basePrefix, err := parseS3BaseURI(m.Connection.BaseURI)
	if err != nil {
		return nil, err
	}

	var lister provider.PrefixLister
	if scope.RequiresPrefixLister(m.Build.Scope) {
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

		lister = prov
	}

	return scope.Compile(ctx, m.Build.Scope, basePrefix, lister)
}

func prefixPatterns(basePrefix string, patterns []string) []string {
	if basePrefix == "" {
		return patterns
	}
	// Ensure basePrefix ends with '/'
	if !strings.HasSuffix(basePrefix, "/") {
		basePrefix += "/"
	}

	out := make([]string, 0, len(patterns))
	for _, raw := range patterns {
		p := strings.TrimSpace(raw)
		if p == "" {
			continue
		}
		p = strings.TrimPrefix(p, "/")

		// Avoid double-prefixing if user already provided a full-key pattern.
		if strings.HasPrefix(p, basePrefix) {
			out = append(out, p)
			continue
		}
		out = append(out, basePrefix+p)
	}
	return out
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
func buildIndexSetParams(m *manifest.IndexManifest, ident effectiveIdentity, filtersHash string, scopeHash string) indexstore.IndexSetParams {
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
		bp.FiltersHash = strings.TrimSpace(filtersHash)
	}
	bp.ScopeHash = strings.TrimSpace(scopeHash)

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
func showIndexBuildPlan(ctx context.Context, cmd *cobra.Command, m *manifest.IndexManifest, ident effectiveIdentity, buildFilters *indexBuildFilters) error {
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
		if buildFilters != nil && buildFilters.Filter != nil {
			_, _ = fmt.Fprintf(os.Stdout, "  filters: %s\n", buildFilters.Filter.String())
			_, _ = fmt.Fprintf(os.Stdout, "  filters_hash: %s\n", buildFilters.FiltersHash)
		}
	}

	if m.Build != nil && m.Build.Scope != nil {
		_, _ = fmt.Fprintln(os.Stdout)
		_, _ = fmt.Fprintln(os.Stdout, "Scope Plan (build.scope):")
		plan, err := compileScopePlan(ctx, m)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stdout, "  error: %v\n", err)
		} else if plan == nil || len(plan.Prefixes) == 0 {
			_, _ = fmt.Fprintln(os.Stdout, "  count: 0")
		} else {
			if err := validateScopePlan(plan.Prefixes, indexBuildScopeMaxPrefix); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(os.Stdout, "  count: %d\n", len(plan.Prefixes))
			if warning := scopePlanWarning(plan.Prefixes, indexBuildScopeWarnPrefix); warning != "" {
				_, _ = fmt.Fprintf(os.Stdout, "  warning: %s\n", warning)
			}
			maxShow := 10
			if len(plan.Prefixes) < maxShow {
				maxShow = len(plan.Prefixes)
			}
			for i := 0; i < maxShow; i++ {
				_, _ = fmt.Fprintf(os.Stdout, "  - %s\n", plan.Prefixes[i])
			}
			if len(plan.Prefixes) > maxShow {
				_, _ = fmt.Fprintf(os.Stdout, "  ... (%d more)\n", len(plan.Prefixes)-maxShow)
			}
		}
		_, _ = fmt.Fprintln(os.Stdout, "  note: scope plan overrides derived match prefixes")
	}

	// Derived crawl prefixes (used when build.scope is absent).
	// This is the most important “cost shape” signal for large buckets.
	_, _ = fmt.Fprintln(os.Stdout)
	_, _ = fmt.Fprintln(os.Stdout, "Derived Crawl Prefixes:")
	_, _ = fmt.Fprintf(os.Stdout, "  source: build.match.includes/excludes (anchored to base_uri)\n")
	prefixes, err := deriveCrawlPrefixesForPlan(m)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stdout, "  error: %v\n", err)
	} else {
		_, _ = fmt.Fprintf(os.Stdout, "  count: %d\n", len(prefixes))
		maxShow := 10
		if len(prefixes) < maxShow {
			maxShow = len(prefixes)
		}
		for i := 0; i < maxShow; i++ {
			_, _ = fmt.Fprintf(os.Stdout, "  - %s\n", prefixes[i])
		}
		if len(prefixes) > maxShow {
			_, _ = fmt.Fprintf(os.Stdout, "  ... (%d more)\n", len(prefixes)-maxShow)
		}
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

type resolvedIndexDB struct {
	Path          string
	IdentityDir   string
	WriteIdentity bool
}

// resolveIndexDBPath resolves the index database path.
func resolveIndexDBPath(explicit string, identityResult *indexstore.IndexSetIdentityResult) (resolvedIndexDB, error) {
	if explicit != "" {
		return resolvedIndexDB{Path: explicit}, nil
	}

	identity := GetAppIdentity()
	if identity == nil || strings.TrimSpace(identity.ConfigName) == "" {
		return resolvedIndexDB{}, fmt.Errorf("app identity is not available to derive default index path")
	}
	if identityResult == nil {
		return resolvedIndexDB{}, fmt.Errorf("index identity is required to derive default index path")
	}

	dataDir := gfconfig.GetAppDataDir(identity.ConfigName)
	indexDir := filepath.Join(dataDir, "indexes", identityResult.DirName)
	return resolvedIndexDB{
		Path:          filepath.Join(indexDir, "index.db"),
		IdentityDir:   indexDir,
		WriteIdentity: true,
	}, nil
}

func writeIndexIdentityFile(indexDir string, identityResult *indexstore.IndexSetIdentityResult) error {
	if indexDir == "" || identityResult == nil {
		return nil
	}

	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return fmt.Errorf("create index directory: %w", err)
	}

	identityPath := filepath.Join(indexDir, "identity.json")
	if err := os.WriteFile(identityPath, []byte(identityResult.CanonicalJSON+"\n"), 0644); err != nil {
		return fmt.Errorf("write identity.json: %w", err)
	}
	return nil
}

func writeIndexManifestFile(indexDir string, m *manifest.IndexManifest) error {
	if indexDir == "" || m == nil {
		return nil
	}

	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return fmt.Errorf("create index directory: %w", err)
	}

	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	manifestPath := filepath.Join(indexDir, "manifest.json")
	if err := os.WriteFile(manifestPath, append(b, '\n'), 0644); err != nil {
		return fmt.Errorf("write manifest.json: %w", err)
	}
	return nil
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
	filter *match.CompositeFilter,
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

	baseBucket, basePrefix, err := parseS3BaseURI(m.Connection.BaseURI)
	if err != nil {
		return nil, fmt.Errorf("parse base_uri: %w", err)
	}
	if baseBucket != "" && baseBucket != m.Connection.Bucket {
		return nil, fmt.Errorf("base_uri bucket %q does not match connection.bucket %q", baseBucket, m.Connection.Bucket)
	}

	var scopePlan *scope.Plan
	if m.Build != nil && m.Build.Scope != nil {
		var lister provider.PrefixLister
		if scope.RequiresPrefixLister(m.Build.Scope) {
			lister = prov
		}
		plan, err := scope.Compile(ctx, m.Build.Scope, basePrefix, lister)
		if err != nil {
			return nil, fmt.Errorf("build.scope: %w", err)
		}
		if plan == nil || len(plan.Prefixes) == 0 {
			return nil, fmt.Errorf("build.scope produced no crawl prefixes")
		}
		if err := validateScopePlan(plan.Prefixes, indexBuildScopeMaxPrefix); err != nil {
			return nil, err
		}
		if warning := scopePlanWarning(plan.Prefixes, indexBuildScopeWarnPrefix); warning != "" {
			_, _ = fmt.Fprintf(os.Stderr, "warning: %s\n", warning)
		}
		scopePlan = plan
	}

	// Create matcher from build config with nil-guard.
	// IMPORTANT: patterns are applied to full provider keys, so we prefix all
	// build patterns with the base prefix derived from base_uri. This ensures
	// index builds never enumerate outside base_uri (CRITICAL isolation invariant).
	matchCfg := match.Config{
		IncludeHidden: false,
	}
	if m.Build != nil && m.Build.Match != nil {
		matchCfg.Includes = prefixPatterns(basePrefix, m.Build.Match.Includes)
		matchCfg.Excludes = prefixPatterns(basePrefix, m.Build.Match.Excludes)
		matchCfg.IncludeHidden = m.Build.Match.IncludeHidden
	}
	// Default to include everything under base prefix.
	if len(matchCfg.Includes) == 0 {
		matchCfg.Includes = []string{basePrefix + "**"}
	}

	matcher, err := match.New(matchCfg)
	if err != nil {
		return nil, fmt.Errorf("create matcher: %w", err)
	}

	// Create streaming ingest writer
	writer := newIndexIngestWriter(db, indexSetID, run, m.Connection.BaseURI, basePrefix, indexIngestWriterConfig{
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
	if filter != nil {
		c = c.WithFilter(filter)
	}
	if scopePlan != nil {
		c = c.WithPrefixes(scopePlan.Prefixes)
	}
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
	allowSoftDelete bool,
) error {
	// Update run status
	if err := indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, result.FinalStatus, nil); err != nil {
		return fmt.Errorf("update run status: %w", err)
	}

	// ENTARCH: Only soft-delete for successful runs
	// Per softdelete.go policy: partial runs may have missing objects due to
	// incomplete traversal, not actual deletions.
	if result.FinalStatus == indexstore.RunStatusSuccess && allowSoftDelete {
		deleted, err := indexstore.MarkObjectsDeletedNotSeenInRun(ctx, db, indexSetID, run.RunID, run.StartedAt)
		if err != nil {
			return fmt.Errorf("mark deleted objects: %w", err)
		}
		result.ObjectsDeleted = deleted
	}

	return nil
}
