package cmd

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/internal/indexcompare"
	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/crawler"
	"github.com/3leaps/gonimbus/pkg/indexbuild"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/scope"
	"github.com/3leaps/gonimbus/pkg/uri"
)

const operationIndexBuild = "index-build"

// indexBuildAfterIdentityGuard is a test-only interposition point at the
// durable validation-to-publication boundary. Production leaves it nil.
var indexBuildAfterIdentityGuard func(path string) error

var indexBuildCmd = &cobra.Command{
	Use:   "build",
	Short: "Build an index from crawl results",
	Long: `Build a local index by crawling a cloud storage location.

Default artifact format is durable (segment-backed durable-v2). Compatibility
SQLite indexes remain available via --format sqlite, and dual-format parity
builds via --format both.

Default durable builds write identity under the per-index directory and publish
a durable snapshot under the segment cache. They do not create index.db.

  indexes/idx_<hashprefix>/identity.json
  cache/segments/<index_set_id>/runs/<run_id>/...

SQLite compatibility builds still use:
  indexes/idx_<hashprefix>/index.db
  indexes/idx_<hashprefix>/identity.json

Local consumer note: query, list, stats, doctor, and enrich-with-head are
format-aware (durable or SQLite). Use --format sqlite or --format both when you
still need SQLite-only surfaces: query --since-run, stats --prefixes, or full
--resume-run checkpoint recovery. Durable hydrate restores
manifest+segments, not index.db.

The index build process:
1. Loads and validates the index manifest
2. Computes IndexSet identity from connection + build params
3. Creates a new run and crawls the source
4. Publishes the selected artifact format(s)
5. Handles partial runs (throttling, access denied) with structured events

Tip: use 'gonimbus index list' to see IDENTITY status, and 'gonimbus index doctor' to explain a specific idx_<hashprefix> directory.

Example:
  gonimbus index build --job index.yaml
  gonimbus index build --job index.yaml --format sqlite
  gonimbus index build --job index.yaml --json
  gonimbus index build --job index.yaml --storage-provider wasabi --region us-east-1

Machine handoff: --json emits a post-commit gonimbus.index.build_result.v1
receipt on stdout (exact index_set_id + run_id + scope_hash + formats). Do not
rediscover the just-built set from index list. --json is rejected with
--background (the immediate job id is not a committed receipt).`,
	RunE: runIndexBuildCommand,
}

func runIndexBuildCommand(cmd *cobra.Command, args []string) error {
	err := runIndexBuild(cmd, args)
	if err == nil || strings.TrimSpace(indexBuildManagedJobID) == "" {
		return err
	}
	persistManagedIndexBuildFailure(strings.TrimSpace(indexBuildManagedJobID))
	return sanitizeManagedIndexBuildError(err)
}

func persistManagedIndexBuildFailure(jobID string) {
	jobsRoot, err := indexJobsRootDir()
	if err != nil {
		return
	}
	store := jobregistry.NewStore(jobsRoot)
	rec, err := store.Get(jobID)
	if err != nil || (rec.State != jobregistry.JobStateQueued && rec.State != jobregistry.JobStateRunning) {
		return
	}
	now := time.Now().UTC()
	rec.State = jobregistry.JobStateFailed
	rec.EndedAt = &now
	rec.LastHeartbeat = &now
	_ = store.Write(rec)
}

func sanitizeManagedIndexBuildError(err error) error {
	return fmt.Errorf("managed index build failed: %s", reflow.SanitizeOperationCauseMessage(err))
}

// Index build flags.
var (
	indexBuildJobPath            string
	indexBuildDBPath             string
	indexBuildDryRun             bool
	indexBuildBackground         bool
	indexBuildDedupe             bool
	indexBuildManagedJobID       string
	indexBuildStorageProv        string
	indexBuildCloudProv          string
	indexBuildRegionKind         string
	indexBuildRegion             string
	indexBuildEndpointHost       string
	indexBuildScopeWarnPrefix    int
	indexBuildScopeMaxPrefix     int
	indexBuildName               string
	indexBuildSummary            bool
	indexBuildResumeRun          string
	indexBuildSince              string
	indexBuildFormat             string
	indexBuildExperimentalEngine bool
	indexBuildJSON               bool
)

func init() {
	indexCmd.AddCommand(indexBuildCmd)

	// Required
	indexBuildCmd.Flags().StringVarP(&indexBuildJobPath, "job", "j", "", "Path to index manifest (required)")

	// Optional
	indexBuildCmd.Flags().StringVar(&indexBuildDBPath, "db", "", "Index database path or libsql DSN (default is per-index under data dir)")
	indexBuildCmd.Flags().BoolVar(&indexBuildDryRun, "dry-run", false, "Validate manifest and show plan without building")
	indexBuildCmd.Flags().BoolVar(&indexBuildBackground, "background", false, "Run index build as a managed background job")
	indexBuildCmd.Flags().BoolVar(&indexBuildDedupe, "dedupe", false, "Refuse to start if an identical job is already running")
	indexBuildCmd.Flags().BoolVar(&indexBuildSummary, "summary", false, "Print top-level object distribution after a completed build")
	indexBuildCmd.Flags().BoolVar(&indexBuildJSON, "json", false, "Emit machine-stable build_result.v1 receipt on stdout after successful commit")
	indexBuildCmd.Flags().StringVar(&indexBuildManagedJobID, "_managed-job-id", "", "(internal) Managed job id")
	_ = indexBuildCmd.Flags().MarkHidden("_managed-job-id")
	indexBuildCmd.Flags().StringVar(&indexBuildName, "name", "", "Optional job name (recorded in job registry)")
	indexBuildCmd.Flags().StringVar(&indexBuildResumeRun, "resume-run", "", "Resume a failed-resumable index build run by run id")
	indexBuildCmd.Flags().StringVar(&indexBuildSince, "since", "", "Incremental build lower bound timestamp or auto (narrows date-partition scope when possible)")
	indexBuildCmd.Flags().StringVar(&indexBuildFormat, "format", "durable", "Index artifact format to build (durable, sqlite, both)")
	indexBuildCmd.Flags().BoolVar(&indexBuildExperimentalEngine, "experimental-engine", false, "(deprecated) alias for --format durable")
	_ = indexBuildCmd.Flags().MarkHidden("experimental-engine")
	if flag := indexBuildCmd.Flags().Lookup("since"); flag != nil {
		flag.NoOptDefVal = "auto"
	}
	indexBuildCmd.Flags().IntVar(&indexBuildScopeWarnPrefix, "scope-warn-prefixes", 10000, "Warn if build.scope expands to more than N prefixes (0 disables)")
	indexBuildCmd.Flags().IntVar(&indexBuildScopeMaxPrefix, "scope-max-prefixes", 50000, "Fail build if build.scope expands beyond N prefixes (0 disables)")

	// Provider identity overrides (ENTARCH: explicit, never inferred)
	indexBuildCmd.Flags().StringVar(&indexBuildStorageProv, "storage-provider", "", "Storage provider (aws_s3, cloudflare_r2, wasabi, gcs, azure_blob, generic_s3)")
	indexBuildCmd.Flags().StringVar(&indexBuildCloudProv, "cloud-provider", "", "Cloud provider (aws, gcp, azure, cloudflare, other)")
	indexBuildCmd.Flags().StringVar(&indexBuildRegionKind, "region-kind", "", "Region naming scheme (aws, gcp, azure)")
	indexBuildCmd.Flags().StringVar(&indexBuildRegion, "region", "", "Region name")
	indexBuildCmd.Flags().StringVar(&indexBuildEndpointHost, "endpoint-host", "", "Endpoint host (host[:port])")
}

// validateIndexBuildResumeInvocation checks flags that conflict with --resume-run.
// Resume is always the SQLite checkpoint path; the durable default must not reject
// the printed `index build --resume-run <id>` command when --format was omitted.
func validateIndexBuildResumeInvocation(cmd *cobra.Command) error {
	if indexBuildExperimentalEngine {
		return fmt.Errorf("--experimental-engine is not compatible with --resume-run")
	}
	if cmd != nil && cmd.Flags().Changed("format") && selectedIndexBuildFormat() != "sqlite" {
		return fmt.Errorf("--resume-run is not compatible with --format %s; resume uses the SQLite checkpoint lifecycle (omit --format or pass --format sqlite)", selectedIndexBuildFormat())
	}
	return nil
}

func runIndexBuild(cmd *cobra.Command, args []string) (runErr error) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	resumeRun := strings.TrimSpace(indexBuildResumeRun)
	if indexBuildExperimentalEngine {
		_, _ = fmt.Fprintln(os.Stderr, "warning: --experimental-engine is deprecated; use --format durable")
	}
	// Receipt mode is commit-boundary only.
	if indexBuildJSON && indexBuildBackground {
		return fmt.Errorf("--json is not compatible with --background; the immediate job id is not a committed build receipt")
	}
	if indexBuildJSON && indexBuildDryRun {
		return fmt.Errorf("--json is not compatible with --dry-run; dry-run does not emit a committed build receipt")
	}
	if indexBuildJSON && resumeRun != "" {
		return fmt.Errorf("--json is not compatible with --resume-run; resume does not emit a build_result receipt in this cut")
	}
	// Resume is a SQLite checkpoint lifecycle path. Dispatch it before build-format
	// validation so the durable default does not break the printed operator hint
	// (`gonimbus index build --resume-run <run_id>` with no --format).
	if resumeRun != "" {
		if err := validateIndexBuildResumeInvocation(cmd); err != nil {
			return err
		}
		return runIndexBuildResume(ctx, cmd, resumeRun)
	}
	if err := validateIndexBuildFormatFlags(""); err != nil {
		return err
	}
	if strings.TrimSpace(indexBuildJobPath) == "" {
		return fmt.Errorf("--job is required")
	}

	// Managed mode: translate SIGTERM into context cancellation.
	if strings.TrimSpace(indexBuildManagedJobID) != "" {
		managedCtx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
		defer cancel()
		ctx = managedCtx
	}

	// Background mode: start a managed child process and return.
	if indexBuildBackground {
		if err := validateIndexBuildBackgroundFlags(); err != nil {
			return err
		}
		execRoot, err := indexJobsRootDir()
		if err != nil {
			return err
		}
		exec := jobregistry.NewExecutor(execRoot)
		invocation, err := resolvedCurrentIndexBuildInvocation()
		if err != nil {
			return err
		}
		job, err := exec.StartIndexBuildBackground(indexBuildJobPath, strings.TrimSpace(indexBuildName), jobregistry.BackgroundOptions{
			Dedupe:     indexBuildDedupe,
			Since:      strings.TrimSpace(indexBuildSince),
			Invocation: &invocation,
		})
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(os.Stdout, "%s\n", job.JobID)
		return nil
	}

	var store *jobregistry.Store
	var job *jobregistry.JobRecord
	var loadedManagedManifest *manifest.IndexManifest

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

		managedJobID := strings.TrimSpace(indexBuildManagedJobID)
		if managedJobID != "" {
			job, loadedManagedManifest, err = claimManagedIndexBuildJob(store, managedJobID)
			if err != nil {
				return err
			}
		} else {
			now := time.Now().UTC()
			jobID := uuid.New().String()
			job = &jobregistry.JobRecord{
				JobID:        jobID,
				Type:         jobregistry.JobTypeIndexBuild,
				Name:         strings.TrimSpace(indexBuildName),
				State:        jobregistry.JobStateRunning,
				ManifestPath: absManifestPath,
				StdoutPath:   filepath.Join(store.JobDir(jobID), "stdout.log"),
				StderrPath:   filepath.Join(store.JobDir(jobID), "stderr.log"),
				CreatedAt:    now,
				StartedAt:    &now,
			}
		}

		// In managed mode, stdout/stderr are redirected by the parent.
		// In foreground mode, we don't capture logs yet, but we still expose
		// the expected paths for consistency.
		if managedJobID == "" {
			_ = mkdirAppDataDir(store.JobDir(job.JobID))
			if f, err := store.OpenLog(job.JobID, "stdout.log", false); err == nil {
				_ = f.Close()
			}
			if f, err := store.OpenLog(job.JobID, "stderr.log", false); err == nil {
				_ = f.Close()
			}
		}

		if managedJobID == "" {
			if err := store.Write(job); err != nil {
				return fmt.Errorf("write job record: %w", err)
			}
		}
	}

	// Load and validate manifest
	m := loadedManagedManifest
	var manifestLoadErr error
	if m == nil {
		m, manifestLoadErr = manifest.LoadIndexManifest(indexBuildJobPath)
	}
	if manifestLoadErr != nil {
		if store != nil && job != nil {
			job.State = jobregistry.JobStateFailed
			ended := time.Now().UTC()
			job.EndedAt = &ended
			_ = store.Write(job)
		}
		return fmt.Errorf("load index manifest: %w", manifestLoadErr)

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
	if err := validateIndexBuildFormatManifest(m); err != nil {
		return err
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
	checkpointCfg := indexBuildCheckpointConfigFromManifest(m, identity, buildFilters.FiltersHash, scopeHash)

	// Build IndexSetParams
	params := buildIndexSetParams(m, identity, buildFilters.FiltersHash, scopeHash)

	identityResult, err := indexstore.ComputeIndexSetID(params)
	if err != nil {
		return fmt.Errorf("compute index identity: %w", err)
	}

	// Show plan in dry-run mode. This intentionally does not create/open the
	// index database, so --since auto fails closed in the displayed plan.
	if indexBuildDryRun {
		sincePlan, err := planIndexBuildSince(ctx, m, strings.TrimSpace(indexBuildSince), nil, time.Now().UTC())
		if err != nil {
			return err
		}
		return showIndexBuildPlan(ctx, cmd, m, identity, buildFilters, sincePlan, identityResult, scopeHash)
	}

	maintenanceHolder := "index-build-" + uuid.NewString()
	if job != nil && strings.TrimSpace(job.JobID) != "" {
		maintenanceHolder = "index-build-" + strings.TrimSpace(job.JobID)
	}
	maintenance, err := acquireIndexSetMaintenance(ctx, identityResult.IndexSetID, maintenanceHolder)
	if err != nil {
		if store != nil && job != nil {
			job.State = jobregistry.JobStateFailed
			ended := time.Now().UTC()
			job.EndedAt = &ended
			_ = store.Write(job)
		}
		return fmt.Errorf("acquire index-set maintenance lease: %w", err)
	}
	defer func() { _ = maintenance.Release() }()
	ctx = maintenance.Context()

	// Resolve the local compatibility target before format dispatch. Durable
	// builds do not open index.db, but they do publish identity.json and must not
	// bless a markerless canonical SQLite artifact as a side effect.
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
	segmentSetRoot, err := indexSubstrateSegmentCacheDir(identityResult.IndexSetID)
	if err != nil {
		return err
	}

	if selectedIndexBuildFormat() == "durable" {
		if resolvedDB.Canonical {
			guard, err := indexreader.OpenSQLiteIdentityPublicationGuard(ctx, indexreader.SQLiteWriteTargetOptions{
				Path:           resolvedDB.Path,
				IdentityPath:   filepath.Join(resolvedDB.IdentityDir, "identity.json"),
				SegmentSetRoot: segmentSetRoot,
				IndexSetID:     identityResult.IndexSetID,
				Authority:      maintenance.Authority(),
				MaxMarkerBytes: int64(maxHubMarkerBytes),
			})
			if err != nil {
				return err
			}
			if indexBuildAfterIdentityGuard != nil {
				if err := indexBuildAfterIdentityGuard(resolvedDB.Path); err != nil {
					return errors.Join(err, guard.Close())
				}
			}
			if err := guard.PublishIdentity(identityResult); err != nil {
				return errors.Join(err, guard.Close())
			}
			if err := guard.PublishManifest(m); err != nil {
				return errors.Join(err, guard.Close())
			}
			if err := guard.Close(); err != nil {
				return err
			}
		}
		cmd.SilenceUsage = true
		summary, identityDir, err := runIndexBuildDurable(ctx, m, identityResult, buildFilters, resolvedDB, maintenance.Authority())
		if err != nil {
			if store != nil && job != nil {
				job.State = jobregistry.JobStateFailed
				ended := time.Now().UTC()
				job.EndedAt = &ended
				_ = store.Write(job)
			}
			return err
		}
		receipt := newDurableBuildResultRecord(summary, scopeHash, "durable", []string{"durable-v2"})
		if store != nil && job != nil {
			job.IndexDir = identityDir
			job.IndexSetID = summary.IndexSetID
			job.RunID = summary.RunID
			job.Receipt = jobBuildReceiptIdentity(receipt)
			job.PID = os.Getpid()
			job.State = jobregistry.JobStateSuccess
			ended := time.Now().UTC()
			job.EndedAt = &ended
			if err := persistCommittedIndexBuildJob(store, job); err != nil {
				return err
			}
		}
		_, _ = fmt.Fprintf(os.Stderr, "\nIndex build completed\n")
		_, _ = fmt.Fprintf(os.Stderr, "  format: durable\n")
		_, _ = fmt.Fprintf(os.Stderr, "  run_id: %s\n", summary.RunID)
		_, _ = fmt.Fprintf(os.Stderr, "  index_set_id: %s\n", summary.IndexSetID)
		_, _ = fmt.Fprintf(os.Stderr, "  objects_observed: %d\n", summary.ObjectsObserved)
		_, _ = fmt.Fprintf(os.Stderr, "  segments: %d\n", len(summary.Manifest.Segments))
		if indexBuildJSON {
			if err := emitIndexBuildResultJSON(cmd.OutOrStdout(), receipt); err != nil {
				return err
			}
		}
		return nil
	}

	// Canonical SQLite builds use one library-owned validation/binding/open
	// operation. Explicit external paths retain the caller-owned indexstore
	// behavior and are never given canonical trust by this adapter.
	var db *sql.DB
	var canonicalTarget *indexreader.SQLiteWriteTarget
	if resolvedDB.Canonical {
		canonicalTarget, err = indexreader.OpenSQLiteWriteTarget(ctx, indexreader.SQLiteWriteTargetOptions{
			Path:           resolvedDB.Path,
			IdentityPath:   filepath.Join(resolvedDB.IdentityDir, "identity.json"),
			SegmentSetRoot: segmentSetRoot,
			IndexSetID:     identityResult.IndexSetID,
			Authority:      maintenance.Authority(),
			MaxMarkerBytes: int64(maxHubMarkerBytes),
		})
		if err != nil {
			return err
		}
		db = canonicalTarget.DB()
		defer func() {
			if canonicalTarget != nil {
				runErr = errors.Join(runErr, canonicalTarget.Close())
			}
		}()
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
		if canonicalTarget == nil {
			return fmt.Errorf("canonical metadata publication requires the library-owned SQLite write target")
		}
		if err := canonicalTarget.PublishIdentity(identityResult); err != nil {
			return err
		}
		if err := canonicalTarget.PublishManifest(m); err != nil {
			return err
		}
	}

	if db == nil {
		db, err = indexstore.Open(ctx, cfg)
		if err != nil {
			return fmt.Errorf("open index database: %w", err)
		}
		defer func() { _ = db.Close() }()
	}

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
	checkpointCfg.IndexSetID = indexSet.IndexSetID

	sincePlan, err := resolveIndexBuildSince(ctx, db, indexSet.IndexSetID, m, strings.TrimSpace(indexBuildSince), time.Now().UTC())
	if err != nil {
		return err
	}
	bindIndexBuildSince(&checkpointCfg, sincePlan)
	crawlManifest := manifestForSincePlan(m, sincePlan)
	runtimeFilter := combineIndexBuildFilters(buildFilters.Filter, sincePlan.Filter)

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
	cmd.SilenceUsage = true

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
	writeIndexBuildSinceStart(os.Stderr, sincePlan)
	var result *indexBuildResult
	var crawlErr error
	var compareReport *indexcompare.Report
	var bothSummary indexbuild.Summary
	var bothSummaryOK bool
	if selectedIndexBuildFormat() == "both" {
		bothResult, err := runIndexBuildBothFormats(ctx, m, db, indexSet, run, identityResult, buildFilters, maintenance.Authority())
		result = bothResult.Result
		crawlErr = err
		if err == nil {
			bothSummary = bothResult.Summary
			bothSummaryOK = true
		}
		if bothResult.Report != nil {
			compareReport = bothResult.Report
			enc := json.NewEncoder(cmd.OutOrStdout())
			if emitErr := enc.Encode(compareReport); emitErr != nil && crawlErr == nil {
				crawlErr = fmt.Errorf("emit compare result: %w", emitErr)
			}
		}
	} else {
		result, crawlErr = runCrawlForIndex(ctx, crawlManifest, db, indexSet.IndexSetID, run, runtimeFilter, nil, sincePlan.Enabled)
	}
	if crawlErr != nil {
		classification := classifyIndexBuildRunError(crawlErr, m)
		if classification.Resumable && indexBuildCheckpointEligible(checkpointCfg) {
			progress, checkpointErr := writeFailedResumableIndexBuildCheckpoint(context.Background(), db, run.RunID, checkpointCfg, classification.Class, result)
			if checkpointErr == nil {
				writeOperationErrorSummary(cmd.ErrOrStderr(), "Index build failed with resumable checkpoint", operationIndexBuild, run.RunID, classification.Class, progress)
				enc := json.NewEncoder(cmd.OutOrStdout())
				if emitErr := emitOperationErrorRecord(context.Background(), enc, operationIndexBuild, run.RunID, classification.Class, progress); emitErr != nil {
					crawlErr = fmt.Errorf("%w; write operation error record: %v", crawlErr, emitErr)
				}
				if store != nil && job != nil {
					job.State = jobregistry.JobStatePartial
					ended := time.Now().UTC()
					job.EndedAt = &ended
					_ = store.Write(job)
				}
				return fmt.Errorf("index build failed resumable: %w", crawlErr)
			}
			crawlErr = fmt.Errorf("%w; write operation checkpoint: %v", crawlErr, checkpointErr)
		}
		// ENTARCH: Distinguish context cancellation from fatal errors.
		// - Cancellation (Ctrl-C, timeout): record as "partial" (data was flushed, just incomplete)
		// - Other errors: record as "failed" (something actually broke)
		if errors.Is(crawlErr, context.Canceled) || errors.Is(crawlErr, context.DeadlineExceeded) {
			// Context cancellation - mark as partial, skip soft-delete
			_ = indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusPartial, nil)
			_, _ = fmt.Fprintf(os.Stderr, "\nIndex build interrupted\n")
			_, _ = fmt.Fprintf(os.Stderr, "  run_id: %s\n", run.RunID)
			_, _ = fmt.Fprintf(os.Stderr, "  status: %s\n", indexstore.RunStatusPartial)
			summary := indexBuildSummaryFromResult(result)
			_, _ = fmt.Fprintf(os.Stderr, "  objects_ingested: %d\n", summary.ObjectsIngested)
			_, _ = fmt.Fprintf(os.Stderr, "  prefixes_ingested: %d\n", summary.PrefixesIngested)
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
	allowSoftDelete := !sincePlan.Enabled && (m.Build == nil || m.Build.Scope == nil)
	if err := finalizeIndexRun(ctx, db, indexSet.IndexSetID, run, result, allowSoftDelete, indexBuildSinceEvents(run.RunID, sincePlan, time.Now().UTC())); err != nil {
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

	if compareReport != nil && !compareReport.ParityPassed {
		if store != nil && job != nil {
			job.State = jobregistry.JobStateFailed
			ended := time.Now().UTC()
			job.EndedAt = &ended
			_ = store.Write(job)
		}
		return fmt.Errorf("index format parity failed")
	}
	if indexBuildSummary {
		if err := printIndexBuildSummary(ctx, db, indexSet.IndexSetID, run.RunID, os.Stderr); err != nil {
			return err
		}
	}
	// The bound target close is part of the canonical commit boundary. Do not
	// persist job success or emit a terminal receipt until the canonical name,
	// authority, and bound file have been revalidated and sidecars are closed.
	if canonicalTarget != nil {
		if err := canonicalTarget.Close(); err != nil {
			canonicalTarget = nil
			if store != nil && job != nil {
				job.State = jobregistry.JobStateFailed
				ended := time.Now().UTC()
				job.EndedAt = &ended
				_ = store.Write(job)
			}
			return fmt.Errorf("close canonical SQLite write target: %w", err)
		}
		canonicalTarget = nil
	}

	terminalReceipt, receiptOK, receiptErr := committedIndexBuildResultRecord(selectedIndexBuildFormat(), result.FinalStatus, bothSummary, bothSummaryOK, indexSet.IndexSetID, run.RunID, scopeHash, result.ObjectsIngested)
	if receiptErr != nil {
		return receiptErr
	}

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
		if receiptOK {
			job.Receipt = jobBuildReceiptIdentity(terminalReceipt)
		}
		if receiptOK {
			if err := persistCommittedIndexBuildJob(store, job); err != nil {
				return err
			}
		} else {
			_ = store.Write(job)
		}
	}
	if result.ObjectsDeleted > 0 {
		_, _ = fmt.Fprintf(os.Stderr, "  objects_soft_deleted: %d\n", result.ObjectsDeleted)
	}
	if result.FinalStatus == indexstore.RunStatusSuccess && !allowSoftDelete {
		if sincePlan.Enabled {
			_, _ = fmt.Fprintf(os.Stderr, "  note: soft-delete skipped for --since build (not full coverage)\n")
		} else {
			_, _ = fmt.Fprintf(os.Stderr, "  note: soft-delete skipped for scoped build (not full coverage by default)\n")
		}
	}
	writeIndexBuildDeltaReport(os.Stderr, result, sincePlan)
	if result.FinalStatus == indexstore.RunStatusPartial {
		_, _ = fmt.Fprintf(os.Stderr, "  note: partial run (some errors encountered)\n")
	}
	// Terminal receipt after commit. Success is authoritative for consumers that
	// require status=success. SQLite partial also emits an explicit non-authoritative
	// terminal record so automation never sees exit 0 with empty stdout under --json.
	// Fatal failure returns nonzero and emits no receipt.
	if indexBuildJSON {
		if receiptOK {
			if err := emitIndexBuildResultJSON(cmd.OutOrStdout(), terminalReceipt); err != nil {
				return err
			}
		}
	}

	return nil
}

// emitCommittedIndexBuildJSON writes the terminal build_result record for a
// committed sqlite/both path. Durable success is emitted on its own return path.
// Failed final status emits nothing (caller already returns error).
func emitCommittedIndexBuildJSON(
	w io.Writer,
	format string,
	finalStatus indexstore.RunStatus,
	bothSummary indexbuild.Summary,
	bothSummaryOK bool,
	indexSetID, runID, scopeHash string,
	objectsIngested int64,
) error {
	rec, ok, err := committedIndexBuildResultRecord(format, finalStatus, bothSummary, bothSummaryOK, indexSetID, runID, scopeHash, objectsIngested)
	if err != nil || !ok {
		return err
	}
	return emitIndexBuildResultJSON(w, rec)
}

func committedIndexBuildResultRecord(
	format string,
	finalStatus indexstore.RunStatus,
	bothSummary indexbuild.Summary,
	bothSummaryOK bool,
	indexSetID, runID, scopeHash string,
	objectsIngested int64,
) (indexBuildResultRecord, bool, error) {
	switch format {
	case "both":
		if finalStatus != indexstore.RunStatusSuccess {
			return indexBuildResultRecord{}, false, nil
		}
		if !bothSummaryOK {
			return indexBuildResultRecord{}, false, fmt.Errorf("build result: both-format summary missing after success")
		}
		return newBothBuildResultRecord(bothSummary, scopeHash, objectsIngested), true, nil
	case "sqlite":
		switch finalStatus {
		case indexstore.RunStatusSuccess, indexstore.RunStatusPartial:
			status := "success"
			if finalStatus == indexstore.RunStatusPartial {
				status = "partial"
			}
			return newSQLiteBuildResultRecord(indexSetID, runID, scopeHash, status, objectsIngested), true, nil
		default:
			return indexBuildResultRecord{}, false, nil
		}
	default:
		return indexBuildResultRecord{}, false, nil
	}
}

func runIndexBuildResume(ctx context.Context, cmd *cobra.Command, runID string) error {
	if strings.TrimSpace(indexBuildJobPath) != "" {
		return fmt.Errorf("--job is not accepted with --resume-run; resume uses checkpointed build config")
	}
	if strings.TrimSpace(indexBuildDBPath) != "" {
		return fmt.Errorf("--db is not accepted with --resume-run; resume uses the default index database recorded by the run")
	}
	if indexBuildDryRun {
		return fmt.Errorf("--dry-run is not compatible with --resume-run")
	}
	if indexBuildBackground {
		return fmt.Errorf("--background is not compatible with --resume-run")
	}
	if indexBuildSummary {
		return fmt.Errorf("--summary is not compatible with --resume-run")
	}
	if strings.TrimSpace(indexBuildSince) != "" {
		return fmt.Errorf("--since is not accepted with --resume-run; resume uses checkpointed build config")
	}

	opStore, err := openDefaultOperationCheckpointStore(ctx)
	if err != nil {
		return err
	}
	env, err := opStore.ReadCheckpoint(ctx, operationIndexBuild, runID)
	if err != nil {
		return fmt.Errorf("read operation checkpoint: %w", err)
	}
	var payload indexBuildCheckpointPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return fmt.Errorf("parse index build checkpoint payload: %w", err)
	}
	if !validFullIndexSetID(payload.Config.IndexSetID) {
		return fmt.Errorf("resume checkpoint does not carry a valid full index_set_id")
	}
	if !payload.Config.UsesDefaultIndexDB {
		return fmt.Errorf("--resume-run %s is not supported for non-default index database paths in this slice", runID)
	}
	maintenance, err := acquireIndexSetMaintenance(ctx, payload.Config.IndexSetID, "index-build-resume-"+uuid.NewString())
	if err != nil {
		return fmt.Errorf("acquire index-set maintenance lease: %w", err)
	}
	defer func() { _ = maintenance.Release() }()
	ctx = maintenance.Context()

	resolved, err := findIndexRunInDefaultIndexes(ctx, runID, payload.Config.IndexSetID, maintenance)
	if err != nil {
		return err
	}
	defer closeResolvedIndexRun(resolved)
	db := resolved.db
	indexSet := resolved.indexSet
	run := resolved.run

	if payload.Config.IndexSetID != "" && payload.Config.IndexSetID != run.IndexSetID {
		return opcheckpoint.ErrIdentityMismatch
	}
	if err := validateIndexRunResumeCandidate(run, indexSet, payload.Config.SourceType, "index build", env.Status); err != nil {
		return err
	}

	fingerprint, err := validateIndexBuildCheckpointPayloadIdentity(ctx, db, env, payload)
	if err != nil {
		return err
	}
	if err := opStore.ValidateIdentity(env, opcheckpoint.Identity{
		Operation:         operationIndexBuild,
		RunID:             runID,
		ConfigFingerprint: fingerprint,
	}); err != nil {
		return err
	}

	lease, err := opStore.ClaimLease(ctx, operationIndexBuild, runID, "gonimbus-"+uuid.NewString(), resumeLeaseTTL)
	if err != nil {
		return err
	}
	heartbeat, leaseCtx, err := startResumeLeaseHeartbeat(ctx, opStore, operationIndexBuild, lease)
	if err != nil {
		return err
	}
	ctx = leaseCtx
	defer func() {
		_ = heartbeat.Stop()
		_ = opStore.ReleaseLease(operationIndexBuild, *lease)
	}()
	if err := recoverIndexRunResumeCrash(context.Background(), db, run); err != nil {
		return err
	}

	m := &payload.Config.Manifest
	if err := validateIdentity(m, effectiveIdentityFromCheckpoint(payload.Config.Identity)); err != nil {
		return err
	}
	oldScopeWarnPrefix := indexBuildScopeWarnPrefix
	oldScopeMaxPrefix := indexBuildScopeMaxPrefix
	indexBuildScopeWarnPrefix = payload.Config.ScopeWarnPrefixes
	indexBuildScopeMaxPrefix = payload.Config.ScopeMaxPrefixes
	defer func() {
		indexBuildScopeWarnPrefix = oldScopeWarnPrefix
		indexBuildScopeMaxPrefix = oldScopeMaxPrefix
	}()
	buildFilters, err := computeIndexBuildFilters(m)
	if err != nil {
		return err
	}
	sincePlan, err := indexBuildSincePlanFromCheckpoint(&payload.Config)
	if err != nil {
		return err
	}
	runtimeFilter := combineIndexBuildFilters(buildFilters.Filter, sincePlan.Filter)
	crawlManifest := manifestForSincePlan(m, sincePlan)

	if err := indexstore.MarkIndexRunResumingWithEvents(context.Background(), db, runID, []indexstore.RunEvent{
		indexRunLifecycleEvent(runID, "resume_started", string(opcheckpoint.ErrorClassInterrupted), time.Now().UTC()),
	}); err != nil {
		return err
	}
	cmd.SilenceUsage = true

	result, crawlErr := runCrawlForIndex(ctx, crawlManifest, db, run.IndexSetID, run, runtimeFilter, payload.CrawlPrefixes, sincePlan.Enabled)
	if crawlErr != nil {
		classification := classifyIndexBuildRunError(crawlErr, m)
		if classification.Resumable {
			progress := indexBuildProgress(result)
			if prefixes := indexBuildCrawlPrefixes(result); len(prefixes) > 0 {
				payload.CrawlPrefixes = prefixes
			}
			payload.Summary = indexBuildSummaryFromResult(result)
			if err := bindIndexBuildCrawlPrefixes(&payload.Config, payload.CrawlPrefixes); err != nil {
				crawlErr = fmt.Errorf("%w; bind crawl prefix plan: %v", crawlErr, err)
				return fmt.Errorf("index build resume failed: %w", crawlErr)
			}
			fingerprint, err = checkpointFingerprint(payload.Config)
			if err != nil {
				crawlErr = fmt.Errorf("%w; compute checkpoint fingerprint: %v", crawlErr, err)
				return fmt.Errorf("index build resume failed: %w", crawlErr)
			}
			if err := stopResumeLeaseHeartbeatBeforeFailedResumableCheckpoint(heartbeat); err != nil {
				return fmt.Errorf("index build resume failed: %w", err)
			}
			if writeErr := writeIndexRunCheckpoint(context.Background(), opStore, db, runID, operationIndexBuild, fingerprint, classification.Class, progress, payload); writeErr != nil {
				crawlErr = fmt.Errorf("%w; write operation checkpoint: %v", crawlErr, writeErr)
			} else {
				writeOperationErrorSummary(cmd.ErrOrStderr(), "Index build resume failed with resumable checkpoint", operationIndexBuild, runID, classification.Class, progress)
				enc := json.NewEncoder(cmd.OutOrStdout())
				if emitErr := emitOperationErrorRecord(context.Background(), enc, operationIndexBuild, runID, classification.Class, progress); emitErr != nil {
					crawlErr = fmt.Errorf("%w; write operation error record: %v", crawlErr, emitErr)
				}
			}
		} else if err := indexstore.UpdateIndexRunStatus(context.Background(), db, runID, indexstore.RunStatusFailed, nil); err != nil {
			crawlErr = fmt.Errorf("%w; update index run status: %v", crawlErr, err)
		}
		return fmt.Errorf("index build resume failed: %w", crawlErr)
	}

	if err := stopResumeLeaseHeartbeat(heartbeat); err != nil {
		return err
	}
	promoteCtx := context.Background()
	allowSoftDelete := !sincePlan.Enabled && (m.Build == nil || m.Build.Scope == nil)
	if err := finalizeIndexRun(promoteCtx, db, indexSet.IndexSetID, run, result, allowSoftDelete, []indexstore.RunEvent{
		indexRunLifecycleEvent(runID, "resume_completed", "", time.Now().UTC()),
	}); err != nil {
		return fmt.Errorf("finalize index run: %w", err)
	}

	_, _ = fmt.Fprintf(os.Stderr, "\nIndex build resume completed\n")
	_, _ = fmt.Fprintf(os.Stderr, "  run_id: %s\n", runID)
	_, _ = fmt.Fprintf(os.Stderr, "  index_set_id: %s\n", indexSet.IndexSetID)
	_, _ = fmt.Fprintf(os.Stderr, "  status: %s\n", result.FinalStatus)
	_, _ = fmt.Fprintf(os.Stderr, "  objects_ingested: %d\n", result.ObjectsIngested)
	_, _ = fmt.Fprintf(os.Stderr, "  prefixes_ingested: %d\n", result.PrefixesIngested)

	env.Status = opcheckpoint.StatusSuccess
	env.Progress = indexBuildProgress(result)
	env.Events = append(env.Events, opcheckpoint.CheckpointEvent{Type: "resume_completed", At: time.Now().UTC()})
	payload.Summary = indexBuildSummaryFromResult(result)
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal completed checkpoint payload: %w", err)
	}
	env.Payload = rawPayload
	if err := opStore.WriteCheckpoint(context.Background(), *env); err != nil {
		return fmt.Errorf("write completed checkpoint: %w", err)
	}
	return nil
}

type indexBuildCheckpointPayload struct {
	Config        indexBuildCheckpointConfig  `json:"config"`
	CrawlPrefixes []string                    `json:"crawl_prefixes,omitempty"`
	Summary       indexBuildCheckpointSummary `json:"summary"`
}

type indexBuildCheckpointConfig struct {
	IndexSetID                       string                     `json:"index_set_id,omitempty"`
	SourceType                       string                     `json:"source_type"`
	Manifest                         manifest.IndexManifest     `json:"manifest"`
	Identity                         indexBuildIdentityState    `json:"identity"`
	FiltersHash                      string                     `json:"filters_hash,omitempty"`
	ScopeHash                        string                     `json:"scope_hash,omitempty"`
	CrawlPrefixesHash                string                     `json:"crawl_prefixes_hash,omitempty"`
	SinceMode                        string                     `json:"since_mode,omitempty"`
	SinceWatermark                   string                     `json:"since_watermark,omitempty"`
	SinceAutoFallback                bool                       `json:"since_auto_fallback,omitempty"`
	SinceRuntimeScope                *manifest.IndexScopeConfig `json:"since_runtime_scope,omitempty"`
	SinceEnumerationReductionPartial bool                       `json:"since_enumeration_reduction_partial,omitempty"`
	ScopeWarnPrefixes                int                        `json:"scope_warn_prefixes"`
	ScopeMaxPrefixes                 int                        `json:"scope_max_prefixes"`
	UsesDefaultIndexDB               bool                       `json:"uses_default_index_db"`
}

type indexBuildIdentityState struct {
	StorageProvider string `json:"storage_provider,omitempty"`
	CloudProvider   string `json:"cloud_provider,omitempty"`
	RegionKind      string `json:"region_kind,omitempty"`
	Region          string `json:"region,omitempty"`
	EndpointHost    string `json:"endpoint_host,omitempty"`
}

type indexBuildCheckpointSummary struct {
	ObjectsIngested  int64  `json:"objects_ingested"`
	PrefixesIngested int64  `json:"prefixes_ingested"`
	ObjectsDeleted   int64  `json:"objects_deleted,omitempty"`
	FinalStatus      string `json:"final_status,omitempty"`
}

func indexBuildCheckpointConfigFromManifest(m *manifest.IndexManifest, identity effectiveIdentity, filtersHash, scopeHash string) indexBuildCheckpointConfig {
	sourceType := "crawl"
	if m != nil && m.Build != nil && strings.TrimSpace(m.Build.Source) != "" {
		sourceType = strings.TrimSpace(m.Build.Source)
	}
	cfg := indexBuildCheckpointConfig{
		SourceType:         sourceType,
		Identity:           indexBuildIdentityState(identity),
		FiltersHash:        strings.TrimSpace(filtersHash),
		ScopeHash:          strings.TrimSpace(scopeHash),
		ScopeWarnPrefixes:  indexBuildScopeWarnPrefix,
		ScopeMaxPrefixes:   indexBuildScopeMaxPrefix,
		UsesDefaultIndexDB: strings.TrimSpace(indexBuildDBPath) == "",
	}
	if m != nil {
		cfg.Manifest = *m
	}
	return cfg
}

func effectiveIdentityFromCheckpoint(state indexBuildIdentityState) effectiveIdentity {
	return effectiveIdentity(state)
}

func indexBuildSummaryFromResult(result *indexBuildResult) indexBuildCheckpointSummary {
	if result == nil {
		return indexBuildCheckpointSummary{}
	}
	return indexBuildCheckpointSummary{
		ObjectsIngested:  result.ObjectsIngested,
		PrefixesIngested: result.PrefixesIngested,
		ObjectsDeleted:   result.ObjectsDeleted,
		FinalStatus:      string(result.FinalStatus),
	}
}

func indexBuildCheckpointEligible(cfg indexBuildCheckpointConfig) bool {
	return cfg.UsesDefaultIndexDB
}

func writeFailedResumableIndexBuildCheckpoint(
	ctx context.Context,
	db *sql.DB,
	runID string,
	cfg indexBuildCheckpointConfig,
	class opcheckpoint.ErrorClass,
	result *indexBuildResult,
) (map[string]int64, error) {
	progress := indexBuildProgress(result)
	payload := indexBuildCheckpointPayload{
		Config:        cfg,
		CrawlPrefixes: indexBuildCrawlPrefixes(result),
		Summary:       indexBuildSummaryFromResult(result),
	}
	if err := bindIndexBuildCrawlPrefixes(&payload.Config, payload.CrawlPrefixes); err != nil {
		return progress, fmt.Errorf("bind crawl prefix plan: %w", err)
	}
	fingerprint, err := checkpointFingerprint(payload.Config)
	if err != nil {
		return progress, fmt.Errorf("compute checkpoint fingerprint: %w", err)
	}
	opStore, err := openDefaultOperationCheckpointStore(ctx)
	if err != nil {
		return progress, fmt.Errorf("open operation checkpoint store: %w", err)
	}
	if err := writeIndexRunCheckpoint(ctx, opStore, db, runID, operationIndexBuild, fingerprint, class, progress, payload); err != nil {
		return progress, err
	}
	return progress, nil
}

func indexBuildCrawlPrefixes(result *indexBuildResult) []string {
	if result == nil || len(result.CrawlPrefixes) == 0 {
		return nil
	}
	return append([]string(nil), result.CrawlPrefixes...)
}

func bindIndexBuildCrawlPrefixes(cfg *indexBuildCheckpointConfig, prefixes []string) error {
	if cfg == nil {
		return fmt.Errorf("index build checkpoint config is nil")
	}
	hash, err := hashIndexBuildCrawlPrefixes(prefixes)
	if err != nil {
		return err
	}
	cfg.CrawlPrefixesHash = hash
	return nil
}

func validateIndexBuildCheckpointPayloadIdentity(ctx context.Context, db *sql.DB, env *opcheckpoint.Envelope, payload indexBuildCheckpointPayload) (string, error) {
	if err := validateIndexBuildCrawlPrefixes(payload.Config, payload.CrawlPrefixes); err != nil {
		return "", err
	}
	return validateCheckpointIdentityAgainstIndexRun(ctx, db, env, operationIndexBuild, payload.Config)
}

func validateIndexBuildCrawlPrefixes(cfg indexBuildCheckpointConfig, prefixes []string) error {
	hash, err := hashIndexBuildCrawlPrefixes(prefixes)
	if err != nil {
		return err
	}
	if hash != cfg.CrawlPrefixesHash {
		return opcheckpoint.ErrIdentityMismatch
	}
	return nil
}

func hashIndexBuildCrawlPrefixes(prefixes []string) (string, error) {
	if len(prefixes) == 0 {
		return "", nil
	}
	canonical := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		if prefix != strings.TrimSpace(prefix) {
			return "", opcheckpoint.ErrIdentityMismatch
		}
		canonical = append(canonical, prefix)
	}
	sort.Strings(canonical)
	b, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sha := sha256.Sum256(b)
	return hex.EncodeToString(sha[:]), nil
}

func indexBuildProgress(result *indexBuildResult) map[string]int64 {
	if result == nil {
		return nil
	}
	progress := map[string]int64{
		"objects_ingested":  result.ObjectsIngested,
		"prefixes_ingested": result.PrefixesIngested,
	}
	if len(result.CrawlPrefixes) > 0 {
		progress["crawl_prefixes"] = int64(len(result.CrawlPrefixes))
	}
	return progress
}

func classifyIndexBuildRunError(err error, _ *manifest.IndexManifest) opcheckpoint.Classification {
	if err == nil {
		return opcheckpoint.Classification{Class: opcheckpoint.ErrorClassRuntimeFailure, Resumable: false}
	}
	return opcheckpoint.ClassifyFatalError(err, opcheckpoint.ClassifierInput{
		Interrupted: errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
	})
}

func validateIndexBuildBackgroundFlags() error {
	if indexBuildDryRun {
		return fmt.Errorf("--background is not compatible with --dry-run")
	}
	if indexBuildSummary {
		return fmt.Errorf("--background is not compatible with --summary")
	}
	return nil
}

func printIndexBuildSummary(ctx context.Context, db *sql.DB, indexSetID, runID string, w io.Writer) error {
	rows, err := indexstore.GetTopLevelObjectSummaryForRun(ctx, db, indexSetID, runID)
	if err != nil {
		return fmt.Errorf("get build summary: %w", err)
	}

	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, "Top-level object summary:")
	if len(rows) == 0 {
		_, _ = fmt.Fprintln(w, "  (no objects seen in this run)")
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "  PREFIX\tOBJECTS\tSIZE")
	for _, row := range rows {
		_, _ = fmt.Fprintf(tw, "  %s\t%d\t%s\n",
			displayPrefix(row.Prefix),
			row.ObjectCount,
			formatBytes(row.TotalSizeBytes),
		)
	}
	return tw.Flush()
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

func parseBaseURIForProvider(baseURI string, providerName string) (bucket string, prefix string, err error) {
	parsed, err := uri.ParseURI(baseURI)
	if err != nil {
		return "", "", err
	}
	if parsed.Provider != providerName {
		return "", "", fmt.Errorf("base_uri provider %q does not match connection.provider %q", parsed.Provider, providerName)
	}
	if !parsed.IsPrefix() {
		return "", "", fmt.Errorf("base uri path must end with '/': %s", baseURI)
	}
	return parsed.Bucket, parsed.Key, nil
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
	_, basePrefix, err := parseBaseURIForProvider(m.Connection.BaseURI, m.Connection.Provider)
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
	if m.Connection.Provider != string(provider.ProviderS3) && m.Connection.Provider != string(provider.ProviderGCS) {
		return nil, fmt.Errorf("scope plan not supported for provider %q", m.Connection.Provider)
	}

	_, basePrefix, err := parseBaseURIForProvider(m.Connection.BaseURI, m.Connection.Provider)
	if err != nil {
		return nil, err
	}

	var lister provider.PrefixLister
	if scope.RequiresPrefixLister(m.Build.Scope) {
		prov, err := providerdispatch.NewSource(ctx, &uri.ObjectURI{
			Provider: m.Connection.Provider,
			Bucket:   m.Connection.Bucket,
		}, providerdispatch.SourceOptions{
			Command: operationIndexBuild,
			S3: providerdispatch.S3Options{
				Region:         m.Connection.Region,
				Endpoint:       m.Connection.Endpoint,
				Profile:        m.Connection.Profile,
				ForcePathStyle: m.Connection.Endpoint != "",
			},
			GCS: providerdispatch.GCSOptions{
				Project: m.Connection.Project,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("create provider: %w", err)
		}
		defer func() { _ = prov.Close() }()

		lister, err = providerdispatch.RequireCapability[provider.PrefixLister](prov, operationIndexBuild, m.Connection.Provider, "PrefixLister")
		if err != nil {
			return nil, err
		}
	}

	return scope.Compile(ctx, m.Build.Scope, basePrefix, lister)
}

func scopePlanAuthHint(err error, profile string) string {
	if !isAWSSSOExpiredError(err) {
		return ""
	}
	return awsSSOLoginHint(profile)
}

func isAWSSSOExpiredError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "invalidgrantexception"):
		return true
	case strings.Contains(msg, "token has expired"):
		return true
	case strings.Contains(msg, "sso") && strings.Contains(msg, "session") && (strings.Contains(msg, "expired") || strings.Contains(msg, "invalid")):
		return true
	default:
		return false
	}
}

func awsSSOLoginHint(profile string) string {
	profile = strings.TrimSpace(profile)
	if profile == "" {
		return "hint: AWS SSO token appears expired; run `aws sso login` to refresh."
	}
	return fmt.Sprintf("hint: AWS SSO token appears expired; run `aws sso login --profile %s` to refresh.", profile)
}

func writeScopePlanError(w io.Writer, err error, profile string) {
	if hint := scopePlanAuthHint(err, profile); hint != "" {
		_, _ = fmt.Fprintf(w, "  %s\n", hint)
	}
	_, _ = fmt.Fprintf(w, "  error: %v\n", err)
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
//
// Stream convention note (deliberate departure — do not copy elsewhere):
// CLI progress, warnings, and status go to stderr; stdout is reserved for
// structured command output (JSON/JSONL, machine-consumed results). See
// ADR-0004 (progress/errors out of the content payload) and the general
// adapter stream split in ADR-0006. Dry-run plans are an intentional exception:
// the plan *is* the command result (human-readable text on stdout), same as
// crawl --dry-run. Keep operator chatter on stderr for non-dry-run paths.
func showIndexBuildPlan(ctx context.Context, cmd *cobra.Command, m *manifest.IndexManifest, ident effectiveIdentity, buildFilters *indexBuildFilters, sincePlan *indexBuildSincePlan, identityResult *indexstore.IndexSetIdentityResult, scopeHash string) error {
	_, _ = fmt.Fprintln(os.Stdout, "Index Build Plan (dry-run)")
	_, _ = fmt.Fprintln(os.Stdout, "==========================")
	_, _ = fmt.Fprintln(os.Stdout)
	_, _ = fmt.Fprintf(os.Stdout, "Artifact format: %s\n", selectedIndexBuildFormat())
	switch selectedIndexBuildFormat() {
	case "durable":
		_, _ = fmt.Fprintln(os.Stdout, "  note: durable default publishes manifest+segments; no index.db is created")
	case "sqlite":
		_, _ = fmt.Fprintln(os.Stdout, "  note: sqlite compatibility mode materializes index.db")
	case "both":
		_, _ = fmt.Fprintln(os.Stdout, "  note: both mode dual-builds sqlite + durable and emits a parity report")
	}
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
	// Precomputed identity only — do not recompute a different path here.
	if identityResult != nil && strings.TrimSpace(identityResult.IndexSetID) != "" {
		_, _ = fmt.Fprintf(os.Stdout, "  index_set_id: %s\n", identityResult.IndexSetID)
	}
	if strings.TrimSpace(scopeHash) != "" {
		_, _ = fmt.Fprintf(os.Stdout, "  scope_hash: %s\n", scopeHash)
	}

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
		scopeManifest := manifestForSincePlan(m, sincePlan)
		plan, err := compileScopePlan(ctx, scopeManifest)
		if err != nil {
			writeScopePlanError(os.Stdout, err, m.Connection.Profile)
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

	if sincePlan != nil && sincePlan.Enabled {
		_, _ = fmt.Fprintln(os.Stdout)
		_, _ = fmt.Fprintln(os.Stdout, "Since Plan:")
		_, _ = fmt.Fprintf(os.Stdout, "  mode: %s\n", sincePlan.Mode)
		if !sincePlan.Watermark.IsZero() {
			_, _ = fmt.Fprintf(os.Stdout, "  watermark: %s\n", sincePlan.Watermark.Format(time.RFC3339Nano))
		}
		_, _ = fmt.Fprintf(os.Stdout, "  enumeration_reduction: %s\n", enumerationReductionStatus(sincePlan))
		if sincePlan.Reason != "" {
			_, _ = fmt.Fprintf(os.Stdout, "  reason: %s\n", sincePlan.Reason)
		}
		for _, warning := range sincePlan.Warnings {
			_, _ = fmt.Fprintf(os.Stdout, "  warning: %s\n", warning)
		}
		_, _ = fmt.Fprintln(os.Stdout, "  note: --since builds skip soft-delete because they are not full-coverage audits")
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
	Canonical     bool
}

// resolveIndexDBPath resolves the index database path.
func resolveIndexDBPath(explicit string, identityResult *indexstore.IndexSetIdentityResult) (resolvedIndexDB, error) {
	identity := GetAppIdentity()
	if identity == nil || strings.TrimSpace(identity.ConfigName) == "" {
		return resolvedIndexDB{}, fmt.Errorf("app identity is not available to derive default index path")
	}
	if identityResult == nil {
		return resolvedIndexDB{}, fmt.Errorf("index identity is required to derive default index path")
	}

	dataDir, err := indexDataDir()
	if err != nil {
		return resolvedIndexDB{}, err
	}
	indexDir := filepath.Join(dataDir, "indexes", identityResult.DirName)
	expectedPath := filepath.Join(indexDir, "index.db")
	if strings.TrimSpace(explicit) != "" {
		localPath, local, err := explicitLocalIndexDBPath(explicit)
		if err != nil {
			return resolvedIndexDB{}, err
		}
		if !local {
			return resolvedIndexDB{Path: explicit}, nil
		}
		explicitAbs, err := filepath.Abs(filepath.Clean(localPath))
		if err != nil {
			return resolvedIndexDB{}, fmt.Errorf("resolve explicit index database path: %w", err)
		}
		indexesRoot := filepath.Join(dataDir, "indexes")
		rootAbs, err := filepath.Abs(filepath.Clean(indexesRoot))
		if err != nil {
			return resolvedIndexDB{}, err
		}
		resolvedExplicit, err := resolvePathForPolicy(explicitAbs)
		if err != nil {
			return resolvedIndexDB{}, fmt.Errorf("resolve explicit index database path for policy: %w", err)
		}
		resolvedRoot, err := resolvePathForPolicy(rootAbs)
		if err != nil {
			return resolvedIndexDB{}, fmt.Errorf("resolve canonical indexes root for policy: %w", err)
		}
		insideCanonicalRoot := indexDBPathWithinRoot(explicitAbs, rootAbs) || indexDBPathWithinRoot(resolvedExplicit, resolvedRoot)
		if !insideCanonicalRoot {
			return resolvedIndexDB{Path: explicit}, nil
		}
		expectedAbs, err := filepath.Abs(filepath.Clean(expectedPath))
		if err != nil {
			return resolvedIndexDB{}, err
		}
		resolvedExpected, err := resolvePathForPolicy(expectedAbs)
		if err != nil {
			return resolvedIndexDB{}, fmt.Errorf("resolve canonical index database path for policy: %w", err)
		}
		if resolvedExplicit != resolvedExpected {
			return resolvedIndexDB{}, fmt.Errorf("explicit --db inside the canonical indexes root must be the requested set target %s", expectedPath)
		}
		return resolvedIndexDB{Path: expectedPath, IdentityDir: indexDir, WriteIdentity: true, Canonical: true}, nil
	}
	return resolvedIndexDB{
		Path:          expectedPath,
		IdentityDir:   indexDir,
		WriteIdentity: true,
		Canonical:     true,
	}, nil
}

func explicitLocalIndexDBPath(explicit string) (string, bool, error) {
	explicit = strings.TrimSpace(explicit)
	if strings.HasPrefix(explicit, "libsql://") || strings.HasPrefix(explicit, "https://") {
		return "", false, nil
	}
	if !strings.HasPrefix(explicit, "file:") {
		return explicit, true, nil
	}
	parsed, err := url.Parse(explicit)
	if err != nil {
		return "", false, fmt.Errorf("parse explicit index database DSN: %w", err)
	}
	if parsed.Host != "" && parsed.Host != "localhost" {
		return "", false, fmt.Errorf("file index database DSN must not name a remote host")
	}
	path := parsed.Path
	if path == "" {
		path = parsed.Opaque
	}
	if strings.TrimSpace(path) == "" {
		return "", false, fmt.Errorf("file index database DSN path is empty")
	}
	return filepath.FromSlash(path), true, nil
}

func indexDBPathWithinRoot(path, root string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
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
	CrawlPrefixes    []string
	DeltaByPrefix    map[string]indexBuildDeltaCounts
}

type indexBuildDeltaCounts struct {
	Added     int64
	Changed   int64
	Unchanged int64
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
	crawlPrefixesOverride []string,
	deltaReport bool,
) (*indexBuildResult, error) {
	// Create provider.
	prov, err := providerdispatch.NewSource(ctx, &uri.ObjectURI{
		Provider: m.Connection.Provider,
		Bucket:   m.Connection.Bucket,
	}, providerdispatch.SourceOptions{
		Command: operationIndexBuild,
		S3: providerdispatch.S3Options{
			Region:         m.Connection.Region,
			Endpoint:       m.Connection.Endpoint,
			Profile:        m.Connection.Profile,
			ForcePathStyle: m.Connection.Endpoint != "",
		},
		GCS: providerdispatch.GCSOptions{
			Project: m.Connection.Project,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create provider: %w", err)
	}
	defer func() { _ = prov.Close() }()

	baseBucket, basePrefix, err := parseBaseURIForProvider(m.Connection.BaseURI, m.Connection.Provider)
	if err != nil {
		return nil, fmt.Errorf("parse base_uri: %w", err)
	}
	if baseBucket != "" && baseBucket != m.Connection.Bucket {
		return nil, fmt.Errorf("base_uri bucket %q does not match connection.bucket %q", baseBucket, m.Connection.Bucket)
	}

	var scopePlan *scope.Plan
	if len(crawlPrefixesOverride) == 0 && m.Build != nil && m.Build.Scope != nil {
		var lister provider.PrefixLister
		if scope.RequiresPrefixLister(m.Build.Scope) {
			lister, err = providerdispatch.RequireCapability[provider.PrefixLister](prov, operationIndexBuild, m.Connection.Provider, "PrefixLister")
			if err != nil {
				return nil, err
			}
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
		DeltaReport:     deltaReport,
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
	} else if len(crawlPrefixesOverride) > 0 {
		c = c.WithPrefixes(crawlPrefixesOverride)
	}
	crawlPrefixes := crawlPrefixesOverride
	if len(crawlPrefixes) == 0 {
		if scopePlan != nil {
			crawlPrefixes = scopePlan.Prefixes
		} else {
			crawlPrefixes = matcher.Prefixes()
			if len(crawlPrefixes) == 0 {
				crawlPrefixes = []string{""}
			}
		}
	}
	writer.setDeltaPrefixes(crawlPrefixes)
	_, crawlErr := c.Run(ctx)

	// Always close writer to flush remaining batches, even on error.
	// This persists what we've seen so far.
	closeErr := writer.Close()

	// Get result from writer state
	result := writer.Result()
	result.CrawlPrefixes = append([]string(nil), crawlPrefixes...)

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
	events []indexstore.RunEvent,
) error {
	var softDelete *indexstore.IndexRunSoftDelete
	if result.FinalStatus == indexstore.RunStatusSuccess && allowSoftDelete {
		softDelete = &indexstore.IndexRunSoftDelete{
			IndexSetID:   indexSetID,
			RunID:        run.RunID,
			RunStartedAt: run.StartedAt,
		}
	}

	if len(events) > 0 || softDelete != nil {
		deleted, err := indexstore.UpdateIndexRunStatusWithEventsAndSoftDelete(ctx, db, run.RunID, result.FinalStatus, nil, events, softDelete)
		if err != nil {
			return fmt.Errorf("update run status: %w", err)
		}
		result.ObjectsDeleted = deleted
		return nil
	}

	if err := indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, result.FinalStatus, nil); err != nil {
		return fmt.Errorf("update run status: %w", err)
	}
	return nil
}
