package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/internal/indexcompare"
	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/crawler"
	"github.com/3leaps/gonimbus/pkg/indexbuild"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/scope"
	"github.com/3leaps/gonimbus/pkg/uri"
)

var newIndexBuildEngineSource = func(ctx context.Context, src *uri.ObjectURI, opts providerdispatch.SourceOptions) (provider.Provider, error) {
	return providerdispatch.NewSource(ctx, src, opts)
}

func runIndexBuildEngine(ctx context.Context, cfg indexbuild.Config) (indexbuild.Summary, error) {
	return indexbuild.NewRunner(cfg).Build(ctx)
}

type indexBuildBothFormatsResult struct {
	Result  *indexBuildResult
	Summary indexbuild.Summary
	Report  *indexcompare.Report
}

func runIndexBuildBothFormats(ctx context.Context, m *manifest.IndexManifest, db *sql.DB, indexSet *indexstore.IndexSet, run *indexstore.IndexRun, identityResult *indexstore.IndexSetIdentityResult, buildFilters *indexBuildFilters) (indexBuildBothFormatsResult, error) {
	out := indexBuildBothFormatsResult{Result: &indexBuildResult{FinalStatus: indexstore.RunStatusSuccess}}
	if m == nil {
		return out, fmt.Errorf("index manifest is required")
	}
	if indexSet == nil {
		return out, fmt.Errorf("index set is required")
	}
	if run == nil {
		return out, fmt.Errorf("index run is required")
	}
	if identityResult == nil {
		return out, fmt.Errorf("index identity is required")
	}
	baseBucket, basePrefix, err := parseBaseURIForProvider(m.Connection.BaseURI, m.Connection.Provider)
	if err != nil {
		return out, fmt.Errorf("parse base_uri: %w", err)
	}
	if baseBucket != "" && baseBucket != m.Connection.Bucket {
		return out, fmt.Errorf("base_uri bucket %q does not match connection.bucket %q", baseBucket, m.Connection.Bucket)
	}
	prov, err := newIndexBuildEngineSource(ctx, &uri.ObjectURI{
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
		return out, fmt.Errorf("create provider: %w", err)
	}
	defer func() { _ = prov.Close() }()

	crawlPrefixes, err := indexBuildEngineCrawlPrefixes(ctx, m, basePrefix, prov)
	if err != nil {
		return out, err
	}
	coverage, err := indexBuildEngineCoverageFromCrawl(basePrefix, crawlPrefixes)
	if err != nil {
		return out, err
	}
	journalDir, err := indexSubstrateJournalRunDir(indexSet.IndexSetID, run.RunID)
	if err != nil {
		return out, err
	}
	segmentRoot, err := indexSubstrateSegmentCacheDir(indexSet.IndexSetID)
	if err != nil {
		return out, err
	}
	runSegmentDir := filepath.Join(segmentRoot, "runs", run.RunID)
	resolvedDB, err := resolveIndexDBPath(indexBuildDBPath, identityResult)
	if err != nil {
		return out, err
	}
	paths := indexBuildEnginePathConfig(journalDir, runSegmentDir, segmentRoot, run.RunID, resolvedDB.IdentityDir)
	sqliteWriter := newIndexIngestWriter(db, indexSet.IndexSetID, run, m.Connection.BaseURI, basePrefix, indexIngestWriterConfig{
		ObjectBatchSize: DefaultObjectBatchSize,
		PrefixBatchSize: DefaultPrefixBatchSize,
	})
	sqliteWriter.setDeltaPrefixes(crawlPrefixes)
	cfg := indexbuild.Config{
		IndexSetID:           indexSet.IndexSetID,
		RunID:                run.RunID,
		BaseURI:              m.Connection.BaseURI,
		Source:               indexbuild.Source{Provider: prov, ProviderName: m.Connection.Provider},
		Match:                indexBuildEngineMatchConfig(m),
		Filter:               nil,
		Crawl:                indexBuildEngineCrawlConfig(m),
		CrawlPrefixes:        crawlPrefixes,
		ObservationSinks:     []output.Writer{sqliteWriter},
		Paths:                paths,
		Coverage:             coverage,
		RunStartedAt:         run.StartedAt,
		CreatedAt:            time.Now().UTC(),
		TargetRowsPerSegment: 0,
		// Crawl progress already flows via sqliteWriter; segmenting tail is
		// after the crawl and needs an explicit observational hook.
		OnSegmentProgress: newStderrSegmentProgress(os.Stderr),
	}
	if buildFilters != nil {
		cfg.Filter = buildFilters.Filter
	}
	summary, err := runIndexBuildEngine(ctx, cfg)
	out.Summary = summary
	out.Result = sqliteWriter.Result()
	out.Result.CrawlPrefixes = append([]string(nil), crawlPrefixes...)
	if err != nil {
		out.Report = indexBuildBothFormatsFailureReport(indexSet, run, resolvedDB, paths, false, false)
		return out, err
	}
	manifestDoc, err := indexsubstrate.ReadInternalManifestFile(paths.ManifestPath)
	if err != nil {
		out.Report = indexBuildBothFormatsFailureReport(indexSet, run, resolvedDB, paths, true, false)
		return out, fmt.Errorf("read durable manifest: %w", err)
	}
	report, err := indexcompare.Compare(ctx, indexcompare.Input{
		SQLiteDB:             db,
		SQLiteIndexSetID:     indexSet.IndexSetID,
		SQLiteArtifact:       indexcompare.Artifact{ID: indexSet.IndexSetID, Path: resolvedDB.Path},
		DurableManifest:      manifestDoc,
		DurableSegmentDir:    paths.SegmentDir,
		DurableArtifact:      indexcompare.Artifact{ID: run.RunID, Path: paths.ManifestPath},
		ObservationRunID:     run.RunID,
		ObservationStartedAt: run.StartedAt,
	})
	out.Report = &report
	if err != nil {
		return out, fmt.Errorf("compare index formats: %w", err)
	}
	if !report.ParityPassed {
		return out, fmt.Errorf("index format parity failed: projection_mismatches=%d content_identity_mismatches=%d", report.ProjectionMismatches, report.ContentIdentityCheck.Mismatches)
	}
	return out, nil
}

func runIndexBuildDurable(ctx context.Context, m *manifest.IndexManifest, identityResult *indexstore.IndexSetIdentityResult, buildFilters *indexBuildFilters) (indexbuild.Summary, string, error) {
	if m == nil {
		return indexbuild.Summary{}, "", fmt.Errorf("index manifest is required")
	}
	if identityResult == nil {
		return indexbuild.Summary{}, "", fmt.Errorf("index identity is required")
	}
	// Match SQLite/hub run-id contract: run_<digits> (not UUID-with-hyphens).
	runID := fmt.Sprintf("run_%d", time.Now().UnixNano())
	baseBucket, basePrefix, err := parseBaseURIForProvider(m.Connection.BaseURI, m.Connection.Provider)
	if err != nil {
		return indexbuild.Summary{}, "", fmt.Errorf("parse base_uri: %w", err)
	}
	if baseBucket != "" && baseBucket != m.Connection.Bucket {
		return indexbuild.Summary{}, "", fmt.Errorf("base_uri bucket %q does not match connection.bucket %q", baseBucket, m.Connection.Bucket)
	}
	prov, err := newIndexBuildEngineSource(ctx, &uri.ObjectURI{
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
		return indexbuild.Summary{}, "", fmt.Errorf("create provider: %w", err)
	}
	defer func() { _ = prov.Close() }()

	journalDir, err := indexSubstrateJournalRunDir(identityResult.IndexSetID, runID)
	if err != nil {
		return indexbuild.Summary{}, "", err
	}
	segmentRoot, err := indexSubstrateSegmentCacheDir(identityResult.IndexSetID)
	if err != nil {
		return indexbuild.Summary{}, "", err
	}
	runSegmentDir := filepath.Join(segmentRoot, "runs", runID)
	// Durable-only builds never take --db; identity lands under the default
	// per-index directory so operators can still locate the set.
	resolvedDB, err := resolveIndexDBPath("", identityResult)
	if err != nil {
		return indexbuild.Summary{}, "", err
	}
	if resolvedDB.WriteIdentity {
		if err := writeIndexIdentityFile(resolvedDB.IdentityDir, identityResult); err != nil {
			return indexbuild.Summary{}, "", err
		}
		if err := writeIndexManifestFile(resolvedDB.IdentityDir, m); err != nil {
			return indexbuild.Summary{}, "", err
		}
	}

	crawlPrefixes, err := indexBuildEngineCrawlPrefixes(ctx, m, basePrefix, prov)
	if err != nil {
		return indexbuild.Summary{}, "", err
	}
	coverage, err := indexBuildEngineCoverageFromCrawl(basePrefix, crawlPrefixes)
	if err != nil {
		return indexbuild.Summary{}, "", err
	}

	now := time.Now().UTC()
	cfg := indexbuild.Config{
		IndexSetID: identityResult.IndexSetID,
		RunID:      runID,
		BaseURI:    m.Connection.BaseURI,
		Source: indexbuild.Source{
			Provider:     prov,
			ProviderName: m.Connection.Provider,
		},
		Match:         indexBuildEngineMatchConfig(m),
		Filter:        nil,
		Crawl:         indexBuildEngineCrawlConfig(m),
		CrawlPrefixes: crawlPrefixes,
		// Progress-only sink: journalWriter no-ops progress; durable-only had
		// no ObservationSinks and ran silent. Do not reuse indexIngestWriter.
		ObservationSinks:     []output.Writer{newStderrProgressWriter(os.Stderr)},
		Paths:                indexBuildEnginePathConfig(journalDir, runSegmentDir, segmentRoot, runID, resolvedDB.IdentityDir),
		Coverage:             coverage,
		RunStartedAt:         now,
		CreatedAt:            now,
		TargetRowsPerSegment: 0,
		OnSegmentProgress:    newStderrSegmentProgress(os.Stderr),
	}
	if buildFilters != nil {
		cfg.Filter = buildFilters.Filter
	}
	summary, err := runIndexBuildEngine(ctx, cfg)
	if err != nil {
		return indexbuild.Summary{}, "", err
	}
	return summary, resolvedDB.IdentityDir, nil
}

func validateIndexBuildFormatFlags(resumeRun string) error {
	// resumeRun is unused: resume is validated/dispatched before format validation.
	_ = resumeRun
	format := selectedIndexBuildFormat()
	switch format {
	case "sqlite":
		if indexBuildExperimentalEngine {
			return fmt.Errorf("--experimental-engine is not compatible with --format sqlite; use --format durable")
		}
		return nil
	case "durable":
		return validateIndexBuildDurableGlobalFlags("--format durable")
	case "both":
		if indexBuildExperimentalEngine {
			return fmt.Errorf("--format both is not compatible with --experimental-engine")
		}
		return validateIndexBuildBothGlobalFlags()
	default:
		return fmt.Errorf("--format must be one of: durable, sqlite, both")
	}
}

func validateIndexBuildDurableGlobalFlags(flagName string) error {
	switch {
	case indexBuildBackground:
		return fmt.Errorf("%s is not compatible with --background in this slice", flagName)
	case indexBuildDedupe:
		return fmt.Errorf("%s is not compatible with --dedupe in this slice", flagName)
	case indexBuildSummary:
		return fmt.Errorf("%s is not compatible with --summary in this slice", flagName)
	case strings.TrimSpace(indexBuildDBPath) != "":
		return fmt.Errorf("%s does not use --db; durable builds publish segment artifacts, not index.db. Use --format sqlite or --format both for SQLite compatibility", flagName)
	case strings.TrimSpace(indexBuildSince) != "":
		return fmt.Errorf("%s is not compatible with --since in this slice", flagName)
	default:
		return nil
	}
}

func validateIndexBuildBothGlobalFlags() error {
	switch {
	case indexBuildBackground:
		return fmt.Errorf("--format both is not compatible with --background in this slice")
	case indexBuildDedupe:
		return fmt.Errorf("--format both is not compatible with --dedupe in this slice")
	case strings.TrimSpace(indexBuildSince) != "":
		return fmt.Errorf("--format both is not compatible with --since in this slice")
	default:
		return nil
	}
}

func validateIndexBuildFormatManifest(m *manifest.IndexManifest) error {
	format := selectedIndexBuildFormat()
	switch format {
	case "sqlite":
		return nil
	case "durable", "both":
		flagName := "--format " + format
		// build.scope is allowed: it compiles to an explicit LIST prefix plan.
		// Match predicates that drop objects inside a covered prefix stay rejected
		// via validateIndexBuildDurableFullBaseMatch (faithful coverage).
		if m != nil && m.Build != nil {
			switch strings.TrimSpace(m.Build.Source) {
			case "", manifest.DefaultIndexSource:
				// ok
			default:
				return fmt.Errorf("%s supports crawl source only", flagName)
			}
		}
		return validateIndexBuildDurableFullBaseMatch(m, flagName)
	default:
		return fmt.Errorf("--format must be one of: durable, sqlite, both")
	}
}

func validateIndexBuildDurableFullBaseMatch(m *manifest.IndexManifest, flagName string) error {
	if m == nil || m.Build == nil || m.Build.Match == nil {
		return nil
	}
	matchCfg := m.Build.Match
	if len(matchCfg.Excludes) > 0 {
		return fmt.Errorf("%s does not support build.match.excludes in this slice", flagName)
	}
	if matchCfg.Filters != nil {
		return fmt.Errorf("%s does not support build.match.filters in this slice", flagName)
	}
	if matchCfg.IncludeHidden {
		return fmt.Errorf("%s does not support build.match.include_hidden=true in this slice", flagName)
	}
	if !isDefaultIndexBuildIncludes(matchCfg.Includes) {
		return fmt.Errorf("%s supports only default build.match.includes %q in this slice", flagName, manifest.DefaultIndexIncludes)
	}
	return nil
}

func isDefaultIndexBuildIncludes(includes []string) bool {
	if len(includes) == 0 {
		return true
	}
	if len(includes) != 1 {
		return false
	}
	return includes[0] == manifest.DefaultIndexIncludes
}

func selectedIndexBuildFormat() string {
	if indexBuildExperimentalEngine {
		// Hidden alias forces the durable path for one compatibility cycle.
		format := strings.TrimSpace(strings.ToLower(indexBuildFormat))
		if format == "" || format == "durable" {
			return "durable"
		}
	}
	format := strings.TrimSpace(strings.ToLower(indexBuildFormat))
	if format == "" {
		return "durable"
	}
	return format
}

func indexBuildEngineMatchConfig(m *manifest.IndexManifest) indexbuild.MatchConfig {
	if m == nil || m.Build == nil || m.Build.Match == nil {
		return indexbuild.MatchConfig{Includes: []string{manifest.DefaultIndexIncludes}}
	}
	return indexbuild.MatchConfig{
		Includes:      append([]string(nil), m.Build.Match.Includes...),
		Excludes:      append([]string(nil), m.Build.Match.Excludes...),
		IncludeHidden: m.Build.Match.IncludeHidden,
	}
}

func indexBuildEngineCrawlConfig(m *manifest.IndexManifest) crawler.Config {
	cfg := crawler.DefaultConfig()
	if m == nil || m.Build == nil || m.Build.Crawl == nil {
		return cfg
	}
	if m.Build.Crawl.Concurrency > 0 {
		cfg.Concurrency = m.Build.Crawl.Concurrency
	}
	if m.Build.Crawl.RateLimit > 0 {
		cfg.RateLimit = m.Build.Crawl.RateLimit
	}
	if m.Build.Crawl.ProgressEvery > 0 {
		cfg.ProgressEvery = m.Build.Crawl.ProgressEvery
	}
	return cfg
}

func indexBuildEngineCrawlPrefixes(ctx context.Context, m *manifest.IndexManifest, basePrefix string, prov provider.Provider) ([]string, error) {
	if m != nil && m.Build != nil && m.Build.Scope != nil {
		var lister provider.PrefixLister
		var err error
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
		return append([]string(nil), plan.Prefixes...), nil
	}
	return nil, nil
}

func indexBuildEnginePathConfig(journalDir, runSegmentDir, segmentRoot, runID, indexDBDir string) indexbuild.PathConfig {
	return indexbuild.PathConfig{
		JournalDir:   journalDir,
		SegmentDir:   runSegmentDir,
		ManifestPath: filepath.Join(runSegmentDir, "manifest.json"),
		CompletePath: filepath.Join(runSegmentDir, "complete.json"),
		LatestPath:   filepath.Join(segmentRoot, "latest.json"),
		IndexDBDir:   indexDBDir,
	}
}

// indexBuildEngineCoverageFromCrawl derives durable coverage from the exact
// crawl-prefix plan that drives observation, then fail-closed verifies set
// equality. Both durable-only and --format both must call this helper so no
// path can skip the faithful-coverage gate.
//
// Unscoped (empty crawl plan): single confirmed-complete attestation over the
// base prefix. Scoped: one confirmed-complete, non-windowed attestation per
// crawl prefix. Never roll up to a parent/base prefix.
//
// This adapter does not load PriorRows in this slice; when prior-row loading
// lands alongside scope, scoped incremental tombstone soundness needs re-review.
func indexBuildEngineCoverageFromCrawl(basePrefix string, crawlPrefixes []string) ([]indexbuild.CoverageAttestation, error) {
	coverage, err := deriveIndexBuildCoverageFromCrawlPrefixes(basePrefix, crawlPrefixes)
	if err != nil {
		return nil, err
	}
	if err := validateFaithfulIndexBuildCoverage(basePrefix, crawlPrefixes, coverage); err != nil {
		return nil, err
	}
	return coverage, nil
}

func deriveIndexBuildCoverageFromCrawlPrefixes(basePrefix string, crawlPrefixes []string) ([]indexbuild.CoverageAttestation, error) {
	if len(crawlPrefixes) == 0 {
		// Bucket-root base_uri (s3://bucket/) yields empty provider key; use the
		// relative-root sentinel so faithful-coverage validation and later
		// normalizeCoverageForBaseURI agree (blank is not a missing prefix).
		return []indexbuild.CoverageAttestation{{
			Scope:    &indexbuild.Scope{Prefix: unscopedBaseCoveragePrefix(basePrefix)},
			Basis:    indexbuild.CoverageBasisConfirmed,
			Complete: true,
		}}, nil
	}
	out := make([]indexbuild.CoverageAttestation, 0, len(crawlPrefixes))
	for _, raw := range crawlPrefixes {
		prefix := exactCoveragePrefixKey(raw)
		if prefix == "" {
			return nil, fmt.Errorf("crawl prefix plan contains an empty prefix")
		}
		out = append(out, indexbuild.CoverageAttestation{
			// Window must stay nil: date_partitions ranges are discovery-time
			// prefix selection, never post-LIST temporal windows.
			Scope:    &indexbuild.Scope{Prefix: prefix},
			Basis:    indexbuild.CoverageBasisConfirmed,
			Complete: true,
		})
	}
	return out, nil
}

// unscopedBaseCoveragePrefix maps the unscoped base provider key into the
// coverage equality key. Empty key (bucket root) is the relative-root sentinel.
func unscopedBaseCoveragePrefix(basePrefix string) string {
	key := exactCoveragePrefixKey(basePrefix)
	if key == "" {
		return indexsubstrate.RelativeRootScopePrefix
	}
	return key
}

// validateFaithfulIndexBuildCoverage enforces exact SET EQUALITY between the
// crawl-prefix plan and coverage attestation prefixes. Subset and parent/child
// containment checks are intentionally rejected (they enable roll-up overclaim).
func validateFaithfulIndexBuildCoverage(basePrefix string, crawlPrefixes []string, coverage []indexbuild.CoverageAttestation) error {
	if len(coverage) == 0 {
		return fmt.Errorf("durable coverage attestation is required")
	}
	for i, entry := range coverage {
		if entry.Scope == nil {
			return fmt.Errorf("durable coverage[%d] scope is required", i)
		}
		if entry.Scope.Window != nil {
			return fmt.Errorf("durable coverage[%d] must not set a temporal window", i)
		}
		if entry.Basis != indexbuild.CoverageBasisConfirmed || !entry.Complete {
			return fmt.Errorf("durable coverage[%d] must be confirmed and complete", i)
		}
		if len(entry.Gaps) > 0 {
			return fmt.Errorf("durable coverage[%d] must not declare gaps in this slice", i)
		}
	}

	if len(crawlPrefixes) == 0 {
		// Unscoped full-base path: exactly one base attestation.
		if len(coverage) != 1 {
			return fmt.Errorf("unscoped durable coverage must have exactly one base attestation, got %d", len(coverage))
		}
		got := exactCoveragePrefixKey(coverage[0].Scope.Prefix)
		// Accept legacy blank root or the relative-root sentinel as equivalent.
		if got == "" {
			got = indexsubstrate.RelativeRootScopePrefix
		}
		want := unscopedBaseCoveragePrefix(basePrefix)
		if got != want {
			return fmt.Errorf("unscoped durable coverage prefix %q does not match base prefix %q", got, want)
		}
		return nil
	}

	for i, entry := range coverage {
		if exactCoveragePrefixKey(entry.Scope.Prefix) == "" {
			return fmt.Errorf("durable coverage[%d] prefix is required", i)
		}
	}

	crawlSet := make(map[string]struct{}, len(crawlPrefixes))
	for _, raw := range crawlPrefixes {
		key := exactCoveragePrefixKey(raw)
		if key == "" {
			return fmt.Errorf("crawl prefix plan contains an empty prefix")
		}
		crawlSet[key] = struct{}{}
	}
	if len(crawlSet) != len(crawlPrefixes) {
		return fmt.Errorf("crawl prefix plan has duplicate prefixes after normalize")
	}

	covSet := make(map[string]struct{}, len(coverage))
	for i, entry := range coverage {
		key := exactCoveragePrefixKey(entry.Scope.Prefix)
		if _, dup := covSet[key]; dup {
			return fmt.Errorf("durable coverage has duplicate prefix %q", key)
		}
		covSet[key] = struct{}{}
		if _, ok := crawlSet[key]; !ok {
			return fmt.Errorf("durable coverage[%d] prefix %q is not in the crawl plan (roll-up or extra coverage is not publishable)", i, key)
		}
	}
	if len(covSet) != len(crawlSet) {
		return fmt.Errorf("durable coverage prefixes (%d) must equal crawl plan prefixes (%d) exactly", len(covSet), len(crawlSet))
	}
	for key := range crawlSet {
		if _, ok := covSet[key]; !ok {
			return fmt.Errorf("durable coverage is missing crawl plan prefix %q", key)
		}
	}
	return nil
}

// exactCoveragePrefixKey is the equality key for crawl↔coverage set checks.
// It trims whitespace only — no parent-prefix collapsing (that would be roll-up).
func exactCoveragePrefixKey(prefix string) string {
	return strings.TrimSpace(prefix)
}

func indexBuildBothFormatsFailureReport(indexSet *indexstore.IndexSet, run *indexstore.IndexRun, resolvedDB resolvedIndexDB, paths indexbuild.PathConfig, durablePublished bool, comparisonRan bool) *indexcompare.Report {
	report := &indexcompare.Report{
		Type:                 indexcompare.CompareResultType,
		ProjectionVersion:    indexcompare.ProjectionVersion,
		ProjectionSemantics:  indexcompare.DefaultProjectionSemantics(),
		ComparatorVersion:    indexcompare.ComparatorVersion,
		SQLiteMaterialized:   true,
		DurablePublished:     durablePublished,
		ComparisonRan:        comparisonRan,
		ParityPassed:         false,
		ContentIdentityCheck: indexcompare.ContentIdentityCheck{Semantics: "provider_etag_equivalence"},
		SQLiteArtifact:       indexcompare.Artifact{Path: resolvedDB.Path},
		DurableArtifact:      indexcompare.Artifact{Path: paths.ManifestPath},
	}
	if indexSet != nil {
		report.SQLiteArtifact.ID = indexSet.IndexSetID
	}
	if run != nil {
		report.ObservationRunID = run.RunID
		report.ObservationStartedAt = run.StartedAt.UTC().Format(time.RFC3339Nano)
		report.DurableArtifact.ID = run.RunID
	}
	return report
}
