package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/crawler"
	"github.com/3leaps/gonimbus/pkg/indexbuild"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

var newIndexBuildEngineSource = func(ctx context.Context, src *uri.ObjectURI, opts providerdispatch.SourceOptions) (provider.Provider, error) {
	return providerdispatch.NewSource(ctx, src, opts)
}

func runIndexBuildEngine(ctx context.Context, cfg indexbuild.Config) (indexbuild.Summary, error) {
	return indexbuild.NewRunner(cfg).Build(ctx)
}

func runIndexBuildExperimentalEngine(ctx context.Context, m *manifest.IndexManifest, identityResult *indexstore.IndexSetIdentityResult, buildFilters *indexBuildFilters) (indexbuild.Summary, string, error) {
	if m == nil {
		return indexbuild.Summary{}, "", fmt.Errorf("index manifest is required")
	}
	if identityResult == nil {
		return indexbuild.Summary{}, "", fmt.Errorf("index identity is required")
	}
	runID := "run_" + uuid.NewString()
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
	resolvedDB, err := resolveIndexDBPath(indexBuildDBPath, identityResult)
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

	now := time.Now().UTC()
	cfg := indexbuild.Config{
		IndexSetID: identityResult.IndexSetID,
		RunID:      runID,
		BaseURI:    m.Connection.BaseURI,
		Source: indexbuild.Source{
			Provider:     prov,
			ProviderName: m.Connection.Provider,
		},
		Match:                indexBuildEngineMatchConfig(m),
		Filter:               nil,
		Crawl:                indexBuildEngineCrawlConfig(m),
		Paths:                indexBuildEnginePathConfig(journalDir, runSegmentDir, segmentRoot, runID, resolvedDB.IdentityDir),
		Coverage:             indexBuildEngineCoverage(basePrefix),
		RunStartedAt:         now,
		CreatedAt:            now,
		TargetRowsPerSegment: 0,
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

func validateIndexBuildExperimentalEngineGlobalFlags(resumeRun string) error {
	switch {
	case strings.TrimSpace(resumeRun) != "":
		return fmt.Errorf("--experimental-engine is not compatible with --resume-run")
	case indexBuildDryRun:
		return fmt.Errorf("--experimental-engine is not compatible with --dry-run")
	case indexBuildBackground:
		return fmt.Errorf("--experimental-engine is not compatible with --background")
	case indexBuildDedupe:
		return fmt.Errorf("--experimental-engine is not compatible with --dedupe")
	case indexBuildSummary:
		return fmt.Errorf("--experimental-engine is not compatible with --summary")
	case strings.TrimSpace(indexBuildDBPath) != "":
		return fmt.Errorf("--experimental-engine is not compatible with --db")
	case strings.TrimSpace(indexBuildSince) != "":
		return fmt.Errorf("--experimental-engine is not compatible with --since")
	default:
		return nil
	}
}

func validateIndexBuildExperimentalEngineManifest(m *manifest.IndexManifest) error {
	if m != nil && m.Build != nil {
		if m.Build.Scope != nil {
			return fmt.Errorf("--experimental-engine does not support build.scope in this slice")
		}
		switch strings.TrimSpace(m.Build.Source) {
		case "", manifest.DefaultIndexSource:
			return nil
		default:
			return fmt.Errorf("--experimental-engine supports crawl source only")
		}
	}
	return nil
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

func indexBuildEngineCoverage(basePrefix string) []indexbuild.CoverageAttestation {
	// This adapter does not load PriorRows in this slice, so full-base coverage
	// cannot tombstone CLI rows. When prior-row loading is added, coverage must
	// account for active match exclusions before attesting complete coverage.
	return []indexbuild.CoverageAttestation{{
		Scope:    &indexbuild.Scope{Prefix: basePrefix},
		Basis:    indexbuild.CoverageBasisConfirmed,
		Complete: true,
	}}
}
