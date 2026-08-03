package cmd

import (
	"fmt"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/crawler"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// applySourceConnectionPool exact-assigns HTTP pool knobs on source construction
// options from ResolveConnectionPool(admittedN). Callers must pre-resolve
// admitted N (engine defaults/clamps) before calling.
func applySourceConnectionPool(opts *providerdispatch.SourceOptions, admittedN int) error {
	if opts == nil {
		return fmt.Errorf("source options is nil")
	}
	pool, err := provider.ResolveConnectionPool(admittedN)
	if err != nil {
		return err
	}
	opts.S3.MaxIdleConnsPerHost = pool.MaxIdleConnsPerHost
	opts.S3.MaxConnsPerHost = pool.MaxConnsPerHost
	opts.GCS.MaxIdleConnsPerHost = pool.MaxIdleConnsPerHost
	opts.GCS.MaxConnsPerHost = pool.MaxConnsPerHost
	return nil
}

// resolvedCrawlAdmittedN returns crawl LIST concurrency after defaulting zero/negative
// to the crawler default (matches engine RequestBudget sizing).
func resolvedCrawlAdmittedN(concurrency int) int {
	if concurrency <= 0 {
		return crawler.DefaultConfig().Concurrency
	}
	return concurrency
}

// indexBuildSourceOptions is the single production boundary for index-build
// source construction pool policy (engine both-format, durable, and SQLite crawl).
func indexBuildSourceOptions(m *manifest.IndexManifest) (providerdispatch.SourceOptions, error) {
	if m == nil {
		return providerdispatch.SourceOptions{}, fmt.Errorf("index manifest is nil")
	}
	opts := providerdispatch.SourceOptions{
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
	}
	if err := applySourceConnectionPool(&opts, resolvedCrawlAdmittedN(indexBuildEngineCrawlConfig(m).Concurrency)); err != nil {
		return providerdispatch.SourceOptions{}, err
	}
	return opts, nil
}
