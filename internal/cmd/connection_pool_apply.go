package cmd

import (
	"fmt"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/crawler"
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
