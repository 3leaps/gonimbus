package shard

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/3leaps/gonimbus/pkg/provider"
)

type Config struct {
	Enabled         bool
	Depth           int
	MaxShards       int
	ListConcurrency int
	Delimiter       string
}

// Discover expands a base prefix into shard prefixes using delimiter listing.
//
// Depth controls how many delimiter levels to expand.
//
// Notes:
// - This is best-effort and does not attempt to estimate object counts.
// - When MaxShards > 0, the returned slice is truncated.
func Discover(ctx context.Context, p provider.Provider, basePrefix string, cfg Config) ([]string, error) {
	if !cfg.Enabled {
		return []string{basePrefix}, nil
	}
	lister, ok := p.(provider.PrefixLister)
	if !ok {
		return nil, fmt.Errorf("provider does not support delimiter prefix discovery")
	}

	depth := cfg.Depth
	if depth <= 0 {
		depth = 1
	}
	delimiter := cfg.Delimiter
	if delimiter == "" {
		delimiter = "/"
	}

	listConcurrency := cfg.ListConcurrency
	if listConcurrency <= 0 {
		listConcurrency = 1
	}

	current := []string{basePrefix}
	for i := 0; i < depth; i++ {
		next, err := expandLevel(ctx, lister, current, delimiter, listConcurrency, cfg.MaxShards)
		if err != nil {
			return nil, err
		}
		// Keep deterministic ordering for plans/tests.
		sort.Strings(next)
		current = next
	}

	if cfg.MaxShards > 0 && len(current) > cfg.MaxShards {
		return current[:cfg.MaxShards], nil
	}
	return current, nil
}

func expandLevel(ctx context.Context, lister provider.PrefixLister, prefixes []string, delimiter string, concurrency int, maxShards int) ([]string, error) {
	if len(prefixes) == 0 {
		return nil, nil
	}
	if concurrency <= 0 {
		concurrency = 1
	}

	workCh := make(chan string, len(prefixes))
	for _, p := range prefixes {
		workCh <- p
	}
	close(workCh)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var total atomic.Int64
	var mu sync.Mutex
	var out []string
	var firstErr atomic.Value

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for prefix := range workCh {
				if ctx.Err() != nil {
					return
				}

				children, err := listAllPrefixes(ctx, lister, prefix, delimiter)
				if err != nil {
					if firstErr.Load() == nil {
						firstErr.Store(err)
						cancel()
					}
					return
				}

				mu.Lock()
				if len(children) == 0 {
					out = append(out, prefix)
					total.Add(1)
				} else {
					out = append(out, children...)
					total.Add(int64(len(children)))
				}
				mu.Unlock()

				if maxShards > 0 && total.Load() >= int64(maxShards) {
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()

	if err := firstErr.Load(); err != nil {
		return nil, err.(error)
	}

	if maxShards > 0 && len(out) > maxShards {
		out = out[:maxShards]
	}
	return out, nil
}

func listAllPrefixes(ctx context.Context, lister provider.PrefixLister, prefix string, delimiter string) ([]string, error) {
	var token string
	var out []string
	for {
		res, err := lister.ListCommonPrefixes(ctx, provider.ListCommonPrefixesOptions{Prefix: prefix, Delimiter: delimiter, ContinuationToken: token})
		if err != nil {
			return nil, err
		}
		out = append(out, res.Prefixes...)
		if !res.IsTruncated || res.ContinuationToken == "" {
			return out, nil
		}
		token = res.ContinuationToken
	}
}
