package shard

import (
	"context"
	"fmt"

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

	current := []string{basePrefix}
	for i := 0; i < depth; i++ {
		next := make([]string, 0)
		for _, prefix := range current {
			children, err := listAllPrefixes(ctx, lister, prefix, delimiter)
			if err != nil {
				return nil, err
			}
			if len(children) == 0 {
				next = append(next, prefix)
				continue
			}
			next = append(next, children...)
			if cfg.MaxShards > 0 && len(next) >= cfg.MaxShards {
				return next[:cfg.MaxShards], nil
			}
		}
		current = next
	}

	if cfg.MaxShards > 0 && len(current) > cfg.MaxShards {
		return current[:cfg.MaxShards], nil
	}
	return current, nil
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
