package scope

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/provider"
)

type fakePrefixLister struct {
	prefixes map[string][]string
}

func (f *fakePrefixLister) ListCommonPrefixes(ctx context.Context, opts provider.ListCommonPrefixesOptions) (*provider.ListCommonPrefixesResult, error) {
	if f.prefixes == nil {
		return nil, errors.New("no prefixes configured")
	}
	values, ok := f.prefixes[opts.Prefix]
	if !ok {
		return &provider.ListCommonPrefixesResult{}, nil
	}
	out := make([]string, len(values))
	copy(out, values)
	return &provider.ListCommonPrefixesResult{Prefixes: out}, nil
}

func TestCompilePrefixList(t *testing.T) {
	cfg := &manifest.IndexScopeConfig{
		Type:       "prefix_list",
		BasePrefix: "70860/",
		Prefixes:   []string{"device-a/2025-12-01/", "device-b/2025-12-02/", "", "device-a/2025-12-01/"},
	}

	plan, err := Compile(context.Background(), cfg, "data/", nil)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, []string{
		"data/70860/device-a/2025-12-01/",
		"data/70860/device-b/2025-12-02/",
	}, plan.Prefixes)
}

func TestCompileUnion(t *testing.T) {
	cfg := &manifest.IndexScopeConfig{
		Type: "union",
		Scopes: []manifest.IndexScopeConfig{
			{
				Type:       "prefix_list",
				BasePrefix: "70860/",
				Prefixes:   []string{"device-a/"},
			},
			{
				Type:       "prefix_list",
				BasePrefix: "70861/",
				Prefixes:   []string{"device-b/"},
			},
		},
	}

	plan, err := Compile(context.Background(), cfg, "data/", nil)
	require.NoError(t, err)
	require.Equal(t, []string{
		"data/70860/device-a/",
		"data/70861/device-b/",
	}, plan.Prefixes)
}

func TestCompileDatePartitions(t *testing.T) {
	cfg := &manifest.IndexScopeConfig{
		Type:       "date_partitions",
		BasePrefix: "70860/",
		Discover: &manifest.IndexScopeDiscoverConfig{Segments: []manifest.IndexScopeDiscoverSegment{
			{Index: 0, Allow: []string{"device-a", "device-b"}},
		}},
		Date: &manifest.IndexScopeDateConfig{
			SegmentIndex: 1,
			Format:       "2006-01-02",
			Range: &manifest.IndexScopeDateRange{
				After:  "2025-12-01",
				Before: "2025-12-03",
			},
		},
	}
	lister := &fakePrefixLister{prefixes: map[string][]string{
		"data/70860/": {"data/70860/device-a/", "data/70860/device-b/"},
	}}

	plan, err := Compile(context.Background(), cfg, "data/", lister)
	require.NoError(t, err)
	require.Equal(t, []string{
		"data/70860/device-a/2025-12-01/",
		"data/70860/device-a/2025-12-02/",
		"data/70860/device-b/2025-12-01/",
		"data/70860/device-b/2025-12-02/",
	}, plan.Prefixes)
}

func TestCompileDatePartitionsRequiresLister(t *testing.T) {
	cfg := &manifest.IndexScopeConfig{
		Type: "date_partitions",
		Date: &manifest.IndexScopeDateConfig{
			SegmentIndex: 0,
			Range: &manifest.IndexScopeDateRange{
				After:  time.Now().Format("2006-01-02"),
				Before: time.Now().Add(24 * time.Hour).Format("2006-01-02"),
			},
		},
	}

	_, err := Compile(context.Background(), cfg, "", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires provider prefix listing")
}
