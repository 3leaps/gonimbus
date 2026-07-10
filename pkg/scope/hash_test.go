package scope

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/manifest"
)

func TestHashConfig_StableForEquivalentInputs(t *testing.T) {
	cfg1 := &manifest.IndexScopeConfig{
		Type:       "prefix_list",
		BasePrefix: "data/",
		Prefixes:   []string{"b/", "a/", "a/"},
	}
	cfg2 := &manifest.IndexScopeConfig{
		Type:       "prefix_list",
		BasePrefix: "data/",
		Prefixes:   []string{"a/", "b/"},
	}

	h1, err := HashConfig(cfg1)
	require.NoError(t, err)
	h2, err := HashConfig(cfg2)
	require.NoError(t, err)
	require.Equal(t, h1, h2)
}

func TestHashConfig_ChangesWhenScopeChanges(t *testing.T) {
	cfg := &manifest.IndexScopeConfig{
		Type:       "date_partitions",
		BasePrefix: "data/",
		Date: &manifest.IndexScopeDateConfig{
			SegmentIndex: 0,
			Range: &manifest.IndexScopeDateRange{
				After:  "2025-12-01",
				Before: "2025-12-03",
			},
		},
	}

	h1, err := HashConfig(cfg)
	require.NoError(t, err)

	cfg.Date.Range.Before = "2025-12-04"
	h2, err := HashConfig(cfg)
	require.NoError(t, err)
	require.NotEqual(t, h1, h2)
}

func TestHashConfig_ChangesWhenDiscoverAllowChanges(t *testing.T) {
	cfg := &manifest.IndexScopeConfig{
		Type:       "date_partitions",
		BasePrefix: "data/",
		Discover: &manifest.IndexScopeDiscoverConfig{
			Segments: []manifest.IndexScopeDiscoverSegment{
				{Index: 0, Allow: []string{"site-a", "site-b"}},
			},
		},
		Date: &manifest.IndexScopeDateConfig{
			SegmentIndex: 1,
			Range: &manifest.IndexScopeDateRange{
				After:  "2026-04-01",
				Before: "2026-05-01",
			},
		},
	}
	h1, err := HashConfig(cfg)
	require.NoError(t, err)

	cfg.Discover.Segments[0].Allow = []string{"site-a"}
	h2, err := HashConfig(cfg)
	require.NoError(t, err)
	require.NotEqual(t, h1, h2)

	cfg.Discover.Segments[0].GlobAllow = []string{"site-*"}
	h3, err := HashConfig(cfg)
	require.NoError(t, err)
	require.NotEqual(t, h2, h3)
}

func TestHashConfig_RejectsInvalidRange(t *testing.T) {
	cfg := &manifest.IndexScopeConfig{
		Type: "date_partitions",
		Date: &manifest.IndexScopeDateConfig{
			SegmentIndex: 0,
			Range: &manifest.IndexScopeDateRange{
				After:  time.Now().Format("2006-01-02"),
				Before: time.Now().Add(-24 * time.Hour).Format("2006-01-02"),
			},
		},
	}

	_, err := HashConfig(cfg)
	require.Error(t, err)
}
