package cmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/match"
)

func TestComputeFiltersHashFromConfig_StableForEquivalentInputs(t *testing.T) {
	cfg1 := &match.FilterConfig{
		Size: &match.SizeFilterConfig{Min: "1KB"},
		Modified: &match.DateFilterConfig{
			After:  "2025-12-01",
			Before: "2026-01-01",
		},
		KeyRegex: "\\.xml$",
	}

	cfg2 := &match.FilterConfig{
		Size: &match.SizeFilterConfig{Min: "1000B"},
		Modified: &match.DateFilterConfig{
			After:  "2025-12-01T00:00:00Z",
			Before: "2026-01-01T00:00:00Z",
		},
		KeyRegex: "  \\.xml$  ",
	}

	h1, err := computeFiltersHashFromConfig(cfg1)
	require.NoError(t, err)
	require.NotEmpty(t, h1)

	h2, err := computeFiltersHashFromConfig(cfg2)
	require.NoError(t, err)
	require.Equal(t, h1, h2)
}

func TestComputeFiltersHashFromConfig_ChangesWhenValueChanges(t *testing.T) {
	cfg := &match.FilterConfig{
		Size: &match.SizeFilterConfig{Min: "1KB"},
		Modified: &match.DateFilterConfig{
			After:  "2025-12-01",
			Before: "2026-01-01",
		},
		KeyRegex: "\\.xml$",
	}

	h1, err := computeFiltersHashFromConfig(cfg)
	require.NoError(t, err)
	require.NotEmpty(t, h1)

	cfg.KeyRegex = "\\.json$"
	h2, err := computeFiltersHashFromConfig(cfg)
	require.NoError(t, err)
	require.NotEmpty(t, h2)
	require.NotEqual(t, h1, h2)
}

func TestComputeFiltersHashFromConfig_RejectsInvalidBounds(t *testing.T) {
	cfg := &match.FilterConfig{
		Size: &match.SizeFilterConfig{Min: "2KB", Max: "1KB"},
	}
	_, err := computeFiltersHashFromConfig(cfg)
	require.Error(t, err)
}

func TestComputeFiltersHashFromConfig_NormalizesDatesToUTC(t *testing.T) {
	cfg := &match.FilterConfig{Modified: &match.DateFilterConfig{After: "2025-12-01T00:00:00-05:00"}}
	h, err := computeFiltersHashFromConfig(cfg)
	require.NoError(t, err)
	require.NotEmpty(t, h)

	cfg2 := &match.FilterConfig{Modified: &match.DateFilterConfig{After: time.Date(2025, 12, 1, 5, 0, 0, 0, time.UTC).Format(time.RFC3339)}}
	h2, err := computeFiltersHashFromConfig(cfg2)
	require.NoError(t, err)
	require.Equal(t, h, h2)
}
