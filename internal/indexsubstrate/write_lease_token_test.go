package indexsubstrate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func readWriteLeaseToken(t *testing.T, root string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, writeLeaseFileName))
	require.NoError(t, err)
	var doc writeLeaseDoc
	require.NoError(t, json.Unmarshal(raw, &doc))
	return doc.Token
}

// The token names one acquisition. Acquisitions are sequential because the lease
// is exclusive, so a handoff is a release followed by an acquire — and those can
// land in one clock tick on a platform whose clock is coarse.
func TestWriteLeaseTokenDiffersAcrossAcquisitions(t *testing.T) {
	root := t.TempDir()
	seen := make(map[string]int, 64)
	for i := range 64 {
		lease, err := AcquireWriteLease(root, "idx_test", "holder", 0)
		require.NoError(t, err)
		token := readWriteLeaseToken(t, root)
		require.NoError(t, lease.Release())

		require.NotEmpty(t, token, "every acquisition must record a token")
		if previous, collided := seen[token]; collided {
			t.Fatalf("acquisitions %d and %d recorded the same token %q", previous, i, token)
		}
		seen[token] = i
	}
}

// The defect this replaced was a token that was only a clock reading, so two
// acquisitions in one tick were indistinguishable. Whether that collision shows
// up in a run depends on the platform's clock resolution, and the platform this
// suite usually runs on has a fine one — so pin the dependence on the clock
// directly, rather than waiting for a same-tick collision to appear.
func TestWriteLeaseTokenIsNotAClockReadingAlone(t *testing.T) {
	root := t.TempDir()
	lease, err := AcquireWriteLease(root, "idx_test", "holder", 0)
	require.NoError(t, err)
	token := readWriteLeaseToken(t, root)
	require.NoError(t, lease.Release())

	_, err = strconv.ParseInt(token, 10, 64)
	require.Error(t, err,
		"a token that parses as a single integer carries nothing the clock did not supply")
	require.Equal(t, token, lease.Token(), "the recorded token must be the one the lease reports")
}

func TestMaintenanceWriteLeaseTokenIsDistinctAndLabelled(t *testing.T) {
	// Maintenance acquisition refuses a path alias, and the temp root is one on
	// darwin. Resolving unconditionally matches what the existing lease tests do.
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	first, err := AcquireWriteLease(root, "idx_test", "holder", 0)
	require.NoError(t, err)
	require.NoError(t, first.Release())

	seen := make(map[string]struct{}, 8)
	for range 8 {
		lease, err := AcquireWriteLeaseForMaintenance(root, "idx_test", "holder")
		require.NoError(t, err)
		token := lease.Token()
		require.NoError(t, lease.Release())

		require.True(t, strings.HasPrefix(token, "maintenance-"),
			"a maintenance acquisition must stay identifiable as one")
		_, collided := seen[token]
		require.False(t, collided, "maintenance acquisitions must be distinguishable from each other")
		seen[token] = struct{}{}
	}
}
