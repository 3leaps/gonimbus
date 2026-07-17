package indexbuild

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// TestBuildContinuitySpillWorkspaceOverrideConstrainsMerge proves the
// Config.SpillWorkspaceBytes override reaches the durable streaming merge. A
// successive build stages the full prior current-state into the spill workspace
// before merging, so a 1-byte budget must fail that stage with the typed
// MaxWorkspaceBytes error while leaving the prior latest intact (no clobber), and
// a generous explicit budget must let the same successive build complete.
func TestBuildContinuitySpillWorkspaceOverrideConstrainsMerge(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	run1Objs := []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
		obj("data/b.xml", `"b1"`, 2, base),
		obj("data/c.xml", `"c1"`, 3, base),
	}
	run2Objs := []provider.ObjectSummary{
		obj("data/a.xml", `"a2"`, 9, base.Add(time.Hour)),
		obj("data/c.xml", `"c1"`, 3, base),
		obj("data/d.xml", `"d1"`, 4, base.Add(time.Hour)),
	}

	t.Run("tiny budget fails successive merge without clobber", func(t *testing.T) {
		ctx := context.Background()
		setRoot := t.TempDir()
		latestPath := filepath.Join(setRoot, "latest.json")

		_, err := NewRunner(contConfig(setRoot, "run1", run1Objs, base)).Build(ctx)
		require.NoError(t, err)
		snap1, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
		require.NoError(t, err)

		cfg2 := contConfig(setRoot, "run2", run2Objs, base.Add(time.Hour))
		cfg2.SpillWorkspaceBytes = 1 // below the parent spill-run header; must trip
		_, err = NewRunner(cfg2).Build(ctx)
		require.Error(t, err)
		require.ErrorContains(t, err, "MaxWorkspaceBytes exceeded")

		// Fail-closed: latest still points at run1's baseline, unchanged.
		snapAfter, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
		require.NoError(t, err)
		require.True(t, snapAfter.Manifest.Lineage.Baseline, "latest must remain run1 baseline")
		require.Equal(t, snap1.Complete.ManifestSHA256, snapAfter.Complete.ManifestSHA256, "no latest advance")
	})

	t.Run("generous budget completes successive merge", func(t *testing.T) {
		ctx := context.Background()
		setRoot := t.TempDir()
		latestPath := filepath.Join(setRoot, "latest.json")

		_, err := NewRunner(contConfig(setRoot, "run1", run1Objs, base)).Build(ctx)
		require.NoError(t, err)

		cfg2 := contConfig(setRoot, "run2", run2Objs, base.Add(time.Hour))
		cfg2.SpillWorkspaceBytes = 64 << 20 // generous explicit override
		_, err = NewRunner(cfg2).Build(ctx)
		require.NoError(t, err)

		snap2, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
		require.NoError(t, err)
		require.False(t, snap2.Manifest.Lineage.Baseline, "run2 must advance to a continuous child")
		require.Equal(t, 2, snap2.Manifest.Lineage.Generation)
		require.Equal(t, "run1", snap2.Manifest.StateParent.RunID)
	})
}
