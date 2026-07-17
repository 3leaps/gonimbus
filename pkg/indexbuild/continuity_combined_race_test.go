package indexbuild

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// TestCombinedStreamingPublishAndContinuityLifecycle is the combined
// streaming-publish + continuity assertion: a three-generation lifecycle where every
// child run streams its verified multi-segment parent through the bounded
// one-descriptor reader into the spill-merge streaming publish, over a
// multi-prefix concurrent crawl. Run under the suite's -race gate this
// exercises the streaming publish machinery and the continuity activation
// together on one path — lineage generations, digest-bound parents, tombstone
// and reappear semantics, and full current-state row accounting at a row/
// segment volume that makes the parent stream walk many segments.
func TestCombinedStreamingPublishAndContinuityLifecycle(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 11, 8, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	const initial = 180
	key := func(i int) string { return fmt.Sprintf("data/p%d/obj-%03d.xml", i%6, i) }
	rel := func(i int) string { return fmt.Sprintf("p%d/obj-%03d.xml", i%6, i) }

	mkcfg := func(runID string, objs []provider.ObjectSummary, started time.Time) Config {
		cfg := contConfig(setRoot, runID, objs, started)
		// Small segments so the parent stream spans many segment files.
		cfg.TargetRowsPerSegment = 7
		return cfg
	}

	// Run 1 — baseline: 180 objects across six prefixes.
	var gen1 []provider.ObjectSummary
	for i := 0; i < initial; i++ {
		gen1 = append(gen1, obj(key(i), fmt.Sprintf(`"e1-%03d"`, i), int64(i+1), base.Add(time.Duration(i)*time.Minute)))
	}
	_, err := NewRunner(mkcfg("run1", gen1, base)).Build(ctx)
	require.NoError(t, err)

	snap1, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.True(t, snap1.Manifest.Lineage.Baseline)
	require.Greater(t, len(snap1.Manifest.Segments), 20, "parent must span many segments for the bounded stream")
	require.Equal(t, initial, snap1.Manifest.Counts.Rows)

	// Run 2 — churn: i%5==0 changed, i%5==1 deleted, 30 added.
	started2 := base.Add(2 * time.Hour)
	var gen2 []provider.ObjectSummary
	for i := 0; i < initial; i++ {
		switch i % 5 {
		case 0:
			gen2 = append(gen2, obj(key(i), fmt.Sprintf(`"e2-%03d"`, i), int64(i+1), started2))
		case 1:
			// deleted: not listed
		default:
			gen2 = append(gen2, obj(key(i), fmt.Sprintf(`"e1-%03d"`, i), int64(i+1), base.Add(time.Duration(i)*time.Minute)))
		}
	}
	for i := initial; i < initial+30; i++ {
		gen2 = append(gen2, obj(key(i), fmt.Sprintf(`"e2-%03d"`, i), int64(i+1), started2))
	}
	_, err = NewRunner(mkcfg("run2", gen2, started2)).Build(ctx)
	require.NoError(t, err)

	snap2, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.False(t, snap2.Manifest.Lineage.Baseline)
	require.Equal(t, 2, snap2.Manifest.Lineage.Generation)
	require.NotNil(t, snap2.Manifest.StateParent)
	require.Equal(t, "run1", snap2.Manifest.StateParent.RunID)
	require.Equal(t, snap1.Complete.ManifestSHA256, snap2.Manifest.StateParent.ManifestSHA256)

	_, rows2, err := ReadLatest(latestPath)
	require.NoError(t, err)
	require.Len(t, rows2, initial+30, "current state carries every key ever observed (tombstones included)")
	by2 := map[string]ObjectState{}
	for _, r := range rows2 {
		by2[r.RelKey] = r
	}
	var tombstoned2 int
	for i := 0; i < initial; i++ {
		state, ok := by2[rel(i)]
		require.True(t, ok, rel(i))
		switch i % 5 {
		case 0:
			require.Equal(t, fmt.Sprintf(`"e2-%03d"`, i), state.ETag)
			require.Nil(t, state.DeletedAt)
		case 1:
			require.NotNil(t, state.DeletedAt, "unobserved key under confirmed-complete coverage must be tombstoned: %s", rel(i))
			tombstoned2++
		default:
			require.Equal(t, fmt.Sprintf(`"e1-%03d"`, i), state.ETag)
			require.Nil(t, state.DeletedAt)
			require.Equal(t, "run1", state.FirstSeenRunID, "unchanged key keeps first-seen lineage")
		}
	}
	require.Equal(t, initial/5, tombstoned2)

	// Run 3 — reappear half the deleted (even i), keep the rest deleted.
	started3 := base.Add(4 * time.Hour)
	var gen3 []provider.ObjectSummary
	for i := 0; i < initial+30; i++ {
		if i < initial && i%5 == 1 && i%2 == 1 {
			continue // still deleted
		}
		etag := fmt.Sprintf(`"e3-%03d"`, i)
		gen3 = append(gen3, obj(key(i), etag, int64(i+1), started3))
	}
	_, err = NewRunner(mkcfg("run3", gen3, started3)).Build(ctx)
	require.NoError(t, err)

	snap3, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.Equal(t, 3, snap3.Manifest.Lineage.Generation)
	require.Equal(t, "run2", snap3.Manifest.StateParent.RunID)
	require.Equal(t, snap2.Complete.ManifestSHA256, snap3.Manifest.StateParent.ManifestSHA256)

	_, rows3, err := ReadLatest(latestPath)
	require.NoError(t, err)
	require.Len(t, rows3, initial+30)
	by3 := map[string]ObjectState{}
	for _, r := range rows3 {
		by3[r.RelKey] = r
	}
	for i := 0; i < initial; i++ {
		if i%5 != 1 {
			continue
		}
		state := by3[rel(i)]
		if i%2 == 1 {
			require.NotNil(t, state.DeletedAt, "still-deleted key stays tombstoned: %s", rel(i))
		} else {
			require.Nil(t, state.DeletedAt, "reappeared key clears its tombstone: %s", rel(i))
			require.Equal(t, fmt.Sprintf(`"e3-%03d"`, i), state.ETag)
		}
	}
}
