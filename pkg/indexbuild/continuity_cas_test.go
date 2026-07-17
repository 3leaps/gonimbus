package indexbuild

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// interposeOnFirstListProvider runs interpose exactly once, at the first List
// call — deterministically between the runner's lease-held parent capture
// (which happens before the crawl) and the publish-time latest CAS (which
// happens after it).
type interposeOnFirstListProvider struct {
	objects   []provider.ObjectSummary
	interpose func()
	once      sync.Once
}

func (p *interposeOnFirstListProvider) List(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	p.once.Do(p.interpose)
	var out []provider.ObjectSummary
	for _, obj := range p.objects {
		if strings.HasPrefix(obj.Key, opts.Prefix) {
			out = append(out, obj)
		}
	}
	return &provider.ListResult{Objects: out}, nil
}

func (*interposeOnFirstListProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (*interposeOnFirstListProvider) Close() error { return nil }

// TestBuildStaleParentWhenLatestAdvancesMidCrawl pins the conditional-latest /
// parent-advance CAS: run "racer" captures parent run1, and while its crawl is
// in flight the authoritative latest advances to a different valid same-set
// run. The publish-time CAS must refuse with ErrStaleParent, must not clobber
// the advanced latest, and must leave the racer's run unreachable from the
// authoritative latest lineage. The racer's own run artifacts may legitimately
// exist as a valid non-latest run awaiting GC — the CAS is checked after
// segments/manifest/complete are written, and that is not a defect.
//
// Concurrency model under test: cooperative writers serialized by the write
// lease. The mid-crawl advance simulates an actor that advanced latest outside
// that serialization (or after a lease epoch the racer no longer holds); the
// guarantee proven is that the racer cannot advance latest over state it did
// not capture — not an atomic-filesystem-CAS claim against arbitrary actors.
func TestBuildStaleParentWhenLatestAdvancesMidCrawl(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	latestPath := contRunPaths(setRoot, "run1").LatestPath

	// Run 1: the parent generation the racer will capture.
	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
	}, base)).Build(ctx)
	require.NoError(t, err)
	latestParent, err := os.ReadFile(latestPath)
	require.NoError(t, err)

	// A second legitimate build advances latest to run_interloper (a valid
	// same-set continuity child of run1), then latest is restored to run1 so
	// the racer captures run1 as its parent. Both runs' full digest chains
	// remain on disk throughout.
	_, err = NewRunner(contConfig(setRoot, "run_interloper", []provider.ObjectSummary{
		obj("data/a.xml", `"a2"`, 1, base.Add(time.Hour)),
	}, base.Add(time.Hour))).Build(ctx)
	require.NoError(t, err)
	latestInterloper, err := os.ReadFile(latestPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(latestPath, latestParent, 0o600))

	// The racer: captures parent run1, then latest advances mid-crawl.
	racer := contConfig(setRoot, "run_racer", nil, base.Add(2*time.Hour))
	racer.Source = Source{Provider: &interposeOnFirstListProvider{
		objects: []provider.ObjectSummary{
			obj("data/a.xml", `"a3"`, 1, base.Add(2*time.Hour)),
		},
		interpose: func() {
			require.NoError(t, os.WriteFile(latestPath, latestInterloper, 0o600))
		},
	}, ProviderName: "s3"}

	_, err = NewRunner(racer).Build(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)
	// The refusal is the advance-time CAS (captured run1 vs live interloper),
	// not an earlier capture/validation failure.
	require.Contains(t, err.Error(), "run_interloper")

	// The racer legitimately wrote its run artifacts before the late CAS —
	// segments/manifest/complete precede the advance decision. They form a
	// valid non-latest run for GC inventory; "no artifacts" is NOT the
	// contract here.
	require.DirExists(t, racer.Paths.SegmentDir)

	// No latest clobber: the advanced latest is byte-identical.
	latestAfter, err := os.ReadFile(latestPath)
	require.NoError(t, err)
	require.Equal(t, latestInterloper, latestAfter, "a stale racer must never advance or clobber latest")

	// The authoritative latest resolves to the interloper, whose lineage binds
	// run1 — the racer is unreachable from the authoritative chain.
	snap, err := indexsubstrate.OpenLatestPublishedSnapshotBounded(latestPath,
		indexsubstrate.DefaultMaxPublishedMarkerBytes, indexsubstrate.DefaultMaxPublishedManifestBytes)
	require.NoError(t, err)
	require.Equal(t, "run_interloper", snap.Complete.RunID)
	require.NotNil(t, snap.Manifest.StateParent)
	require.Equal(t, "run1", snap.Manifest.StateParent.RunID)
	require.NotEqual(t, "run_racer", snap.Manifest.StateParent.RunID)
}

// TestBuildFirstPublishRefusesWhenLatestAppearsMidCrawl pins the
// expected-absent arm of the CAS: a first publisher captures an absent latest,
// and while its crawl is in flight another first publisher's valid same-set
// latest appears. The late publisher must refuse with ErrStaleParent and must
// not overwrite the winner's latest.
func TestBuildFirstPublishRefusesWhenLatestAppearsMidCrawl(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)

	// The winning first publisher lives in a sibling tree for the same index
	// set; its latest bytes (with a digest chain that verifies from disk) are
	// what appears at the loser's latest path mid-crawl.
	winnerRoot := t.TempDir()
	_, err := NewRunner(contConfig(winnerRoot, "run_winner", []provider.ObjectSummary{
		obj("data/w.xml", `"w1"`, 1, base),
	}, base)).Build(ctx)
	require.NoError(t, err)
	winnerLatest, err := os.ReadFile(contRunPaths(winnerRoot, "run_winner").LatestPath)
	require.NoError(t, err)

	loserRoot := t.TempDir()
	loserLatestPath := contRunPaths(loserRoot, "run_loser").LatestPath
	loser := contConfig(loserRoot, "run_loser", nil, base.Add(time.Hour))
	loser.Source = Source{Provider: &interposeOnFirstListProvider{
		objects: []provider.ObjectSummary{
			obj("data/l.xml", `"l1"`, 1, base.Add(time.Hour)),
		},
		interpose: func() {
			require.NoError(t, os.WriteFile(loserLatestPath, winnerLatest, 0o600))
		},
	}, ProviderName: "s3"}

	_, err = NewRunner(loser).Build(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)

	latestAfter, err := os.ReadFile(loserLatestPath)
	require.NoError(t, err)
	require.Equal(t, winnerLatest, latestAfter, "the losing first publisher must not overwrite the winner's latest")

	snap, err := indexsubstrate.OpenLatestPublishedSnapshotBounded(loserLatestPath,
		indexsubstrate.DefaultMaxPublishedMarkerBytes, indexsubstrate.DefaultMaxPublishedManifestBytes)
	require.NoError(t, err)
	require.Equal(t, "run_winner", snap.Complete.RunID)
}
