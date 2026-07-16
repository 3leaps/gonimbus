package indexbuild

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexenrich"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// listHeadProvider serves List (for build crawl) and Head (for enrich) from a
// fixed object/metadata set.
type listHeadProvider struct {
	objects []provider.ObjectSummary
	metas   map[string]*provider.ObjectMeta
}

func (p *listHeadProvider) List(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	var out []provider.ObjectSummary
	for _, o := range p.objects {
		if strings.HasPrefix(o.Key, opts.Prefix) {
			out = append(out, o)
		}
	}
	return &provider.ListResult{Objects: out}, nil
}

func (p *listHeadProvider) Head(_ context.Context, key string) (*provider.ObjectMeta, error) {
	if m := p.metas[key]; m != nil {
		return m, nil
	}
	return nil, provider.ErrNotFound
}

func (p *listHeadProvider) Close() error { return nil }

// TestBuildEnrichBuildPreservesHeadEnrichment is the anti-regression for
// dropping HEAD (Slice-C) enrichment across continuity: build -> enrich ->
// build, where the second build captures the enriched latest as its exact parent
// and unchanged observed rows retain their HEAD enrichment fields.
func TestBuildEnrichBuildPreservesHeadEnrichment(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	objs := []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
		obj("data/b.xml", `"b1"`, 2, base),
	}
	archived := "ARCHIVED"
	prov := &listHeadProvider{
		objects: objs,
		metas: map[string]*provider.ObjectMeta{
			"data/a.xml": {ObjectSummary: obj("data/a.xml", `"a1"`, 1, base), ContentType: "application/xml", ArchiveStatus: archived},
			"data/b.xml": {ObjectSummary: obj("data/b.xml", `"b1"`, 2, base), ContentType: "text/plain"},
		},
	}

	// Build run1 (baseline, no HEAD enrichment yet).
	cfg1 := contConfig(setRoot, "run1", objs, base)
	cfg1.Source = Source{Provider: prov, ProviderName: "s3"}
	_, err := NewRunner(cfg1).Build(ctx)
	require.NoError(t, err)

	// Enrich -> a new run carrying HEAD content-type / archive-status. The enrich
	// path does not emit continuity lineage, so this run is a verified
	// pre-continuity state source (a legacy-shaped, no-lineage latest).
	enrichRes, err := indexenrich.Run(ctx, indexenrich.Config{
		IndexSetID:     "idx_cont",
		BaseURI:        "s3://bucket/data/",
		Provider:       prov,
		SegmentSetRoot: setRoot,
		JournalRoot:    filepath.Join(setRoot, "enrich-journals"),
	})
	require.NoError(t, err)
	enrichedSnap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.Nil(t, enrichedSnap.Manifest.Lineage, "enrich run is pre-continuity (no lineage)")
	_, enrichedRows, err := ReadLatest(latestPath)
	require.NoError(t, err)
	ea := rowByKey(enrichedRows, "a.xml")
	require.NotNil(t, ea.ContentType, "enrich sets content-type")
	require.Equal(t, "application/xml", *ea.ContentType)

	// Build run3 over the enriched latest; a and b unchanged.
	cfg3 := contConfig(setRoot, "run3", objs, base.Add(2*time.Hour))
	cfg3.Source = Source{Provider: prov, ProviderName: "s3"}
	_, err = NewRunner(cfg3).Build(ctx)
	require.NoError(t, err)

	// Pre-continuity baseline (case 2): a build over a verified no-lineage parent
	// becomes a baseline generation 1 bound to that parent as a state source.
	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.NotNil(t, snap.Manifest.Lineage)
	require.True(t, snap.Manifest.Lineage.Baseline, "build over pre-continuity parent is a baseline")
	require.Equal(t, indexsubstrate.LineageBaselineGeneration, snap.Manifest.Lineage.Generation)
	require.NotNil(t, snap.Manifest.StateParent)
	require.Equal(t, enrichRes.RunID, snap.Manifest.StateParent.RunID, "state parent bound to the enriched run")

	_, rows, err := ReadLatest(latestPath)
	require.NoError(t, err)
	a := rowByKey(rows, "a.xml")
	// HEAD enrichment preserved for the unchanged observed row.
	require.NotNil(t, a.ContentType, "content-type must survive build over enriched parent")
	require.Equal(t, "application/xml", *a.ContentType)
	require.NotNil(t, a.ArchiveStatus)
	require.Equal(t, archived, *a.ArchiveStatus)
	require.NotNil(t, a.HeadEnrichedAt, "head-enriched-at must survive")
	// First-seen lineage still points at the original run.
	require.Equal(t, "run1", a.FirstSeenRunID)
}
