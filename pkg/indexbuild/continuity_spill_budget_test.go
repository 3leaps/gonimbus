package indexbuild

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// TestBuildContinuitySpillWorkspaceOverrideConstrainsMerge proves the
// Config.Spill.WorkspaceBytes override reaches the durable streaming merge. A
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
		cfg2.Spill.WorkspaceBytes = 1 // below the parent spill-run header; must trip
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
		cfg2.Spill.WorkspaceBytes = 64 << 20 // generous explicit override
		summary, err := NewRunner(cfg2).Build(ctx)
		require.NoError(t, err)
		// The successive merge staged the parent into the workspace, so the
		// observed peak is surfaced (capacity evidence) and stays under the bound.
		require.Positive(t, summary.PeakWorkspaceBytes, "successive merge must report a peak workspace")
		require.Less(t, summary.PeakWorkspaceBytes, int64(64<<20), "peak must stay under the sized bound")

		snap2, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
		require.NoError(t, err)
		require.False(t, snap2.Manifest.Lineage.Baseline, "run2 must advance to a continuous child")
		require.Equal(t, 2, snap2.Manifest.Lineage.Generation)
		require.Equal(t, "run1", snap2.Manifest.StateParent.RunID)
	})
}

// TestRetrySpillWorkspaceOverridePropagates proves the Spill override also reaches
// the merge through the public Retry path: a re-publish from sealed journals
// stages the parent into the workspace, so a tiny budget fails fail-closed
// (latest byte-identical) and a generous budget completes.
func TestRetrySpillWorkspaceOverridePropagates(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
		obj("data/b.xml", `"b1"`, 2, base),
	}, base)).Build(ctx)
	require.NoError(t, err)

	run2Start := base.Add(time.Hour)
	cfg2 := contConfig(setRoot, "run2", []provider.ObjectSummary{
		obj("data/a.xml", `"a2"`, 9, run2Start),
		obj("data/b.xml", `"b1"`, 2, base),
	}, run2Start)
	sum2, err := NewRunner(cfg2).Build(ctx)
	require.NoError(t, err)

	latestBefore, err := os.ReadFile(latestPath)
	require.NoError(t, err)

	// Undersized budget: Retry re-publish must fail closed, latest unchanged.
	rcTiny := retryConfigFor(cfg2, sum2)
	rcTiny.Spill.WorkspaceBytes = 1
	_, err = Retry(ctx, rcTiny)
	require.Error(t, err)
	require.ErrorContains(t, err, "MaxWorkspaceBytes exceeded")

	latestAfter, err := os.ReadFile(latestPath)
	require.NoError(t, err)
	require.Equal(t, latestBefore, latestAfter, "refused Retry must not advance latest")

	// Generous budget completes the same Retry re-publish.
	rcOK := retryConfigFor(cfg2, sum2)
	rcOK.Spill.WorkspaceBytes = 64 << 20
	_, err = Retry(ctx, rcOK)
	require.NoError(t, err)
}

// TestBuildSpillRecordBudgetConstrainsJournalValidation proves the Spill.RecordBytes
// override reaches the sealed-journal validation pass through public Build: an
// undersized record budget fails a successive build closed (typed MaxRecordBytes,
// latest unchanged) and a sufficient budget completes it.
func TestBuildSpillRecordBudgetConstrainsJournalValidation(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	run1Objs := []provider.ObjectSummary{obj("data/a.xml", `"a1"`, 1, base)}
	run2Objs := []provider.ObjectSummary{obj("data/a.xml", `"a2"`, 9, base.Add(time.Hour))}

	ctx := context.Background()
	setRoot := t.TempDir()
	latestPath := filepath.Join(setRoot, "latest.json")

	_, err := NewRunner(contConfig(setRoot, "run1", run1Objs, base)).Build(ctx)
	require.NoError(t, err)
	snap1, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)

	// Record budget of 1 byte is below any journal line (the header); Build seals
	// the journal then refuses at the validation pass, latest untouched.
	tiny := contConfig(setRoot, "run2", run2Objs, base.Add(time.Hour))
	tiny.Spill.RecordBytes = 1
	_, err = NewRunner(tiny).Build(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "MaxRecordBytes exceeded")
	snapAfter, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.Equal(t, snap1.Complete.ManifestSHA256, snapAfter.Complete.ManifestSHA256, "refused build must not advance latest")

	// A sufficient record budget completes the same successive build.
	ok := contConfig(setRoot, "run2", run2Objs, base.Add(time.Hour))
	ok.Spill.RecordBytes = 1 << 20
	_, err = NewRunner(ok).Build(ctx)
	require.NoError(t, err)
	snap2, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.Equal(t, 2, snap2.Manifest.Lineage.Generation)
}

// TestRetrySpillRecordBudgetPropagates proves Spill.RecordBytes bounds public
// Retry from its FIRST journal read: the undersized refusal comes from the
// coverage-provenance binding pass (boundCrawlPlanFromJournals), typed and
// fail-closed (latest byte-identical) — not merely from a later publish pass —
// and a sufficient budget completes.
func TestRetrySpillRecordBudgetPropagates(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/a.xml", `"a1"`, 1, base),
		obj("data/b.xml", `"b1"`, 2, base),
	}, base)).Build(ctx)
	require.NoError(t, err)

	run2Start := base.Add(time.Hour)
	cfg2 := contConfig(setRoot, "run2", []provider.ObjectSummary{
		obj("data/a.xml", `"a2"`, 9, run2Start),
		obj("data/b.xml", `"b1"`, 2, base),
	}, run2Start)
	sum2, err := NewRunner(cfg2).Build(ctx)
	require.NoError(t, err)

	latestBefore, err := os.ReadFile(latestPath)
	require.NoError(t, err)

	rcTiny := retryConfigFor(cfg2, sum2)
	rcTiny.Spill.RecordBytes = 1
	_, err = Retry(ctx, rcTiny)
	require.Error(t, err)
	require.ErrorContains(t, err, "MaxRecordBytes exceeded")
	// The refusal must come from the bounded provenance pass — Retry's first
	// journal read — with the same typed capacity error as the publish path.
	require.ErrorContains(t, err, "coverage provenance", "first (provenance) validation pass must be bounded")
	var sme *indexsubstrate.SpillMergeError
	require.True(t, errors.As(err, &sme), "expected typed spill-merge error, got %v", err)
	require.Equal(t, indexsubstrate.SpillMergeBudgetExhausted, sme.Category)
	latestAfter, err := os.ReadFile(latestPath)
	require.NoError(t, err)
	require.Equal(t, latestBefore, latestAfter, "refused Retry must not advance latest")

	rcOK := retryConfigFor(cfg2, sum2)
	rcOK.Spill.RecordBytes = 1 << 20
	_, err = Retry(ctx, rcOK)
	require.NoError(t, err)
}

// TestBoundCrawlPlanFromJournalsHonorsRecordBudget proves the provenance
// binding pass itself is capacity-bounded: an over-budget journal refuses with
// the typed SpillMergeBudgetExhausted (never an unbounded read), and the
// resolved default budget returns the sealed plan.
func TestBoundCrawlPlanFromJournalsHonorsRecordBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard-0001.jsonl")
	writeSealedJournalWithPlan(t, path, []string{"data/siteA/"})

	_, err := boundCrawlPlanFromJournals([]string{path}, 1)
	require.Error(t, err)
	require.ErrorContains(t, err, "MaxRecordBytes exceeded")
	var sme *indexsubstrate.SpillMergeError
	require.True(t, errors.As(err, &sme), "expected typed spill-merge error, got %v", err)
	require.Equal(t, indexsubstrate.SpillMergeBudgetExhausted, sme.Category)

	plan, err := boundCrawlPlanFromJournals([]string{path}, indexsubstrate.DefaultSpillMergeBudget().MaxRecordBytes)
	require.NoError(t, err)
	require.Equal(t, []string{"data/siteA/"}, plan)
}

// TestBuildRejectsInvalidSpillBudgetBeforeCrawl proves an explicit invalid
// library budget refuses public Build with typed SpillMergeInvalidConfig during
// config normalization — before any event, journal, crawl, or publish side
// effect; zero fields still select defaults.
func TestBuildRejectsInvalidSpillBudgetBeforeCrawl(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		mut  func(*Config)
		msg  string
	}{
		{"negative record budget", func(c *Config) { c.Spill.RecordBytes = -1 }, "MaxRecordBytes must be >= 1"},
		{"negative workspace budget", func(c *Config) { c.Spill.WorkspaceBytes = -1 }, "MaxWorkspaceBytes must be >= 1"},
		// An extreme finite value the raw-byte surfaces admit must refuse typed
		// pre-crawl — never reach the scanner-capacity translation and panic.
		{"record budget above supported maximum", func(c *Config) { c.Spill.RecordBytes = math.MaxInt64 }, "supported maximum"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setRoot := t.TempDir()
			cfg := contConfig(setRoot, "run1", []provider.ObjectSummary{obj("data/a.xml", `"a1"`, 1, base)}, base)
			tc.mut(&cfg)

			_, err := NewRunner(cfg).Build(context.Background())
			require.Error(t, err)
			require.ErrorContains(t, err, tc.msg)
			var sme *indexsubstrate.SpillMergeError
			require.True(t, errors.As(err, &sme), "expected typed spill-merge error, got %v", err)
			require.Equal(t, indexsubstrate.SpillMergeInvalidConfig, sme.Category)

			// Refused before side effects: no journal sealed, no latest advanced.
			require.NoDirExists(t, cfg.Paths.JournalDir, "refusal must precede journal creation")
			require.NoFileExists(t, cfg.Paths.LatestPath)
		})
	}
}

// TestRetryRejectsInvalidSpillBudgetBeforeSideEffects proves the same explicit
// invalid budget refuses public Retry during config normalization — before
// authority acquisition, the provenance journal read, or any publish side
// effect (latest byte-identical).
func TestRetryRejectsInvalidSpillBudgetBeforeSideEffects(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	cfg := contConfig(setRoot, "run1", []provider.ObjectSummary{obj("data/a.xml", `"a1"`, 1, base)}, base)
	sum, err := NewRunner(cfg).Build(ctx)
	require.NoError(t, err)

	latestBefore, err := os.ReadFile(latestPath)
	require.NoError(t, err)

	rc := retryConfigFor(cfg, sum)
	rc.Spill.RecordBytes = -1
	_, err = Retry(ctx, rc)
	require.Error(t, err)
	require.ErrorContains(t, err, "MaxRecordBytes must be >= 1")
	var sme *indexsubstrate.SpillMergeError
	require.True(t, errors.As(err, &sme), "expected typed spill-merge error, got %v", err)
	require.Equal(t, indexsubstrate.SpillMergeInvalidConfig, sme.Category)

	latestAfter, err := os.ReadFile(latestPath)
	require.NoError(t, err)
	require.Equal(t, latestBefore, latestAfter, "refused Retry must not touch latest")
}
