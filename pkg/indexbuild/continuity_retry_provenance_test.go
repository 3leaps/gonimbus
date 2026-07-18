package indexbuild

// U4 coverage-authority remediation regressions. Coverage authorizes tombstones
// over verified-parent rows, so the plan that bounds coverage must be the exact
// observation universe — not a caller-forgeable value. These prove:
//   - the crawl-plan bytes used for the equality gate are the exact bytes crawled
//     (no lossy normalization opening a false-tombstone window);
//   - a scoped build's observation selector cannot silently reduce below the plan;
//   - ineligible coverage is refused before any side effect, not after a crawl;
//   - public Retry binds coverage to sealed-journal provenance and cannot widen
//     the tombstone universe, with legacy/disagreeing journals failing closed.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// scopedCountingConfig is scopedContConfig with a List-counting provider so a
// pre-side-effect refusal can be proven (zero List calls, no journal, no latest).
func scopedCountingConfig(setRoot, runID string, objs []provider.ObjectSummary, prefixes []string, started time.Time) (Config, *countingListProvider) {
	cfg := scopedContConfig(setRoot, runID, objs, prefixes, started)
	prov := &countingListProvider{inner: fakeProvider{objects: objs}}
	cfg.Source = Source{Provider: prov, ProviderName: "s3"}
	return cfg, prov
}

func requireRefusedBeforeSideEffects(t *testing.T, cfg Config, prov *countingListProvider, err error, wantErr string) {
	t.Helper()
	require.Error(t, err)
	require.Contains(t, err.Error(), wantErr)
	require.Zero(t, prov.lists.Load(), "refusal must precede any provider crawl")
	require.NoDirExists(t, cfg.Paths.JournalDir, "refusal must not create a journal")
	require.NoFileExists(t, cfg.Paths.LatestPath, "refusal must not publish latest")
}

// TestBuildRefusesNonCanonicalCrawlPlan proves the equality gate compares the
// exact bytes that drive LIST: a leading-slash or whitespace-padded plan entry
// (which would normalize to a different string than it crawls) is refused before
// any side effect, closing the lossy-normalization false-tombstone window.
func TestBuildRefusesNonCanonicalCrawlPlan(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	objs := []provider.ObjectSummary{obj("data/siteA/a1.xml", `"a1"`, 1, base)}
	cases := []struct {
		name string
		plan []string
	}{
		{"leading slash", []string{"/data/siteA/"}},
		{"trailing whitespace", []string{"data/siteA/ "}},
		{"leading whitespace", []string{" data/siteA/"}},
		{"empty entry", []string{""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRoot := t.TempDir()
			cfg, prov := scopedCountingConfig(setRoot, "run1", objs, tc.plan, base)
			_, err := NewRunner(cfg).Build(ctx)
			requireRefusedBeforeSideEffects(t, cfg, prov, err, "crawl prefix plan")
		})
	}
}

// TestBuildRefusesDurableSelectorReduction proves any durable build (scoped or
// unscoped) refuses match/filter that would observe fewer objects than its
// recorded plan attests complete, mirroring the CLI adapter's faithful-coverage
// gate at the engine seam so a direct-library caller cannot authorize false
// tombstones by filtering — and so the stamped plan is a truthful record of the
// observation universe.
func TestBuildRefusesDurableSelectorReduction(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	objs := []provider.ObjectSummary{obj("data/siteA/a1.xml", `"a1"`, 1, base)}
	cases := []struct {
		name string
		mut  func(cfg *Config)
	}{
		{"excludes", func(cfg *Config) { cfg.Match.Excludes = []string{"**/*.tmp"} }},
		{"include_hidden", func(cfg *Config) { cfg.Match.IncludeHidden = true }},
		{"non-default include", func(cfg *Config) { cfg.Match.Includes = []string{"data/siteA/**"} }},
		{"post-list filter", func(cfg *Config) { cfg.Filter = &match.CompositeFilter{} }},
	}
	for _, scoped := range []bool{true, false} {
		for _, tc := range cases {
			name := tc.name
			if scoped {
				name = "scoped/" + name
			} else {
				name = "unscoped/" + name
			}
			t.Run(name, func(t *testing.T) {
				setRoot := t.TempDir()
				plan := []string{"data/siteA/"}
				if !scoped {
					plan = nil
				}
				cfg, prov := scopedCountingConfig(setRoot, "run1", objs, plan, base)
				if !scoped {
					// Unscoped: matcher-derived, base coverage.
					cfg.CrawlPrefixes = nil
					cfg.Coverage = []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true}}
				}
				tc.mut(&cfg)
				_, err := NewRunner(cfg).Build(ctx)
				requireRefusedBeforeSideEffects(t, cfg, prov, err, "durable build")
			})
		}
	}
}

// TestBuildRefusesIneligibleCoverageForCrawlPlan proves exact-prefix but
// ineligible coverage (inferred / incomplete / gapped) refuses before the crawl
// and journal, not after — the faithful-attestation boundary is pre-side-effect.
func TestBuildRefusesIneligibleCoverageForCrawlPlan(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	objs := []provider.ObjectSummary{obj("data/siteA/a1.xml", `"a1"`, 1, base)}
	cases := []struct {
		name    string
		cov     CoverageAttestation
		wantErr string
	}{
		{"inferred basis", CoverageAttestation{Scope: &Scope{Prefix: "data/siteA/"}, Basis: CoverageBasisInferred, Complete: true}, "confirmed and complete"},
		{"incomplete", CoverageAttestation{Scope: &Scope{Prefix: "data/siteA/"}, Basis: CoverageBasisConfirmed, Complete: false}, "confirmed and complete"},
		{"gapped", CoverageAttestation{Scope: &Scope{Prefix: "data/siteA/"}, Basis: CoverageBasisConfirmed, Complete: true, Gaps: []Scope{{Prefix: "data/siteA/sub/"}}}, "must not declare gaps"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRoot := t.TempDir()
			cfg, prov := scopedCountingConfig(setRoot, "run1", objs, []string{"data/siteA/"}, base)
			cfg.Coverage = []CoverageAttestation{tc.cov}
			_, err := NewRunner(cfg).Build(ctx)
			requireRefusedBeforeSideEffects(t, cfg, prov, err, tc.wantErr)
		})
	}
}

// TestRetryRefusesCoverageWiderThanJournalPlan proves public Retry binds its
// coverage authority to the sealed journal's crawl-plan provenance: a siteA-only
// journal retried with widened base coverage refuses (latest byte-identical, the
// unobserved siteB parent row never tombstoned), while exact-plan coverage
// succeeds and retains siteB active.
func TestRetryRefusesCoverageWiderThanJournalPlan(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
		obj("data/siteB/b1.xml", `"b1"`, 2, base),
	}, base)).Build(ctx)
	require.NoError(t, err)

	// run2 scoped to siteA -> journal sealed with plan ["data/siteA/"].
	run2Start := base.Add(time.Hour)
	sum2, err := NewRunner(scopedContConfig(setRoot, "run2", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1x"`, 9, run2Start),
	}, []string{"data/siteA/"}, run2Start)).Build(ctx)
	require.NoError(t, err)

	latestBefore, err := os.ReadFile(latestPath)
	require.NoError(t, err)

	// Widened base coverage over the siteA-only journal must refuse.
	widen := retryConfigFor(scopedContConfig(setRoot, "run2", nil, []string{"data/siteA/"}, run2Start), sum2)
	widen.Coverage = []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true}}
	_, err = Retry(ctx, widen)
	require.Error(t, err)
	require.Contains(t, err.Error(), "retry coverage authority")

	latestAfter, err := os.ReadFile(latestPath)
	require.NoError(t, err)
	require.Equal(t, latestBefore, latestAfter, "refused Retry must not advance latest")
	_, rows, err := ReadLatest(latestPath)
	require.NoError(t, err)
	require.Nil(t, rowByKey(rows, "siteB/b1.xml").DeletedAt, "widened Retry must not tombstone the unobserved siteB parent")

	// Exact siteA coverage (matching the sealed plan) succeeds; siteB retained.
	_, err = Retry(ctx, retryConfigFor(scopedContConfig(setRoot, "run2", nil, []string{"data/siteA/"}, run2Start), sum2))
	require.NoError(t, err)
	_, rows2, err := ReadLatest(latestPath)
	require.NoError(t, err)
	require.Nil(t, rowByKey(rows2, "siteB/b1.xml").DeletedAt, "exact-plan Retry retains the unobserved siteB parent active")
}

// writeSealedJournalWithPlan writes a minimal valid sealed journal carrying the
// given crawl-plan provenance (nil = legacy / pre-provenance).
func writeSealedJournalWithPlan(t *testing.T, path string, crawlPrefixes []string) {
	t.Helper()
	started := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	jw, err := indexsubstrate.CreateJournal(path, indexsubstrate.JournalHeader{
		Type:               indexsubstrate.JournalHeaderType,
		JournalID:          "jrn_test_0001",
		IndexSetID:         "idx_cont",
		RunID:              "run_test",
		Shard:              "shard-0001",
		CrawlPrefixes:      crawlPrefixes,
		IndexSchemaVersion: indexsubstrate.IndexSchemaVersion,
		StartedAt:          started,
	})
	require.NoError(t, err)
	_, err = jw.Append(indexsubstrate.ObjectRecord{
		Op:         indexsubstrate.ObjectRecordOpObserve,
		RelKey:     "siteA/a1.xml",
		ObservedAt: started,
	})
	require.NoError(t, err)
	require.NoError(t, jw.Seal(started))
	require.NoError(t, jw.Close())
}

// TestBoundCrawlPlanFromJournalsFailsClosedOnLegacyJournal proves a journal with
// no recorded plan (pre-provenance) fails closed — recovery cannot prove the
// observation universe, so caller coverage is never accepted as tombstone
// authority over verified-parent rows.
func TestBoundCrawlPlanFromJournalsFailsClosedOnLegacyJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard-0001.jsonl")
	writeSealedJournalWithPlan(t, path, nil)
	_, err := boundCrawlPlanFromJournals([]string{path}, indexsubstrate.DefaultSpillMergeBudget().MaxRecordBytes)
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)
	require.Contains(t, err.Error(), "predates crawl-plan provenance")
}

// TestBoundCrawlPlanFromJournalsRefusesDisagreement proves multiple journals
// must agree on their sealed plan (order-independent); disagreement fails closed.
func TestBoundCrawlPlanFromJournalsRefusesDisagreement(t *testing.T) {
	dir := t.TempDir()
	p1 := filepath.Join(dir, "a", "shard-0001.jsonl")
	p2 := filepath.Join(dir, "b", "shard-0001.jsonl")
	writeSealedJournalWithPlan(t, p1, []string{"data/siteA/"})
	writeSealedJournalWithPlan(t, p2, []string{"data/siteB/"})
	_, err := boundCrawlPlanFromJournals([]string{p1, p2}, indexsubstrate.DefaultSpillMergeBudget().MaxRecordBytes)
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)
	require.Contains(t, err.Error(), "disagree")

	// Agreement (recorded in either order) returns the shared plan.
	p3 := filepath.Join(dir, "c", "shard-0001.jsonl")
	writeSealedJournalWithPlan(t, p3, []string{"data/siteA/"})
	plan, err := boundCrawlPlanFromJournals([]string{p1, p3}, indexsubstrate.DefaultSpillMergeBudget().MaxRecordBytes)
	require.NoError(t, err)
	require.Equal(t, []string{"data/siteA/"}, plan)
}

// rewriteJournalHeaderPlan mutates only the crawl_prefixes on a sealed journal's
// header line, leaving records and footer byte-identical — the exact
// post-seal tamper the integrity digest must catch.
func rewriteJournalHeaderPlan(t *testing.T, path string, newPlan []string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.Split(string(raw), "\n")
	require.NotEmpty(t, lines)
	var header map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &header))
	header["crawl_prefixes"] = newPlan
	edited, err := json.Marshal(header)
	require.NoError(t, err)
	lines[0] = string(edited)
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o600))
}

// TestJournalHeaderPlanMutationFailsIntegrity proves a post-seal edit of the
// header's crawl_prefixes is caught by the content digest: ValidateJournal,
// boundCrawlPlanFromJournals, and public Retry all refuse, and latest stays
// byte-identical (no false tombstone of the unobserved siteB parent).
func TestJournalHeaderPlanMutationFailsIntegrity(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
		obj("data/siteB/b1.xml", `"b1"`, 2, base),
	}, base)).Build(ctx)
	require.NoError(t, err)

	run2Start := base.Add(time.Hour)
	sum2, err := NewRunner(scopedContConfig(setRoot, "run2", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1x"`, 9, run2Start),
	}, []string{"data/siteA/"}, run2Start)).Build(ctx)
	require.NoError(t, err)
	require.Len(t, sum2.JournalPaths, 1)
	journalPath := sum2.JournalPaths[0]

	latestBefore, err := os.ReadFile(latestPath)
	require.NoError(t, err)

	// Forge the header to claim a widened base plan; records/footer untouched.
	rewriteJournalHeaderPlan(t, journalPath, []string{"data/"})

	// The integrity digest catches it at every consumer.
	_, err = indexsubstrate.ValidateJournal(journalPath)
	require.ErrorIs(t, err, indexsubstrate.ErrInvalidJournal)
	_, err = boundCrawlPlanFromJournals([]string{journalPath}, indexsubstrate.DefaultSpillMergeBudget().MaxRecordBytes)
	require.Error(t, err)

	// Retry with the widened coverage that the forged header would authorize.
	rc := retryConfigFor(scopedContConfig(setRoot, "run2", nil, []string{"data/siteA/"}, run2Start), sum2)
	rc.Coverage = []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true}}
	_, err = Retry(ctx, rc)
	require.Error(t, err)

	latestAfter, err := os.ReadFile(latestPath)
	require.NoError(t, err)
	require.Equal(t, latestBefore, latestAfter, "forged-header Retry must not advance latest")
	_, rows, err := ReadLatest(latestPath)
	require.NoError(t, err)
	require.Nil(t, rowByKey(rows, "siteB/b1.xml").DeletedAt, "forged provenance must not tombstone siteB")
}

// TestJournalRecordMutationFailsIntegrity proves record tampering and truncation
// are caught by the content digest on read.
func TestJournalRecordMutationFailsIntegrity(t *testing.T) {
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
	}, base)).Build(context.Background())
	require.NoError(t, err)
	journalPath := filepath.Join(setRoot, "runs", "run1", "journals", "shard-0001.jsonl")

	// Baseline: the untampered journal verifies.
	_, err = indexsubstrate.ValidateJournal(journalPath)
	require.NoError(t, err)

	// Mutate a byte inside the record line (change the observed rel_key).
	raw, err := os.ReadFile(journalPath)
	require.NoError(t, err)
	mutated := strings.Replace(string(raw), "a1.xml", "a2.xml", 1)
	require.NotEqual(t, string(raw), mutated)
	require.NoError(t, os.WriteFile(journalPath, []byte(mutated), 0o600))
	_, err = indexsubstrate.ValidateJournal(journalPath)
	require.ErrorIs(t, err, indexsubstrate.ErrInvalidJournal)
}

// writeUnauthenticatedJournalWithPlan writes a structurally valid sealed journal
// that records a crawl plan but carries no footer content digest — a legacy /
// pre-integrity artifact whose plan is not tamper-evident.
func writeUnauthenticatedJournalWithPlan(t *testing.T, path string, plan []string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	started := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	header := map[string]any{
		"type": "gonimbus.index.journal_header.v1", "journal_id": "jrn_legacy_0001",
		"index_set_id": "idx_cont", "run_id": "run_legacy", "shard": "shard-0001",
		"crawl_prefixes": plan, "index_schema_version": indexsubstrate.IndexSchemaVersion, "started_at": started,
	}
	record := map[string]any{
		"type": "gonimbus.index.object_record.v1", "journal_id": "jrn_legacy_0001",
		"sequence": 1, "op": "observe", "rel_key": "siteA/a1.xml", "observed_at": started,
	}
	footer := map[string]any{
		"type": "gonimbus.index.journal_footer.v1", "journal_id": "jrn_legacy_0001",
		"records": 1, "completed_at": started, // no content_sha256
	}
	var b strings.Builder
	for _, v := range []map[string]any{header, record, footer} {
		line, err := json.Marshal(v)
		require.NoError(t, err)
		b.Write(line)
		b.WriteByte('\n')
	}
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o600))
}

// TestBoundCrawlPlanRejectsUnauthenticatedPlan proves a plan carried by a
// journal with no content digest fails closed — a plan added to an
// unauthenticated artifact is not provenance, even though the journal is
// otherwise structurally valid and readable.
func TestBoundCrawlPlanRejectsUnauthenticatedPlan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard-0001.jsonl")
	writeUnauthenticatedJournalWithPlan(t, path, []string{"data/siteA/"})
	// Structurally valid (legacy footer without a digest is readable)...
	_, err := indexsubstrate.ValidateJournal(path)
	require.NoError(t, err)
	// ...but not trusted as provenance.
	_, err = boundCrawlPlanFromJournals([]string{path}, indexsubstrate.DefaultSpillMergeBudget().MaxRecordBytes)
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)
	require.Contains(t, err.Error(), "not content-integrity sealed")
}

// TestBoundCrawlPlanRejectsNonCanonicalJournalPlan proves a structurally valid
// journal whose recorded plan is non-canonical or duplicated is rejected rather
// than trimmed/deduplicated into agreement.
func TestBoundCrawlPlanRejectsNonCanonicalJournalPlan(t *testing.T) {
	dir := t.TempDir()
	for i, plan := range [][]string{
		{" data/siteA/"},
		{"/data/siteA/"},
		{"data/siteA/", "data/siteA/"},
	} {
		path := filepath.Join(dir, string(rune('a'+i)), "shard-0001.jsonl")
		writeSealedJournalWithPlan(t, path, plan)
		_, err := boundCrawlPlanFromJournals([]string{path}, indexsubstrate.DefaultSpillMergeBudget().MaxRecordBytes)
		require.Error(t, err)
		require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)
	}
}

// sealScopedJournalForRun writes a genuine sealed scoped journal for a
// not-yet-published run (via the same journal writer Build uses), so recovery
// can be exercised over a parent without first publishing the run.
func sealScopedJournalForRun(t *testing.T, setRoot, runID string, plan []string, objs []provider.ObjectSummary, started time.Time) string {
	t.Helper()
	paths := contRunPaths(setRoot, runID)
	journalPath := filepath.Join(paths.JournalDir, "shard-0001.jsonl")
	jw, err := newJournalWriter(journalWriterConfig{
		Path:          journalPath,
		IndexSetID:    "idx_cont",
		RunID:         runID,
		StartedAt:     started,
		BaseURI:       "s3://bucket/data/",
		BasePrefix:    "data/",
		CrawlPrefixes: plan,
		Now:           func() time.Time { return started },
	})
	require.NoError(t, err)
	for _, o := range objs {
		require.NoError(t, jw.WriteObject(context.Background(), &output.ObjectRecord{
			Key: o.Key, ETag: o.ETag, Size: o.Size, LastModified: o.LastModified, StorageClass: o.StorageClass,
		}))
	}
	require.NoError(t, jw.Seal())
	require.NoError(t, jw.Close())
	return journalPath
}

// TestRetryRecoversNotYetPublishedScopedJournal proves a genuine recovery: a
// sealed but never-published run2 journal bound to siteA publishes over its
// siteA+siteB parent with exact siteA coverage (siteB retained active), while
// widened base coverage over the same honest journal refuses.
func TestRetryRecoversNotYetPublishedScopedJournal(t *testing.T) {
	ctx := context.Background()
	setRoot := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	latestPath := filepath.Join(setRoot, "latest.json")

	_, err := NewRunner(contConfig(setRoot, "run1", []provider.ObjectSummary{
		obj("data/siteA/a1.xml", `"a1"`, 1, base),
		obj("data/siteB/b1.xml", `"b1"`, 2, base),
	}, base)).Build(ctx)
	require.NoError(t, err)

	run2Start := base.Add(time.Hour)
	journalPath := sealScopedJournalForRun(t, setRoot, "run2", []string{"data/siteA/"},
		[]provider.ObjectSummary{obj("data/siteA/a1.xml", `"a1x"`, 9, run2Start)}, run2Start)

	baseRetry := func() RetryConfig {
		return RetryConfig{
			IndexSetID:   "idx_cont",
			RunID:        "run2",
			BaseURI:      "s3://bucket/data/",
			Paths:        contRunPaths(setRoot, "run2"),
			JournalPaths: []string{journalPath},
			RunStartedAt: run2Start,
		}
	}

	// Widened base coverage over the honest siteA journal refuses; run2 unpublished.
	widen := baseRetry()
	widen.Coverage = []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true}}
	_, err = Retry(ctx, widen)
	require.Error(t, err)
	require.Contains(t, err.Error(), "retry coverage authority")
	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.Equal(t, "run1", snap.Complete.RunID, "refused recovery must not publish run2")

	// Exact siteA coverage recovers run2 over run1; siteB retained active.
	exact := baseRetry()
	exact.Coverage = []CoverageAttestation{{Scope: &Scope{Prefix: "data/siteA/"}, Basis: CoverageBasisConfirmed, Complete: true}}
	_, err = Retry(ctx, exact)
	require.NoError(t, err)
	snap2, err := indexsubstrate.OpenLatestPublishedSnapshot(latestPath)
	require.NoError(t, err)
	require.Equal(t, "run2", snap2.Complete.RunID)
	require.Equal(t, 2, snap2.Manifest.Lineage.Generation)
	_, rows, err := ReadLatest(latestPath)
	require.NoError(t, err)
	require.Equal(t, `"a1x"`, rowByKey(rows, "siteA/a1.xml").ETag)
	b1 := rowByKey(rows, "siteB/b1.xml")
	require.Nil(t, b1.DeletedAt, "unobserved siteB parent retained active on recovery")
	require.Equal(t, "run1", b1.LastSeenRunID)
}

// TestUnscopedNarrowCoverageRefusesBeforeSideEffects proves the coherent
// unscoped contract (Option A): a narrower-than-base coverage refuses at Build
// before any side effect, so no snapshot is produced whose own Retry (which
// binds coverage to the stamped [base] plan) it could not recover. The
// full-base unscoped Build-then-Retry symmetry is proven by
// TestRunnerBuildPublishesDeterministicSnapshotAndRetryParity under these gates.
func TestUnscopedNarrowCoverageRefusesBeforeSideEffects(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	setRoot := t.TempDir()
	cfg, prov := scopedCountingConfig(setRoot, "run1",
		[]provider.ObjectSummary{obj("data/siteA/a1.xml", `"a1"`, 1, base)}, nil, base)
	cfg.CrawlPrefixes = nil
	cfg.Coverage = []CoverageAttestation{{Scope: &Scope{Prefix: "data/siteA/"}, Basis: CoverageBasisConfirmed, Complete: true}}
	_, err := NewRunner(cfg).Build(ctx)
	requireRefusedBeforeSideEffects(t, cfg, prov, err, "not in the crawl prefix plan")
}
