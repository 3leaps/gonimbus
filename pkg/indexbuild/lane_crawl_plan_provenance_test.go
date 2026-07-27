package indexbuild

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
)

// writeSealedLaneJournal seals a journal carrying an explicit crawl-plan mode so
// the recovery-authority branches can be exercised over genuine sealed artifacts
// (content-integrity digest included) rather than hand-built headers.
func writeSealedLaneJournal(t *testing.T, path string, ordinal int, mode string, crawlPrefixes []string) {
	t.Helper()
	started := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	jw, err := indexsubstrate.CreateJournal(path, indexsubstrate.JournalHeader{
		Type:               indexsubstrate.JournalHeaderType,
		JournalID:          fmt.Sprintf("jrn_run_lane_%04d", ordinal),
		IndexSetID:         "idx_lane",
		RunID:              "run_lane",
		Shard:              fmt.Sprintf("shard-%04d", ordinal),
		CrawlPrefixes:      crawlPrefixes,
		CrawlPlanMode:      mode,
		IndexSchemaVersion: indexsubstrate.IndexSchemaVersion,
		StartedAt:          started,
	})
	require.NoError(t, err)
	_, err = jw.Append(indexsubstrate.ObjectRecord{
		Op:         indexsubstrate.ObjectRecordOpObserve,
		RelKey:     fmt.Sprintf("lane%d/a1.xml", ordinal),
		ObservedAt: started,
	})
	require.NoError(t, err)
	require.NoError(t, jw.Seal(started))
	require.NoError(t, jw.Close())
}

func laneJournalPath(t *testing.T, dir string, ordinal int) string {
	t.Helper()
	return filepath.Join(dir, fmt.Sprintf("shard-%04d.jsonl", ordinal))
}

func boundLanePlan(t *testing.T, paths ...string) ([]string, error) {
	t.Helper()
	return boundCrawlPlanFromJournals(paths, indexsubstrate.DefaultSpillMergeBudget().MaxRecordBytes)
}

// TestLaneLocalPlansUnionAcrossFullSet proves the accepting branch: a complete
// lane-local set derives the canonical union of the lane subsets, which is the
// whole-run plan. This is the negative control for the omitted-lane exploit
// below — without it, a refusal there would prove nothing, since a rule that
// refuses everything would also "pass".
func TestLaneLocalPlansUnionAcrossFullSet(t *testing.T) {
	dir := t.TempDir()
	laneA, laneB := laneJournalPath(t, dir, 1), laneJournalPath(t, dir, 2)
	writeSealedLaneJournal(t, laneA, 1, indexsubstrate.CrawlPlanModeLaneLocal, []string{"data/siteA/"})
	writeSealedLaneJournal(t, laneB, 2, indexsubstrate.CrawlPlanModeLaneLocal, []string{"data/siteB/"})

	plan, err := boundLanePlan(t, laneA, laneB)
	require.NoError(t, err)
	require.Equal(t, []string{"data/siteA/", "data/siteB/"}, plan)

	// The derived union is exactly the authority the whole-run coverage claims.
	require.NoError(t, validateCoverageMatchesCrawlPlan("s3://bucket/data/", plan, fullLaneCoverage()))
}

// TestOmittedLaneCannotWidenCoverageAuthority is the authority exploit. A single
// sealed lane is an individually valid, integrity-sealed journal; under the
// whole-plan-in-every-header form it would attest complete coverage of prefixes
// it never listed, so a recovery supplying only that lane with legitimate
// whole-run coverage would tombstone the omitted lane's rows. Lane-local
// provenance narrows the derived plan to what was actually supplied, so the
// coverage gate refuses before any publish artifact or latest change.
func TestOmittedLaneCannotWidenCoverageAuthority(t *testing.T) {
	dir := t.TempDir()
	laneA := laneJournalPath(t, dir, 1)
	writeSealedLaneJournal(t, laneA, 1, indexsubstrate.CrawlPlanModeLaneLocal, []string{"data/siteA/"})
	writeSealedLaneJournal(t, laneJournalPath(t, dir, 2), 2, indexsubstrate.CrawlPlanModeLaneLocal, []string{"data/siteB/"})

	// Only lane A is supplied; lane B is silently omitted.
	plan, err := boundLanePlan(t, laneA)
	require.NoError(t, err)
	require.Equal(t, []string{"data/siteA/"}, plan, "an omitted lane must narrow the derived plan, never inherit whole-run coverage")

	err = validateCoverageMatchesCrawlPlan("s3://bucket/data/", plan, fullLaneCoverage())
	require.Error(t, err)
	require.Contains(t, err.Error(), "not in the crawl prefix plan")
}

// TestLaneLocalJournalsWithoutModeCannotPresentAsLegacy proves the discriminator
// is not cosmetic: lane-local journals that fail to record the mode fall into the
// legacy branch, where their disjoint subsets are a plan disagreement and refuse.
// The mode must be written, not inferred from the data.
func TestLaneLocalJournalsWithoutModeCannotPresentAsLegacy(t *testing.T) {
	dir := t.TempDir()
	laneA, laneB := laneJournalPath(t, dir, 1), laneJournalPath(t, dir, 2)
	writeSealedLaneJournal(t, laneA, 1, "", []string{"data/siteA/"})
	writeSealedLaneJournal(t, laneB, 2, "", []string{"data/siteB/"})

	_, err := boundLanePlan(t, laneA, laneB)
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)
	require.Contains(t, err.Error(), "disagree on their crawl-prefix plan")
}

// TestDuplicateLaneMembershipRefused is the control the earlier data-derived
// trichotomy could not reach. A duplicate-assignment defect severe enough to give
// two lanes identical plans is exactly the input an "all plans identical means
// legacy" rule reads as its accepting branch. With an explicit mode, identical
// lane-local plans are reachable as the duplicate-membership refusal they are.
func TestDuplicateLaneMembershipRefused(t *testing.T) {
	dir := t.TempDir()
	laneA, laneB := laneJournalPath(t, dir, 1), laneJournalPath(t, dir, 2)
	writeSealedLaneJournal(t, laneA, 1, indexsubstrate.CrawlPlanModeLaneLocal, []string{"data/siteA/"})
	writeSealedLaneJournal(t, laneB, 2, indexsubstrate.CrawlPlanModeLaneLocal, []string{"data/siteA/"})

	_, err := boundLanePlan(t, laneA, laneB)
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)
	require.Contains(t, err.Error(), "must be disjoint")
}

// TestPartiallyOverlappingLaneMembershipRefused pins that duplicate detection is
// per exact plan entry across the set, not merely a whole-plan comparison.
func TestPartiallyOverlappingLaneMembershipRefused(t *testing.T) {
	dir := t.TempDir()
	laneA, laneB := laneJournalPath(t, dir, 1), laneJournalPath(t, dir, 2)
	writeSealedLaneJournal(t, laneA, 1, indexsubstrate.CrawlPlanModeLaneLocal, []string{"data/siteA/", "data/siteB/"})
	writeSealedLaneJournal(t, laneB, 2, indexsubstrate.CrawlPlanModeLaneLocal, []string{"data/siteB/", "data/siteC/"})

	_, err := boundLanePlan(t, laneA, laneB)
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)
	require.Contains(t, err.Error(), `"data/siteB/"`)
}

// TestMixedProvenanceModesFailClosed proves a set mixing the legacy whole-plan
// form with lane-local subsets is refused rather than resolved under either rule.
func TestMixedProvenanceModesFailClosed(t *testing.T) {
	dir := t.TempDir()
	legacy, lane := laneJournalPath(t, dir, 1), laneJournalPath(t, dir, 2)
	writeSealedLaneJournal(t, legacy, 1, "", []string{"data/siteA/", "data/siteB/"})
	writeSealedLaneJournal(t, lane, 2, indexsubstrate.CrawlPlanModeLaneLocal, []string{"data/siteB/"})

	_, err := boundLanePlan(t, legacy, lane)
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)
	require.Contains(t, err.Error(), "mix legacy whole-plan and lane-local")
}

// TestUnrecognizedPlanModeRefusesRatherThanDefaulting proves a mode value this
// reader does not know refuses, so a form written by a newer writer can never be
// silently reinterpreted under legacy rules.
func TestUnrecognizedPlanModeRefusesRatherThanDefaulting(t *testing.T) {
	path := laneJournalPath(t, t.TempDir(), 1)
	writeSealedLaneJournal(t, path, 1, "shard-local", []string{"data/siteA/"})

	_, err := boundLanePlan(t, path)
	require.Error(t, err)
	require.ErrorIs(t, err, indexsubstrate.ErrStaleParent)
	require.Contains(t, err.Error(), "unrecognized crawl_plan_mode")
}

// TestHierarchicallyOverlappingPrefixesAcrossLanesAccepted pins that disjointness
// is defined over exact plan entries, not key spaces. "data/a/" and "data/a/b/"
// are distinct valid entries and may legitimately live in separate lanes.
func TestHierarchicallyOverlappingPrefixesAcrossLanesAccepted(t *testing.T) {
	dir := t.TempDir()
	laneA, laneB := laneJournalPath(t, dir, 1), laneJournalPath(t, dir, 2)
	writeSealedLaneJournal(t, laneA, 1, indexsubstrate.CrawlPlanModeLaneLocal, []string{"data/a/"})
	writeSealedLaneJournal(t, laneB, 2, indexsubstrate.CrawlPlanModeLaneLocal, []string{"data/a/b/"})

	plan, err := boundLanePlan(t, laneA, laneB)
	require.NoError(t, err)
	require.Equal(t, []string{"data/a/", "data/a/b/"}, plan)
}

// TestLaneLocalUnionIsOrderIndependent proves the derived union is canonical, so
// the order journals are supplied in cannot change recovery authority.
func TestLaneLocalUnionIsOrderIndependent(t *testing.T) {
	dir := t.TempDir()
	laneA, laneB := laneJournalPath(t, dir, 1), laneJournalPath(t, dir, 2)
	writeSealedLaneJournal(t, laneA, 1, indexsubstrate.CrawlPlanModeLaneLocal, []string{"data/siteB/"})
	writeSealedLaneJournal(t, laneB, 2, indexsubstrate.CrawlPlanModeLaneLocal, []string{"data/siteA/"})

	forward, err := boundLanePlan(t, laneA, laneB)
	require.NoError(t, err)
	reverse, err := boundLanePlan(t, laneB, laneA)
	require.NoError(t, err)
	require.Equal(t, forward, reverse)
	require.Equal(t, []string{"data/siteA/", "data/siteB/"}, forward)
}

// TestLegacyIdenticalPlansUnchanged proves the absent-mode branch keeps today's
// meaning exactly: journals recording the same whole-run plan return it, in the
// recorded order rather than a normalized one.
func TestLegacyIdenticalPlansUnchanged(t *testing.T) {
	dir := t.TempDir()
	first, second := laneJournalPath(t, dir, 1), laneJournalPath(t, dir, 2)
	writeSealedLaneJournal(t, first, 1, "", []string{"data/siteB/", "data/siteA/"})
	writeSealedLaneJournal(t, second, 2, "", []string{"data/siteA/", "data/siteB/"})

	plan, err := boundLanePlan(t, first, second)
	require.NoError(t, err)
	require.Equal(t, []string{"data/siteB/", "data/siteA/"}, plan, "legacy agreement is order-independent but the returned plan is the recorded one")
}

// TestLaneAdmissionRefusesEmptyPlan proves an empty lane plan is refused where it
// happens. Sealed, such a journal is reported by recovery as predating crawl-plan
// provenance — a legacy diagnosis for a live lane-assignment bug.
func TestLaneAdmissionRefusesEmptyPlan(t *testing.T) {
	_, err := newJournalWriter(journalWriterConfig{
		Path:          filepath.Join(t.TempDir(), "shard-0002.jsonl"),
		IndexSetID:    "idx_lane",
		RunID:         "run_lane",
		StartedAt:     time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC),
		BaseURI:       "s3://bucket/data/",
		BasePrefix:    "data/",
		CrawlPrefixes: nil,
		CrawlPlanMode: indexsubstrate.CrawlPlanModeLaneLocal,
		LaneOrdinal:   2,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty crawl-prefix plan")
}

// TestLaneAdmissionRefusesUnrecognizedMode pins the write boundary as well as the
// recovery boundary: a lane can never seal a mode this build cannot interpret.
func TestLaneAdmissionRefusesUnrecognizedMode(t *testing.T) {
	_, err := newJournalWriter(journalWriterConfig{
		Path:          filepath.Join(t.TempDir(), "shard-0001.jsonl"),
		IndexSetID:    "idx_lane",
		RunID:         "run_lane",
		StartedAt:     time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC),
		BaseURI:       "s3://bucket/data/",
		BasePrefix:    "data/",
		CrawlPrefixes: []string{"data/siteA/"},
		CrawlPlanMode: "shard-local",
		LaneOrdinal:   1,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unrecognized journal crawl plan mode")
}

// fullLaneCoverage is the legitimate whole-run attestation a recovery caller
// supplies when it believes it holds the complete journal set.
func fullLaneCoverage() []CoverageAttestation {
	return []CoverageAttestation{
		{Scope: &Scope{Prefix: "data/siteA/"}, Basis: CoverageBasisConfirmed, Complete: true},
		{Scope: &Scope{Prefix: "data/siteB/"}, Basis: CoverageBasisConfirmed, Complete: true},
	}
}
