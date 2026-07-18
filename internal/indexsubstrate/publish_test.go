package indexsubstrate

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPublishEnrichOnlyRequiresParentToken(t *testing.T) {
	cfg, _ := publishTestConfig(t)
	cfg.Mode = PublicationModeEnrichOnly
	cfg.ExpectedParent = nil
	cfg.ParentManifests = nil
	_, err := PublishSnapshot(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ExpectedParent")
}

func TestPublishUnknownModeRejected(t *testing.T) {
	cfg, _ := publishTestConfig(t)
	cfg.Mode = PublicationMode("not-a-mode")
	_, err := PublishSnapshot(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown publication mode")
}

func TestPublishEnrichOnlyRejectsForgedCoverageToken(t *testing.T) {
	// Real parent with coverage A; caller forges token CoverageSHA256=B and child
	// coverage=B while keeping real parent set/run/manifest digests.
	cfg, _ := publishTestConfig(t)
	parent, err := PublishSnapshot(cfg)
	require.NoError(t, err)
	require.True(t, parent.LatestAdvanced)

	parentSnap, err := OpenLatestPublishedSnapshot(cfg.LatestPath)
	require.NoError(t, err)
	honestCov, err := CoverageSHA256(parentSnap.Manifest.Coverage)
	require.NoError(t, err)
	require.NotEmpty(t, honestCov)

	forgedCoverage := []CoverageAttestation{{
		Scope:    &Scope{Prefix: RelativeRootScopePrefix},
		Basis:    CoverageBasisConfirmed,
		Complete: true,
	}}
	forgedDigest, err := CoverageSHA256(forgedCoverage)
	require.NoError(t, err)
	require.NotEqual(t, honestCov, forgedDigest)

	_, rows, err := ReadLatestPublishedRows(cfg.LatestPath)
	require.NoError(t, err)
	cfg2, _ := publishTestConfig(t)
	cfg2.LatestPath = cfg.LatestPath
	cfg2.Mode = PublicationModeEnrichOnly
	cfg2.RunID = "run_enrich_forge"
	cfg2.PriorRows = rows
	cfg2.Coverage = forgedCoverage
	cfg2.ExpectedParent = &ExpectedParentToken{
		IndexSetID:     parentSnap.Complete.IndexSetID,
		RunID:          parentSnap.Complete.RunID,
		ManifestSHA256: parentSnap.Complete.ManifestSHA256,
		CoverageSHA256: forgedDigest, // forged to match child, not live parent
	}
	cfg2.ParentManifests = []ManifestReference{{
		IndexSetID:     parentSnap.Complete.IndexSetID,
		RunID:          parentSnap.Complete.RunID,
		ManifestSHA256: parentSnap.Complete.ManifestSHA256,
	}}
	jpath := filepath.Join(t.TempDir(), "enrich.jsonl")
	jw, err := CreateJournal(jpath, JournalHeader{
		Type: JournalHeaderType, JournalID: "jrn_f", IndexSetID: cfg2.IndexSetID,
		RunID: cfg2.RunID, Shard: "e", IndexSchemaVersion: IndexSchemaVersion, StartedAt: cfg2.RunStartedAt,
	})
	require.NoError(t, err)
	if len(rows) > 0 {
		_, err = jw.Append(ObjectRecord{
			Type: ObjectRecordType, Op: ObjectRecordOpEnrich, RelKey: rows[0].RelKey,
			ObservedAt: cfg2.RunStartedAt, ContentType: strPtr("text/plain"),
		})
		require.NoError(t, err)
	}
	require.NoError(t, jw.Seal(cfg2.CreatedAt))
	require.NoError(t, jw.Close())
	cfg2.JournalPaths = []string{jpath}
	// Rebind lease to the parent's segment root (cfg2 fixture root must not authorize cfg.LatestPath).
	rebindPublishLease(t, &cfg2, filepath.Dir(cfg.LatestPath), cfg.WriteLease)

	// Child-to-token digest matches (both forged), but live parent coverage CAS must fail.
	_, err = PublishSnapshot(cfg2)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrStaleParent) || strings.Contains(err.Error(), "coverage"), err.Error())
}

func TestPublishEnrichOnlyRejectsCoverageDigestMismatch(t *testing.T) {
	cfg, _ := publishTestConfig(t)
	// First publish a real parent so we can enrich over it.
	parent, err := PublishSnapshot(cfg)
	require.NoError(t, err)
	require.True(t, parent.LatestAdvanced)

	parentSnap, err := OpenLatestPublishedSnapshot(cfg.LatestPath)
	require.NoError(t, err)
	covDigest, err := CoverageSHA256(parentSnap.Manifest.Coverage)
	require.NoError(t, err)

	// Enrich-only republish with matching parent identity but mutated coverage.
	cfg2, _ := publishTestConfig(t)
	cfg2.LatestPath = cfg.LatestPath
	cfg2.Mode = PublicationModeEnrichOnly
	cfg2.RunID = "run_enrich_cov"
	cfg2.PriorRows = nil // will need prior rows from parent - use ReadLatest
	_, rows, err := ReadLatestPublishedRows(cfg.LatestPath)
	require.NoError(t, err)
	cfg2.PriorRows = rows
	cfg2.ExpectedParent = &ExpectedParentToken{
		IndexSetID:     parentSnap.Complete.IndexSetID,
		RunID:          parentSnap.Complete.RunID,
		ManifestSHA256: parentSnap.Complete.ManifestSHA256,
		CoverageSHA256: covDigest,
	}
	cfg2.ParentManifests = []ManifestReference{{
		IndexSetID:     parentSnap.Complete.IndexSetID,
		RunID:          parentSnap.Complete.RunID,
		ManifestSHA256: parentSnap.Complete.ManifestSHA256,
	}}
	// Broaden coverage relative to parent.
	cfg2.Coverage = []CoverageAttestation{{
		Scope:    &Scope{Prefix: RelativeRootScopePrefix},
		Basis:    CoverageBasisConfirmed,
		Complete: true,
	}}
	// Need an enrich-only journal — create one with a single enrich record.
	jpath := filepath.Join(t.TempDir(), "enrich.jsonl")
	jw, err := CreateJournal(jpath, JournalHeader{
		Type: JournalHeaderType, JournalID: "jrn_e", IndexSetID: cfg2.IndexSetID,
		RunID: cfg2.RunID, Shard: "e", IndexSchemaVersion: IndexSchemaVersion, StartedAt: cfg2.RunStartedAt,
	})
	require.NoError(t, err)
	if len(rows) > 0 {
		_, err = jw.Append(ObjectRecord{
			Type: ObjectRecordType, Op: ObjectRecordOpEnrich, RelKey: rows[0].RelKey,
			ObservedAt: cfg2.RunStartedAt, ContentType: strPtr("text/plain"),
		})
		require.NoError(t, err)
	}
	require.NoError(t, jw.Seal(cfg2.CreatedAt))
	require.NoError(t, jw.Close())
	cfg2.JournalPaths = []string{jpath}
	rebindPublishLease(t, &cfg2, filepath.Dir(cfg.LatestPath), cfg.WriteLease)

	_, err = PublishSnapshot(cfg2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "coverage must equal")
}

// rebindPublishLease releases the config's fixture lease and any prior root lease,
// then acquires a lease on segmentRoot for cfg.IndexSetID.
func rebindPublishLease(t *testing.T, cfg *PublishConfig, segmentRoot string, prior *WriteLease) {
	t.Helper()
	if cfg.WriteLease != nil {
		_ = cfg.WriteLease.Release()
	}
	if prior != nil {
		_ = prior.Release()
	}
	lease, err := AcquireWriteLease(segmentRoot, cfg.IndexSetID, "publish-test-rebind", 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lease.Release() })
	cfg.WriteLease = lease
}

func strPtr(s string) *string { return &s }

func TestPublishRequiresWriteLease(t *testing.T) {
	cfg, _ := publishTestConfig(t)
	cfg.WriteLease = nil
	_, err := PublishSnapshot(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "write lease is required")
}

func TestPublishRejectsReleasedWriteLease(t *testing.T) {
	cfg, _ := publishTestConfig(t)
	require.NoError(t, cfg.WriteLease.Release())
	_, err := PublishSnapshot(cfg)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrWriteLeaseLost)
	require.NoFileExists(t, cfg.LatestPath)
}

func TestPublishRejectsLeaseForOtherSegmentRoot(t *testing.T) {
	cfg, _ := publishTestConfig(t)
	// Swap in a lease held under a different root while LatestPath stays on cfg root.
	otherRoot := t.TempDir()
	foreign, err := AcquireWriteLease(otherRoot, cfg.IndexSetID, "foreign-root", 0)
	require.NoError(t, err)
	defer func() { _ = foreign.Release() }()
	_ = cfg.WriteLease.Release()
	cfg.WriteLease = foreign

	_, err = PublishSnapshot(cfg)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrWriteLeaseScope)
	require.NoFileExists(t, cfg.LatestPath)
}

func TestPublishRejectsLeaseForOtherIndexSetID(t *testing.T) {
	cfg, _ := publishTestConfig(t)
	// Same root, wrong index set binding on the lease.
	require.NoError(t, cfg.WriteLease.Release())
	wrongID, err := AcquireWriteLease(filepath.Dir(cfg.LatestPath), "idx_other", "wrong-id", 0)
	require.NoError(t, err)
	defer func() { _ = wrongID.Release() }()
	cfg.WriteLease = wrongID

	_, err = PublishSnapshot(cfg)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrWriteLeaseScope)
	require.NoFileExists(t, cfg.LatestPath)
}

func TestPublishPostLatestHookErrorStillReportsLatestAdvanced(t *testing.T) {
	config, _ := publishTestConfig(t)
	config.AfterStep = failAfterStep(PublishStepLatestAdvanced)
	result, err := PublishSnapshot(config)
	require.Error(t, err)
	require.True(t, result.LatestAdvanced, "latest advance must be visible after post-commit hook failure")
	require.FileExists(t, config.LatestPath)
	_, _, readErr := ReadLatestPublishedRows(config.LatestPath)
	require.NoError(t, readErr)
}

func TestReadManifestRowsBoundedRejectsOverLimitBeforeWalk(t *testing.T) {
	manifest := InternalManifest{
		Counts:   ManifestCounts{Rows: 3, ActiveRows: 3},
		Segments: []SegmentDescriptor{{Rows: 3}},
	}
	// Cap below declared rows: must fail at budget validation without needing segment files.
	_, err := ReadManifestRowsBounded(t.TempDir(), manifest, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds limit")
}

func TestValidateManifestCountBudgetRejectsNegativeCounts(t *testing.T) {
	err := validateManifestCountBudget(InternalManifest{
		Counts:   ManifestCounts{Rows: -1, ActiveRows: 0},
		Segments: nil,
	}, -1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-negative")
}

func TestValidateManifestCountBudgetRejectsInconsistentSegmentSum(t *testing.T) {
	err := validateManifestCountBudget(InternalManifest{
		Counts:   ManifestCounts{Rows: 5},
		Segments: []SegmentDescriptor{{Rows: 2}, {Rows: 2}},
	}, -1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not equal")
}

func TestReadManifestRowsBoundedAcceptsExactLimit(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "a.xml", 1, `"a"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "b.xml", 2, `"b"`, base, nil, nil, nil, nil),
	}
	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir: dir, IndexSetID: "idx_test", RunID: "run_exact", CreatedAt: base, TargetRowsPerSegment: 10,
	}, rows)
	require.NoError(t, err)
	got, err := ReadManifestRowsBounded(dir, manifest, 2)
	require.NoError(t, err)
	require.Len(t, got, 2)
}

func TestReadManifestRowsBoundedStopsWhenActualExceedsDeclared(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	rows := []CurrentObjectRow{
		segmentTestRow("idx_test", "a.xml", 1, `"a"`, base, nil, nil, nil, nil),
		segmentTestRow("idx_test", "b.xml", 2, `"b"`, base, nil, nil, nil, nil),
	}
	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir: dir, IndexSetID: "idx_test", RunID: "run_under", CreatedAt: base, TargetRowsPerSegment: 10,
	}, rows)
	require.NoError(t, err)
	// Corrupt declared counts to under-claim while leaving a digest-valid segment that
	// still contains two rows. Walker must stop at declared counts.rows, not grow to a
	// larger global cap.
	require.Len(t, manifest.Segments, 1)
	manifest.Counts.Rows = 1
	manifest.Counts.ActiveRows = 1
	manifest.Segments[0].Rows = 1

	_, err = ReadManifestRowsBounded(dir, manifest, 2_000_000)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceed declared counts.rows")
}

func TestPublishSnapshotAdvancesLatestAfterCompletePipeline(t *testing.T) {
	config, expectedRows := publishTestConfig(t)
	var steps []PublishStep
	config.AfterStep = func(step PublishStep) error {
		steps = append(steps, step)
		return nil
	}

	result, err := PublishSnapshot(config)
	require.NoError(t, err)
	require.Equal(t, []PublishStep{
		PublishStepJournalsValidated,
		PublishStepCoverageValidated,
		PublishStepCompacted,
		PublishStepSegmentsWritten,
		PublishStepManifestWritten,
		PublishStepCompleteWritten,
		PublishStepLatestAdvanced,
	}, steps)
	require.Equal(t, ManifestType, result.Manifest.Type)
	require.Equal(t, config.Coverage, result.Manifest.Coverage)
	require.FileExists(t, config.ManifestPath)
	require.FileExists(t, config.CompletePath)
	require.FileExists(t, config.LatestPath)

	manifest, rows, err := ReadLatestPublishedRows(config.LatestPath)
	require.NoError(t, err)
	require.Equal(t, result.Manifest, manifest)
	require.Equal(t, expectedRows, rows)
}

func TestPublishSnapshotFailureBeforeManifestLeavesNoVisibleSnapshot(t *testing.T) {
	config, _ := publishTestConfig(t)
	config.AfterStep = failAfterStep(PublishStepSegmentsWritten)

	_, err := PublishSnapshot(config)
	require.Error(t, err)
	require.NoFileExists(t, config.ManifestPath)
	require.NoFileExists(t, config.CompletePath)
	require.NoFileExists(t, config.LatestPath)

	_, _, readErr := ReadLatestPublishedRows(config.LatestPath)
	require.ErrorIs(t, readErr, ErrSnapshotNotPublished)
}

func TestPublishSnapshotFailureAfterManifestBeforeLatestLeavesNoVisibleSnapshot(t *testing.T) {
	config, _ := publishTestConfig(t)
	config.AfterStep = failAfterStep(PublishStepManifestWritten)

	_, err := PublishSnapshot(config)
	require.Error(t, err)
	require.FileExists(t, config.ManifestPath)
	require.NoFileExists(t, config.CompletePath)
	require.NoFileExists(t, config.LatestPath)

	_, _, readErr := ReadLatestPublishedRows(config.LatestPath)
	require.ErrorIs(t, readErr, ErrSnapshotNotPublished)
}

func TestPublishSnapshotRetriesFromSealedJournals(t *testing.T) {
	config, expectedRows := publishTestConfig(t)
	config.AfterStep = failAfterStep(PublishStepManifestWritten)
	_, err := PublishSnapshot(config)
	require.Error(t, err)
	require.FileExists(t, config.ManifestPath)
	require.NoFileExists(t, config.LatestPath)

	config.AfterStep = nil
	result, err := PublishSnapshot(config)
	require.NoError(t, err)
	require.FileExists(t, config.LatestPath)

	manifest, rows, err := ReadLatestPublishedRows(config.LatestPath)
	require.NoError(t, err)
	require.Equal(t, result.Manifest, manifest)
	require.Equal(t, expectedRows, rows)
}

func TestPublishSnapshotPersistsParentReachabilityMetadata(t *testing.T) {
	config, _ := publishTestConfig(t)
	config.ParentManifests = []ManifestReference{{
		IndexSetID:     "idx_test",
		RunID:          "run_parent",
		ManifestSHA256: strings.Repeat("e", 64),
	}}

	result, err := PublishSnapshot(config)
	require.NoError(t, err)
	require.Equal(t, config.ParentManifests, result.Manifest.ParentManifests)
	require.Equal(t, DefaultManifestReachability(), result.Manifest.Reachability)

	manifest, _, err := ReadLatestPublishedRows(config.LatestPath)
	require.NoError(t, err)
	require.Equal(t, config.ParentManifests, manifest.ParentManifests)
	require.Equal(t, DefaultManifestReachability(), manifest.Reachability)
}

func TestPublishSnapshotFailureAfterCompleteCanAdvanceLatestOnRetry(t *testing.T) {
	config, expectedRows := publishTestConfig(t)
	config.AfterStep = failAfterStep(PublishStepCompleteWritten)
	_, err := PublishSnapshot(config)
	require.Error(t, err)
	require.FileExists(t, config.ManifestPath)
	require.FileExists(t, config.CompletePath)
	require.NoFileExists(t, config.LatestPath)

	config.AfterStep = nil
	_, err = PublishSnapshot(config)
	require.NoError(t, err)
	_, rows, err := ReadLatestPublishedRows(config.LatestPath)
	require.NoError(t, err)
	require.Equal(t, expectedRows, rows)
}

func TestReadLatestPublishedRowsVerifiesSegmentDigestBeforeTrust(t *testing.T) {
	config, _ := publishTestConfig(t)
	result, err := PublishSnapshot(config)
	require.NoError(t, err)
	require.NotEmpty(t, result.Manifest.Segments)

	segmentPath := filepath.Join(config.SegmentDir, result.Manifest.Segments[0].Path)
	require.NoError(t, os.WriteFile(segmentPath, []byte("not parquet anymore"), 0o600))

	_, _, err = ReadLatestPublishedRows(config.LatestPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "digest mismatch")
}

func TestReadLatestPublishedRowsRejectsDigestBoundWrongManifestIdentity(t *testing.T) {
	config, _ := publishTestConfig(t)
	result, err := PublishSnapshot(config)
	require.NoError(t, err)

	wrongManifest := result.Manifest
	wrongManifest.IndexSetID = "idx_other"
	wrongManifest.RunID = "run_other"
	wrongManifestPath := filepath.Join(filepath.Dir(config.ManifestPath), "wrong-manifest.json")
	require.NoError(t, WriteInternalManifestFile(wrongManifestPath, wrongManifest))
	wrongManifestDigest, err := sha256HexFile(wrongManifestPath)
	require.NoError(t, err)

	complete, err := readCompleteDocFile(config.CompletePath)
	require.NoError(t, err)
	complete.ManifestPath = wrongManifestPath
	complete.ManifestSHA256 = wrongManifestDigest
	data, err := marshalIndentedJSON(complete)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(config.CompletePath, data, 0o600))

	_, rows, err := ReadLatestPublishedRows(config.LatestPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "manifest index_set_id mismatch")
	require.Nil(t, rows)
}

func TestPublishSnapshotRejectsUnconfirmedCoverage(t *testing.T) {
	config, _ := publishTestConfig(t)
	config.Coverage = []CoverageAttestation{{
		Scope:    &Scope{Prefix: "data/"},
		Basis:    CoverageBasisInferred,
		Complete: true,
	}}

	_, err := PublishSnapshot(config)
	require.ErrorIs(t, err, ErrInvalidCoverage)
	require.NoFileExists(t, config.ManifestPath)
	require.NoFileExists(t, config.LatestPath)
}

func TestPublishSnapshotAcceptsExplicitRelativeRootCoverage(t *testing.T) {
	config, _ := publishTestConfig(t)
	config.Coverage = []CoverageAttestation{{
		Scope:    &Scope{Prefix: RelativeRootScopePrefix},
		Basis:    CoverageBasisConfirmed,
		Complete: true,
	}}

	_, err := PublishSnapshot(config)
	require.NoError(t, err)
	require.FileExists(t, config.LatestPath)
}

func publishTestConfig(t *testing.T) (PublishConfig, []CurrentObjectRow) {
	t.Helper()
	root := t.TempDir()
	base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	runStartedAt := base.Add(time.Hour)
	standard := "STANDARD"
	old := segmentTestRow("idx_test", "data/missing.xml", 9, `"old"`, base.Add(-time.Hour), &standard, nil, nil, nil)
	old.LastSeenRunID = "run_old"
	old.LastSeenAt = base.Add(-time.Hour)

	journalPath := filepath.Join(root, "journals", "jrn_a.jsonl")
	writer, err := CreateJournal(journalPath, JournalHeader{
		Type:               JournalHeaderType,
		JournalID:          "jrn_a",
		IndexSetID:         "idx_test",
		RunID:              "run_test",
		Shard:              "shard-0001",
		Scope:              &Scope{Prefix: "data/"},
		IndexSchemaVersion: IndexSchemaVersion,
		StartedAt:          runStartedAt,
	})
	require.NoError(t, err)
	size := int64(10)
	_, err = writer.Append(ObjectRecord{
		Op:           ObjectRecordOpObserve,
		RelKey:       "data/a.xml",
		ObservedAt:   runStartedAt.Add(time.Minute),
		SizeBytes:    &size,
		ETag:         `"etag-a"`,
		StorageClass: &standard,
	})
	require.NoError(t, err)
	require.NoError(t, writer.Seal(runStartedAt.Add(2*time.Minute)))
	require.NoError(t, writer.Close())

	// Lease must bind the same segment-set root that owns latest.json.
	lease, err := AcquireWriteLease(root, "idx_test", "publish-test", 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lease.Release() })

	config := PublishConfig{
		IndexSetID:           "idx_test",
		RunID:                "run_test",
		RunStartedAt:         runStartedAt,
		CreatedAt:            runStartedAt.Add(3 * time.Minute),
		PriorRows:            []CurrentObjectRow{old},
		JournalPaths:         []string{journalPath},
		Coverage:             []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true}},
		SegmentDir:           filepath.Join(root, "segments"),
		ManifestPath:         filepath.Join(root, "manifests", "manifest.json"),
		CompletePath:         filepath.Join(root, "complete.json"),
		LatestPath:           filepath.Join(root, "latest.json"),
		TargetRowsPerSegment: 1,
		WriteLease:           lease,
	}
	result, err := CompactJournalFiles(CompactionInput{
		IndexSetID:   config.IndexSetID,
		RunID:        config.RunID,
		RunStartedAt: config.RunStartedAt,
		PriorRows:    config.PriorRows,
		Coverage:     config.Coverage,
	}, config.JournalPaths)
	require.NoError(t, err)
	return config, result.Rows
}

func failAfterStep(target PublishStep) func(PublishStep) error {
	return func(step PublishStep) error {
		if step == target {
			return fmt.Errorf("injected interruption")
		}
		return nil
	}
}

// TestPublishRefusesOverBudgetJournalRecordBeforeValidation proves MaxRecordBytes
// bounds the sealed-journal validation pass (not only the later streaming scan):
// an over-budget record fails closed with a typed SpillMergeBudgetExhausted before
// the journal is declared validated or coverage is validated, and latest never
// advances.
func TestPublishRefusesOverBudgetJournalRecordBeforeValidation(t *testing.T) {
	cfg, _ := publishTestConfig(t)
	var steps []PublishStep
	cfg.AfterStep = func(s PublishStep) error { steps = append(steps, s); return nil }
	cfg.SpillBudget.MaxRecordBytes = 1 // below any real journal line (the header)

	_, err := PublishSnapshot(cfg)
	require.Error(t, err)
	var sme *SpillMergeError
	require.True(t, errors.As(err, &sme), "expected typed spill-merge error, got %v", err)
	require.Equal(t, SpillMergeBudgetExhausted, sme.Category)
	require.Contains(t, err.Error(), "MaxRecordBytes exceeded")
	require.NotContains(t, steps, PublishStepJournalsValidated, "must refuse before journals are declared validated")
	require.NotContains(t, steps, PublishStepCoverageValidated)

	// First publication refused: no latest advanced.
	_, err = OpenLatestPublishedSnapshot(cfg.LatestPath)
	require.Error(t, err)
}

// TestPublishSufficientRecordBudgetValidates is the sufficient-budget
// companion: a budget above the journal's largest line validates and
// publishes normally. The exact boundary is pinned by
// TestPublishRecordBudgetExactPayloadLimit.
func TestPublishSufficientRecordBudgetValidates(t *testing.T) {
	cfg, _ := publishTestConfig(t)
	var steps []PublishStep
	cfg.AfterStep = func(s PublishStep) error { steps = append(steps, s); return nil }
	cfg.SpillBudget.MaxRecordBytes = 1 << 20 // comfortably above the journal lines

	result, err := PublishSnapshot(cfg)
	require.NoError(t, err)
	require.True(t, result.LatestAdvanced)
	require.Contains(t, steps, PublishStepJournalsValidated)
}

// publishRecordBudgetTestConfig builds a publishable first-publication config
// whose journal header (wide crawl-prefix plan) is decisively the largest
// journal line, so the exact-limit arms below bound on a known payload through
// both journal readers (bounded validation pass and streaming scan) without a
// smaller internal spill line tripping first.
func publishRecordBudgetTestConfig(t *testing.T) PublishConfig {
	t.Helper()
	root := t.TempDir()
	base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	runStartedAt := base.Add(time.Hour)

	prefixes := make([]string, 0, 64)
	for i := 0; i < 64; i++ {
		prefixes = append(prefixes, fmt.Sprintf("data/site-%04d/", i))
	}
	journalPath := filepath.Join(root, "journals", "jrn_a.jsonl")
	writer, err := CreateJournal(journalPath, JournalHeader{
		Type:               JournalHeaderType,
		JournalID:          "jrn_a",
		IndexSetID:         "idx_test",
		RunID:              "run_test",
		Shard:              "shard-0001",
		Scope:              &Scope{Prefix: "data/"},
		CrawlPrefixes:      prefixes,
		IndexSchemaVersion: IndexSchemaVersion,
		StartedAt:          runStartedAt,
	})
	require.NoError(t, err)
	size := int64(10)
	standard := "STANDARD"
	_, err = writer.Append(ObjectRecord{
		Op:           ObjectRecordOpObserve,
		RelKey:       "data/a.xml",
		ObservedAt:   runStartedAt.Add(time.Minute),
		SizeBytes:    &size,
		ETag:         `"etag-a"`,
		StorageClass: &standard,
	})
	require.NoError(t, err)
	require.NoError(t, writer.Seal(runStartedAt.Add(2*time.Minute)))
	require.NoError(t, writer.Close())

	lease, err := AcquireWriteLease(root, "idx_test", "publish-test", 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lease.Release() })

	return PublishConfig{
		IndexSetID:           "idx_test",
		RunID:                "run_test",
		RunStartedAt:         runStartedAt,
		CreatedAt:            runStartedAt.Add(3 * time.Minute),
		JournalPaths:         []string{journalPath},
		Coverage:             []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true}},
		SegmentDir:           filepath.Join(root, "segments"),
		ManifestPath:         filepath.Join(root, "manifests", "manifest.json"),
		CompletePath:         filepath.Join(root, "complete.json"),
		LatestPath:           filepath.Join(root, "latest.json"),
		TargetRowsPerSegment: 1,
		WriteLease:           lease,
	}
}

// largestJournalPayload returns the byte length of the largest journal line
// with its terminator ("\n" or "\r\n") excluded — the payload MaxRecordBytes
// bounds.
func largestJournalPayload(t *testing.T, path string) int64 {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var largest int
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if len(line) > largest {
			largest = len(line)
		}
	}
	require.Positive(t, largest)
	return int64(largest)
}

// TestPublishRecordBudgetExactPayloadLimit pins the record-budget boundary
// end-to-end through PublishSnapshot, covering both journal readers: a budget
// equal to the largest record payload publishes, a budget one byte under it
// refuses with the typed capacity error before the journals-validated hook, and
// line terminators are framing rather than record bytes (the same payload
// budget publishes a CRLF-framed copy of the journal).
func TestPublishRecordBudgetExactPayloadLimit(t *testing.T) {
	t.Run("exact limit publishes", func(t *testing.T) {
		cfg := publishRecordBudgetTestConfig(t)
		cfg.SpillBudget.MaxRecordBytes = largestJournalPayload(t, cfg.JournalPaths[0])
		result, err := PublishSnapshot(cfg)
		require.NoError(t, err)
		require.True(t, result.LatestAdvanced)
	})

	t.Run("one byte under refuses typed before validation hook", func(t *testing.T) {
		cfg := publishRecordBudgetTestConfig(t)
		var steps []PublishStep
		cfg.AfterStep = func(s PublishStep) error { steps = append(steps, s); return nil }
		cfg.SpillBudget.MaxRecordBytes = largestJournalPayload(t, cfg.JournalPaths[0]) - 1

		_, err := PublishSnapshot(cfg)
		var sme *SpillMergeError
		require.True(t, errors.As(err, &sme), "expected typed spill-merge error, got %v", err)
		require.Equal(t, SpillMergeBudgetExhausted, sme.Category)
		require.Contains(t, err.Error(), "MaxRecordBytes exceeded")
		require.NotContains(t, steps, PublishStepJournalsValidated, "must refuse before journals are declared validated")
		require.NotContains(t, steps, PublishStepCoverageValidated)
		require.NoFileExists(t, cfg.LatestPath)
	})

	t.Run("largest supported record budget publishes without panic", func(t *testing.T) {
		// Every ACCEPTED finite ceiling must return normally: the scanner
		// capacity translation may not overflow at the platform maximum.
		cfg := publishRecordBudgetTestConfig(t)
		cfg.SpillBudget.MaxRecordBytes = MaxSpillRecordBytes
		result, err := PublishSnapshot(cfg)
		require.NoError(t, err)
		require.True(t, result.LatestAdvanced)
	})

	t.Run("CRLF terminator is framing not payload", func(t *testing.T) {
		cfg := publishRecordBudgetTestConfig(t)
		journalPath := cfg.JournalPaths[0]
		budget := largestJournalPayload(t, journalPath)
		raw, err := os.ReadFile(journalPath)
		require.NoError(t, err)
		require.NotContains(t, string(raw), "\r", "fixture must start LF-framed")
		require.NoError(t, os.WriteFile(journalPath, []byte(strings.ReplaceAll(string(raw), "\n", "\r\n")), 0o600))

		cfg.SpillBudget.MaxRecordBytes = budget
		result, err := PublishSnapshot(cfg)
		require.NoError(t, err)
		require.True(t, result.LatestAdvanced)
	})
}

// TestPublishRefusesInvalidSpillBudgetBeforeJournalValidation proves an
// explicit invalid library budget (negative field) refuses with typed
// SpillMergeInvalidConfig before any journal read, publish hook, workspace, or
// artifact — an invalid caller value can never degrade the validation pass to
// unbounded reads.
func TestPublishRefusesInvalidSpillBudgetBeforeJournalValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*PublishConfig)
		msg  string
	}{
		{"negative record budget", func(c *PublishConfig) { c.SpillBudget.MaxRecordBytes = -1 }, "MaxRecordBytes must be >= 1"},
		{"negative workspace budget", func(c *PublishConfig) { c.SpillBudget.MaxWorkspaceBytes = -1 }, "MaxWorkspaceBytes must be >= 1"},
		{"record budget above supported maximum", func(c *PublishConfig) { c.SpillBudget.MaxRecordBytes = math.MaxInt64 }, "supported maximum"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := publishTestConfig(t)
			var steps []PublishStep
			cfg.AfterStep = func(s PublishStep) error { steps = append(steps, s); return nil }
			tc.mut(&cfg)

			_, err := PublishSnapshot(cfg)
			var sme *SpillMergeError
			require.True(t, errors.As(err, &sme), "expected typed spill-merge error, got %v", err)
			require.Equal(t, SpillMergeInvalidConfig, sme.Category)
			require.Contains(t, err.Error(), tc.msg)
			require.Empty(t, steps, "no publish hook may fire on an invalid budget")
			require.NoFileExists(t, cfg.LatestPath)
		})
	}
}
