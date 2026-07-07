package indexsubstrate

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
