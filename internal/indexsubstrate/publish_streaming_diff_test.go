package indexsubstrate

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPublishSnapshotStreamingMatchesLegacyCompaction is the production
// publish-path differential proof: PublishSnapshot (routed through the streaming
// spill/merge current-state source and streaming segment writer) commits the same
// manifest counts, per-segment digests, and current-state rows as the legacy
// materialized CompactJournalFiles -> WriteSegmentSet path, for a representative
// add/change/delete/reappear set with prior HEAD enrichment.
//
// It guards behavioural identity of the publish-path swap; the streaming
// primitives' own tests already prove them byte-identical in isolation.
func TestPublishSnapshotStreamingMatchesLegacyCompaction(t *testing.T) {
	root := t.TempDir()
	base := time.Date(2026, 7, 14, 9, 0, 0, 0, time.UTC)
	runStartedAt := base.Add(time.Hour)
	standard := "STANDARD"
	xml := "application/xml"

	// Prior current-state rows (as if from a parent snapshot):
	//   unchanged.xml   -> re-observed identically; enrichment must survive.
	//   changed.xml     -> re-observed with a new etag/size.
	//   deleted.xml     -> not re-observed under complete coverage -> tombstone.
	//   reappear.xml    -> prior tombstone; re-observed this run -> active again.
	unchanged := segmentTestRow("idx_diff", "data/unchanged.xml", 11, `"unch"`, base, &standard, &xml, nil, nil)
	changed := segmentTestRow("idx_diff", "data/changed.xml", 22, `"old-ch"`, base, &standard, &xml, nil, nil)
	deleted := segmentTestRow("idx_diff", "data/deleted.xml", 33, `"del"`, base, &standard, nil, nil, nil)
	reappear := segmentTestRow("idx_diff", "data/reappear.xml", 44, `"gone"`, base, &standard, nil, nil, nil)
	delAt := base.Add(-30 * time.Minute)
	reappear.DeletedAt = &delAt
	priorRows := []CurrentObjectRow{changed, deleted, reappear, unchanged}

	// Journal: observe unchanged (identical), changed (new etag/size), reappear
	// (comes back), and new.xml (net-new). deleted.xml is intentionally absent.
	journalPath := filepath.Join(root, "journals", "jrn_diff.jsonl")
	writer, err := CreateJournal(journalPath, JournalHeader{
		Type:               JournalHeaderType,
		JournalID:          "jrn_diff",
		IndexSetID:         "idx_diff",
		RunID:              "run_diff",
		Shard:              "shard-0001",
		Scope:              &Scope{Prefix: "data/"},
		IndexSchemaVersion: IndexSchemaVersion,
		StartedAt:          runStartedAt,
	})
	require.NoError(t, err)
	observe := func(relKey string, size int64, etag string) {
		s := size
		_, appendErr := writer.Append(ObjectRecord{
			Op:           ObjectRecordOpObserve,
			RelKey:       relKey,
			ObservedAt:   runStartedAt.Add(time.Minute),
			SizeBytes:    &s,
			ETag:         etag,
			StorageClass: &standard,
		})
		require.NoError(t, appendErr)
	}
	observe("data/changed.xml", 99, `"new-ch"`)
	observe("data/new.xml", 55, `"new"`)
	observe("data/reappear.xml", 44, `"back"`)
	observe("data/unchanged.xml", 11, `"unch"`)
	require.NoError(t, writer.Seal(runStartedAt.Add(2*time.Minute)))
	require.NoError(t, writer.Close())

	coverage := []CoverageAttestation{{Scope: &Scope{Prefix: "data/"}, Basis: CoverageBasisConfirmed, Complete: true}}
	createdAt := runStartedAt.Add(3 * time.Minute)

	// --- Legacy path (independent oracle): Compact -> WriteSegmentSet. ---
	legacy, err := CompactJournalFiles(CompactionInput{
		IndexSetID:   "idx_diff",
		RunID:        "run_diff",
		RunStartedAt: runStartedAt,
		PriorRows:    priorRows,
		Coverage:     coverage,
	}, []string{journalPath})
	require.NoError(t, err)
	legacyDir := filepath.Join(root, "legacy-segments")
	legacyManifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:                    legacyDir,
		IndexSetID:             "idx_diff",
		RunID:                  "run_diff",
		CreatedAt:              createdAt,
		TargetRowsPerSegment:   2,
		AllowExistingIdentical: true,
		Coverage:               coverage,
	}, legacy.Rows)
	require.NoError(t, err)

	// --- Streaming path under test: PublishSnapshot commits via the merge. ---
	lease, err := AcquireWriteLease(root, "idx_diff", "diff-test", 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lease.Release() })
	config := PublishConfig{
		IndexSetID:           "idx_diff",
		RunID:                "run_diff",
		RunStartedAt:         runStartedAt,
		CreatedAt:            createdAt,
		PriorRows:            priorRows,
		JournalPaths:         []string{journalPath},
		Coverage:             coverage,
		SegmentDir:           filepath.Join(root, "segments"),
		ManifestPath:         filepath.Join(root, "manifests", "manifest.json"),
		CompletePath:         filepath.Join(root, "complete.json"),
		LatestPath:           filepath.Join(root, "latest.json"),
		TargetRowsPerSegment: 2,
		WriteLease:           lease,
	}
	result, err := PublishSnapshotContext(context.Background(), config)
	require.NoError(t, err)
	require.True(t, result.LatestAdvanced)

	// Manifest counts and per-segment digests must match the legacy oracle.
	require.Equal(t, legacyManifest.Counts, result.Manifest.Counts, "manifest counts diverge from legacy compaction")
	require.Len(t, result.Manifest.Segments, len(legacyManifest.Segments))
	for i := range legacyManifest.Segments {
		require.Equal(t, legacyManifest.Segments[i].Rows, result.Manifest.Segments[i].Rows, "segment %d row count", i)
		require.Equal(t, legacyManifest.Segments[i].Digest, result.Manifest.Segments[i].Digest, "segment %d digest", i)
	}

	// Summary-only compaction result: observed/enrichment record counts match the
	// legacy oracle, and the full row/tombstone slices are intentionally not
	// materialized on the streaming path.
	require.Equal(t, legacy.ObservedRecords, result.Compaction.ObservedRecords, "observed record count diverges")
	require.Equal(t, legacy.EnrichmentRecords, result.Compaction.EnrichmentRecords, "enrichment record count diverges")
	require.Nil(t, result.Compaction.Rows, "streaming path must not materialize the full row slice")
	require.Nil(t, result.Compaction.Tombstones, "streaming path must not materialize the tombstone slice")

	// Committed current-state rows must equal the legacy oracle exactly, including
	// preserved HEAD enrichment on the unchanged row and revived reappear row.
	_, publishedRows, err := ReadLatestPublishedRows(config.LatestPath)
	require.NoError(t, err)
	require.Equal(t, legacy.Rows, publishedRows, "streaming published rows diverge from legacy compaction")

	byKey := make(map[string]CurrentObjectRow, len(publishedRows))
	for _, r := range publishedRows {
		byKey[r.RelKey] = r
	}
	// unchanged: enrichment (content-type, head-enriched-at) and first-seen lineage preserved.
	gotUnchanged, ok := byKey["data/unchanged.xml"]
	require.True(t, ok)
	require.NotNil(t, gotUnchanged.ContentType)
	require.Equal(t, xml, *gotUnchanged.ContentType, "HEAD enrichment content-type dropped on unchanged row")
	require.NotNil(t, gotUnchanged.HeadEnrichedAt)
	require.Equal(t, unchanged.FirstSeenAt, gotUnchanged.FirstSeenAt, "first-seen lineage not preserved")
	require.Nil(t, gotUnchanged.DeletedAt)
	// deleted: tombstoned (present with DeletedAt set) under complete coverage.
	gotDeleted, ok := byKey["data/deleted.xml"]
	require.True(t, ok)
	require.NotNil(t, gotDeleted.DeletedAt, "deleted row must be tombstoned under complete coverage")
	// reappear: revived (DeletedAt cleared) after re-observation.
	gotReappear, ok := byKey["data/reappear.xml"]
	require.True(t, ok)
	require.Nil(t, gotReappear.DeletedAt, "reappeared row must clear its prior tombstone")
	// new: present and active.
	gotNew, ok := byKey["data/new.xml"]
	require.True(t, ok)
	require.Nil(t, gotNew.DeletedAt)
}

// TestPublishSnapshotCompactedHookFailurePreservesSourceCleanupError proves that
// when the post-compaction hook fails while the prepared current-state source is
// still caller-owned, a sticky source-cleanup failure is preserved alongside the
// hook error rather than being discarded. Losing the source-close error could
// strand protected spill residue while reporting only the observational hook
// failure. No manifest/complete/latest advance may occur.
func TestPublishSnapshotCompactedHookFailurePreservesSourceCleanupError(t *testing.T) {
	config, _ := publishTestConfig(t)

	hookErr := errors.New("injected compacted-hook failure")
	config.AfterStep = func(step PublishStep) error {
		if step == PublishStepCompacted {
			return hookErr
		}
		return nil
	}

	// Force the caller-owned stateSource.Close() to fail deterministically.
	cleanupErr := errors.New("injected source cleanup failure")
	testForceBoundEpochDeleteErr = cleanupErr
	t.Cleanup(func() { testForceBoundEpochDeleteErr = nil })

	_, err := PublishSnapshotContext(context.Background(), config)
	require.Error(t, err)
	require.ErrorIs(t, err, hookErr, "hook failure must remain discoverable")
	require.ErrorIs(t, err, cleanupErr, "source cleanup failure must not be silently dropped")

	// The failure occurred before any artifact publication or latest advance.
	require.NoFileExists(t, config.ManifestPath)
	require.NoFileExists(t, config.CompletePath)
	require.NoFileExists(t, config.LatestPath)
	_, _, readErr := ReadLatestPublishedRows(config.LatestPath)
	require.ErrorIs(t, readErr, ErrSnapshotNotPublished)
}

// TestPublishSnapshotContextRefusesNonUTCRunStartedAt proves the active publish
// seam refuses a non-UTC run start on the raw caller value before normalization
// launders it through .UTC(), with no segment/manifest/complete/latest side
// effect. The authoritative-time wire rule (UTC only) must hold at activation,
// not merely inside the lineage path.
func TestPublishSnapshotContextRefusesNonUTCRunStartedAt(t *testing.T) {
	config, _ := publishTestConfig(t)
	edt := time.FixedZone("EDT", -4*60*60)
	// Same instant, non-UTC zone (non-zero offset) — must refuse, not convert.
	config.RunStartedAt = config.RunStartedAt.In(edt)

	_, err := PublishSnapshotContext(context.Background(), config)
	require.Error(t, err)
	require.Contains(t, err.Error(), "UTC", "must surface the authoritative-time classification")

	require.NoDirExists(t, config.SegmentDir, "no segment dir before a refused run start")
	require.NoFileExists(t, config.ManifestPath)
	require.NoFileExists(t, config.CompletePath)
	require.NoFileExists(t, config.LatestPath)
}

// TestPublishSnapshotContextAcceptsUTCRunStartedAt is the paired acceptance case:
// an explicit UTC run start publishes normally through the streaming seam.
func TestPublishSnapshotContextAcceptsUTCRunStartedAt(t *testing.T) {
	config, _ := publishTestConfig(t)
	config.RunStartedAt = config.RunStartedAt.UTC()
	result, err := PublishSnapshotContext(context.Background(), config)
	require.NoError(t, err)
	require.True(t, result.LatestAdvanced)
}
