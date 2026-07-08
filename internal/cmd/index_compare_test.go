package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexcompare"
	"github.com/3leaps/gonimbus/internal/indexsubstrate"
)

func TestIndexCompareDurableDeltaEmitsJSON(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	beforeManifestPath, beforeDir := writeCompareCommandSnapshot(t, "run_before", base, nil)
	afterManifestPath, afterDir := writeCompareCommandSnapshot(t, "run_after", base.Add(time.Hour), []indexsubstrate.CurrentObjectRow{
		compareCommandRow("data/added.xml", 10, base.Add(time.Hour), "etag-added"),
	})

	cmd := newCompareDurableDeltaTestCommand()
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"--before-manifest", beforeManifestPath,
		"--before-segments", beforeDir,
		"--after-manifest", afterManifestPath,
		"--after-segments", afterDir,
		"--max-changes", "1",
	})
	cmd.SetContext(context.Background())
	require.NoError(t, cmd.Execute())

	var report indexcompare.DurableDeltaReport
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
	require.Equal(t, "gonimbus.index.durable_delta_result.v1", report.Type)
	require.Equal(t, int64(1), report.Added)
	require.Len(t, report.Changes, 1)
	require.Equal(t, "data/added.xml", report.Changes[0].RelKey)
}

func newCompareDurableDeltaTestCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "durable-delta", RunE: runIndexCompareDurableDelta}
	cmd.Flags().String("before-manifest", "", "")
	cmd.Flags().String("before-segments", "", "")
	cmd.Flags().String("after-manifest", "", "")
	cmd.Flags().String("after-segments", "", "")
	cmd.Flags().Int("max-changes", indexcompare.DefaultMaxMismatches, "")
	return cmd
}

func writeCompareCommandSnapshot(t *testing.T, runID string, createdAt time.Time, rows []indexsubstrate.CurrentObjectRow) (string, string) {
	t.Helper()
	root := t.TempDir()
	segmentDir := filepath.Join(root, "segments")
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir:                  segmentDir,
		IndexSetID:           "idx_compare_command",
		RunID:                runID,
		CreatedAt:            createdAt,
		TargetRowsPerSegment: 1,
		Coverage: []indexsubstrate.CoverageAttestation{{
			Scope:    &indexsubstrate.Scope{Prefix: "data/"},
			Basis:    indexsubstrate.CoverageBasisConfirmed,
			Complete: true,
		}},
	}, rows)
	require.NoError(t, err)
	manifestPath := filepath.Join(root, "manifest.json")
	require.NoError(t, indexsubstrate.WriteInternalManifestFile(manifestPath, manifest))
	return manifestPath, segmentDir
}

func compareCommandRow(relKey string, size int64, modified time.Time, etag string) indexsubstrate.CurrentObjectRow {
	return indexsubstrate.CurrentObjectRow{
		IndexSetID:       "idx_compare_command",
		RelKey:           relKey,
		SizeBytes:        size,
		LastModified:     &modified,
		ETag:             etag,
		FirstSeenRunID:   "run_after",
		FirstSeenAt:      modified,
		LastChangedRunID: "run_after",
		LastChangedAt:    modified,
		LastSeenRunID:    "run_after",
		LastSeenAt:       modified,
	}
}
