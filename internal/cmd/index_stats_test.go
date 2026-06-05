package cmd

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

func TestPrintStatsJSONIncludesFailedResumableDiscovery(t *testing.T) {
	endedAt := time.Date(2026, 6, 4, 12, 10, 0, 0, time.UTC)
	summary := &indexstore.IndexSetSummary{
		IndexSetID:          "idx_1234",
		BaseURI:             "s3://bucket/base/",
		Provider:            "s3",
		CreatedAt:           time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC),
		TotalRuns:           2,
		SuccessfulRuns:      1,
		FailedResumableRuns: 1,
		LatestRun: &indexstore.IndexRun{
			RunID:      "run_resumable",
			StartedAt:  time.Date(2026, 6, 4, 12, 5, 0, 0, time.UTC),
			EndedAt:    &endedAt,
			SourceType: enrichHeadSourceType,
			Status:     indexstore.RunStatusFailedResumable,
		},
	}
	indexSet := &indexstore.IndexSet{
		IndexSetID: "idx_1234",
		BaseURI:    "s3://bucket/base/",
		Provider:   "s3",
	}
	runHistory := []indexstore.IndexRun{*summary.LatestRun}

	stdout := captureStdout(t, func() {
		require.NoError(t, printStatsJSON(summary, nil, runHistory, indexSet))
	})

	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &doc))
	runs := doc["runs"].(map[string]any)
	require.Equal(t, float64(1), runs["failed_resumable"])

	latest := doc["latest_run"].(map[string]any)
	require.Equal(t, "run_resumable", latest["run_id"])
	require.Equal(t, "failed-resumable", latest["status"])
	require.Equal(t, "gonimbus index enrich-with-head --resume-run run_resumable", latest["resume_command"])
}
