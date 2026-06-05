package cmd

import (
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

func TestPrintIndexListJSONIncludesFailedResumableResumeCommand(t *testing.T) {
	stdout := captureStdout(t, func() {
		err := printIndexListJSON([]indexListDisplayEntry{{
			DBPath:         "/tmp/index.db",
			DirName:        "idx_1234",
			IdentityPath:   "/tmp/identity.json",
			IdentityStatus: "ok",
			Info: indexstore.IndexListEntry{
				IndexSetID:       "idx_1234",
				BaseURI:          "s3://bucket/base/",
				Provider:         "s3",
				CreatedAt:        time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC),
				RunCount:         1,
				LatestRunID:      "run_123",
				LatestStatus:     string(indexstore.RunStatusFailedResumable),
				LatestSourceType: enrichHeadSourceType,
			},
		}})
		require.NoError(t, err)
	})

	var entries []map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &entries))
	require.Len(t, entries, 1)
	require.Equal(t, "run_123", entries[0]["latest_run_id"])
	require.Equal(t, "failed-resumable", entries[0]["latest_status"])
	require.Equal(t, "gonimbus index enrich-with-head --resume-run run_123", entries[0]["resume_command"])
}

func TestResumeCommandForIndexRun(t *testing.T) {
	require.Equal(t,
		"gonimbus index build --resume-run run_build",
		resumeCommandForIndexRun(string(indexstore.RunStatusFailedResumable), "crawl", "run_build"))
	require.Equal(t,
		"gonimbus index enrich-with-head --resume-run run_enrich",
		resumeCommandForIndexRun(string(indexstore.RunStatusFailedResumable), enrichHeadSourceType, "run_enrich"))
	require.Empty(t, resumeCommandForIndexRun(string(indexstore.RunStatusSuccess), "crawl", "run_success"))
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	fn()

	require.NoError(t, w.Close())
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(data)
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = w
	defer func() { os.Stderr = oldStderr }()

	fn()

	require.NoError(t, w.Close())
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(data)
}
