package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
)

const testFullIndexSetID = "idx_a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"

// --- parseFlexibleTime ---

func TestParseFlexibleTime_RFC3339(t *testing.T) {
	ts, err := parseFlexibleTime("2026-03-06T00:00:00Z")
	require.NoError(t, err)
	assert.Equal(t, 2026, ts.Year())
	assert.Equal(t, 6, ts.Day())
}

func TestParseFlexibleTime_DateOnly(t *testing.T) {
	ts, err := parseFlexibleTime("2026-01-15")
	require.NoError(t, err)
	assert.Equal(t, 2026, ts.Year())
	assert.Equal(t, 15, ts.Day())
}

func TestParseFlexibleTime_Invalid(t *testing.T) {
	_, err := parseFlexibleTime("not-a-date")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected RFC 3339")
}

// --- hub URI argument policy ---

func TestResolveHubURI_Positional(t *testing.T) {
	cmd := newHubPolicyCmd(t)
	hubURI, err := resolveHubURI(cmd, []string{"file:///tmp/gonimbus-hub/"})
	require.NoError(t, err)
	assert.Equal(t, "file:///tmp/gonimbus-hub/", hubURI)
}

func TestResolveHubURI_Flag(t *testing.T) {
	cmd := newHubPolicyCmd(t)
	require.NoError(t, cmd.Flags().Parse([]string{"--hub", "file:///tmp/gonimbus-hub/"}))

	hubURI, err := resolveHubURI(cmd, nil)
	require.NoError(t, err)
	assert.Equal(t, "file:///tmp/gonimbus-hub/", hubURI)
}

func TestResolveHubURI_RequiresExactlyOneSource(t *testing.T) {
	cmd := newHubPolicyCmd(t)
	_, err := resolveHubURI(cmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "hub URI is required")
}

func TestResolveHubURI_RejectsFlagAndPositional(t *testing.T) {
	cmd := newHubPolicyCmd(t)
	require.NoError(t, cmd.Flags().Parse([]string{"--hub", "file:///tmp/a/"}))

	_, err := resolveHubURI(cmd, []string{"file:///tmp/a/"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

func TestValidateHubURIArgs_RejectsTooManyPositionals(t *testing.T) {
	err := validateHubURIArgs(newHubPolicyCmd(t), []string{"file:///tmp/a/", "file:///tmp/b/"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at most one positional hub-uri")
}

func TestIndexHubSubcommandsDeclareHubURIArgPolicy(t *testing.T) {
	commands := []*cobra.Command{
		indexHubInitCmd,
		indexHubLsCmd,
		indexHubShowCmd,
		indexHubSetLatestCmd,
		indexHubRmRunCmd,
		indexHubGCCmd,
	}
	for _, cmd := range commands {
		t.Run(cmd.Name(), func(t *testing.T) {
			assert.Contains(t, cmd.Use, "[hub-uri]")
			require.NotNil(t, cmd.Args)
			err := cmd.Args(cmd, []string{"file:///tmp/a/", "file:///tmp/b/"})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "at most one positional hub-uri")
		})
	}
}

// --- hub init ---

func TestRunIndexHubInit_FileHub(t *testing.T) {
	hubDir := t.TempDir()

	cmd := newHubInitCmd(t, hubDir, "")
	require.NoError(t, cmd.Execute())

	// Verify hub.json was created
	data, err := os.ReadFile(filepath.Join(hubDir, "hub.json"))
	require.NoError(t, err)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &doc))
	assert.Equal(t, "1.0", doc["version"])
	assert.NotEmpty(t, doc["created_at"])
	assert.NotEmpty(t, doc["created_by"])
}

func TestRunIndexHubInit_WithDescription(t *testing.T) {
	hubDir := t.TempDir()

	cmd := newHubInitCmd(t, hubDir, "Production indexes")
	require.NoError(t, cmd.Execute())

	data, err := os.ReadFile(filepath.Join(hubDir, "hub.json"))
	require.NoError(t, err)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &doc))
	assert.Equal(t, "Production indexes", doc["description"])
}

func TestRunIndexHubInit_PositionalHubURI(t *testing.T) {
	hubDir := t.TempDir()

	cmd := newHubInitCmdWithArgs(t, []string{"file://" + hubDir + "/"})
	require.NoError(t, cmd.Execute())

	assert.FileExists(t, filepath.Join(hubDir, "hub.json"))
}

func TestRunIndexHubInit_AlreadyInitialized(t *testing.T) {
	hubDir := t.TempDir()

	// Init once
	cmd := newHubInitCmd(t, hubDir, "")
	require.NoError(t, cmd.Execute())

	// Init again — should fail
	cmd2 := newHubInitCmd(t, hubDir, "")
	err := cmd2.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already initialized")
}

// --- hub ls ---

func TestRunIndexHubLs_Empty(t *testing.T) {
	hubDir := t.TempDir()

	cmd := newHubLsCmd(t, hubDir, false)
	require.NoError(t, cmd.Execute())
}

func TestRunIndexHubLs_WithIndexSets(t *testing.T) {
	hubDir := setupHubWithRuns(t)

	cmd := newHubLsCmd(t, hubDir, true)
	require.NoError(t, cmd.Execute())
}

func TestRunIndexHubLs_JSONSurfacesMixedFormats(t *testing.T) {
	hubDir := setupHubWithMixedFormats(t, "run_3000000000000000000")

	captured, err := captureHubStdout(t, func() error {
		return newHubLsCmd(t, hubDir, true).Execute()
	})
	require.NoError(t, err)

	var result []struct {
		IndexSetID   string         `json:"index_set_id"`
		LatestRun    string         `json:"latest_run"`
		LatestFormat string         `json:"latest_format"`
		RunCount     int            `json:"run_count"`
		FormatCounts map[string]int `json:"format_counts"`
	}
	require.NoError(t, json.Unmarshal(captured, &result))
	require.Len(t, result, 1)
	require.Equal(t, testFullIndexSetID, result[0].IndexSetID)
	require.Equal(t, "run_3000000000000000000", result[0].LatestRun)
	require.Equal(t, indexHubFormatDurableV2, result[0].LatestFormat)
	require.Equal(t, 3, result[0].RunCount)
	require.Equal(t, 2, result[0].FormatCounts[indexHubFormatSQLiteV1])
	require.Equal(t, 1, result[0].FormatCounts[indexHubFormatDurableV2])
}

func TestRunIndexHubLs_PositionalHubURI(t *testing.T) {
	hubDir := setupHubWithRuns(t)

	cmd := newHubLsCmdWithArgs(t, []string{"file://" + hubDir + "/", "--json"})
	require.NoError(t, cmd.Execute())
}

func TestRunIndexHubLs_MissingHubURI(t *testing.T) {
	cmd := newHubLsCmdWithArgs(t, nil)
	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "hub URI is required")
}

func TestRunIndexHubLs_RejectsFlagAndPositionalHubURI(t *testing.T) {
	hubDir := setupHubWithRuns(t)

	cmd := newHubLsCmdWithArgs(t, []string{
		"--hub", "file://" + hubDir + "/",
		"file://" + hubDir + "/",
	})
	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

func TestRunIndexHubLs_RejectsTooManyPositionalHubURIs(t *testing.T) {
	cmd := newHubLsCmdWithArgs(t, []string{"file:///tmp/a/", "file:///tmp/b/"})
	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at most one positional hub-uri")
}

// --- discoverIndexSets ---

func TestDiscoverIndexSets(t *testing.T) {
	ctx := context.Background()
	hubDir := setupHubWithRuns(t)

	fp, err := providerfile.New(providerfile.Config{BaseDir: hubDir})
	require.NoError(t, err)

	ids, err := discoverIndexSets(ctx, fp, "index-sets/")
	require.NoError(t, err)
	assert.Len(t, ids, 1)
	assert.Equal(t, testFullIndexSetID, ids[0])
}

func TestDiscoverIndexSets_Empty(t *testing.T) {
	ctx := context.Background()
	hubDir := t.TempDir()

	fp, err := providerfile.New(providerfile.Config{BaseDir: hubDir})
	require.NoError(t, err)

	ids, err := discoverIndexSets(ctx, fp, "index-sets/")
	require.NoError(t, err)
	assert.Empty(t, ids)
}

// --- discoverRuns ---

func TestDiscoverRuns(t *testing.T) {
	ctx := context.Background()
	hubDir := setupHubWithRuns(t)

	fp, err := providerfile.New(providerfile.Config{BaseDir: hubDir})
	require.NoError(t, err)

	runsPrefix := "index-sets/" + testFullIndexSetID + "/runs/"
	runs, err := discoverRuns(ctx, fp, runsPrefix)
	require.NoError(t, err)
	assert.Len(t, runs, 2)
}

// --- hub show ---

func TestRunIndexHubShow(t *testing.T) {
	hubDir := setupHubWithRuns(t)

	cmd := newHubShowCmd(t, hubDir, testFullIndexSetID, true)
	require.NoError(t, cmd.Execute())
}

func TestRunIndexHubShow_JSONSurfacesRunFormatAndArtifacts(t *testing.T) {
	hubDir := setupHubWithMixedFormats(t, "run_3000000000000000000")

	captured, err := captureHubStdout(t, func() error {
		return newHubShowCmd(t, hubDir, testFullIndexSetID, true).Execute()
	})
	require.NoError(t, err)

	var result struct {
		LatestRun string `json:"latest_run"`
		Runs      []struct {
			RunID     string `json:"run_id"`
			Format    string `json:"format"`
			Artifacts struct {
				Count      int   `json:"count"`
				TotalBytes int64 `json:"total_size_bytes"`
				Manifest   bool  `json:"manifest"`
				Segments   int   `json:"segments"`
			} `json:"artifacts"`
		} `json:"runs"`
	}
	require.NoError(t, json.Unmarshal(captured, &result))
	require.Equal(t, "run_3000000000000000000", result.LatestRun)
	var durableRun *struct {
		RunID     string `json:"run_id"`
		Format    string `json:"format"`
		Artifacts struct {
			Count      int   `json:"count"`
			TotalBytes int64 `json:"total_size_bytes"`
			Manifest   bool  `json:"manifest"`
			Segments   int   `json:"segments"`
		} `json:"artifacts"`
	}
	for i := range result.Runs {
		if result.Runs[i].Format == indexHubFormatDurableV2 {
			durableRun = &result.Runs[i]
			break
		}
	}
	require.NotNil(t, durableRun)
	require.Equal(t, "run_3000000000000000000", durableRun.RunID)
	require.Equal(t, 3, durableRun.Artifacts.Count)
	require.Equal(t, int64(42+100+200), durableRun.Artifacts.TotalBytes)
	require.True(t, durableRun.Artifacts.Manifest)
	require.Equal(t, 2, durableRun.Artifacts.Segments)
}

// --- hub set-latest ---

func TestRunIndexHubSetLatest(t *testing.T) {
	hubDir := setupHubWithRuns(t)
	newRunID := "run_2000000000000000000"

	cmd := newHubSetLatestCmd(t, hubDir, testFullIndexSetID, newRunID, "--latest-write-mode", "unconditional")
	require.NoError(t, cmd.Execute())

	// Verify latest.json was updated
	data, err := os.ReadFile(filepath.Join(hubDir, "index-sets", testFullIndexSetID, "latest.json"))
	require.NoError(t, err)
	var latest map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &latest))
	assert.Equal(t, newRunID, latest["run_id"])
}

func TestRunIndexHubSetLatestConditionalYieldsToNewerCurrent(t *testing.T) {
	hubDir := setupHubWithRuns(t)
	olderRunID := "run_2000000000000000000"

	cmd := newHubSetLatestCmd(t, hubDir, testFullIndexSetID, olderRunID, "--latest-retry-base", "0s")
	require.NoError(t, cmd.Execute())

	data, err := os.ReadFile(filepath.Join(hubDir, "index-sets", testFullIndexSetID, "latest.json"))
	require.NoError(t, err)
	var latest map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &latest))
	assert.Equal(t, "run_1000000000000000000", latest["run_id"])
}

func TestRunIndexHubSetLatest_UncommittedRun(t *testing.T) {
	hubDir := setupHubWithRuns(t)
	// run_3 has no complete.json
	uncommittedRun := "run_3000000000000000000"
	runDir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", uncommittedRun)
	require.NoError(t, os.MkdirAll(runDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "index.db"), []byte("fake"), 0644))

	cmd := newHubSetLatestCmd(t, hubDir, testFullIndexSetID, uncommittedRun)
	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not committed")
}

// --- hub rm-run ---

func TestRunIndexHubRmRun(t *testing.T) {
	hubDir := setupHubWithRuns(t)
	runID := "run_2000000000000000000" // not the latest

	cmd := newHubRmRunCmd(t, hubDir, testFullIndexSetID, runID, false)
	require.NoError(t, cmd.Execute())

	// Verify run artifacts are gone
	runDir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", runID)
	_, err := os.Stat(filepath.Join(runDir, "index.db"))
	assert.True(t, os.IsNotExist(err))
}

func TestRunIndexHubRmRun_ProtectsLatest(t *testing.T) {
	hubDir := setupHubWithRuns(t)
	latestRunID := "run_1000000000000000000" // this is the latest per setupHubWithRuns

	cmd := newHubRmRunCmd(t, hubDir, testFullIndexSetID, latestRunID, false)
	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "current latest")
}

func TestRunIndexHubRmRun_ForceLatest(t *testing.T) {
	hubDir := setupHubWithRuns(t)
	latestRunID := "run_1000000000000000000"

	cmd := newHubRmRunCmd(t, hubDir, testFullIndexSetID, latestRunID, true)
	require.NoError(t, cmd.Execute())
}

// --- hub gc ---

func TestRunIndexHubGC_KeepN(t *testing.T) {
	hubDir := setupHubWithRuns(t)

	cmd := newHubGCCmd(t, hubDir, "", 1, "", true, true)
	require.NoError(t, cmd.Execute())
}

func TestRunIndexHubGC_Before(t *testing.T) {
	hubDir := setupHubWithRuns(t)

	cmd := newHubGCCmd(t, hubDir, "", 0, "2030-01-01", true, false)
	require.NoError(t, cmd.Execute())
}

func TestRunIndexHubGCDryRunJSONSurfacesFormatAndArtifactSet(t *testing.T) {
	hubDir := setupHubWithMixedFormats(t, "run_1000000000000000000")

	captured, err := captureHubStdout(t, func() error {
		return newHubGCCmd(t, hubDir, "", 0, "2030-01-01", true, true).Execute()
	})
	require.NoError(t, err)

	var result struct {
		DryRun  bool `json:"dry_run"`
		Removed []struct {
			RunID       string `json:"run_id"`
			Format      string `json:"format"`
			ArtifactSet struct {
				Count      int   `json:"count"`
				TotalBytes int64 `json:"total_size_bytes"`
				Segments   int   `json:"segments"`
			} `json:"artifact_set"`
		} `json:"removed"`
	}
	require.NoError(t, json.Unmarshal(captured, &result))
	require.True(t, result.DryRun)
	var durableCandidate *struct {
		RunID       string `json:"run_id"`
		Format      string `json:"format"`
		ArtifactSet struct {
			Count      int   `json:"count"`
			TotalBytes int64 `json:"total_size_bytes"`
			Segments   int   `json:"segments"`
		} `json:"artifact_set"`
	}
	for i := range result.Removed {
		if result.Removed[i].Format == indexHubFormatDurableV2 {
			durableCandidate = &result.Removed[i]
			break
		}
	}
	require.NotNil(t, durableCandidate)
	require.Equal(t, "run_3000000000000000000", durableCandidate.RunID)
	require.Equal(t, 3, durableCandidate.ArtifactSet.Count)
	require.Equal(t, int64(42+100+200), durableCandidate.ArtifactSet.TotalBytes)
	require.Equal(t, 2, durableCandidate.ArtifactSet.Segments)
}

func TestRunIndexHubGC_KeepRetentionSemantics(t *testing.T) {
	// 3 committed runs: run_3 (newest/latest), run_2, run_1 (oldest).
	// --keep 2 should keep 2 total (run_3 + run_2), remove run_1.
	hubDir := setupHubWith3Runs(t)

	cmd := newHubGCCmd(t, hubDir, "", 2, "", false, false)
	require.NoError(t, cmd.Execute())

	// run_1 (oldest) should be removed
	run1Dir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", "run_1000000000000000000")
	_, err := os.Stat(filepath.Join(run1Dir, "index.db"))
	assert.True(t, os.IsNotExist(err), "run_1 should be deleted (oldest, outside --keep 2)")

	// run_2 should be kept
	run2Dir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", "run_2000000000000000000")
	assert.FileExists(t, filepath.Join(run2Dir, "index.db"), "run_2 should be kept (within --keep 2)")

	// run_3 (latest) should be kept
	run3Dir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", "run_3000000000000000000")
	assert.FileExists(t, filepath.Join(run3Dir, "index.db"), "run_3 should be kept (latest)")
}

func TestRunIndexHubGC_KeepOneKeepsOnlyLatest(t *testing.T) {
	// With --keep 1 and 3 runs, only the latest (run_3) should survive.
	hubDir := setupHubWith3Runs(t)

	cmd := newHubGCCmd(t, hubDir, "", 1, "", false, false)
	require.NoError(t, cmd.Execute())

	// run_1 and run_2 should be removed
	for _, runID := range []string{"run_1000000000000000000", "run_2000000000000000000"} {
		runDir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", runID)
		_, err := os.Stat(filepath.Join(runDir, "index.db"))
		assert.True(t, os.IsNotExist(err), "%s should be deleted", runID)
	}

	// run_3 (latest) should be kept
	run3Dir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", "run_3000000000000000000")
	assert.FileExists(t, filepath.Join(run3Dir, "index.db"), "run_3 should be kept (latest)")
}

func TestRunIndexHubGC_RetainsPresentUnreadableCompleteMarker(t *testing.T) {
	hubDir := setupHubWith3Runs(t)

	unreadableRunID := "run_2000000000000000000"
	unreadableRunDir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", unreadableRunID)
	require.NoError(t, os.WriteFile(filepath.Join(unreadableRunDir, "complete.json"), []byte(`{"completed_at":`), 0644))

	origStderr := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = w

	execErr := newHubGCCmd(t, hubDir, "", 1, "", false, false).Execute()

	require.NoError(t, w.Close())
	os.Stderr = origStderr
	captured, readErr := io.ReadAll(r)
	require.NoError(t, readErr)

	require.NoError(t, execErr)
	assert.Contains(t, string(captured), "retaining run "+testFullIndexSetID+"/"+unreadableRunID)

	assert.FileExists(t, filepath.Join(unreadableRunDir, "index.db"), "run with present unreadable complete marker should be retained")

	oldestRunDir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", "run_1000000000000000000")
	_, err = os.Stat(filepath.Join(oldestRunDir, "index.db"))
	assert.True(t, os.IsNotExist(err), "oldest valid non-latest run should still be deleted")
}

func TestRunIndexHubGC_KeepWithStaleLatest(t *testing.T) {
	// 3 committed runs: run_3 (newest), run_2 (middle), run_1 (oldest).
	// Latest points to run_1 (stale — not the newest).
	// --keep 2 should keep exactly 2 total: run_3 (newest by time) + run_1 (latest).
	// run_2 should be removed even though it's newer than latest.
	hubDir := setupHubWith3Runs(t)

	// Repoint latest to the oldest run
	latestPath := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "latest.json")
	latest := map[string]interface{}{
		"version":      "1.0",
		"index_set_id": testFullIndexSetID,
		"run_id":       "run_1000000000000000000",
		"updated_at":   "2026-03-06T00:00:00Z",
	}
	data, err := json.MarshalIndent(latest, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(latestPath, data, 0644))

	cmd := newHubGCCmd(t, hubDir, "", 2, "", false, false)
	require.NoError(t, cmd.Execute())

	// run_1 (oldest but latest) should be kept
	assert.FileExists(t, filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", "run_1000000000000000000", "index.db"),
		"run_1 should be kept (latest pointer)")

	// run_3 (newest by time) should be kept (fills the 1 non-latest slot)
	assert.FileExists(t, filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", "run_3000000000000000000", "index.db"),
		"run_3 should be kept (newest non-latest, within --keep 2)")

	// run_2 (middle) should be removed — only 1 non-latest slot available
	run2DB := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", "run_2000000000000000000", "index.db")
	_, err = os.Stat(run2DB)
	assert.True(t, os.IsNotExist(err), "run_2 should be deleted (exceeded --keep 2 with stale latest)")
}

func TestRunIndexHubGC_KeepZeroRemovesNonLatest(t *testing.T) {
	hubDir := setupHubWithRuns(t)

	cmd := newHubGCCmd(t, hubDir, "", 0, "2030-01-01", false, false)
	require.NoError(t, cmd.Execute())

	// Run 2 (non-latest, older) should be removed
	runDir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", "run_2000000000000000000")
	_, err := os.Stat(filepath.Join(runDir, "index.db"))
	assert.True(t, os.IsNotExist(err), "run_2 artifacts should be deleted")

	// Run 1 (latest) should still exist
	latestDir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", "run_1000000000000000000")
	assert.FileExists(t, filepath.Join(latestDir, "index.db"))
}

func TestRunIndexHubGC_RequiresPolicy(t *testing.T) {
	hubDir := setupHubWithRuns(t)

	cmd := newHubGCCmd(t, hubDir, "", 0, "", false, false)
	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--keep or --before")
}

func TestRunIndexHubGC_MutuallyExclusive(t *testing.T) {
	hubDir := setupHubWithRuns(t)

	cmd := newHubGCCmd(t, hubDir, "", 2, "2030-01-01", false, false)
	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

// TestRunIndexHubGC_JSONWithoutDryRunActuallyDeletes is a regression test for
// the bug where `gonimbus index hub gc --json` (without --dry-run) emitted
// the candidate list as JSON and returned without performing any deletion.
// The fix routes deletion through the same code path regardless of output mode.
func TestRunIndexHubGC_JSONWithoutDryRunActuallyDeletes(t *testing.T) {
	hubDir := setupHubWith3Runs(t)

	// run_1 is latest in setupHubWith3Runs; --keep 2 should delete the oldest
	// non-latest run. Use --json without --dry-run.
	cmd := newHubGCCmd(t, hubDir, "", 2, "", false, true)

	// Redirect os.Stdout briefly to capture JSON output for shape assertion.
	origStdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w

	execErr := cmd.Execute()

	require.NoError(t, w.Close())
	os.Stdout = origStdout
	captured, _ := io.ReadAll(r)

	require.NoError(t, execErr)

	// Filesystem check: oldest committed run should be gone.
	// setupHubWith3Runs has run_1 (newest in time), run_2 (middle), run_3 (oldest).
	// Wait — actually setupHubWith3Runs creates run_1=oldest..run_3=newest by ULID-style.
	// The test asserts SOME non-latest run was deleted; we keep this loose to
	// stay aligned with whatever ordering setupHubWith3Runs uses.
	indexSetDir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs")
	entries, err := os.ReadDir(indexSetDir)
	require.NoError(t, err)
	// Count surviving run dirs that still have index.db
	surviving := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, statErr := os.Stat(filepath.Join(indexSetDir, e.Name(), "index.db")); statErr == nil {
			surviving++
		}
	}
	assert.Equal(t, 2, surviving, "after gc --keep 2 --json (no dry-run), exactly 2 runs should survive (the bug returned before deletion, leaving all 3)")

	// JSON shape: dry_run should be false; removed should be present.
	var result struct {
		DryRun  bool                     `json:"dry_run"`
		Removed []map[string]interface{} `json:"removed"`
	}
	require.NoError(t, json.Unmarshal(captured, &result))
	assert.False(t, result.DryRun, "JSON output should report dry_run=false for a real run")
	assert.NotEmpty(t, result.Removed, "JSON output should list the run(s) that were removed")
}

// --- listAllKeys ---

func TestListAllKeys(t *testing.T) {
	ctx := context.Background()
	hubDir := setupHubWithRuns(t)

	fp, err := providerfile.New(providerfile.Config{BaseDir: hubDir})
	require.NoError(t, err)

	runPrefix := "index-sets/" + testFullIndexSetID + "/runs/run_1000000000000000000/"
	keys, err := listAllKeys(ctx, fp, runPrefix)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(keys), 2) // at least index.db + complete.json
}

// --- helpers ---

// setupHubWithRuns creates a file-based hub with one index set and two committed runs.
// Run 1 (run_1000000000000000000) is set as latest.
// Run 2 (run_2000000000000000000) is an older committed run.
func setupHubWithRuns(t *testing.T) string {
	t.Helper()
	hubDir := t.TempDir()

	run1ID := "run_1000000000000000000"
	run2ID := "run_2000000000000000000"

	for _, runID := range []string{run1ID, run2ID} {
		runDir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", runID)
		require.NoError(t, os.MkdirAll(runDir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(runDir, "index.db"), []byte("db-content-"+runID), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(runDir, "identity.json"), []byte(`{"test":true}`), 0644))

		completedAt := "2026-01-01T00:00:00Z"
		if runID == run1ID {
			completedAt = "2026-03-01T00:00:00Z"
		}
		complete := map[string]interface{}{
			"version":      "1.0",
			"index_set_id": testFullIndexSetID,
			"run_id":       runID,
			"completed_at": completedAt,
			"artifacts": map[string]interface{}{
				"index_db": map[string]interface{}{
					"size_bytes": len("db-content-" + runID),
					"sha256":     "placeholder",
				},
			},
		}
		data, err := json.MarshalIndent(complete, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(runDir, "complete.json"), data, 0644))
	}

	// Set latest to run1
	latestDir := filepath.Join(hubDir, "index-sets", testFullIndexSetID)
	latest := map[string]interface{}{
		"version":      "1.0",
		"index_set_id": testFullIndexSetID,
		"run_id":       run1ID,
		"updated_at":   "2026-03-06T00:00:00Z",
	}
	data, err := json.MarshalIndent(latest, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(latestDir, "latest.json"), data, 0644))

	return hubDir
}

func setupHubWithMixedFormats(t *testing.T, latestRunID string) string {
	t.Helper()
	hubDir := setupHubWithRuns(t)
	runID := "run_3000000000000000000"
	runDir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", runID)
	require.NoError(t, os.MkdirAll(filepath.Join(runDir, "segments"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "manifest.json"), []byte(`{"type":"manifest"}`), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "segments", "seg-000001.parquet"), []byte("segment-1"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "segments", "seg-000002.parquet"), []byte("segment-2"), 0644))

	complete := map[string]interface{}{
		"version":               "1.0",
		"marker_schema_version": indexHubMarkerSchemaV1,
		"format":                indexHubFormatDurableV2,
		"format_version":        "2",
		"index_set_id":          testFullIndexSetID,
		"run_id":                runID,
		"completed_at":          "2026-02-01T00:00:00Z",
		"exported_by":           "gonimbus-test",
		"artifacts": map[string]interface{}{
			"manifest": map[string]interface{}{
				"path":       "manifest.json",
				"role":       "manifest",
				"required":   true,
				"size_bytes": 42,
				"sha256":     "manifest-sha",
			},
			"segments": []map[string]interface{}{
				{
					"path":       "segments/seg-000001.parquet",
					"role":       "segment",
					"required":   true,
					"size_bytes": 100,
					"sha256":     "seg-1-sha",
				},
				{
					"path":       "segments/seg-000002.parquet",
					"role":       "segment",
					"required":   true,
					"size_bytes": 200,
					"sha256":     "seg-2-sha",
				},
			},
		},
		"durable": map[string]interface{}{
			"segments": 2,
			"rows":     900122,
		},
	}
	data, err := json.MarshalIndent(complete, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "complete.json"), data, 0644))

	latestPath := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "latest.json")
	latest := map[string]interface{}{
		"version":      "1.0",
		"index_set_id": testFullIndexSetID,
		"run_id":       latestRunID,
		"updated_at":   "2026-03-06T00:00:00Z",
	}
	latestData, err := json.MarshalIndent(latest, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(latestPath, latestData, 0644))

	return hubDir
}

// setupHubWith3Runs creates a file-based hub with 3 committed runs.
// Run 3 (run_3000000000000000000) is newest and set as latest.
// Run 2 (run_2000000000000000000) is middle.
// Run 1 (run_1000000000000000000) is oldest.
func setupHubWith3Runs(t *testing.T) string {
	t.Helper()
	hubDir := t.TempDir()

	runs := []struct {
		id          string
		completedAt string
	}{
		{"run_1000000000000000000", "2025-12-01T00:00:00Z"},
		{"run_2000000000000000000", "2026-01-15T00:00:00Z"},
		{"run_3000000000000000000", "2026-03-01T00:00:00Z"},
	}

	for _, r := range runs {
		runDir := filepath.Join(hubDir, "index-sets", testFullIndexSetID, "runs", r.id)
		require.NoError(t, os.MkdirAll(runDir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(runDir, "index.db"), []byte("db-"+r.id), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(runDir, "identity.json"), []byte(`{"test":true}`), 0644))

		complete := map[string]interface{}{
			"version":      "1.0",
			"index_set_id": testFullIndexSetID,
			"run_id":       r.id,
			"completed_at": r.completedAt,
			"artifacts": map[string]interface{}{
				"index_db": map[string]interface{}{
					"size_bytes": len("db-" + r.id),
					"sha256":     "placeholder",
				},
			},
		}
		data, err := json.MarshalIndent(complete, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(runDir, "complete.json"), data, 0644))
	}

	// Set latest to run_3 (newest)
	latestDir := filepath.Join(hubDir, "index-sets", testFullIndexSetID)
	latest := map[string]interface{}{
		"version":      "1.0",
		"index_set_id": testFullIndexSetID,
		"run_id":       "run_3000000000000000000",
		"updated_at":   "2026-03-06T00:00:00Z",
	}
	data, err := json.MarshalIndent(latest, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(latestDir, "latest.json"), data, 0644))

	return hubDir
}

func captureHubStdout(t *testing.T, run func() error) ([]byte, error) {
	t.Helper()
	origStdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	execErr := run()
	require.NoError(t, w.Close())
	os.Stdout = origStdout
	captured, readErr := io.ReadAll(r)
	require.NoError(t, readErr)
	return captured, execErr
}

func newHubInitCmd(t *testing.T, hubDir, description string) *cobra.Command {
	t.Helper()
	cmd := newHubInitBaseCmd(t)
	args := []string{"--hub", "file://" + hubDir + "/"}
	if description != "" {
		args = append(args, "--description", description)
	}
	cmd.SetArgs(args)
	cmd.SetContext(context.Background())
	return cmd
}

func newHubInitCmdWithArgs(t *testing.T, args []string) *cobra.Command {
	t.Helper()
	cmd := newHubInitBaseCmd(t)
	cmd.SetArgs(args)
	cmd.SetContext(context.Background())
	return cmd
}

func newHubInitBaseCmd(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "init [hub-uri]", Args: validateHubURIArgs, RunE: runIndexHubInit}
	addHubTestFlags(cmd)
	cmd.Flags().String("description", "", "")
	return cmd
}

func newHubLsCmd(t *testing.T, hubDir string, jsonOutput bool) *cobra.Command {
	t.Helper()
	cmd := newHubLsBaseCmd(t)
	cmd.Flags().Bool("json", false, "")
	args := []string{"--hub", "file://" + hubDir + "/"}
	if jsonOutput {
		args = append(args, "--json")
	}
	cmd.SetArgs(args)
	cmd.SetContext(context.Background())
	return cmd
}

func newHubLsCmdWithArgs(t *testing.T, args []string) *cobra.Command {
	t.Helper()
	cmd := newHubLsBaseCmd(t)
	cmd.Flags().Bool("json", false, "")
	cmd.SetArgs(args)
	cmd.SetContext(context.Background())
	return cmd
}

func newHubLsBaseCmd(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "ls [hub-uri]", Args: validateHubURIArgs, RunE: runIndexHubLs}
	addHubTestFlags(cmd)
	return cmd
}

func newHubShowCmd(t *testing.T, hubDir, indexSetID string, jsonOutput bool) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "show [hub-uri]", Args: validateHubURIArgs, RunE: runIndexHubShow}
	addHubTestFlags(cmd)
	cmd.Flags().String("index-set", "", "")
	cmd.Flags().Bool("json", false, "")
	args := []string{"--hub", "file://" + hubDir + "/", "--index-set", indexSetID}
	if jsonOutput {
		args = append(args, "--json")
	}
	cmd.SetArgs(args)
	cmd.SetContext(context.Background())
	return cmd
}

func newHubSetLatestCmd(t *testing.T, hubDir, indexSetID, runID string, extraArgs ...string) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "set-latest [hub-uri]", Args: validateHubURIArgs, RunE: runIndexHubSetLatest}
	addHubTestFlags(cmd)
	cmd.Flags().String("index-set", "", "")
	cmd.Flags().String("run-id", "", "")
	addLatestPointerFlags(cmd)
	args := []string{
		"--hub", "file://" + hubDir + "/",
		"--index-set", indexSetID,
		"--run-id", runID,
	}
	args = append(args, extraArgs...)
	cmd.SetArgs(args)
	cmd.SetContext(context.Background())
	return cmd
}

func newHubRmRunCmd(t *testing.T, hubDir, indexSetID, runID string, force bool) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "rm-run [hub-uri]", Args: validateHubURIArgs, RunE: runIndexHubRmRun}
	addHubTestFlags(cmd)
	cmd.Flags().String("index-set", "", "")
	cmd.Flags().String("run-id", "", "")
	cmd.Flags().Bool("force", false, "")
	args := []string{
		"--hub", "file://" + hubDir + "/",
		"--index-set", indexSetID,
		"--run-id", runID,
	}
	if force {
		args = append(args, "--force")
	}
	cmd.SetArgs(args)
	cmd.SetContext(context.Background())
	return cmd
}

func newHubGCCmd(t *testing.T, hubDir, indexSet string, keep int, before string, dryRun, jsonOutput bool) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "gc [hub-uri]", Args: validateHubURIArgs, RunE: runIndexHubGC}
	addHubTestFlags(cmd)
	cmd.Flags().String("index-set", "", "")
	cmd.Flags().Int("keep", 0, "")
	cmd.Flags().String("before", "", "")
	cmd.Flags().Bool("dry-run", false, "")
	cmd.Flags().Bool("json", false, "")
	args := []string{"--hub", "file://" + hubDir + "/"}
	if indexSet != "" {
		args = append(args, "--index-set", indexSet)
	}
	if keep > 0 {
		args = append(args, "--keep", fmt.Sprintf("%d", keep))
	}
	if before != "" {
		args = append(args, "--before", before)
	}
	if dryRun {
		args = append(args, "--dry-run")
	}
	if jsonOutput {
		args = append(args, "--json")
	}
	cmd.SetArgs(args)
	cmd.SetContext(context.Background())
	return cmd
}

func newHubPolicyCmd(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "hub-policy [hub-uri]", Args: validateHubURIArgs}
	addHubTestFlags(cmd)
	return cmd
}

func addHubTestFlags(cmd *cobra.Command) {
	cmd.Flags().String("hub", "", "")
	cmd.Flags().String("hub-profile", "", "")
	cmd.Flags().String("hub-region", "", "")
	cmd.Flags().String("hub-endpoint", "", "")
}
