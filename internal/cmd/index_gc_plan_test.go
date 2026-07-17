package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
)

func TestBuildIndexGCPlanDurableMultiRootDeterministic(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	journalRoot := filepath.Join(dataRoot, "journals", "crawl", env.indexSetID)
	require.NoError(t, os.MkdirAll(filepath.Join(journalRoot, "run_cli_1"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(journalRoot, "run_cli_1", "crawl.jsonl"), []byte("sealed\n"), 0o600))

	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	first, err := buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, now)
	require.NoError(t, err)
	second, err := buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, now.Add(time.Hour))
	require.NoError(t, err)

	require.Equal(t, first.PlanSHA256, second.PlanSHA256)
	require.Equal(t, indexGCPlanType, first.Type)
	require.Len(t, first.Candidates, 1)
	require.Empty(t, first.Warnings)
	candidate := first.Candidates[0]
	require.Equal(t, env.indexSetID, candidate.Info.IndexSetID)
	require.Equal(t, []string{"durable-v2"}, candidate.Formats)
	require.Len(t, candidate.Targets, 3)
	require.Equal(t, []string{"segment-set", "identity", "journals"}, []string{
		candidate.Targets[0].Kind,
		candidate.Targets[1].Kind,
		candidate.Targets[2].Kind,
	})
	require.Positive(t, candidate.PlanSize)
	for _, target := range candidate.Targets {
		require.Len(t, target.TreeSHA256, 64)
		require.DirExists(t, target.Path)
	}
}

func TestBuildIndexGCPlanGroupsBothFormatsAsOneSet(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	db := openIndexGCTestDB(t, filepath.Join(env.identityDir, "index.db"))
	require.NoError(t, indexstore.Migrate(context.Background(), db))
	set, _, err := indexstore.FindOrCreateIndexSet(context.Background(), db, env.params)
	require.NoError(t, err)
	require.Equal(t, env.indexSetID, set.IndexSetID)
	require.NoError(t, db.Close())

	plan, err := buildIndexGCPlan(context.Background(), 0, "", 0, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Len(t, plan.Candidates, 1, "warnings: %#v", plan.Warnings)
	require.Equal(t, []string{"durable-v2", "sqlite-v1"}, plan.Candidates[0].Formats)
	require.Len(t, plan.Candidates[0].Targets, 2, "shared identity root must appear exactly once")
}

func TestBuildIndexGCPlanSQLiteReadOnlyIsImmutable(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	dbPath := filepath.Join(env.identityDir, "index.db")
	db := openIndexGCTestDB(t, dbPath)
	require.NoError(t, indexstore.Migrate(context.Background(), db))
	_, _, err := indexstore.FindOrCreateIndexSet(context.Background(), db, env.params)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")

	require.NoError(t, os.Chmod(dbPath, 0o400))
	require.NoError(t, os.Chmod(env.identityDir, 0o500))
	t.Cleanup(func() {
		_ = os.Chmod(env.identityDir, 0o700)
		_ = os.Chmod(dbPath, 0o600)
	})
	before := snapshotIndexGCTreeState(t, env.identityDir)
	plan, err := buildIndexGCPlan(context.Background(), 0, "", 0, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Len(t, plan.Candidates, 1, "warnings: %#v", plan.Warnings)
	require.Equal(t, []string{"durable-v2", "sqlite-v1"}, plan.Candidates[0].Formats)
	require.Equal(t, before, snapshotIndexGCTreeState(t, env.identityDir))
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")
}

func TestBuildIndexGCPlanRetainsOlderSQLiteSchemaWithoutMigration(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	dbPath := filepath.Join(env.identityDir, "index.db")
	db := openIndexGCTestDB(t, dbPath)
	_, err := db.ExecContext(context.Background(), `CREATE TABLE schema_meta (id INTEGER PRIMARY KEY, schema_version INTEGER NOT NULL)`)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `INSERT INTO schema_meta (id, schema_version) VALUES (1, 4)`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	before := snapshotIndexGCTreeState(t, env.identityDir)

	plan, err := buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Empty(t, plan.Candidates)
	require.Condition(t, func() bool {
		for _, warning := range plan.Warnings {
			if warning.IndexSetID == env.indexSetID && strings.Contains(warning.Reason, "without migration") {
				return true
			}
		}
		return false
	})
	require.Equal(t, before, snapshotIndexGCTreeState(t, env.identityDir))

	readDB, err := indexstore.OpenLocalReadOnly(context.Background(), dbPath)
	require.NoError(t, err)
	defer func() { _ = readDB.Close() }()
	var version int
	require.NoError(t, readDB.QueryRowContext(context.Background(), `SELECT schema_version FROM schema_meta WHERE id=1`).Scan(&version))
	require.Equal(t, 4, version)
}

func TestBuildIndexGCPlanRetainsSQLiteWithTransactionSidecars(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed func(t *testing.T, env durableCLIEnv, dbPath string) func()
	}{
		{
			name: "live wal writer",
			seed: func(t *testing.T, env durableCLIEnv, dbPath string) func() {
				db := openIndexGCTestDB(t, dbPath)
				require.NoError(t, indexstore.Migrate(context.Background(), db))
				_, err := db.ExecContext(context.Background(), `PRAGMA wal_autocheckpoint=0`)
				require.NoError(t, err)
				_, _, err = indexstore.FindOrCreateIndexSet(context.Background(), db, env.params)
				require.NoError(t, err)
				require.FileExists(t, dbPath+"-wal")
				require.FileExists(t, dbPath+"-shm")
				return func() { _ = db.Close() }
			},
		},
		{
			name: "rollback journal",
			seed: func(t *testing.T, env durableCLIEnv, dbPath string) func() {
				db := openIndexGCTestDB(t, dbPath)
				require.NoError(t, indexstore.Migrate(context.Background(), db))
				_, _, err := indexstore.FindOrCreateIndexSet(context.Background(), db, env.params)
				require.NoError(t, err)
				require.NoError(t, db.Close())
				require.NoError(t, os.WriteFile(dbPath+"-journal", []byte("conservative rollback marker\n"), 0o600))
				return func() {}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetAppDataRootTestState(t)
			dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
			t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
			env := seedDurableOnlyAppData(t, dataRoot, nil)
			dbPath := filepath.Join(env.identityDir, "index.db")
			cleanup := tc.seed(t, env, dbPath)
			defer cleanup()
			before := snapshotIndexGCTreeState(t, env.identityDir)

			plan, err := buildIndexGCPlan(context.Background(), 0, "", 0, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
			require.NoError(t, err)
			require.Empty(t, plan.Candidates)
			require.Condition(t, func() bool {
				for _, warning := range plan.Warnings {
					if warning.IndexSetID == env.indexSetID && strings.Contains(warning.Reason, "transaction sidecars") {
						return true
					}
				}
				return false
			})
			require.Equal(t, before, snapshotIndexGCTreeState(t, env.identityDir))
		})
	}
}

func TestBuildIndexGCPlanRejectsSQLiteSymlinkBeforeOpen(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	outsideDir := t.TempDir()
	outsideDB := filepath.Join(outsideDir, "outside.db")
	db := openIndexGCTestDB(t, outsideDB)
	require.NoError(t, indexstore.Migrate(context.Background(), db))
	_, _, err := indexstore.FindOrCreateIndexSet(context.Background(), db, env.params)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	before := snapshotIndexGCTreeState(t, outsideDir)
	if err := os.Symlink(outsideDB, filepath.Join(env.identityDir, "index.db")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	plan, err := buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Empty(t, plan.Candidates)
	require.Equal(t, before, snapshotIndexGCTreeState(t, outsideDir))
	require.NoFileExists(t, outsideDB+"-wal")
	require.NoFileExists(t, outsideDB+"-shm")
}

type indexGCTestPathState struct {
	Mode    os.FileMode
	Size    int64
	ModTime time.Time
	Content string
}

func snapshotIndexGCTreeState(t *testing.T, root string) map[string]indexGCTestPathState {
	t.Helper()
	out := make(map[string]indexGCTestPathState)
	require.NoError(t, filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		state := indexGCTestPathState{Mode: info.Mode(), Size: info.Size(), ModTime: info.ModTime()}
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			state.Content = string(data)
		} else if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			state.Content = target
		}
		out[filepath.ToSlash(rel)] = state
		return nil
	}))
	return out
}

func TestBuildIndexGCPlanFailsClosedOnSymlinkAndHeldLease(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	segmentRoot := filepath.Join(dataRoot, "cache", "segments", env.indexSetID)

	lease, err := indexsubstrate.AcquireWriteLease(segmentRoot, env.indexSetID, "test-peer", 0)
	require.NoError(t, err)
	plan, err := buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Empty(t, plan.Candidates)
	require.NotEmpty(t, plan.Warnings)
	require.NoError(t, lease.Release())

	outside := t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(segmentRoot, "outside-link")))
	plan, err = buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Empty(t, plan.Candidates)
	require.NotEmpty(t, plan.Warnings)
}

func TestBuildIndexGCPlanFailsClosedOnArtifactRootAlias(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	_ = seedDurableOnlyAppData(t, dataRoot, nil)
	require.NoError(t, os.MkdirAll(filepath.Join(dataRoot, "journals"), 0o700))
	require.NoError(t, os.Symlink(t.TempDir(), filepath.Join(dataRoot, "journals", "crawl")))

	_, err := buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
	require.ErrorContains(t, err, "artifact root")
}

func TestBuildIndexGCPlanRetainsCorruptDurableRoot(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	latestPath := filepath.Join(dataRoot, "cache", "segments", env.indexSetID, "latest.json")
	require.NoError(t, os.WriteFile(latestPath, []byte("not-json\n"), 0o600))

	plan, err := buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Empty(t, plan.Candidates)
	require.Condition(t, func() bool {
		for _, warning := range plan.Warnings {
			if warning.IndexSetID == env.indexSetID && strings.Contains(warning.Reason, "unproven") {
				return true
			}
		}
		return false
	})
}

func TestBuildIndexGCPlanRetainsSetWithActiveManagedJob(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	jobsRoot := filepath.Join(dataRoot, "jobs", "index-build")
	store := jobregistry.NewStore(jobsRoot)
	require.NoError(t, store.Write(&jobregistry.JobRecord{
		JobID:      "11111111-1111-4111-8111-111111111111",
		Type:       jobregistry.JobTypeIndexBuild,
		State:      jobregistry.JobStateRunning,
		IndexSetID: env.indexSetID,
		CreatedAt:  time.Now().UTC(),
	}))

	plan, err := buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Empty(t, plan.Candidates)
	require.Condition(t, func() bool {
		for _, warning := range plan.Warnings {
			if warning.IndexSetID == env.indexSetID && warning.Reason != "" {
				return true
			}
		}
		return false
	})
}

func TestBuildIndexGCPlanJobInspectionIsStrictAndReadOnly(t *testing.T) {
	t.Run("dead pid record stays byte identical", func(t *testing.T) {
		resetAppDataRootTestState(t)
		dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
		t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
		env := seedDurableOnlyAppData(t, dataRoot, nil)
		store := jobregistry.NewStore(filepath.Join(dataRoot, "jobs", "index-build"))
		require.NoError(t, store.Write(&jobregistry.JobRecord{
			JobID:      "11111111-1111-4111-8111-111111111111",
			Type:       jobregistry.JobTypeIndexBuild,
			State:      jobregistry.JobStateRunning,
			PID:        1 << 30,
			IndexSetID: env.indexSetID,
			CreatedAt:  time.Now().UTC(),
		}))
		before, err := os.ReadFile(store.JobPath("11111111-1111-4111-8111-111111111111"))
		require.NoError(t, err)
		plan, err := buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		require.Empty(t, plan.Candidates)
		after, err := os.ReadFile(store.JobPath("11111111-1111-4111-8111-111111111111"))
		require.NoError(t, err)
		require.Equal(t, before, after)
	})

	for _, tc := range []struct {
		name string
		seed func(t *testing.T, jobsRoot string)
	}{
		{
			name: "malformed canonical record",
			seed: func(t *testing.T, jobsRoot string) {
				jobDir := filepath.Join(jobsRoot, "22222222-2222-4222-8222-222222222222")
				require.NoError(t, os.MkdirAll(jobDir, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(jobDir, "job.json"), []byte("{broken\n"), 0o600))
			},
		},
		{
			name: "omitted persisted state",
			seed: func(t *testing.T, jobsRoot string) {
				jobDir := filepath.Join(jobsRoot, "22222222-2222-4222-8222-222222222222")
				require.NoError(t, os.MkdirAll(jobDir, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(jobDir, "job.json"), []byte(`{"job_id":"22222222-2222-4222-8222-222222222222"}`+"\n"), 0o600))
			},
		},
		{
			name: "arbitrary persisted state",
			seed: func(t *testing.T, jobsRoot string) {
				jobDir := filepath.Join(jobsRoot, "22222222-2222-4222-8222-222222222222")
				require.NoError(t, os.MkdirAll(jobDir, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(jobDir, "job.json"), []byte(`{"job_id":"22222222-2222-4222-8222-222222222222","state":"runnnig"}`+"\n"), 0o600))
			},
		},
		{
			name: "symlinked canonical job directory",
			seed: func(t *testing.T, jobsRoot string) {
				require.NoError(t, os.MkdirAll(jobsRoot, 0o700))
				if err := os.Symlink(t.TempDir(), filepath.Join(jobsRoot, "22222222-2222-4222-8222-222222222222")); err != nil {
					t.Skipf("symlink unavailable: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetAppDataRootTestState(t)
			dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
			t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
			_ = seedDurableOnlyAppData(t, dataRoot, nil)
			tc.seed(t, filepath.Join(dataRoot, "jobs", "index-build"))
			_, err := buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
			require.ErrorContains(t, err, "inspect active index jobs")
		})
	}
}

func TestBuildIndexGCPlanRetainsSetWithActiveCheckpoint(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	store, err := opcheckpoint.Open(context.Background(), opcheckpoint.Config{AppDataDir: dataRoot})
	require.NoError(t, err)
	payload := []byte(`{"config":{"index_set_id":"` + env.indexSetID + `"}}`)
	require.NoError(t, store.WriteCheckpoint(context.Background(), opcheckpoint.Envelope{
		Operation:         "index-build",
		RunID:             "run_checkpoint_1",
		ConfigFingerprint: strings.Repeat("a", 64),
		Status:            opcheckpoint.StatusFailedResumable,
		Payload:           payload,
	}))

	plan, err := buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Empty(t, plan.Candidates)
	require.Condition(t, func() bool {
		for _, warning := range plan.Warnings {
			if warning.IndexSetID == env.indexSetID && strings.Contains(warning.Reason, "checkpoint") {
				return true
			}
		}
		return false
	})
}

// TestDoctorInventoriesVerificationProjectionsAndGCStaysSetScoped pins the
// contract-8 lifecycle classification for run-scoped verification projections:
// doctor inventories the attempts by name, a dry-run GC leaves the residue
// byte-intact (run-scoping grants isolation, never deletion authority), and
// the only removal path is the receipt-backed whole-set deletion — which must
// succeed with the residue present rather than treating it as an unexpected
// tree.
func TestDoctorInventoriesVerificationProjectionsAndGCStaysSetScoped(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	segmentRoot := filepath.Join(dataRoot, "cache", "segments", env.indexSetID)

	for _, attempt := range []string{"run_attempt_a", "run_attempt_b"} {
		attemptDir := filepath.Join(segmentRoot, "verification", attempt)
		require.NoError(t, os.MkdirAll(attemptDir, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(attemptDir, "index.db"), []byte("projection-"+attempt), 0o600))
	}

	entry, err := inspectDurableForDoctor(indexreader.Meta{
		Format:      indexreader.FormatDurableV2,
		IndexSetID:  env.indexSetID,
		IdentityDir: env.identityDir,
		SourcePath:  filepath.Join(segmentRoot, "latest.json"),
	}, indexDoctorOptions{})
	require.NoError(t, err)
	require.Equal(t, 2, entry.VerificationProjectionCount)
	require.Equal(t, []string{"run_attempt_a", "run_attempt_b"}, entry.VerificationProjections)
	var classified bool
	for _, note := range entry.Notes {
		if strings.Contains(note, "receipt-backed set-scoped GC") {
			classified = true
		}
	}
	require.True(t, classified, "inventory note must state the deletion-authority posture")

	// Dry-run GC: no sub-set deletion path may touch the residue.
	dry := &cobra.Command{Use: "gc"}
	dry.Flags().String("max-age", "24h", "")
	dry.Flags().Int("keep-last", 0, "")
	dry.Flags().Bool("dry-run", true, "")
	dry.Flags().Bool("json", false, "")
	require.NoError(t, runIndexGC(dry, nil))
	for _, attempt := range []string{"run_attempt_a", "run_attempt_b"} {
		data, readErr := os.ReadFile(filepath.Join(segmentRoot, "verification", attempt, "index.db"))
		require.NoError(t, readErr)
		require.Equal(t, []byte("projection-"+attempt), data, "dry-run must leave verification residue byte-intact")
	}

	// Receipt-backed whole-set deletion is the one legitimate removal path and
	// must succeed with the residue present.
	cmd := &cobra.Command{Use: "gc"}
	cmd.Flags().String("max-age", "24h", "")
	cmd.Flags().Int("keep-last", 0, "")
	cmd.Flags().Bool("dry-run", false, "")
	cmd.Flags().Bool("json", false, "")
	require.NoError(t, runIndexGC(cmd, nil))
	require.NoDirExists(t, segmentRoot, "whole-set removal includes the verification residue under set-scoped authority")
	require.NoDirExists(t, env.identityDir)
}

func TestRunIndexGCExecutesLeasedWholeSetDeletion(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	segmentRoot := filepath.Join(dataRoot, "cache", "segments", env.indexSetID)

	cmd := &cobra.Command{Use: "gc"}
	cmd.Flags().String("max-age", "24h", "")
	cmd.Flags().Int("keep-last", 0, "")
	cmd.Flags().Bool("dry-run", false, "")
	cmd.Flags().Bool("json", false, "")
	require.NoError(t, runIndexGC(cmd, nil))
	require.NoDirExists(t, env.identityDir)
	require.NoDirExists(t, segmentRoot)
	require.NoDirExists(t, filepath.Join(dataRoot, "journals", "crawl", env.indexSetID))
}

func TestRunIndexGCExecutesSQLiteOnlyWholeSetDeletion(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	db := openIndexGCTestDB(t, filepath.Join(env.identityDir, "index.db"))
	require.NoError(t, indexstore.Migrate(context.Background(), db))
	set, _, err := indexstore.FindOrCreateIndexSet(context.Background(), db, env.params)
	require.NoError(t, err)
	require.Equal(t, env.indexSetID, set.IndexSetID)
	require.NoError(t, db.Close())
	require.NoError(t, os.RemoveAll(filepath.Join(dataRoot, "cache", "segments", env.indexSetID)))

	plan, err := buildIndexGCPlan(context.Background(), time.Nanosecond, "1ns", 0, time.Now().UTC().Add(time.Second))
	require.NoError(t, err)
	require.Len(t, plan.Candidates, 1)
	require.Equal(t, []string{"sqlite-v1"}, plan.Candidates[0].Formats)
	cmd := newIndexGCExecutionTestCommand()
	require.NoError(t, cmd.Flags().Set("max-age", "1ns"))
	require.NoError(t, runIndexGC(cmd, nil))
	require.NoDirExists(t, env.identityDir)
}

func TestRunIndexGCDryRunPlanEqualsExecutionReceipt(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	journalSetRoot := filepath.Join(dataRoot, "journals", "crawl", env.indexSetID)
	require.NoError(t, os.MkdirAll(filepath.Join(journalSetRoot, "run_cli_1"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(journalSetRoot, "run_cli_1", "crawl.jsonl"), []byte("sealed\n"), 0o600))

	dryPlan, err := buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, time.Now().UTC())
	require.NoError(t, err)
	require.Len(t, dryPlan.Candidates, 1)
	require.Len(t, dryPlan.Candidates[0].Targets, 3)

	cmd := newIndexGCExecutionTestCommand()
	require.NoError(t, runIndexGC(cmd, nil))
	store, err := openDefaultOperationCheckpointStore(context.Background())
	require.NoError(t, err)
	checkpoints, err := listIndexGCDeleteCheckpoints(store)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	require.Equal(t, opcheckpoint.StatusSuccess, checkpoints[0].Status)
	payload, err := parseAndValidateIndexGCDeletePayload(store, &checkpoints[0])
	require.NoError(t, err)
	require.Equal(t, dryPlan.PlanSHA256, payload.PlanSHA256)
	require.Len(t, payload.Candidates, 1)
	require.Len(t, payload.Candidates[0].Targets, 3)
	for _, target := range payload.Candidates[0].Targets {
		require.Equal(t, "deleted", target.State)
		require.NoDirExists(t, target.Path)
		require.NoDirExists(t, target.QuarantinePath)
	}
	checkpointPath, err := store.CheckpointPath(operationIndexGCDelete, checkpoints[0].RunID)
	require.NoError(t, err)
	info, err := os.Stat(checkpointPath)
	require.NoError(t, err)
	// Unix mode bits are not preserved on Windows; still require a regular file
	// under an existing parent directory.
	require.True(t, info.Mode().IsRegular())
	if runtime.GOOS != "windows" {
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
	info, err = os.Stat(filepath.Dir(checkpointPath))
	require.NoError(t, err)
	require.True(t, info.IsDir())
	if runtime.GOOS != "windows" {
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	}
}

func TestRunIndexGCRestartConvergesAfterEveryDeletionBoundary(t *testing.T) {
	for boundaryNumber := 1; boundaryNumber <= 8; boundaryNumber++ {
		t.Run(fmt.Sprintf("boundary_%02d", boundaryNumber), func(t *testing.T) {
			resetAppDataRootTestState(t)
			dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
			t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
			env := seedDurableOnlyAppData(t, dataRoot, nil)
			journalSetRoot := filepath.Join(dataRoot, "journals", "crawl", env.indexSetID)
			require.NoError(t, os.MkdirAll(filepath.Join(journalSetRoot, "run_cli_1"), 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(journalSetRoot, "run_cli_1", "crawl.jsonl"), []byte("sealed\n"), 0o600))

			seen := 0
			indexGCTestExecutionHooks = indexGCExecutionHooks{afterBoundary: func(_ string) error {
				seen++
				if seen == boundaryNumber {
					return errors.New("crash")
				}
				return nil
			}}
			t.Cleanup(func() { indexGCTestExecutionHooks = indexGCExecutionHooks{} })
			require.Error(t, runIndexGC(newIndexGCExecutionTestCommand(), nil))

			indexGCTestExecutionHooks = indexGCExecutionHooks{}
			require.NoError(t, runIndexGC(newIndexGCExecutionTestCommand(), nil))
			require.NoDirExists(t, env.identityDir)
			require.NoDirExists(t, filepath.Join(dataRoot, "cache", "segments", env.indexSetID))
			require.NoDirExists(t, journalSetRoot)
			for _, root := range []string{
				filepath.Join(dataRoot, "indexes"),
				filepath.Join(dataRoot, "cache", "segments"),
				filepath.Join(dataRoot, "journals", "crawl"),
			} {
				entries, err := os.ReadDir(root)
				require.NoError(t, err)
				for _, entry := range entries {
					require.False(t, strings.HasPrefix(entry.Name(), ".gonimbus-gc-"), entry.Name())
				}
			}
		})
	}
}

func TestRunIndexGCRecoversAfterPartialRecursiveQuarantineRemoval(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	require.NoError(t, os.WriteFile(filepath.Join(env.identityDir, "alpha.txt"), []byte("alpha\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(env.identityDir, "beta.txt"), []byte("beta\n"), 0o600))

	interrupted := false
	indexGCTestExecutionHooks = indexGCExecutionHooks{afterBoundary: func(boundary string) error {
		if !interrupted && strings.HasPrefix(boundary, "delete-entry:"+env.indexSetID+":identity:") {
			interrupted = true
			return errors.New("crash during recursive removal")
		}
		return nil
	}}
	t.Cleanup(func() { indexGCTestExecutionHooks = indexGCExecutionHooks{} })
	require.Error(t, runIndexGC(newIndexGCExecutionTestCommand(), nil))
	require.True(t, interrupted)

	store, err := openDefaultOperationCheckpointStore(context.Background())
	require.NoError(t, err)
	checkpoints, err := listIndexGCDeleteCheckpoints(store)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	payload, err := parseAndValidateIndexGCDeletePayload(store, &checkpoints[0])
	require.NoError(t, err)
	var identityTarget indexGCDeleteTarget
	for _, target := range payload.Candidates[0].Targets {
		if target.Kind == "identity" {
			identityTarget = target
		}
	}
	require.Equal(t, "deleting", identityTarget.State)
	require.NotEmpty(t, identityTarget.DeleteEntries)
	remaining, err := os.ReadDir(identityTarget.QuarantinePath)
	require.NoError(t, err)
	require.Less(t, len(remaining), 3, "at least one authorized child must already be removed")

	indexGCTestExecutionHooks = indexGCExecutionHooks{}
	require.NoError(t, runIndexGC(newIndexGCExecutionTestCommand(), nil))
	checkpoints, err = listIndexGCDeleteCheckpoints(store)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1, "recovery must complete the original transaction")
	require.Equal(t, opcheckpoint.StatusSuccess, checkpoints[0].Status)
	receipt, err := parseAndValidateIndexGCDeletePayload(store, &checkpoints[0])
	require.NoError(t, err)
	require.NotNil(t, receipt.CompletedAt)
	require.Positive(t, receipt.RemovedBytes)
	require.NoDirExists(t, env.identityDir)
	require.NoDirExists(t, filepath.Join(dataRoot, "cache", "segments", env.indexSetID))
}

func TestRunIndexGCAuthorizationOverflowFailsBeforeFirstRenameAndCanRetry(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	segmentRoot := filepath.Join(dataRoot, "cache", "segments", env.indexSetID)
	journalRoot := filepath.Join(dataRoot, "journals", "crawl", env.indexSetID)
	require.NoError(t, os.MkdirAll(filepath.Join(journalRoot, "run_cli_1"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(journalRoot, "run_cli_1", "crawl.jsonl"), []byte("sealed\n"), 0o600))
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("%03d-%s.dat", i, strings.Repeat("x", 80))
		require.NoError(t, os.WriteFile(filepath.Join(env.identityDir, name), []byte(fmt.Sprintf("entry-%03d\n", i)), 0o600))
	}

	identityBefore := snapshotIndexGCTreeState(t, env.identityDir)
	segmentBefore := snapshotIndexGCTreeState(t, segmentRoot)
	journalBefore := snapshotIndexGCTreeState(t, journalRoot)
	oldLimit := indexGCMaxIntentBytes
	indexGCMaxIntentBytes = 8 << 10
	t.Cleanup(func() { indexGCMaxIntentBytes = oldLimit })
	boundaryCalled := false
	indexGCTestExecutionHooks = indexGCExecutionHooks{afterBoundary: func(string) error {
		boundaryCalled = true
		return nil
	}}
	t.Cleanup(func() { indexGCTestExecutionHooks = indexGCExecutionHooks{} })

	err := runIndexGC(newIndexGCExecutionTestCommand(), nil)
	require.ErrorContains(t, err, "exceeds bounded record capacity")
	require.False(t, boundaryCalled, "authorization overflow must occur before the persisted-intent boundary")
	require.Equal(t, identityBefore, snapshotIndexGCTreeState(t, env.identityDir))
	require.Equal(t, segmentBefore, snapshotIndexGCTreeState(t, segmentRoot))
	require.Equal(t, journalBefore, snapshotIndexGCTreeState(t, journalRoot))
	for _, root := range []string{filepath.Dir(env.identityDir), filepath.Dir(segmentRoot), filepath.Dir(journalRoot)} {
		entries, readErr := os.ReadDir(root)
		require.NoError(t, readErr)
		for _, entry := range entries {
			require.False(t, strings.HasPrefix(entry.Name(), ".gonimbus-gc-"), entry.Name())
		}
	}
	store, err := openDefaultOperationCheckpointStore(context.Background())
	require.NoError(t, err)
	checkpoints, err := listIndexGCDeleteCheckpoints(store)
	require.NoError(t, err)
	require.Empty(t, checkpoints, "capacity failure must not leave an unrecoverable deletion intent")

	indexGCMaxIntentBytes = oldLimit
	indexGCTestExecutionHooks = indexGCExecutionHooks{}
	require.NoError(t, runIndexGC(newIndexGCExecutionTestCommand(), nil))
	checkpoints, err = listIndexGCDeleteCheckpoints(store)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	require.Equal(t, opcheckpoint.StatusSuccess, checkpoints[0].Status)
	require.NoDirExists(t, env.identityDir)
	require.NoDirExists(t, segmentRoot)
	require.NoDirExists(t, journalRoot)
}

func TestRunIndexGCRevalidatesTargetAndActiveStateImmediatelyBeforeMutation(t *testing.T) {
	t.Run("target bytes changed", func(t *testing.T) {
		resetAppDataRootTestState(t)
		dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
		t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
		env := seedDurableOnlyAppData(t, dataRoot, nil)
		indexGCTestExecutionHooks = indexGCExecutionHooks{afterBoundary: func(boundary string) error {
			if boundary == "intent" {
				return os.WriteFile(filepath.Join(env.identityDir, "late.txt"), []byte("late\n"), 0o600)
			}
			return nil
		}}
		t.Cleanup(func() { indexGCTestExecutionHooks = indexGCExecutionHooks{} })
		err := runIndexGC(newIndexGCExecutionTestCommand(), nil)
		require.ErrorContains(t, err, "tree authority changed")
		require.DirExists(t, env.identityDir)
		require.DirExists(t, filepath.Join(dataRoot, "cache", "segments", env.indexSetID))
	})

	t.Run("active job appeared", func(t *testing.T) {
		resetAppDataRootTestState(t)
		dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
		t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
		env := seedDurableOnlyAppData(t, dataRoot, nil)
		indexGCTestExecutionHooks = indexGCExecutionHooks{afterBoundary: func(boundary string) error {
			if boundary != "intent" {
				return nil
			}
			store := jobregistry.NewStore(filepath.Join(dataRoot, "jobs", "index-build"))
			return store.Write(&jobregistry.JobRecord{
				JobID:      "99999999-9999-4999-8999-999999999999",
				Type:       jobregistry.JobTypeIndexBuild,
				State:      jobregistry.JobStateRunning,
				IndexSetID: env.indexSetID,
				CreatedAt:  time.Now().UTC(),
			})
		}}
		t.Cleanup(func() { indexGCTestExecutionHooks = indexGCExecutionHooks{} })
		err := runIndexGC(newIndexGCExecutionTestCommand(), nil)
		require.ErrorContains(t, err, "active state appeared")
		require.DirExists(t, env.identityDir)
		require.DirExists(t, filepath.Join(dataRoot, "cache", "segments", env.indexSetID))
	})
}

func TestBuildIndexGCPlanRetainsDurableSetWithoutMaintenanceLockArtifact(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	require.NoError(t, os.Remove(filepath.Join(dataRoot, "cache", "segments", env.indexSetID, ".durable-write.lock")))

	plan, err := buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, time.Now().UTC())
	require.NoError(t, err)
	require.Empty(t, plan.Candidates)
	require.Condition(t, func() bool {
		for _, warning := range plan.Warnings {
			if warning.IndexSetID == env.indexSetID && strings.Contains(warning.Reason, "write lease") {
				return true
			}
		}
		return false
	})
}

func TestRunIndexGCRefusesHeldSetMaintenanceLease(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	guard, err := acquireIndexSetMaintenance(context.Background(), env.indexSetID, "test-active-writer")
	require.NoError(t, err)
	defer func() { _ = guard.Release() }()

	err = runIndexGC(newIndexGCExecutionTestCommand(), nil)
	require.ErrorContains(t, err, "lease")
	require.DirExists(t, env.identityDir)
	require.DirExists(t, filepath.Join(dataRoot, "cache", "segments", env.indexSetID))
}

func TestRunIndexGCRecoveryRejectsTamperedQuarantinePath(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	indexGCTestExecutionHooks = indexGCExecutionHooks{afterBoundary: func(boundary string) error {
		if boundary == "intent" {
			return errors.New("stop after intent")
		}
		return nil
	}}
	t.Cleanup(func() { indexGCTestExecutionHooks = indexGCExecutionHooks{} })
	require.Error(t, runIndexGC(newIndexGCExecutionTestCommand(), nil))
	indexGCTestExecutionHooks = indexGCExecutionHooks{}

	store, err := openDefaultOperationCheckpointStore(context.Background())
	require.NoError(t, err)
	checkpoints, err := listIndexGCDeleteCheckpoints(store)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	var payload indexGCDeletePayload
	require.NoError(t, json.Unmarshal(checkpoints[0].Payload, &payload))
	outside := t.TempDir()
	sentinel := filepath.Join(outside, "sentinel.txt")
	require.NoError(t, os.WriteFile(sentinel, []byte("keep\n"), 0o600))
	payload.Candidates[0].Targets[0].QuarantinePath = filepath.Join(outside, "forged")
	rawPayload, err := json.Marshal(payload)
	require.NoError(t, err)
	checkpoints[0].Payload = rawPayload
	rawEnvelope, err := json.MarshalIndent(checkpoints[0], "", "  ")
	require.NoError(t, err)
	checkpointPath, err := store.CheckpointPath(operationIndexGCDelete, checkpoints[0].RunID)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(checkpointPath, append(rawEnvelope, '\n'), 0o600))

	err = runIndexGC(newIndexGCExecutionTestCommand(), nil)
	require.ErrorContains(t, err, "quarantine path")
	require.FileExists(t, sentinel)
	require.DirExists(t, env.identityDir)
	require.DirExists(t, filepath.Join(dataRoot, "cache", "segments", env.indexSetID))
}

func newIndexGCExecutionTestCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "gc"}
	cmd.Flags().String("max-age", "24h", "")
	cmd.Flags().Int("keep-last", 0, "")
	cmd.Flags().Bool("dry-run", false, "")
	cmd.Flags().Bool("json", false, "")
	return cmd
}

func TestRunIndexGCRejectsNonPositiveRetentionValues(t *testing.T) {
	for _, tc := range []struct {
		name     string
		maxAge   string
		keepLast int
		want     string
	}{
		{name: "negative keep-last", keepLast: -1, want: "--keep-last"},
		{name: "negative max-age", maxAge: "-1d", want: "greater than zero"},
		{name: "zero max-age", maxAge: "0h", want: "greater than zero"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{Use: "gc"}
			cmd.Flags().String("max-age", tc.maxAge, "")
			cmd.Flags().Int("keep-last", tc.keepLast, "")
			cmd.Flags().Bool("dry-run", true, "")
			cmd.Flags().Bool("json", false, "")
			require.ErrorContains(t, runIndexGC(cmd, nil), tc.want)
		})
	}
}

func TestSelectIndexGCPlanCandidatesRejectsInvalidRetention(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	_, err := selectIndexGCPlanCandidates(nil, 0, false, -1, now)
	require.ErrorContains(t, err, "keep-last")
	_, err = selectIndexGCPlanCandidates(nil, 0, true, 0, now)
	require.ErrorContains(t, err, "max-age")
	_, err = selectIndexGCPlanCandidates(nil, -time.Hour, true, 0, now)
	require.ErrorContains(t, err, "max-age")
}

func TestLookupHeldDurableWriteLeaseMatchesAbsAndIndexSet(t *testing.T) {
	raw := t.TempDir()
	dir, err := filepath.EvalSymlinks(raw)
	require.NoError(t, err)
	dir, err = filepath.Abs(dir)
	require.NoError(t, err)
	id := "idx_" + strings.Repeat("ab", 32)

	seed, err := indexsubstrate.AcquireWriteLease(dir, id, "seed", 0)
	require.NoError(t, err)
	require.NoError(t, seed.Release())

	lease, err := indexsubstrate.AcquireWriteLeaseForMaintenance(dir, id, "lookup-test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lease.Release() })

	held := map[string]*indexsubstrate.WriteLease{lease.SegmentSetRoot(): lease}
	// Exact canonical key.
	require.Same(t, lease, lookupHeldDurableWriteLease(held, lease.SegmentSetRoot(), id))
	// Raw temp path that Abs-resolves equivalently after symlink expansion.
	require.Same(t, lease, lookupHeldDurableWriteLease(held, dir, id))
	// Unique index-set fallback when the path form is unrelated.
	require.Same(t, lease, lookupHeldDurableWriteLease(held, filepath.Join(t.TempDir(), "other"), id))
	// Wrong index set must not match when the path misses.
	require.Nil(t, lookupHeldDurableWriteLease(held, filepath.Join(t.TempDir(), "other"), "idx_"+strings.Repeat("cd", 32)))
}

func TestBuildIndexGCPlanUnderHeldMaintenanceLeaseMatchesPreAcquirePlan(t *testing.T) {
	// Execution revalidates the immutable plan after AcquireWriteLeaseForMaintenance.
	// Held exclusive lock must not change PlanSHA256 (Windows historically diverged
	// when the lock file was content-hashed while LockFileEx-held).
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)

	before, err := buildIndexGCPlan(context.Background(), 24*time.Hour, "24h", 0, now)
	require.NoError(t, err)
	require.Len(t, before.Candidates, 1)
	require.NotEmpty(t, before.PlanSHA256)

	var segmentRoot string
	for _, target := range before.Candidates[0].Targets {
		if target.Kind == "segment-set" {
			segmentRoot = target.Path
			break
		}
	}
	require.NotEmpty(t, segmentRoot, "durable plan must include segment-set target")
	lease, err := indexsubstrate.AcquireWriteLeaseForMaintenance(segmentRoot, env.indexSetID, "gc-revalidate")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lease.Release() })

	held := map[string]*indexsubstrate.WriteLease{lease.SegmentSetRoot(): lease}
	after, err := buildIndexGCPlanWithLeases(context.Background(), 24*time.Hour, "24h", 0, now, held, "gc_test")
	require.NoError(t, err)
	require.Equal(t, before.PlanSHA256, after.PlanSHA256, "held maintenance lease must not rewrite plan digest")
	require.Len(t, after.Candidates, 1)
	require.Equal(t, before.Candidates[0].Targets, after.Candidates[0].Targets)
}

func TestHashIndexGCTreePackageWriteLeaseExceptionIsSegmentRootOnly(t *testing.T) {
	// Nested same-basename files and non-segment targets must remain content-bound.
	// Only segment-set root .durable-write.lock uses lease-meta.
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "published"), 0o755))
	nested := filepath.Join(dir, "published", indexsubstrate.WriteLeaseFileName)
	require.NoError(t, os.WriteFile(nested, []byte("AAAA"), 0o600))
	rootLock := filepath.Join(dir, indexsubstrate.WriteLeaseFileName)
	require.NoError(t, os.WriteFile(rootLock, []byte("LOCK"), 0o600))
	other := filepath.Join(dir, "payload.bin")
	require.NoError(t, os.WriteFile(other, []byte("data"), 0o600))

	// Nested same-basename: same-size rewrite must change digest even for segment-set.
	size1, dig1, err := hashIndexGCTree(dir, "segment-set")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(nested, []byte("BBBB"), 0o600))
	size2, dig2, err := hashIndexGCTree(dir, "segment-set")
	require.NoError(t, err)
	require.Equal(t, size1, size2)
	require.NotEqual(t, dig1, dig2, "nested .durable-write.lock must be content-bound")

	// Root package lock on segment-set: same-size rewrite of diagnostic bytes must NOT change digest.
	require.NoError(t, os.WriteFile(rootLock, []byte("L0CK"), 0o600)) // same length as LOCK
	size3, dig3, err := hashIndexGCTree(dir, "segment-set")
	require.NoError(t, err)
	require.Equal(t, size2, size3)
	require.Equal(t, dig2, dig3, "segment-set root package lock is metadata-only")

	// Journals target kind: root same-basename must be content-bound.
	journalRoot := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(journalRoot, indexsubstrate.WriteLeaseFileName), []byte("AAAA"), 0o600))
	_, j1, err := hashIndexGCTree(journalRoot, "journals")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(journalRoot, indexsubstrate.WriteLeaseFileName), []byte("BBBB"), 0o600))
	_, j2, err := hashIndexGCTree(journalRoot, "journals")
	require.NoError(t, err)
	require.NotEqual(t, j1, j2, "journals root same-basename must be content-bound")

	// Identity target kind likewise content-bound.
	idRoot := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(idRoot, indexsubstrate.WriteLeaseFileName), []byte("AAAA"), 0o600))
	_, i1, err := hashIndexGCTree(idRoot, "identity")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(idRoot, indexsubstrate.WriteLeaseFileName), []byte("BBBB"), 0o600))
	_, i2, err := hashIndexGCTree(idRoot, "identity")
	require.NoError(t, err)
	require.NotEqual(t, i1, i2, "identity root same-basename must be content-bound")

	require.True(t, isIndexGCPackageWriteLeaseRel("segment-set", indexsubstrate.WriteLeaseFileName))
	require.True(t, isIndexGCPackageWriteLeaseRel("segment-set", "./"+indexsubstrate.WriteLeaseFileName))
	require.False(t, isIndexGCPackageWriteLeaseRel("segment-set", "published/"+indexsubstrate.WriteLeaseFileName))
	require.False(t, isIndexGCPackageWriteLeaseRel("journals", indexsubstrate.WriteLeaseFileName))
	require.False(t, isIndexGCPackageWriteLeaseRel("identity", indexsubstrate.WriteLeaseFileName))
}
