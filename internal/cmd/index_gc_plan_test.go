package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
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
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: filepath.Join(env.identityDir, "index.db")})
	require.NoError(t, err)
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
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(context.Background(), db))
	_, _, err = indexstore.FindOrCreateIndexSet(context.Background(), db, env.params)
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
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `CREATE TABLE schema_meta (id INTEGER PRIMARY KEY, schema_version INTEGER NOT NULL)`)
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
				db, err := indexstore.Open(context.Background(), indexstore.Config{Path: dbPath})
				require.NoError(t, err)
				require.NoError(t, indexstore.Migrate(context.Background(), db))
				_, err = db.ExecContext(context.Background(), `PRAGMA wal_autocheckpoint=0`)
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
				db, err := indexstore.Open(context.Background(), indexstore.Config{Path: dbPath})
				require.NoError(t, err)
				require.NoError(t, indexstore.Migrate(context.Background(), db))
				_, _, err = indexstore.FindOrCreateIndexSet(context.Background(), db, env.params)
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
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: outsideDB})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(context.Background(), db))
	_, _, err = indexstore.FindOrCreateIndexSet(context.Background(), db, env.params)
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

func TestRunIndexGCRequiresDryRunUntilExecutorLands(t *testing.T) {
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
	err := runIndexGC(cmd, nil)
	require.Error(t, err)
	require.True(t, containsAll(err.Error(), "not enabled", "--dry-run"))
	require.DirExists(t, env.identityDir)
	require.DirExists(t, segmentRoot)
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

func containsAll(value string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(value, part) {
			return false
		}
	}
	return true
}
