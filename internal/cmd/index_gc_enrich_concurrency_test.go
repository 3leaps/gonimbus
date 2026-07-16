package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/indexbuild"
	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/indexenrich"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

type blockingEnrichHeadProvider struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingEnrichHeadProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return nil, nil
}

func (p *blockingEnrichHeadProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	p.once.Do(func() { close(p.entered) })
	select {
	case <-p.release:
		return &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key}, ContentType: "application/octet-stream"}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *blockingEnrichHeadProvider) Close() error { return nil }

func TestIndexEnrichSQLiteHeldSetGuardRejectsBeforeDBMutation(t *testing.T) {
	env := seedSQLiteEnrichGCAppData(t)
	before := snapshotIndexGCTreeState(t, env.identityDir)

	guard, err := acquireIndexSetMaintenance(context.Background(), env.indexSetID, "gc-test-held")
	require.NoError(t, err)
	defer func() { require.NoError(t, guard.Release()) }()

	var providerCalls atomic.Int64
	oldProvider := newEnrichHeadProvider
	newEnrichHeadProvider = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		providerCalls.Add(1)
		return &fakeEnrichProvider{}, nil
	}
	t.Cleanup(func() { newEnrichHeadProvider = oldProvider })

	cmd, _ := configureIndexEnrichCommandForGCTest(t)
	err = runIndexEnrichWithHead(cmd, []string{env.indexSetID})
	require.ErrorContains(t, err, "maintenance lease")
	require.Zero(t, providerCalls.Load())
	require.Equal(t, before, snapshotIndexGCTreeState(t, env.identityDir))
}

func TestIndexEnrichSnapshotAuthorityLossStopsBeforeMutation(t *testing.T) {
	env := seedSQLiteEnrichGCAppData(t)
	// This test pins the SQLite enrich path; keep the durable latest hidden so
	// resolution selects the set-root SQLite database.
	_ = hideDurableLatestForSQLiteSelection(t, env.indexSetID)
	before := snapshotIndexGCTreeState(t, env.identityDir)
	segmentRoot, err := indexSubstrateSegmentCacheDir(env.indexSetID)
	require.NoError(t, err)
	var injectionErr error
	indexEnrichWithHeadBeforeResolvedReaderClose = func(indexreader.Meta) {
		authorityRoot, err := indexcoord.AuthorityRoot(segmentRoot)
		if err != nil {
			injectionErr = err
			return
		}
		lockPath := filepath.Join(authorityRoot, env.indexSetID+".lock")
		if err := os.Remove(lockPath); err != nil {
			injectionErr = err
			return
		}
		injectionErr = os.WriteFile(lockPath, []byte("replacement\n"), 0o600)
	}
	t.Cleanup(func() { indexEnrichWithHeadBeforeResolvedReaderClose = nil })

	var providerCalls atomic.Int64
	oldProvider := newEnrichHeadProvider
	newEnrichHeadProvider = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		providerCalls.Add(1)
		return &fakeEnrichProvider{}, nil
	}
	t.Cleanup(func() { newEnrichHeadProvider = oldProvider })

	cmd, _ := configureIndexEnrichCommandForGCTest(t)
	err = runIndexEnrichWithHead(cmd, []string{env.indexSetID})
	require.NoError(t, injectionErr)
	require.ErrorContains(t, err, "validate resolved index snapshot before enrich mutation")
	require.ErrorIs(t, err, indexcoord.ErrLost)
	require.Zero(t, providerCalls.Load())
	require.Equal(t, before, snapshotIndexGCTreeState(t, env.identityDir))
}

func TestCanonicalSQLiteIdentityMismatchFailsClosedUnderRealAuthority(t *testing.T) {
	env := seedSQLiteEnrichGCAppData(t)
	dbPath := filepath.Join(env.identityDir, "index.db")
	otherParams := env.params
	otherParams.BaseURI = "s3://test-bucket/mismatched-identity/"
	otherIdentity, err := indexstore.ComputeIndexSetID(otherParams)
	require.NoError(t, err)
	require.NoError(t, writeIndexIdentityFile(env.identityDir, otherIdentity))

	segmentRoot, err := indexSubstrateSegmentCacheDir(env.indexSetID)
	require.NoError(t, err)
	realAuthority, err := indexcoord.Acquire(context.Background(), segmentRoot, env.indexSetID, "gc-real-set")
	require.NoError(t, err)
	defer func() { require.NoError(t, realAuthority.Release()) }()
	before := snapshotIndexGCTreeState(t, env.identityDir)
	beforeDB, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	beforeInfo, err := os.Stat(dbPath)
	require.NoError(t, err)

	entry, err := inspectIndexDBForDoctor(context.Background(), dbPath, indexDoctorOptions{})
	require.ErrorContains(t, err, "identity/scope mismatch")
	require.Nil(t, entry)
	require.NoError(t, realAuthority.AssertHeldFor(env.indexSetID, segmentRoot))
	require.Equal(t, before, snapshotIndexGCTreeState(t, env.identityDir))
	afterDB, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	afterInfo, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.Equal(t, beforeDB, afterDB)
	require.Equal(t, beforeInfo.Mode(), afterInfo.Mode())
	require.Equal(t, beforeInfo.ModTime(), afterInfo.ModTime())
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")
}

func TestCanonicalSQLiteMissingIdentityRefusesDuringActiveWriter(t *testing.T) {
	env := seedSQLiteEnrichGCAppData(t)
	dbPath := filepath.Join(env.identityDir, "index.db")
	segmentRoot, err := indexSubstrateSegmentCacheDir(env.indexSetID)
	require.NoError(t, err)
	writerAuthority, err := indexcoord.Acquire(context.Background(), segmentRoot, env.indexSetID, "active-writer")
	require.NoError(t, err)
	defer func() { require.NoError(t, writerAuthority.Release()) }()
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	_, err = db.ExecContext(context.Background(), `UPDATE index_sets SET created_at = ? WHERE index_set_id = ?`, time.Now().UTC().Format(time.RFC3339Nano), env.indexSetID)
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(env.identityDir, "identity.json")))
	before := snapshotIndexGCTreeState(t, env.identityDir)

	reader, err := openIndexReader(context.Background(), "", env.indexSetID, "")
	require.ErrorContains(t, err, "canonical SQLite identity requires a valid full index_set_id")
	require.Nil(t, reader)
	require.Equal(t, before, snapshotIndexGCTreeState(t, env.identityDir))
	require.NoError(t, writerAuthority.AssertHeldFor(env.indexSetID, segmentRoot))
}

func TestIndexEnrichSQLiteResumeHeldSetGuardRejectsBeforeDBMutation(t *testing.T) {
	env := seedSQLiteEnrichGCAppData(t)
	before := snapshotIndexGCTreeState(t, env.identityDir)
	config := enrichHeadCheckpointConfig{
		IndexSetID: env.indexSetID,
		Query:      enrichHeadQueryOptions{Parallel: 1},
	}
	fingerprint, err := checkpointFingerprint(config)
	require.NoError(t, err)
	payload, err := json.Marshal(enrichHeadCheckpointPayload{Config: config})
	require.NoError(t, err)
	store, err := openDefaultOperationCheckpointStore(context.Background())
	require.NoError(t, err)
	const runID = "run_resume_guard_test"
	require.NoError(t, store.WriteCheckpoint(context.Background(), opcheckpoint.Envelope{
		Operation:         operationIndexEnrichWithHead,
		RunID:             runID,
		ConfigFingerprint: fingerprint,
		Status:            opcheckpoint.StatusFailedResumable,
		Payload:           payload,
	}))

	guard, err := acquireIndexSetMaintenance(context.Background(), env.indexSetID, "gc-test-held-resume")
	require.NoError(t, err)
	defer func() { require.NoError(t, guard.Release()) }()

	cmd, _ := configureIndexEnrichCommandForGCTest(t)
	err = runIndexEnrichWithHeadResume(context.Background(), cmd, nil, runID)
	require.ErrorContains(t, err, "maintenance lease")
	require.Equal(t, before, snapshotIndexGCTreeState(t, env.identityDir))
}

func TestIndexInitCanonicalMigrationHeldSetGuardRejectsBeforeDBMutation(t *testing.T) {
	env := seedSQLiteEnrichGCAppData(t)
	dbPath := filepath.Join(env.identityDir, "index.db")
	before := snapshotIndexGCTreeState(t, env.identityDir)
	guard, err := acquireIndexSetMaintenance(context.Background(), env.indexSetID, "gc-test-init-held")
	require.NoError(t, err)
	defer func() { require.NoError(t, guard.Release()) }()

	oldDBPath := indexDBPath
	indexDBPath = dbPath
	t.Cleanup(func() { indexDBPath = oldDBPath })
	cmd := &cobra.Command{Use: "init"}
	cmd.SetContext(context.Background())
	err = runIndexInit(cmd, nil)
	require.ErrorContains(t, err, "authority")
	require.Equal(t, before, snapshotIndexGCTreeState(t, env.identityDir))
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")
}

func TestIndexInitAfterIdentityQuarantineCannotRecreateAndGCRecoveryConverges(t *testing.T) {
	env := seedSQLiteEnrichGCAppData(t)
	dbPath := filepath.Join(env.identityDir, "index.db")
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	indexGCTestExecutionHooks = indexGCExecutionHooks{afterBoundary: func(boundary string) error {
		if boundary == "quarantine:"+env.indexSetID+":identity" {
			once.Do(func() { close(entered) })
			<-release
			return errors.New("crash after identity quarantine")
		}
		return nil
	}}
	t.Cleanup(func() { indexGCTestExecutionHooks = indexGCExecutionHooks{} })

	gcDone := make(chan error, 1)
	gcCmd := newIndexGCExecutionTestCommand()
	require.NoError(t, gcCmd.Flags().Set("max-age", "1ns"))
	go func() { gcDone <- runIndexGC(gcCmd, nil) }()
	select {
	case <-entered:
	case err := <-gcDone:
		t.Fatalf("GC stopped before identity quarantine: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("GC did not reach identity quarantine")
	}
	require.NoDirExists(t, env.identityDir)

	oldDBPath := indexDBPath
	indexDBPath = dbPath
	t.Cleanup(func() { indexDBPath = oldDBPath })
	cmd := &cobra.Command{Use: "init"}
	cmd.SetContext(context.Background())
	err := runIndexInit(cmd, nil)
	require.ErrorContains(t, err, "authority")
	require.NoDirExists(t, env.identityDir)
	require.NoFileExists(t, dbPath)
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")

	close(release)
	require.Error(t, <-gcDone)
	indexGCTestExecutionHooks = indexGCExecutionHooks{}
	require.NoError(t, runIndexGC(newIndexGCExecutionTestCommand(), nil))
	store, err := openDefaultOperationCheckpointStore(context.Background())
	require.NoError(t, err)
	checkpoints, err := listIndexGCDeleteCheckpoints(store)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	require.Equal(t, opcheckpoint.StatusSuccess, checkpoints[0].Status)
}

func TestSQLiteListAndDoctorUseNonMutatingAuthorityHeldSnapshots(t *testing.T) {
	env := seedSQLiteEnrichGCAppData(t)
	dbPath := filepath.Join(env.identityDir, "index.db")
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `PRAGMA wal_checkpoint(TRUNCATE)`)
	require.NoError(t, err)
	var mode string
	require.NoError(t, db.QueryRowContext(context.Background(), `PRAGMA journal_mode=DELETE`).Scan(&mode))
	require.Equal(t, "delete", strings.ToLower(mode))
	require.NoError(t, db.Close())
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")
	before := snapshotIndexGCTreeState(t, env.identityDir)

	meta := indexreader.Meta{
		Format: indexreader.FormatSQLiteV1, IndexSetID: env.indexSetID,
		BaseURI: env.params.BaseURI, Provider: env.params.Provider,
		IdentityDir: env.identityDir, SourcePath: dbPath,
	}
	_, err = loadSQLiteListDisplayEntry(context.Background(), meta, env.identityDir, filepath.Base(env.identityDir), filepath.Join(env.identityDir, "identity.json"))
	require.NoError(t, err)
	_, err = inspectIndexDBForDoctor(context.Background(), dbPath, indexDoctorOptions{})
	require.NoError(t, err)
	reader, err := openIndexReader(context.Background(), "", env.indexSetID, "")
	require.NoError(t, err)
	_, err = reader.QueryObjectCount(context.Background(), indexstore.QueryParams{})
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, before, snapshotIndexGCTreeState(t, env.identityDir))
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")

	guard, err := acquireIndexSetMaintenance(context.Background(), env.indexSetID, "gc-held-reader-test")
	require.NoError(t, err)
	defer func() { _ = guard.Release() }()
	_, err = loadSQLiteListDisplayEntry(context.Background(), meta, env.identityDir, filepath.Base(env.identityDir), filepath.Join(env.identityDir, "identity.json"))
	require.ErrorIs(t, err, indexcoord.ErrHeld)
	_, err = inspectIndexDBForDoctor(context.Background(), dbPath, indexDoctorOptions{})
	require.ErrorIs(t, err, indexcoord.ErrHeld)
	reader, err = openIndexReader(context.Background(), "", env.indexSetID, "")
	require.ErrorIs(t, err, indexcoord.ErrHeld)
	require.Nil(t, reader)
	require.Equal(t, before, snapshotIndexGCTreeState(t, env.identityDir))
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")
}

func TestActiveSQLiteEnrichPreventsPlannedGCExecution(t *testing.T) {
	env := seedSQLiteEnrichGCAppData(t)
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	plan, err := buildIndexGCPlan(context.Background(), 0, "", 0, now)
	require.NoError(t, err)
	require.Len(t, plan.Candidates, 1, "warnings: %#v", plan.Warnings)

	blocking := &blockingEnrichHeadProvider{entered: make(chan struct{}), release: make(chan struct{})}
	oldProvider := newEnrichHeadProvider
	newEnrichHeadProvider = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return blocking, nil
	}
	t.Cleanup(func() { newEnrichHeadProvider = oldProvider })

	cmd, out := configureIndexEnrichCommandForGCTest(t)
	// Hide the durable latest so the enrich resolves the SQLite path, then
	// restore it: GC planning/execution needs the proven durable root.
	restoreLatest := hideDurableLatestForSQLiteSelection(t, env.indexSetID)
	enrichDone := make(chan error, 1)
	go func() {
		enrichDone <- runIndexEnrichWithHead(cmd, []string{env.indexSetID})
	}()
	select {
	case <-blocking.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("SQLite enrich did not reach HEAD while holding maintenance authority")
	}
	restoreLatest()

	store, err := openDefaultOperationCheckpointStore(context.Background())
	require.NoError(t, err)
	_, err = executeIndexGCPlan(context.Background(), store, plan, 0, "", 0, now, indexGCExecutionHooks{})
	require.ErrorContains(t, err, "maintenance lease")
	require.DirExists(t, env.identityDir)
	require.DirExists(t, filepath.Join(filepath.Dir(filepath.Dir(env.identityDir)), "cache", "segments", env.indexSetID))

	close(blocking.release)
	select {
	case err := <-enrichDone:
		require.NoError(t, err, out.String())
	case <-time.After(5 * time.Second):
		t.Fatal("SQLite enrich did not finish after HEAD release")
	}
	require.DirExists(t, env.identityDir)
}

func TestActiveSQLiteReaderPreventsGCBeforeQuarantine(t *testing.T) {
	env := seedSQLiteEnrichGCAppData(t)
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	plan, err := buildIndexGCPlan(context.Background(), 0, "", 0, now)
	require.NoError(t, err)
	require.Len(t, plan.Candidates, 1, "warnings: %#v", plan.Warnings)
	before := snapshotIndexGCTreeState(t, env.identityDir)

	// Hide the durable latest so the opened reader is the SQLite one, then
	// restore it: GC execution needs the proven durable root.
	restoreLatest := hideDurableLatestForSQLiteSelection(t, env.indexSetID)
	reader, err := openIndexReader(context.Background(), "", env.indexSetID, "")
	require.NoError(t, err)
	require.Equal(t, indexreader.FormatSQLiteV1, reader.Meta().Format)
	restoreLatest()
	store, err := openDefaultOperationCheckpointStore(context.Background())
	require.NoError(t, err)
	_, err = executeIndexGCPlan(context.Background(), store, plan, 0, "", 0, now, indexGCExecutionHooks{})
	require.ErrorContains(t, err, "maintenance lease")
	require.Equal(t, before, snapshotIndexGCTreeState(t, env.identityDir))
	require.DirExists(t, env.identityDir)
	quarantines, err := filepath.Glob(filepath.Join(filepath.Dir(env.identityDir), ".gonimbus-gc-*"))
	require.NoError(t, err)
	require.Empty(t, quarantines)
	require.NoError(t, reader.Close())

	result, err := executeIndexGCPlan(context.Background(), store, plan, 0, "", 0, now, indexGCExecutionHooks{})
	require.NoError(t, err)
	require.Equal(t, "success", result.Status)
	require.NoDirExists(t, env.identityDir)
}

func TestRecoveredGCPostQuarantineBlocksPublicLibraryWritersAndConverges(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	standard := "STANDARD"
	env := seedDurableOnlyAppData(t, dataRoot, []indexsubstrate.CurrentObjectRow{{
		RelKey: "item.bin", SizeBytes: 10, ETag: `"item"`, StorageClass: &standard,
	}})
	segmentRoot := filepath.Join(dataRoot, "cache", "segments", env.indexSetID)

	indexGCTestExecutionHooks = indexGCExecutionHooks{afterBoundary: func(boundary string) error {
		if boundary == "quarantine:"+env.indexSetID+":segment-set" {
			return errors.New("crash after segment-root quarantine")
		}
		return nil
	}}
	t.Cleanup(func() { indexGCTestExecutionHooks = indexGCExecutionHooks{} })
	require.Error(t, runIndexGC(newIndexGCExecutionTestCommand(), nil))
	require.NoDirExists(t, segmentRoot)

	recoveryEntered := make(chan struct{})
	recoveryRelease := make(chan struct{})
	var once sync.Once
	indexGCTestExecutionHooks = indexGCExecutionHooks{afterBoundary: func(boundary string) error {
		if strings.HasPrefix(boundary, "delete-entry:"+env.indexSetID+":identity:") {
			once.Do(func() { close(recoveryEntered) })
			<-recoveryRelease
		}
		return nil
	}}
	recoveryDone := make(chan error, 1)
	go func() { recoveryDone <- runIndexGC(newIndexGCExecutionTestCommand(), nil) }()
	select {
	case <-recoveryEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("GC recovery did not reach a post-quarantine delete boundary")
	}

	prov := &blockingEnrichHeadProvider{entered: make(chan struct{}), release: make(chan struct{})}
	base := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	buildPaths := indexbuild.PathConfig{
		JournalDir:   filepath.Join(dataRoot, "journals", "crawl", env.indexSetID, "run_library_block"),
		SegmentDir:   filepath.Join(segmentRoot, "runs", "run_library_block"),
		ManifestPath: filepath.Join(segmentRoot, "runs", "run_library_block", "manifest.json"),
		CompletePath: filepath.Join(segmentRoot, "runs", "run_library_block", "complete.json"),
		LatestPath:   filepath.Join(segmentRoot, "latest.json"),
		IndexDBDir:   env.identityDir,
	}
	_, err := indexbuild.NewRunner(indexbuild.Config{
		IndexSetID: env.indexSetID, RunID: "run_library_block", BaseURI: env.params.BaseURI,
		Source: indexbuild.Source{Provider: prov, ProviderName: "s3"}, Match: indexbuild.MatchConfig{Includes: []string{"**"}},
		Paths: buildPaths, Coverage: []indexbuild.CoverageAttestation{{Scope: &indexbuild.Scope{Prefix: "data/"}, Basis: indexbuild.CoverageBasisConfirmed, Complete: true}},
		RunStartedAt: base, CreatedAt: base, TargetRowsPerSegment: 100,
	}).Build(context.Background())
	require.ErrorIs(t, err, indexcoord.ErrHeld)
	require.NoDirExists(t, segmentRoot)

	_, err = indexbuild.Retry(context.Background(), indexbuild.RetryConfig{
		IndexSetID: env.indexSetID, RunID: "run_library_retry", BaseURI: env.params.BaseURI,
		Paths: buildPaths, JournalPaths: []string{filepath.Join(buildPaths.JournalDir, "shard-0001.jsonl")},
		Coverage:     []indexbuild.CoverageAttestation{{Scope: &indexbuild.Scope{Prefix: "data/"}, Basis: indexbuild.CoverageBasisConfirmed, Complete: true}},
		RunStartedAt: base, CreatedAt: base,
	})
	require.ErrorIs(t, err, indexcoord.ErrHeld)
	require.NoDirExists(t, segmentRoot)

	_, err = indexenrich.Run(context.Background(), indexenrich.Config{
		IndexSetID: env.indexSetID, BaseURI: env.params.BaseURI, Provider: prov,
		SegmentSetRoot: segmentRoot, JournalRoot: filepath.Join(dataRoot, "journals", "crawl", env.indexSetID), Parallel: 1,
	})
	require.ErrorIs(t, err, indexcoord.ErrHeld)
	require.NoDirExists(t, segmentRoot)

	close(recoveryRelease)
	select {
	case err := <-recoveryDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("GC recovery did not converge after releasing the deletion boundary")
	}
	store, err := openDefaultOperationCheckpointStore(context.Background())
	require.NoError(t, err)
	checkpoints, err := listIndexGCDeleteCheckpoints(store)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	require.Equal(t, opcheckpoint.StatusSuccess, checkpoints[0].Status)
}

// hideDurableLatestForSQLiteSelection temporarily removes the durable latest
// pointer so format-aware resolution selects the set-root SQLite database
// (durable outranks SQLite whenever a verified latest exists). The returned
// restore puts the pointer back for stages that need the proven durable root
// (e.g. GC planning/execution).
func hideDurableLatestForSQLiteSelection(t *testing.T, indexSetID string) (restore func()) {
	t.Helper()
	latest := filepath.Join(os.Getenv("GONIMBUS_DATA_DIR"), "cache", "segments", indexSetID, "latest.json")
	require.NoError(t, os.Rename(latest, latest+".bak"))
	return func() { require.NoError(t, os.Rename(latest+".bak", latest)) }
}

func seedSQLiteEnrichGCAppData(t *testing.T) durableCLIEnv {
	t.Helper()
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	db := openIndexGCTestDB(t, filepath.Join(env.identityDir, "index.db"))
	ctx := context.Background()
	require.NoError(t, indexstore.Migrate(ctx, db))
	set, _, err := indexstore.FindOrCreateIndexSet(ctx, db, env.params)
	require.NoError(t, err)
	run, err := indexstore.CreateIndexRun(ctx, db, set.IndexSetID, "crawl")
	require.NoError(t, err)
	storageClass := "STANDARD"
	require.NoError(t, indexstore.UpsertObject(ctx, db, indexstore.ObjectRow{
		IndexSetID:    set.IndexSetID,
		RelKey:        "item.bin",
		SizeBytes:     10,
		StorageClass:  &storageClass,
		LastSeenRunID: run.RunID,
		LastSeenAt:    run.StartedAt,
	}))
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusSuccess, nil))
	require.NoError(t, db.Close())
	return env
}

func configureIndexEnrichCommandForGCTest(t *testing.T) (*cobra.Command, *bytes.Buffer) {
	t.Helper()
	cmd := indexEnrichWithHeadCmd
	oldResumeRun, _ := cmd.Flags().GetString("resume-run")
	oldResume, _ := cmd.Flags().GetBool("resume")
	oldParallel, _ := cmd.Flags().GetInt("parallel")
	oldPattern, _ := cmd.Flags().GetString("pattern")
	oldStateOut, _ := cmd.Flags().GetString("state-out")
	t.Cleanup(func() {
		_ = cmd.Flags().Set("resume-run", oldResumeRun)
		_ = cmd.Flags().Set("resume", boolString(oldResume))
		_ = cmd.Flags().Set("parallel", fmt.Sprintf("%d", oldParallel))
		_ = cmd.Flags().Set("pattern", oldPattern)
		_ = cmd.Flags().Set("state-out", oldStateOut)
		cmd.SetOut(nil)
		cmd.SetErr(nil)
		cmd.SetContext(context.Background())
	})
	require.NoError(t, cmd.Flags().Set("resume-run", ""))
	require.NoError(t, cmd.Flags().Set("resume", "false"))
	require.NoError(t, cmd.Flags().Set("parallel", "1"))
	require.NoError(t, cmd.Flags().Set("pattern", ""))
	require.NoError(t, cmd.Flags().Set("state-out", ""))
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetContext(context.Background())
	return cmd, out
}
