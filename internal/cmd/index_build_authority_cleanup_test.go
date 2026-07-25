package cmd

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

// rebindAuthorityPathname replaces every set-authority lock file under
// authorityRoot with a different inode, modelling something swapping the lease
// pathname out from under its holder. It returns the decoy content that must
// survive, and fails the test if there was nothing to rebind — a control that
// silently swapped nothing would prove nothing.
func rebindAuthorityPathname(t *testing.T, authorityRoot string) string {
	t.Helper()
	entries, err := os.ReadDir(authorityRoot)
	require.NoError(t, err)
	const decoy = "decoy-inode-B"
	swapped := 0
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".lock") {
			continue
		}
		lockPath := filepath.Join(authorityRoot, entry.Name())
		decoyPath := lockPath + ".decoy"
		require.NoError(t, os.WriteFile(decoyPath, []byte(decoy), 0o600))
		require.NoError(t, os.Rename(decoyPath, lockPath))
		swapped++
	}
	require.Equal(t, 1, swapped, "exactly one held lease must have been rebound")
	return decoy
}

func authorityRootForTestDataRoot(t *testing.T) string {
	t.Helper()
	segmentCache, err := appDataPath(appDataClassSegmentCache)
	require.NoError(t, err)
	return filepath.Join(segmentCache, indexsubstrate.SetAuthorityDirectoryName)
}

// TestIndexBuildSurfacesAuthorityCleanupRefusal drives the cleanup-refusal path
// through the real command, after work that otherwise succeeded. It pins the
// whole chain: substrate refusal, guard, command result, and managed job record.
func TestIndexBuildSurfacesAuthorityCleanupRefusal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("platform refuses to rebind a held authority pathname; the swap is unconstructible here")
	}
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)

	base := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC)
	objects := []provider.ObjectSummary{
		{Key: "data/site-a/2026-04-01/a.xml", Size: 10, ETag: `"a"`, LastModified: base, StorageClass: "STANDARD"},
	}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return indexBuildEngineFakeProvider{objects: objects}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = writeScopedPrefixManifest(t, []string{"site-a/2026-04-01/"})
	indexBuildFormat = "durable"

	// Rebind the pathname after the build's work, in the window before its
	// deferred cleanup runs.
	var decoy string
	oldAfterWork := indexBuildAfterWork
	indexBuildAfterWork = func() { decoy = rebindAuthorityPathname(t, authorityRootForTestDataRoot(t)) }
	t.Cleanup(func() { indexBuildAfterWork = oldAfterWork })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	var out strings.Builder
	cmd.SetOut(&out)

	err := runIndexBuild(cmd, nil)
	require.Error(t, err, "a refused authority cleanup must reach the command result, not be discarded by a deferred release")
	require.ErrorContains(t, err, "release index-set authority")

	authorityRoot := authorityRootForTestDataRoot(t)
	entries, readErr := os.ReadDir(authorityRoot)
	require.NoError(t, readErr)
	require.Len(t, entries, 1, "the rebound occupant must still be there")
	content, readErr := os.ReadFile(filepath.Join(authorityRoot, entries[0].Name())) // #nosec G304 -- test-owned temp path
	require.NoError(t, readErr)
	require.Equal(t, decoy, string(content), "cleanup must never delete the inode that now occupies the pathname")

	// The lock was released despite the refusal: a fresh acquisition succeeds
	// rather than reporting contention with the finished build.
	segmentCache, err := appDataPath(appDataClassSegmentCache)
	require.NoError(t, err)
	setID := strings.TrimSuffix(entries[0].Name(), ".lock")
	successor, acquireErr := indexsubstrate.AcquireSetAuthority(context.Background(),
		filepath.Join(segmentCache, setID), setID, "index-build-successor")
	require.NoError(t, acquireErr, "a refused cleanup must still have released the lock")
	require.NoError(t, successor.Release())

	// The job record must not still claim success while the command exits
	// non-zero. The build receipt it carries stays as written — the snapshot was
	// committed — but the record's state follows the command.
	record := onlyJobRecord(t)
	require.NotEqual(t, jobregistry.JobStateSuccess, record.State,
		"a job record must not report success for a command that failed its cleanup")
	require.Equal(t, jobregistry.JobStateFailed, record.State)
	require.Contains(t, record.Metadata[managedJobAuthorityCleanupErrorKey], "release index-set authority",
		"the record must carry why an otherwise-complete run is not a success")

	// The two planes, pinned together: the job failed, and the artifact receipt
	// still attests the commit that really happened. A later cleanup refusal does
	// not unmake a committed snapshot, so demoting this receipt would falsify
	// artifact state rather than report an operational failure.
	require.NotNil(t, record.Receipt, "the committed build receipt is preserved")
	require.Equal(t, "success", record.Receipt.Status,
		"the artifact receipt reports the commit, which succeeded")
	require.Equal(t, setID, record.Receipt.IndexSetID)
	require.NotEmpty(t, record.Receipt.RunID)
	require.NotEmpty(t, record.Receipt.FormatsCommitted, "the receipt names the artifacts actually committed")
	require.FileExists(t, filepath.Join(segmentCache, setID, "latest.json"),
		"the committed snapshot the receipt names is really on disk")
}

// TestIndexBuildReportsBothCleanupAndUnpersistedCorrection drives the real
// command through the compound case the contract admits: cleanup refused, and the
// job-record correction unpersistable. Both causes must reach the caller, since
// the command's error is then the only signal that the record is stale — a state
// this contract tolerates but does not hide.
func TestIndexBuildReportsBothCleanupAndUnpersistedCorrection(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("platform refuses to rebind a held authority pathname; the swap is unconstructible here")
	}
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)

	base := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC)
	objects := []provider.ObjectSummary{
		{Key: "data/site-a/2026-04-01/a.xml", Size: 10, ETag: `"a"`, LastModified: base, StorageClass: "STANDARD"},
	}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return indexBuildEngineFakeProvider{objects: objects}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = writeScopedPrefixManifest(t, []string{"site-a/2026-04-01/"})
	indexBuildFormat = "durable"

	// Block the correction by breaking the store path the registry writes through,
	// with the real record stashed intact so the end state stays observable.
	// Permissions are not the lever: the registry repairs the mode of a job
	// directory it owns on every write.
	var brokenJobDir, stashedJobDir string
	oldAfterWork := indexBuildAfterWork
	indexBuildAfterWork = func() {
		rebindAuthorityPathname(t, authorityRootForTestDataRoot(t))
		// Terminal success is already on disk at this point.
		brokenJobDir = onlyJobDir(t)
		stashedJobDir = brokenJobDir + ".stashed"
		require.NoError(t, os.Rename(brokenJobDir, stashedJobDir))
		require.NoError(t, os.WriteFile(brokenJobDir, []byte("not a directory"), 0o600))
	}
	restoreStore := func() {
		if stashedJobDir == "" {
			return
		}
		_ = os.Remove(brokenJobDir)
		_ = os.Rename(stashedJobDir, brokenJobDir)
		stashedJobDir = ""
	}
	t.Cleanup(func() {
		indexBuildAfterWork = oldAfterWork
		restoreStore()
	})

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	var out strings.Builder
	cmd.SetOut(&out)

	err := runIndexBuild(cmd, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "release index-set authority",
		"the cleanup refusal must be reported")
	require.ErrorContains(t, err, "job record still reports success after failed authority cleanup",
		"a correction that could not be persisted must be reported too, or nothing distinguishes it from a corrected record")

	// Restore the store and confirm the honest end state: the persisted record
	// does still claim success, and the receipt it carries is still valid. The
	// command result is the only signal, which is why it must carry both causes.
	restoreStore()
	record := onlyJobRecord(t)
	require.Equal(t, jobregistry.JobStateSuccess, record.State,
		"the record is genuinely stale here; that is what the reported error is for")
	require.NotNil(t, record.Receipt)
	require.Equal(t, "success", record.Receipt.Status)
	require.NotEmpty(t, record.Receipt.RunID)
}

// TestIndexBuildRecordsSuccessWithoutCleanupInterference is the positive control
// for the test above: the same build, with nothing rebinding the pathname,
// succeeds, records success, and leaves no residue. Without it, a build that
// failed for any unrelated reason could keep the refusal test green.
func TestIndexBuildRecordsSuccessWithoutCleanupInterference(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)

	base := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC)
	objects := []provider.ObjectSummary{
		{Key: "data/site-a/2026-04-01/a.xml", Size: 10, ETag: `"a"`, LastModified: base, StorageClass: "STANDARD"},
	}
	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return indexBuildEngineFakeProvider{objects: objects}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = writeScopedPrefixManifest(t, []string{"site-a/2026-04-01/"})
	indexBuildFormat = "durable"

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	var out strings.Builder
	cmd.SetOut(&out)

	require.NoError(t, runIndexBuild(cmd, nil))

	record := onlyJobRecord(t)
	require.Equal(t, jobregistry.JobStateSuccess, record.State)
	require.NotContains(t, record.Metadata, managedJobAuthorityCleanupErrorKey)

	authorityRoot := authorityRootForTestDataRoot(t)
	entries, err := os.ReadDir(authorityRoot)
	if os.IsNotExist(err) {
		return
	}
	require.NoError(t, err)
	require.Empty(t, entries, "a clean run must leave no set-authority residue")
}

// onlyJobRecord returns the single job record this test's build wrote.
func onlyJobRecord(t *testing.T) jobregistry.JobRecord {
	t.Helper()
	root, err := indexJobsRootDir()
	require.NoError(t, err)
	records, err := jobregistry.NewStore(root).List()
	require.NoError(t, err)
	require.Len(t, records, 1)
	return records[0]
}

// onlyJobDir returns the on-disk directory of the single job record this test's
// build wrote.
func onlyJobDir(t *testing.T) string {
	t.Helper()
	root, err := indexJobsRootDir()
	require.NoError(t, err)
	store := jobregistry.NewStore(root)
	records, err := store.List()
	require.NoError(t, err)
	require.Len(t, records, 1)
	return store.JobDir(records[0].JobID)
}

// TestDemoteManagedJobOnCleanupFailure pins the record-demotion rules directly,
// including the states it must leave alone.
func TestDemoteManagedJobOnCleanupFailure(t *testing.T) {
	newRecord := func(state jobregistry.JobState) *jobregistry.JobRecord {
		return &jobregistry.JobRecord{
			JobID:        uuid.NewString(),
			Type:         jobregistry.JobTypeIndexBuild,
			State:        state,
			ManifestPath: "/tmp/manifest.yaml",
			CreatedAt:    time.Now().UTC(),
		}
	}
	cleanupErr := os.ErrPermission

	for _, state := range []jobregistry.JobState{jobregistry.JobStateSuccess, jobregistry.JobStatePartial} {
		store := jobregistry.NewStore(t.TempDir())
		record := newRecord(state)
		require.NoError(t, store.Write(record))
		require.NoError(t, demoteManagedJobOnCleanupFailure(store, record, cleanupErr))
		require.Equal(t, jobregistry.JobStateFailed, record.State, "a %s record must not survive a failed cleanup", state)
		require.Equal(t, cleanupErr.Error(), record.Metadata[managedJobAuthorityCleanupErrorKey])
		require.NotNil(t, record.EndedAt)

		stored, err := store.Get(record.JobID)
		require.NoError(t, err)
		require.Equal(t, jobregistry.JobStateFailed, stored.State, "the demotion must be persisted, not only in memory")
	}

	// An already-failed record keeps the detail it already carries.
	store := jobregistry.NewStore(t.TempDir())
	failed := newRecord(jobregistry.JobStateFailed)
	require.NoError(t, store.Write(failed))
	require.NoError(t, demoteManagedJobOnCleanupFailure(store, failed, cleanupErr))
	require.Equal(t, jobregistry.JobStateFailed, failed.State)
	require.NotContains(t, failed.Metadata, managedJobAuthorityCleanupErrorKey)

	// No cleanup failure, no rewrite.
	success := newRecord(jobregistry.JobStateSuccess)
	require.NoError(t, store.Write(success))
	require.NoError(t, demoteManagedJobOnCleanupFailure(store, success, nil))
	require.Equal(t, jobregistry.JobStateSuccess, success.State)
}

// TestDemoteManagedJobReportsUnpersistedCorrection covers the case that makes the
// correction best-effort rather than guaranteed: terminal success is already on
// disk when cleanup runs, so a store that cannot be written leaves a record
// claiming success for a command that failed.
//
// That discrepancy is real and cannot be closed from here — only by not
// persisting terminal success until cleanup has run. What must never happen is
// the discrepancy going unreported, which is exactly what a discarded write
// error would do.
func TestDemoteManagedJobReportsUnpersistedCorrection(t *testing.T) {
	blockingPath := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blockingPath, []byte("x"), 0o600))
	store := jobregistry.NewStore(filepath.Join(blockingPath, "jobs"))

	record := &jobregistry.JobRecord{
		JobID:        uuid.NewString(),
		Type:         jobregistry.JobTypeIndexBuild,
		State:        jobregistry.JobStateSuccess,
		ManifestPath: "/tmp/manifest.yaml",
		CreatedAt:    time.Now().UTC(),
	}
	err := demoteManagedJobOnCleanupFailure(store, record, os.ErrPermission)
	require.Error(t, err, "an unpersistable correction must be reported, not swallowed")
	require.ErrorContains(t, err, "job record still reports success after failed authority cleanup")
}
