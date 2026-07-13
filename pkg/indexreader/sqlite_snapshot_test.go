package indexreader

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/manifest"
)

func seedSQLiteSnapshotEnv(t *testing.T) (durableTestEnv, string) {
	t.Helper()
	env := setupDurableTestEnv(t, nil)
	dbPath := filepath.Join(env.identityDir, "index.db")
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(context.Background(), db))
	_, _, err = indexstore.FindOrCreateIndexSet(context.Background(), db, env.params)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")
	return env, dbPath
}

func TestSQLiteSnapshotRejectsTransactionSidecarsWithoutDBMutation(t *testing.T) {
	for _, suffix := range []string{"-wal", "-shm", "-journal", "-mj test", "-stmtjrnl-test"} {
		t.Run(suffix, func(t *testing.T) {
			env, dbPath := seedSQLiteSnapshotEnv(t)
			before, err := os.ReadFile(dbPath)
			require.NoError(t, err)
			beforeInfo, err := os.Stat(dbPath)
			require.NoError(t, err)
			sidecarPath := dbPath + suffix
			require.NoError(t, os.WriteFile(sidecarPath, []byte("transaction-state\n"), 0o600))

			snapshot, err := OpenSQLiteSnapshot(context.Background(), SQLiteSnapshotOptions{
				Path: dbPath, SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID,
			})
			require.ErrorContains(t, err, "transaction sidecars")
			require.Nil(t, snapshot)
			after, err := os.ReadFile(dbPath)
			require.NoError(t, err)
			afterInfo, err := os.Stat(dbPath)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, beforeInfo.Mode(), afterInfo.Mode())
			require.Equal(t, beforeInfo.ModTime(), afterInfo.ModTime())
			require.FileExists(t, sidecarPath)
		})
	}
}

func TestSQLiteSnapshotRejectsLiveWALWriterAndDoesNotReadStaleBase(t *testing.T) {
	env, dbPath := seedSQLiteSnapshotEnv(t)
	writerAuthority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "writer")
	require.NoError(t, err)
	defer func() { _ = writerAuthority.Release() }()
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(context.Background(), `UPDATE index_sets SET created_at = ? WHERE index_set_id = ?`, time.Now().UTC().Format(time.RFC3339Nano), env.indexSetID)
	require.NoError(t, err)
	require.FileExists(t, dbPath+"-wal")
	require.FileExists(t, dbPath+"-shm")
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	beforeInfo, err := os.Stat(dbPath)
	require.NoError(t, err)

	snapshot, err := OpenSQLiteSnapshot(context.Background(), SQLiteSnapshotOptions{
		Path: dbPath, SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID,
	})
	require.ErrorContains(t, err, "transaction sidecars")
	require.Nil(t, snapshot)
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	afterInfo, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, beforeInfo.Mode(), afterInfo.Mode())
	require.Equal(t, beforeInfo.ModTime(), afterInfo.ModTime())
}

func TestSQLiteSnapshotAndGCShareAuthorityForReadLifetime(t *testing.T) {
	env, dbPath := seedSQLiteSnapshotEnv(t)
	snapshot, err := OpenSQLiteSnapshot(context.Background(), SQLiteSnapshotOptions{
		Path: dbPath, SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID,
	})
	require.NoError(t, err)

	contender, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "gc")
	require.Error(t, err)
	require.True(t, errors.Is(err, indexcoord.ErrHeld))
	require.Nil(t, contender)
	require.NoError(t, snapshot.Check())
	require.NoError(t, snapshot.Close())
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")

	gcAuthority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "gc")
	require.NoError(t, err)
	defer func() { _ = gcAuthority.Release() }()
	blocked, err := OpenSQLiteSnapshot(context.Background(), SQLiteSnapshotOptions{
		Path: dbPath, SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID,
	})
	require.ErrorIs(t, err, indexcoord.ErrHeld)
	require.Nil(t, blocked)
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")
}

func TestSQLiteSnapshotRejectsAuthorityIDThatDoesNotMatchDatabase(t *testing.T) {
	env, dbPath := seedSQLiteSnapshotEnv(t)
	realAuthority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "real-set-writer")
	require.NoError(t, err)
	defer func() { require.NoError(t, realAuthority.Release()) }()

	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	beforeInfo, err := os.Stat(dbPath)
	require.NoError(t, err)
	const wrongID = "idx_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	snapshot, err := OpenSQLiteSnapshot(context.Background(), SQLiteSnapshotOptions{
		Path: dbPath, SegmentSetRoot: env.segmentRoot, IndexSetID: wrongID,
	})
	require.ErrorContains(t, err, "identity/scope mismatch")
	require.Nil(t, snapshot)
	require.NoError(t, realAuthority.AssertHeldFor(env.indexSetID, env.segmentRoot))
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	afterInfo, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, beforeInfo.Mode(), afterInfo.Mode())
	require.Equal(t, beforeInfo.ModTime(), afterInfo.ModTime())
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")
	require.NoFileExists(t, dbPath+"-journal")
}

func TestSQLiteSnapshotRejectsAmbiguousMultiSetDatabase(t *testing.T) {
	env, dbPath := seedSQLiteSnapshotEnv(t)
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	otherParams := env.params
	otherParams.BaseURI = "s3://test-bucket/other/"
	_, _, err = indexstore.FindOrCreateIndexSet(context.Background(), db, otherParams)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")

	snapshot, err := OpenSQLiteSnapshot(context.Background(), SQLiteSnapshotOptions{
		Path: dbPath, SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID,
	})
	require.ErrorContains(t, err, "database contains 2 index sets, expected exactly 1")
	require.Nil(t, snapshot)
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")
}

func TestListIndexReadersSurfacesUntrustedCanonicalSQLiteIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status IdentityStatus
		mutate func(t *testing.T, path string)
	}{
		{name: "missing", status: IdentityStatusMissing, mutate: func(t *testing.T, path string) {
			require.NoError(t, os.Remove(path))
		}},
		{name: "invalid", status: IdentityStatusInvalid, mutate: func(t *testing.T, path string) {
			require.NoError(t, os.WriteFile(path, []byte("{invalid\n"), 0o600))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, dbPath := seedSQLiteSnapshotEnv(t)
			tc.mutate(t, filepath.Join(env.identityDir, "identity.json"))

			listed, err := ListIndexReaders(context.Background(), env.opts)
			require.NoError(t, err)
			var sqlite *ListedIndex
			for i := range listed {
				if listed[i].Meta.SourcePath == dbPath {
					sqlite = &listed[i]
					break
				}
			}
			require.NotNil(t, sqlite)
			require.Equal(t, FormatSQLiteV1, sqlite.Meta.Format)
			require.Equal(t, tc.status, sqlite.IdentityStatus)
			require.NotEmpty(t, sqlite.IdentityDiagnostic)
			require.Empty(t, sqlite.Meta.IndexSetID, "untrusted discovery must not claim a database identity")
		})
	}
}

func TestValidateSQLiteWriteTargetRefusesMarkerlessAdoption(t *testing.T) {
	env, dbPath := seedSQLiteSnapshotEnv(t)
	require.NoError(t, os.Remove(filepath.Join(env.identityDir, "identity.json")))
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	beforeInfo, err := os.Stat(dbPath)
	require.NoError(t, err)
	authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "build-preflight")
	require.NoError(t, err)
	defer func() { require.NoError(t, authority.Release()) }()

	err = ValidateSQLiteWriteTarget(context.Background(), SQLiteWriteTargetOptions{
		Path: dbPath, IdentityPath: filepath.Join(env.identityDir, "identity.json"),
		SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID, Authority: authority,
	})
	require.ErrorIs(t, err, ErrCanonicalSQLiteAdoption)
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	afterInfo, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, beforeInfo.Mode(), afterInfo.Mode())
	require.Equal(t, beforeInfo.ModTime(), afterInfo.ModTime())
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")

	freshPath := filepath.Join(t.TempDir(), "index.db")
	require.NoError(t, ValidateSQLiteWriteTarget(context.Background(), SQLiteWriteTargetOptions{
		Path: freshPath, SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID, Authority: authority,
	}))
	require.NoFileExists(t, freshPath)
}

func TestSQLiteIdentityPublicationGuardRetainsProofUntilMarkerBoundary(t *testing.T) {
	t.Run("existing database replaced", func(t *testing.T) {
		env, dbPath := seedSQLiteSnapshotEnv(t)
		identity, err := indexstore.ComputeIndexSetID(env.params)
		require.NoError(t, err)
		identityPath := filepath.Join(env.identityDir, "identity.json")
		markerBefore, err := os.ReadFile(identityPath)
		require.NoError(t, err)
		authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "identity-existing")
		require.NoError(t, err)
		defer func() { require.NoError(t, authority.Release()) }()
		guard, err := OpenSQLiteIdentityPublicationGuard(context.Background(), SQLiteWriteTargetOptions{
			Path: dbPath, IdentityPath: identityPath, SegmentSetRoot: env.segmentRoot,
			IndexSetID: env.indexSetID, Authority: authority,
		})
		require.NoError(t, err)
		defer func() { _ = guard.Close() }()

		validatedPath := filepath.Join(t.TempDir(), "validated.db")
		replaceErr := os.Rename(dbPath, validatedPath)
		if replaceErr != nil {
			// Native Windows retains a no-delete share and prevents the
			// interposition itself while the proof is live.
			require.NoError(t, guard.PublishIdentity(identity))
			require.Equal(t, markerBefore, mustReadSQLiteWriteTargetTestFile(t, identityPath))
			return
		}
		require.NoError(t, os.WriteFile(dbPath, []byte("unvalidated replacement\n"), 0o640))
		substituteBefore, err := os.Stat(dbPath)
		require.NoError(t, err)
		substituteBytes := mustReadSQLiteWriteTargetTestFile(t, dbPath)

		err = guard.PublishIdentity(identity)
		require.ErrorIs(t, err, ErrCanonicalSQLiteAdoption)
		require.Equal(t, markerBefore, mustReadSQLiteWriteTargetTestFile(t, identityPath))
		require.Equal(t, substituteBytes, mustReadSQLiteWriteTargetTestFile(t, dbPath))
		substituteAfter, err := os.Stat(dbPath)
		require.NoError(t, err)
		require.Equal(t, substituteBefore.Mode(), substituteAfter.Mode())
		require.Equal(t, substituteBefore.ModTime(), substituteAfter.ModTime())
		require.NoFileExists(t, dbPath+"-wal")
		require.NoFileExists(t, dbPath+"-shm")
	})

	t.Run("absent database appears", func(t *testing.T) {
		env := setupDurableTestEnv(t, nil)
		identity, err := indexstore.ComputeIndexSetID(env.params)
		require.NoError(t, err)
		dbPath := filepath.Join(env.identityDir, "index.db")
		identityPath := filepath.Join(env.identityDir, "identity.json")
		require.NoError(t, os.Remove(identityPath))
		authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "identity-absent")
		require.NoError(t, err)
		defer func() { require.NoError(t, authority.Release()) }()
		guard, err := OpenSQLiteIdentityPublicationGuard(context.Background(), SQLiteWriteTargetOptions{
			Path: dbPath, IdentityPath: identityPath, SegmentSetRoot: env.segmentRoot,
			IndexSetID: env.indexSetID, Authority: authority,
		})
		require.NoError(t, err)
		defer func() { _ = guard.Close() }()
		require.NoError(t, os.WriteFile(dbPath, []byte("appeared after validation\n"), 0o640))
		dbBefore, err := os.Stat(dbPath)
		require.NoError(t, err)
		dbBytes := mustReadSQLiteWriteTargetTestFile(t, dbPath)

		err = guard.PublishIdentity(identity)
		require.ErrorIs(t, err, ErrCanonicalSQLiteAdoption)
		require.NoFileExists(t, identityPath)
		require.Equal(t, dbBytes, mustReadSQLiteWriteTargetTestFile(t, dbPath))
		dbAfter, err := os.Stat(dbPath)
		require.NoError(t, err)
		require.Equal(t, dbBefore.Mode(), dbAfter.Mode())
		require.Equal(t, dbBefore.ModTime(), dbAfter.ModTime())
		require.NoFileExists(t, dbPath+"-wal")
		require.NoFileExists(t, dbPath+"-shm")
	})
}

func TestCanonicalMetadataPublicationRejectsSymlinksAndSwaps(t *testing.T) {
	t.Run("absent database identity symlink", func(t *testing.T) {
		env := setupDurableTestEnv(t, nil)
		identity, err := indexstore.ComputeIndexSetID(env.params)
		require.NoError(t, err)
		identityPath := filepath.Join(env.identityDir, "identity.json")
		require.NoError(t, os.Remove(identityPath))
		outside := filepath.Join(t.TempDir(), "outside-identity.json")
		require.NoError(t, os.WriteFile(outside, []byte("external\n"), 0o640))
		outsideBefore := sqliteWriteTargetTestFileState(t, outside)
		require.NoError(t, os.Symlink(outside, identityPath))
		authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "identity-symlink")
		require.NoError(t, err)
		defer func() { require.NoError(t, authority.Release()) }()
		guard, err := OpenSQLiteIdentityPublicationGuard(context.Background(), SQLiteWriteTargetOptions{
			Path: filepath.Join(env.identityDir, "index.db"), IdentityPath: identityPath,
			SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID, Authority: authority,
		})
		require.NoError(t, err)
		defer func() { _ = guard.Close() }()
		err = guard.PublishIdentity(identity)
		require.ErrorContains(t, err, "non-regular canonical metadata destination")
		sqliteWriteTargetRequireFileState(t, outside, outsideBefore)
		require.NoFileExists(t, filepath.Join(env.identityDir, "index.db"))
	})

	t.Run("existing database valid identity symlink", func(t *testing.T) {
		env, dbPath := seedSQLiteSnapshotEnv(t)
		identityPath := filepath.Join(env.identityDir, "identity.json")
		valid := mustReadSQLiteWriteTargetTestFile(t, identityPath)
		require.NoError(t, os.Remove(identityPath))
		outside := filepath.Join(t.TempDir(), "valid-identity.json")
		require.NoError(t, os.WriteFile(outside, valid, 0o640))
		outsideBefore := sqliteWriteTargetTestFileState(t, outside)
		require.NoError(t, os.Symlink(outside, identityPath))
		authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "existing-identity-symlink")
		require.NoError(t, err)
		defer func() { require.NoError(t, authority.Release()) }()
		guard, err := OpenSQLiteIdentityPublicationGuard(context.Background(), SQLiteWriteTargetOptions{
			Path: dbPath, IdentityPath: identityPath, SegmentSetRoot: env.segmentRoot,
			IndexSetID: env.indexSetID, Authority: authority,
		})
		require.ErrorIs(t, err, ErrCanonicalSQLiteAdoption)
		require.Nil(t, guard)
		sqliteWriteTargetRequireFileState(t, outside, outsideBefore)
	})

	t.Run("identity destination swapped before replace", func(t *testing.T) {
		env := setupDurableTestEnv(t, nil)
		identity, err := indexstore.ComputeIndexSetID(env.params)
		require.NoError(t, err)
		identityPath := filepath.Join(env.identityDir, "identity.json")
		require.NoError(t, os.Remove(identityPath))
		outside := filepath.Join(t.TempDir(), "outside-swap.json")
		require.NoError(t, os.WriteFile(outside, []byte("external\n"), 0o640))
		outsideBefore := sqliteWriteTargetTestFileState(t, outside)
		authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "identity-swap")
		require.NoError(t, err)
		defer func() { require.NoError(t, authority.Release()) }()
		guard, err := OpenSQLiteIdentityPublicationGuard(context.Background(), SQLiteWriteTargetOptions{
			Path: filepath.Join(env.identityDir, "index.db"), IdentityPath: identityPath,
			SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID, Authority: authority,
		})
		require.NoError(t, err)
		defer func() { _ = guard.Close() }()
		canonicalMetadataBeforeReplace = func(path string) error { return os.Symlink(outside, path) }
		defer func() { canonicalMetadataBeforeReplace = nil }()
		err = guard.PublishIdentity(identity)
		require.ErrorContains(t, err, "non-regular canonical metadata destination")
		sqliteWriteTargetRequireFileState(t, outside, outsideBefore)
		_, err = ReadLocalIdentityFile(identityPath, 1<<20)
		require.Error(t, err, "the refused symlink must not become canonical trust")
	})

	t.Run("manifest symlink", func(t *testing.T) {
		env := setupDurableTestEnv(t, nil)
		identityPath := filepath.Join(env.identityDir, "identity.json")
		require.NoError(t, os.Remove(identityPath))
		manifestPath := filepath.Join(env.identityDir, "manifest.json")
		outside := filepath.Join(t.TempDir(), "outside-manifest.json")
		require.NoError(t, os.WriteFile(outside, []byte("external\n"), 0o640))
		outsideBefore := sqliteWriteTargetTestFileState(t, outside)
		require.NoError(t, os.Symlink(outside, manifestPath))
		authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "manifest-symlink")
		require.NoError(t, err)
		defer func() { require.NoError(t, authority.Release()) }()
		guard, err := OpenSQLiteIdentityPublicationGuard(context.Background(), SQLiteWriteTargetOptions{
			Path: filepath.Join(env.identityDir, "index.db"), IdentityPath: identityPath,
			SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID, Authority: authority,
		})
		require.NoError(t, err)
		defer func() { _ = guard.Close() }()
		err = guard.PublishManifest(&manifest.IndexManifest{Version: "1.0"})
		require.ErrorContains(t, err, "non-regular canonical metadata destination")
		sqliteWriteTargetRequireFileState(t, outside, outsideBefore)
		require.NoFileExists(t, identityPath)
	})
}

type sqliteWriteTargetFileState struct {
	data  []byte
	mode  os.FileMode
	mtime time.Time
}

func sqliteWriteTargetTestFileState(t *testing.T, path string) sqliteWriteTargetFileState {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return sqliteWriteTargetFileState{data: mustReadSQLiteWriteTargetTestFile(t, path), mode: info.Mode(), mtime: info.ModTime()}
}

func sqliteWriteTargetRequireFileState(t *testing.T, path string, want sqliteWriteTargetFileState) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, want.data, mustReadSQLiteWriteTargetTestFile(t, path))
	require.Equal(t, want.mode, info.Mode())
	require.Equal(t, want.mtime, info.ModTime())
}

func TestOpenSQLiteWriteTargetCarriesBindingIntoMutation(t *testing.T) {
	env, dbPath := seedSQLiteSnapshotEnv(t)
	authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "bound-build")
	require.NoError(t, err)
	defer func() { require.NoError(t, authority.Release()) }()

	target, err := OpenSQLiteWriteTarget(context.Background(), SQLiteWriteTargetOptions{
		Path: dbPath, IdentityPath: filepath.Join(env.identityDir, "identity.json"),
		SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID, Authority: authority,
	})
	require.NoError(t, err)
	require.NotNil(t, target.DB())
	_, err = target.DB().ExecContext(context.Background(), `UPDATE index_sets SET provider = provider WHERE index_set_id = ?`, env.indexSetID)
	require.NoError(t, err)
	require.NoError(t, target.Check())
	require.NoError(t, target.Close())
	require.Nil(t, target.DB())
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")
	// Exact-epoch cleanup may leave discoverable quarantine residue; it must not
	// leave live hot WAL/SHM aliases.
	aliases, err := filepath.Glob(filepath.Join(env.identityDir, ".gonimbus-index-write-*.db*"))
	require.NoError(t, err)
	require.Empty(t, aliases)
	clearSQLiteQuarantineResidueInTempDir(t, dbPath)
}

func TestSQLiteWriteTargetClosePreservesUnprovenQuarantineResidue(t *testing.T) {
	// Entarch R15: prefix/emptiness/authority must not delete unproven quarantine
	// names at Close. Residue is reported without destruction.
	env, dbPath := seedSQLiteSnapshotEnv(t)
	authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "unproven-residue")
	require.NoError(t, err)
	defer func() { require.NoError(t, authority.Release()) }()

	target, err := OpenSQLiteWriteTarget(context.Background(), SQLiteWriteTargetOptions{
		Path: dbPath, IdentityPath: filepath.Join(env.identityDir, "identity.json"),
		SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID, Authority: authority,
	})
	require.NoError(t, err)

	unproven := filepath.Join(filepath.Dir(dbPath), indexstore.CanonicalSQLiteQuarantinePrefix+"unproven-empty")
	require.NoError(t, os.WriteFile(unproven, nil, 0o600))
	before, err := os.Stat(unproven)
	require.NoError(t, err)

	require.NoError(t, target.Close())
	after, err := os.Stat(unproven)
	require.NoError(t, err, "unproven empty quarantine must survive Close")
	require.Equal(t, before.Size(), after.Size())
	require.Equal(t, before.Mode(), after.Mode())
	require.Contains(t, target.QuarantineResidue(), filepath.Base(unproven))
	found, err := indexstore.SQLiteTransactionSidecars(dbPath)
	require.NoError(t, err)
	require.Contains(t, found, filepath.Base(unproven))
}

func TestSQLiteWriteTargetCloseSkipsResidueWorkWhenAuthorityLost(t *testing.T) {
	env, dbPath := seedSQLiteSnapshotEnv(t)
	authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "lost-authority")
	require.NoError(t, err)

	target, err := OpenSQLiteWriteTarget(context.Background(), SQLiteWriteTargetOptions{
		Path: dbPath, IdentityPath: filepath.Join(env.identityDir, "identity.json"),
		SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID, Authority: authority,
	})
	require.NoError(t, err)

	unproven := filepath.Join(filepath.Dir(dbPath), indexstore.CanonicalSQLiteQuarantinePrefix+"unproven-after-lost-auth")
	require.NoError(t, os.WriteFile(unproven, nil, 0o600))
	require.NoError(t, authority.Release())

	err = target.Close()
	require.Error(t, err)
	require.Contains(t, err.Error(), "authority")
	_, statErr := os.Stat(unproven)
	require.NoError(t, statErr, "lost authority must not delete quarantine residue")
	require.Empty(t, target.QuarantineResidue(), "residue inspection is skipped when authority fails")
}

func TestOpenSQLiteWriteTargetSharesCanonicalLockAndWALNamespace(t *testing.T) {
	env, dbPath := seedSQLiteSnapshotEnv(t)
	authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "canonical-namespace")
	require.NoError(t, err)
	defer func() { require.NoError(t, authority.Release()) }()
	target, err := OpenSQLiteWriteTarget(context.Background(), SQLiteWriteTargetOptions{
		Path: dbPath, IdentityPath: filepath.Join(env.identityDir, "identity.json"),
		SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID, Authority: authority,
	})
	require.NoError(t, err)
	require.NoError(t, setSQLiteWriteTargetBusyTimeout(target.DB()))
	canonical, err := indexstore.Open(context.Background(), indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, setSQLiteWriteTargetBusyTimeout(canonical))

	targetTx, err := target.DB().BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = targetTx.ExecContext(context.Background(), `UPDATE index_sets SET provider = 'target-held' WHERE index_set_id = ?`, env.indexSetID)
	require.NoError(t, err)
	_, err = canonical.ExecContext(context.Background(), `UPDATE index_sets SET provider = 'canonical-competing' WHERE index_set_id = ?`, env.indexSetID)
	require.Error(t, err, "a canonical-name writer must not acquire an independent transaction namespace")
	require.NoError(t, targetTx.Commit())

	canonicalTx, err := canonical.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = canonicalTx.ExecContext(context.Background(), `UPDATE index_sets SET provider = 'canonical-held' WHERE index_set_id = ?`, env.indexSetID)
	require.NoError(t, err)
	_, err = target.DB().ExecContext(context.Background(), `UPDATE index_sets SET provider = 'target-competing' WHERE index_set_id = ?`, env.indexSetID)
	require.Error(t, err, "the guarded writer must share the canonical writer's transaction namespace")
	require.NoError(t, canonicalTx.Commit())
	require.NoError(t, canonical.Close())

	var providerName string
	require.NoError(t, target.DB().QueryRowContext(context.Background(), `SELECT provider FROM index_sets WHERE index_set_id = ?`, env.indexSetID).Scan(&providerName))
	require.Equal(t, "canonical-held", providerName)
	require.NoError(t, target.Close())
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")
	aliases, err := filepath.Glob(filepath.Join(env.identityDir, ".gonimbus-index-write-*.db*"))
	require.NoError(t, err)
	require.Empty(t, aliases)
	clearSQLiteQuarantineResidueInTempDir(t, dbPath)
}

func TestOpenSQLiteSnapshotRefusesEmptyQuarantineResidue(t *testing.T) {
	// Entarch R16: unrecovered quarantine residue — including empty
	// fd-truncated captures — must block later canonical readers.
	env, dbPath := seedSQLiteSnapshotEnv(t)
	emptyResidue := filepath.Join(filepath.Dir(dbPath), indexstore.CanonicalSQLiteQuarantinePrefix+"unrecovered-empty")
	require.NoError(t, os.WriteFile(emptyResidue, nil, 0o600))

	authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "reader-residue")
	require.NoError(t, err)
	defer func() { require.NoError(t, authority.Release()) }()

	_, err = OpenSQLiteSnapshot(context.Background(), SQLiteSnapshotOptions{
		Path:           dbPath,
		SegmentSetRoot: env.segmentRoot,
		IndexSetID:     env.indexSetID,
		Authority:      authority,
	})
	require.Error(t, err, "ADR-0007 requires unrecovered quarantine residue to block later canonical readers")
	require.Contains(t, err.Error(), "sidecar")
	_, statErr := os.Stat(emptyResidue)
	require.NoError(t, statErr, "reader refusal must not delete the residue entry")
}

// clearSQLiteQuarantineResidueInTempDir is a test-local cleanup for multi-step
// fixtures under t.TempDir(). It is not a production API and must not move into
// package source.
func clearSQLiteQuarantineResidueInTempDir(t *testing.T, dbPath string) {
	t.Helper()
	dir := filepath.Dir(dbPath)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, indexstore.CanonicalSQLiteQuarantinePrefix) {
			continue
		}
		require.NoError(t, os.Remove(filepath.Join(dir, name)))
	}
}

func TestSQLiteWriteTargetCrashLeavesCanonicalRecoveryState(t *testing.T) {
	if os.Getenv("GONIMBUS_SQLITE_WRITE_CRASH_HELPER") == "1" {
		segmentRoot := os.Getenv("GONIMBUS_SQLITE_WRITE_SEGMENT_ROOT")
		indexSetID := os.Getenv("GONIMBUS_SQLITE_WRITE_INDEX_SET_ID")
		dbPath := os.Getenv("GONIMBUS_SQLITE_WRITE_DB")
		authority, err := indexcoord.Acquire(context.Background(), segmentRoot, indexSetID, "crash-helper")
		if err != nil {
			t.Fatal(err)
		}
		target, err := OpenSQLiteWriteTarget(context.Background(), SQLiteWriteTargetOptions{
			Path: dbPath, IdentityPath: filepath.Join(filepath.Dir(dbPath), "identity.json"),
			SegmentSetRoot: segmentRoot, IndexSetID: indexSetID, Authority: authority,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := target.DB().ExecContext(context.Background(), `UPDATE index_sets SET provider = 'crash-committed' WHERE index_set_id = ?`, indexSetID); err != nil {
			t.Fatal(err)
		}
		_, _ = fmt.Fprintln(os.Stdout, "committed")
		select {}
	}

	env, dbPath := seedSQLiteSnapshotEnv(t)
	cmd := exec.Command(os.Args[0], "-test.run=^TestSQLiteWriteTargetCrashLeavesCanonicalRecoveryState$") // #nosec G204 -- the current test binary and fixed test selector are intentional.
	cmd.Env = append(os.Environ(),
		"GONIMBUS_SQLITE_WRITE_CRASH_HELPER=1",
		"GONIMBUS_SQLITE_WRITE_SEGMENT_ROOT="+env.segmentRoot,
		"GONIMBUS_SQLITE_WRITE_INDEX_SET_ID="+env.indexSetID,
		"GONIMBUS_SQLITE_WRITE_DB="+dbPath,
	)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	scanner := bufio.NewScanner(stdout)
	require.True(t, scanner.Scan())
	require.Equal(t, "committed", scanner.Text())
	require.NoError(t, cmd.Process.Kill())
	require.Error(t, cmd.Wait())

	require.FileExists(t, dbPath+"-wal")
	require.FileExists(t, dbPath+"-shm")
	aliases, err := filepath.Glob(filepath.Join(env.identityDir, ".gonimbus-index-write-*.db*"))
	require.NoError(t, err)
	require.Empty(t, aliases, "a crash must not leave an alternate database or WAL namespace")

	snapshot, err := OpenSQLiteSnapshot(context.Background(), SQLiteSnapshotOptions{
		Path: dbPath, SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID,
	})
	require.ErrorContains(t, err, "transaction sidecars")
	require.Nil(t, snapshot)
	authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "second-writer")
	require.NoError(t, err)
	second, err := OpenSQLiteWriteTarget(context.Background(), SQLiteWriteTargetOptions{
		Path: dbPath, IdentityPath: filepath.Join(env.identityDir, "identity.json"),
		SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID, Authority: authority,
	})
	require.ErrorContains(t, err, "transaction sidecars")
	require.Nil(t, second)
	require.NoError(t, authority.Release())

	// Explicit recovery uses the same canonical name, observes the committed
	// row, checkpoints it, and converges without any hidden alias state.
	recovery, err := indexstore.Open(context.Background(), indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	var providerName string
	require.NoError(t, recovery.QueryRowContext(context.Background(), `SELECT provider FROM index_sets WHERE index_set_id = ?`, env.indexSetID).Scan(&providerName))
	require.Equal(t, "crash-committed", providerName)
	_, err = recovery.ExecContext(context.Background(), `PRAGMA wal_checkpoint(TRUNCATE)`)
	require.NoError(t, err)
	require.NoError(t, recovery.Close())
	require.NoFileExists(t, dbPath+"-wal")
	require.NoFileExists(t, dbPath+"-shm")

	snapshot, err = OpenSQLiteSnapshot(context.Background(), SQLiteSnapshotOptions{
		Path: dbPath, SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID,
	})
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
}

func setSQLiteWriteTargetBusyTimeout(db *sql.DB) error {
	_, err := db.ExecContext(context.Background(), "PRAGMA busy_timeout=50")
	return err
}

func TestOpenSQLiteWriteTargetRejectsPathReplacementWithoutTouchingSubstitute(t *testing.T) {
	t.Run("existing target", func(t *testing.T) {
		env, dbPath := seedSQLiteSnapshotEnv(t)
		authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "replace-existing")
		require.NoError(t, err)
		defer func() { require.NoError(t, authority.Release()) }()
		before, err := os.ReadFile(dbPath)
		require.NoError(t, err)
		movedPath := filepath.Join(t.TempDir(), "validated-index.db")
		outsidePath := filepath.Join(t.TempDir(), "substitute.db")
		require.NoError(t, os.WriteFile(outsidePath, []byte("outside-substitute\n"), 0o640))
		outsideBefore, err := os.ReadFile(outsidePath)
		require.NoError(t, err)
		outsideInfo, err := os.Stat(outsidePath)
		require.NoError(t, err)
		replacementPerformed := false

		target, err := openSQLiteWriteTarget(context.Background(), SQLiteWriteTargetOptions{
			Path: dbPath, IdentityPath: filepath.Join(env.identityDir, "identity.json"),
			SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID, Authority: authority,
		}, func() error {
			if err := os.Rename(dbPath, movedPath); err != nil {
				return err
			}
			replacementPerformed = true
			return os.Symlink(outsidePath, dbPath)
		})
		require.ErrorIs(t, err, ErrCanonicalSQLiteAdoption)
		require.Nil(t, target)
		if replacementPerformed {
			require.Equal(t, before, mustReadSQLiteWriteTargetTestFile(t, movedPath))
		} else {
			require.Equal(t, before, mustReadSQLiteWriteTargetTestFile(t, dbPath))
			require.NoFileExists(t, movedPath)
		}
		require.Equal(t, outsideBefore, mustReadSQLiteWriteTargetTestFile(t, outsidePath))
		outsideAfter, err := os.Stat(outsidePath)
		require.NoError(t, err)
		require.Equal(t, outsideInfo.Mode(), outsideAfter.Mode())
		require.Equal(t, outsideInfo.ModTime(), outsideAfter.ModTime())
		require.NoFileExists(t, outsidePath+"-wal")
		require.NoFileExists(t, outsidePath+"-shm")
		aliases, err := filepath.Glob(filepath.Join(env.identityDir, ".gonimbus-index-write-*.db*"))
		require.NoError(t, err)
		require.Empty(t, aliases)
	})

	t.Run("atomically reserved absent target", func(t *testing.T) {
		env := setupDurableTestEnv(t, nil)
		dbPath := filepath.Join(env.identityDir, "index.db")
		require.NoFileExists(t, dbPath)
		authority, err := indexcoord.Acquire(context.Background(), env.segmentRoot, env.indexSetID, "replace-reserved")
		require.NoError(t, err)
		defer func() { require.NoError(t, authority.Release()) }()
		movedPath := filepath.Join(t.TempDir(), "reserved-index.db")
		outsidePath := filepath.Join(t.TempDir(), "substitute.db")
		require.NoError(t, os.WriteFile(outsidePath, []byte("outside-substitute\n"), 0o640))
		outsideBefore, err := os.ReadFile(outsidePath)
		require.NoError(t, err)
		outsideInfo, err := os.Stat(outsidePath)
		require.NoError(t, err)
		replacementPerformed := false

		target, err := openSQLiteWriteTarget(context.Background(), SQLiteWriteTargetOptions{
			Path: dbPath, IdentityPath: filepath.Join(env.identityDir, "identity.json"),
			SegmentSetRoot: env.segmentRoot, IndexSetID: env.indexSetID, Authority: authority,
		}, func() error {
			if err := os.Rename(dbPath, movedPath); err != nil {
				return err
			}
			replacementPerformed = true
			return os.Symlink(outsidePath, dbPath)
		})
		require.ErrorIs(t, err, ErrCanonicalSQLiteAdoption)
		require.Nil(t, target)
		if replacementPerformed {
			require.Empty(t, mustReadSQLiteWriteTargetTestFile(t, movedPath), "the reserved inode must not be opened or initialized")
		} else {
			require.NoFileExists(t, dbPath, "a denied interposition must still abandon the untouched reservation")
			require.NoFileExists(t, movedPath)
		}
		require.Equal(t, outsideBefore, mustReadSQLiteWriteTargetTestFile(t, outsidePath))
		outsideAfter, err := os.Stat(outsidePath)
		require.NoError(t, err)
		require.Equal(t, outsideInfo.Mode(), outsideAfter.Mode())
		require.Equal(t, outsideInfo.ModTime(), outsideAfter.ModTime())
		require.NoFileExists(t, outsidePath+"-wal")
		require.NoFileExists(t, outsidePath+"-shm")
		aliases, err := filepath.Glob(filepath.Join(env.identityDir, ".gonimbus-index-write-*.db*"))
		require.NoError(t, err)
		require.Empty(t, aliases)
	})
}

func mustReadSQLiteWriteTargetTestFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
