//go:build !windows

package indexstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestOpenLocalMutableCanonicalRejectsDriverConnectionABAWithoutRecovery(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "index.db")
	substitute := filepath.Join(dir, "substitute.db")
	seedMutableCanonicalTestDB(t, canonical, "canonical")
	seedMutableCanonicalTestDB(t, substitute, "substitute")
	hot, err := Open(context.Background(), Config{Path: substitute})
	require.NoError(t, err)
	_, err = hot.ExecContext(context.Background(), `UPDATE marker SET value = 'hot-substitute'`)
	require.NoError(t, err)
	defer func() { _ = hot.Close() }()
	require.FileExists(t, substitute+"-wal")
	require.FileExists(t, substitute+"-shm")
	canonicalBefore := mutableCanonicalFileState(t, canonical)
	substituteBefore := mutableCanonicalFileState(t, substitute)
	walBefore := mutableCanonicalFileState(t, substitute+"-wal")
	shmBefore := mutableCanonicalFileState(t, substitute+"-shm")

	bound, err := os.OpenFile(canonical, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = bound.Close() }()
	parked := filepath.Join(dir, "canonical.parked")
	hooks := mutableCanonicalOpenHooks{
		afterVFSRegistrationBeforeDriverOpen: func() error {
			if err := os.Rename(canonical, parked); err != nil {
				return err
			}
			if err := os.Rename(substitute, canonical); err != nil {
				return err
			}
			if err := os.Rename(substitute+"-wal", canonical+"-wal"); err != nil {
				return err
			}
			return os.Rename(substitute+"-shm", canonical+"-shm")
		},
		afterDriverOpen: func() error {
			if err := os.Rename(canonical+"-wal", substitute+"-wal"); err != nil {
				return err
			}
			if err := os.Rename(canonical+"-shm", substitute+"-shm"); err != nil {
				return err
			}
			if err := os.Rename(canonical, substitute); err != nil {
				return err
			}
			return os.Rename(parked, canonical)
		},
	}
	db, err := openLocalMutableCanonical(context.Background(), canonical, bound, func() error { return nil }, hooks)
	require.ErrorContains(t, err, "not bound to retained index.db")
	require.Nil(t, db)
	mutableCanonicalRequireFileState(t, canonical, canonicalBefore)
	mutableCanonicalRequireFileState(t, substitute, substituteBefore)
	mutableCanonicalRequireFileState(t, substitute+"-wal", walBefore)
	mutableCanonicalRequireFileState(t, substitute+"-shm", shmBefore)
	require.NoFileExists(t, canonical+"-wal")
	require.NoFileExists(t, canonical+"-shm")
}

func TestOpenLocalMutableCanonicalRejectsDescriptorReuseAndUnrelatedMatchingOpen(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "index.db")
	substitute := filepath.Join(dir, "substitute.db")
	seedMutableCanonicalTestDB(t, canonical, "canonical")
	seedMutableCanonicalTestDB(t, substitute, "substitute")
	bound, err := os.OpenFile(canonical, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = bound.Close() }()
	filler, err := os.OpenFile(substitute, os.O_RDONLY, 0)
	require.NoError(t, err)
	var unrelatedMatch *os.File
	hooks := mutableCanonicalOpenHooks{
		beforeDriverOpen: filler.Close,
		afterDriverOpen: func() error {
			var openErr error
			unrelatedMatch, openErr = os.OpenFile(canonical, os.O_RDWR, 0)
			return openErr
		},
	}

	db, err := openLocalMutableCanonical(context.Background(), substitute, bound, func() error { return nil }, hooks)
	require.ErrorContains(t, err, "exact SQLite driver connection is not bound to retained index.db")
	require.Nil(t, db)
	require.NotNil(t, unrelatedMatch)
	require.NoError(t, unrelatedMatch.Close())
}

func TestOpenLocalMutableCanonicalGuardsFirstSQLiteBoundaryAfterAuthorityCheck(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "index.db")
	substitute := filepath.Join(dir, "substitute.db")
	seedMutableCanonicalTestDB(t, canonical, "canonical")
	seedMutableCanonicalTestDB(t, substitute, "substitute")
	hot, err := Open(context.Background(), Config{Path: substitute})
	require.NoError(t, err)
	_, err = hot.ExecContext(context.Background(), `UPDATE marker SET value = 'hot-substitute'`)
	require.NoError(t, err)
	defer func() { _ = hot.Close() }()
	require.FileExists(t, substitute+"-wal")
	require.FileExists(t, substitute+"-shm")
	canonicalBefore := mutableCanonicalFileState(t, canonical)
	substituteBefore := mutableCanonicalFileState(t, substitute)
	walBefore := mutableCanonicalFileState(t, substitute+"-wal")
	shmBefore := mutableCanonicalFileState(t, substitute+"-shm")

	bound, err := os.OpenFile(canonical, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = bound.Close() }()
	parked := filepath.Join(dir, "canonical.parked")
	hooks := mutableCanonicalOpenHooks{afterAuthorityCheckBeforeSQLite: func() error {
		if err := os.Rename(canonical, parked); err != nil {
			return err
		}
		if err := os.Rename(substitute, canonical); err != nil {
			return err
		}
		if err := os.Rename(substitute+"-wal", canonical+"-wal"); err != nil {
			return err
		}
		return os.Rename(substitute+"-shm", canonical+"-shm")
	}}
	db, err := openLocalMutableCanonical(context.Background(), canonical, bound, func() error { return nil }, hooks)
	require.ErrorContains(t, err, "main-file binding")
	require.Nil(t, db)

	// Restore names only after the VFS has refused its first read boundary.
	// Exact byte/mode/mtime equality proves SQLite never opened or recovered
	// the substituted hot WAL/SHM epoch.
	require.NoError(t, os.Rename(canonical+"-wal", substitute+"-wal"))
	require.NoError(t, os.Rename(canonical+"-shm", substitute+"-shm"))
	require.NoError(t, os.Rename(canonical, substitute))
	require.NoError(t, os.Rename(parked, canonical))
	mutableCanonicalRequireFileState(t, canonical, canonicalBefore)
	mutableCanonicalRequireFileState(t, substitute, substituteBefore)
	mutableCanonicalRequireFileState(t, substitute+"-wal", walBefore)
	mutableCanonicalRequireFileState(t, substitute+"-shm", shmBefore)
	require.NoFileExists(t, canonical+"-wal")
	require.NoFileExists(t, canonical+"-shm")
}

func TestOpenLocalMutableCanonicalRevalidatesAfterConnectionAttestation(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "index.db")
	substitute := filepath.Join(dir, "substitute.db")
	seedMutableCanonicalTestDB(t, canonical, "canonical")
	seedMutableCanonicalTestDB(t, substitute, "substitute")
	canonicalBefore := mutableCanonicalFileState(t, canonical)
	substituteBefore := mutableCanonicalFileState(t, substitute)
	bound, err := os.OpenFile(canonical, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = bound.Close() }()
	boundInfo, err := bound.Stat()
	require.NoError(t, err)
	parked := filepath.Join(dir, "canonical.parked")
	hooks := mutableCanonicalOpenHooks{afterConnectionAttestation: func() error {
		if err := os.Rename(canonical, parked); err != nil {
			return err
		}
		return os.Rename(substitute, canonical)
	}}
	db, err := openLocalMutableCanonical(context.Background(), canonical, bound, func() error {
		named, statErr := os.Lstat(canonical)
		if statErr != nil || !named.Mode().IsRegular() || !os.SameFile(boundInfo, named) {
			return fmt.Errorf("canonical path changed after connection attestation")
		}
		return nil
	}, hooks)
	require.ErrorContains(t, err, "changed after connection attestation")
	require.Nil(t, db)
	require.NoError(t, os.Rename(canonical, substitute))
	require.NoError(t, os.Rename(parked, canonical))
	mutableCanonicalRequireFileState(t, canonical, canonicalBefore)
	mutableCanonicalRequireFileState(t, substitute, substituteBefore)
	require.NoFileExists(t, canonical+"-wal")
	require.NoFileExists(t, canonical+"-shm")
	require.NoFileExists(t, substitute+"-wal")
	require.NoFileExists(t, substitute+"-shm")
}

func TestOpenLocalMutableCanonicalRejectsSidecarsAppearingBeforeVFSCapture(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "index.db")
	substitute := filepath.Join(dir, "substitute.db")
	seedMutableCanonicalTestDB(t, canonical, "canonical")
	seedMutableCanonicalTestDB(t, substitute, "substitute")
	hot, err := Open(context.Background(), Config{Path: substitute})
	require.NoError(t, err)
	_, err = hot.ExecContext(context.Background(), `UPDATE marker SET value = 'hot-substitute'`)
	require.NoError(t, err)
	defer func() { _ = hot.Close() }()
	require.FileExists(t, substitute+"-wal")
	require.FileExists(t, substitute+"-shm")
	canonicalBefore := mutableCanonicalFileState(t, canonical)
	substituteBefore := mutableCanonicalFileState(t, substitute)
	walBefore := mutableCanonicalFileState(t, substitute+"-wal")
	shmBefore := mutableCanonicalFileState(t, substitute+"-shm")

	bound, err := os.OpenFile(canonical, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = bound.Close() }()
	hooks := mutableCanonicalOpenHooks{beforeDriverOpen: func() error {
		if err := os.Rename(substitute+"-wal", canonical+"-wal"); err != nil {
			return err
		}
		return os.Rename(substitute+"-shm", canonical+"-shm")
	}}

	db, err := openLocalMutableCanonical(context.Background(), canonical, bound, func() error { return nil }, hooks)
	require.ErrorContains(t, err, "transaction sidecars appeared after validation")
	require.Nil(t, db)

	// Registration must refuse the injected hot epoch before the base VFS can
	// open or recover it. Restore only after refusal, then prove every artifact
	// remains byte-, mode-, and mtime-identical.
	require.NoError(t, os.Rename(canonical+"-wal", substitute+"-wal"))
	require.NoError(t, os.Rename(canonical+"-shm", substitute+"-shm"))
	mutableCanonicalRequireFileState(t, canonical, canonicalBefore)
	mutableCanonicalRequireFileState(t, substitute, substituteBefore)
	mutableCanonicalRequireFileState(t, substitute+"-wal", walBefore)
	mutableCanonicalRequireFileState(t, substitute+"-shm", shmBefore)
	require.NoFileExists(t, canonical+"-wal")
	require.NoFileExists(t, canonical+"-shm")
}

func TestOpenLocalMutableCanonicalRejectsEverySidecarClassAtVFSCapture(t *testing.T) {
	for _, suffix := range []string{"-wal", "-shm", "-journal", "-mj test", "-stmtjrnl-test"} {
		t.Run(suffix, func(t *testing.T) {
			dir := t.TempDir()
			canonical := filepath.Join(dir, "index.db")
			seedMutableCanonicalTestDB(t, canonical, "canonical")
			bound, err := os.OpenFile(canonical, os.O_RDWR, 0)
			require.NoError(t, err)
			defer func() { _ = bound.Close() }()
			sidecar := canonical + suffix
			data := []byte("untrusted transaction epoch")
			hooks := mutableCanonicalOpenHooks{beforeDriverOpen: func() error {
				return os.WriteFile(sidecar, data, 0o600)
			}}

			db, err := openLocalMutableCanonical(context.Background(), canonical, bound, func() error { return nil }, hooks)
			require.ErrorContains(t, err, "transaction sidecars appeared after validation")
			require.Nil(t, db)
			require.Equal(t, data, mutableCanonicalReadFile(t, sidecar))
		})
	}
}

func TestCanonicalSQLiteVFSRefusedOpenClosesBaseFileAndDropsBookkeeping(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "index.db")
	substitute := filepath.Join(dir, "substitute.db")
	seedMutableCanonicalTestDB(t, canonical, "canonical")
	seedMutableCanonicalTestDB(t, substitute, "substitute")
	bound, err := os.OpenFile(canonical, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = bound.Close() }()
	baselineDescriptors := mutableCanonicalCountBoundDescriptors(t, bound)
	baselineVFSes, baselineFiles := mutableCanonicalVFSBookkeepingCounts()

	tests := []struct {
		name           string
		path           string
		beforeMutation func() error
		hooks          mutableCanonicalOpenHooks
	}{
		{
			name:           "post-base-open-hook",
			path:           canonical,
			beforeMutation: func() error { return nil },
			hooks: mutableCanonicalOpenHooks{afterDriverOpen: func() error {
				return fmt.Errorf("injected post-open refusal")
			}},
		},
		{
			name:           "main-file-attestation",
			path:           substitute,
			beforeMutation: func() error { return nil },
		},
		{
			name:           "post-open-authority-guard",
			path:           canonical,
			beforeMutation: func() error { return fmt.Errorf("injected authority refusal") },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for range 16 {
				db, openErr := openLocalMutableCanonical(context.Background(), tc.path, bound, tc.beforeMutation, tc.hooks)
				require.Error(t, openErr)
				require.Nil(t, db)
			}
			require.Equal(t, baselineDescriptors, mutableCanonicalCountBoundDescriptors(t, bound))
			vfses, files := mutableCanonicalVFSBookkeepingCounts()
			require.Equal(t, baselineVFSes, vfses)
			require.Equal(t, baselineFiles, files)
		})
	}
}

type mutableCanonicalState struct {
	data  []byte
	mode  os.FileMode
	mtime int64
}

func seedMutableCanonicalTestDB(t *testing.T, path, marker string) {
	t.Helper()
	db, err := Open(context.Background(), Config{Path: path})
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `CREATE TABLE marker (value TEXT NOT NULL)`)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `INSERT INTO marker(value) VALUES (?)`, marker)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.NoFileExists(t, path+"-wal")
	require.NoFileExists(t, path+"-shm")
}

func mutableCanonicalFileState(t *testing.T, path string) mutableCanonicalState {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return mutableCanonicalState{data: mutableCanonicalReadFile(t, path), mode: info.Mode(), mtime: info.ModTime().UnixNano()}
}

func mutableCanonicalRequireFileState(t *testing.T, path string, want mutableCanonicalState) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, want.data, mutableCanonicalReadFile(t, path))
	require.Equal(t, want.mode, info.Mode())
	require.Equal(t, want.mtime, info.ModTime().UnixNano())
}

func mutableCanonicalReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func mutableCanonicalCountBoundDescriptors(t *testing.T, bound *os.File) int {
	t.Helper()
	var wanted unix.Stat_t
	require.NoError(t, unix.Fstat(int(bound.Fd()), &wanted))
	count := 0
	// Failed opens allocate from the process's low descriptor range. Scanning a
	// fixed, generous bound avoids /dev/fd enumeration races on macOS while
	// still detecting every descriptor created by this repeated fixture.
	for fd := range 4096 {
		var info unix.Stat_t
		if unix.Fstat(fd, &info) == nil && info.Dev == wanted.Dev && info.Ino == wanted.Ino {
			count++
		}
	}
	return count
}

func mutableCanonicalVFSBookkeepingCounts() (vfses, files int) {
	canonicalSQLiteVFSMu.RLock()
	defer canonicalSQLiteVFSMu.RUnlock()
	return len(canonicalSQLiteVFSes), len(canonicalSQLiteVFSFiles)
}
