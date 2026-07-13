//go:build windows

package indexstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalSQLiteVFSRejectsHandleReuseAndUnrelatedMatchingOpen(t *testing.T) {
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

	db, err = openLocalMutableCanonical(context.Background(), canonical, bound, func() error { return nil }, mutableCanonicalOpenHooks{})
	require.NoError(t, err)
	require.NoError(t, db.Close())
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
	wal, err := os.ReadFile(substitute + "-wal")
	require.NoError(t, err)
	shm, err := os.ReadFile(substitute + "-shm")
	require.NoError(t, err)
	canonicalBefore, err := os.ReadFile(canonical)
	require.NoError(t, err)
	substituteBefore, err := os.ReadFile(substitute)
	require.NoError(t, err)

	bound, err := os.OpenFile(canonical, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = bound.Close() }()
	hooks := mutableCanonicalOpenHooks{beforeDriverOpen: func() error {
		if err := os.WriteFile(canonical+"-wal", wal, 0o600); err != nil {
			return err
		}
		return os.WriteFile(canonical+"-shm", shm, 0o600)
	}}
	db, err := openLocalMutableCanonical(context.Background(), canonical, bound, func() error { return nil }, hooks)
	require.ErrorContains(t, err, "transaction sidecars appeared after validation")
	require.Nil(t, db)
	require.Equal(t, canonicalBefore, mustReadMutableCanonicalWindowsFile(t, canonical))
	require.Equal(t, substituteBefore, mustReadMutableCanonicalWindowsFile(t, substitute))
	require.Equal(t, wal, mustReadMutableCanonicalWindowsFile(t, substitute+"-wal"))
	require.Equal(t, shm, mustReadMutableCanonicalWindowsFile(t, substitute+"-shm"))
	require.Equal(t, wal, mustReadMutableCanonicalWindowsFile(t, canonical+"-wal"))
	require.Equal(t, shm, mustReadMutableCanonicalWindowsFile(t, canonical+"-shm"))
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
}

func mustReadMutableCanonicalWindowsFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
