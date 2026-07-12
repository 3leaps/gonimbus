//go:build windows && !gonimbus_libsql

package indexstore

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func TestOpenLocalReadOnlyWindowsDeniesReplacementWhileBinding(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.db")
	substitute := filepath.Join(dir, "substitute.db")
	writeSQLiteWindowsBindingFixture(t, path, "verified")
	writeSQLiteWindowsBindingFixture(t, substitute, "substitute")
	originalBefore := snapshotSQLiteWindowsFile(t, path)
	substituteBefore := snapshotSQLiteWindowsFile(t, substitute)
	saved := filepath.Join(dir, "saved.db")
	var replacementErr error

	db, err := openLocalReadOnly(context.Background(), path, func() error {
		// Renaming the bound file is the first step of pathname substitution.
		// The restrictive FILE_SHARE_READ handle must deny DELETE access here.
		replacementErr = os.Rename(path, saved)
		if replacementErr == nil {
			_ = os.Rename(saved, path)
		}
		return nil
	})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.Error(t, replacementErr, "Windows must deny replacement while the verified handle is bound")
	require.NoFileExists(t, saved)

	var marker string
	require.NoError(t, db.QueryRowContext(context.Background(), `SELECT value FROM binding_marker`).Scan(&marker))
	require.Equal(t, "verified", marker)
	require.Equal(t, originalBefore, snapshotSQLiteWindowsFile(t, path))
	require.Equal(t, substituteBefore, snapshotSQLiteWindowsFile(t, substitute))
}

func TestOpenLocalReadOnlyWindowsRejectsReparsePoint(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	require.NoError(t, os.Mkdir(target, 0o700))
	targetDB := filepath.Join(target, "index.db")
	writeSQLiteWindowsBindingFixture(t, targetDB, "outside")
	targetBefore := snapshotSQLiteWindowsFile(t, targetDB)
	junction := filepath.Join(dir, "index.db")
	output, err := exec.Command("cmd", "/c", "mklink", "/J", junction, target).CombinedOutput()
	require.NoErrorf(t, err, "create native directory junction: %s", output)
	junctionPtr, err := windows.UTF16PtrFromString(junction)
	require.NoError(t, err)
	attributes, err := windows.GetFileAttributes(junctionPtr)
	require.NoError(t, err)
	require.NotZero(t, attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT, "junction must be a native reparse point")

	db, err := OpenLocalReadOnly(context.Background(), junction)
	require.Error(t, err)
	require.Nil(t, db)
	require.Equal(t, targetBefore, snapshotSQLiteWindowsFile(t, targetDB))
}

type sqliteWindowsFileState struct {
	Mode    os.FileMode
	Size    int64
	ModTime time.Time
	Content string
}

func snapshotSQLiteWindowsFile(t *testing.T, path string) sqliteWindowsFileState {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return sqliteWindowsFileState{Mode: info.Mode(), Size: info.Size(), ModTime: info.ModTime(), Content: string(data)}
}

func writeSQLiteWindowsBindingFixture(t *testing.T, path, marker string) {
	t.Helper()
	db, err := sql.Open(driverLibsql, path)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `CREATE TABLE binding_marker (value TEXT NOT NULL)`)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `INSERT INTO binding_marker (value) VALUES (?)`, marker)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}
