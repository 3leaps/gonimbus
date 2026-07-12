//go:build !windows && !gonimbus_libsql

package indexstore

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenLocalReadOnlyBindsFileBeforePathnameSwap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.db")
	replacement := filepath.Join(dir, "replacement.db")
	writeSQLiteBindingFixture(t, path, "verified")
	writeSQLiteBindingFixture(t, replacement, "substitute")
	saved := filepath.Join(dir, "verified.db")
	swapped := false
	restore := func() {
		if !swapped {
			return
		}
		require.NoError(t, os.Rename(path, replacement))
		require.NoError(t, os.Rename(saved, path))
		swapped = false
	}
	defer restore()

	db, err := openLocalReadOnly(context.Background(), path, func() error {
		if err := os.Rename(path, saved); err != nil {
			return err
		}
		if err := os.Rename(replacement, path); err != nil {
			_ = os.Rename(saved, path)
			return err
		}
		swapped = true
		return nil
	})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	restore()

	var marker string
	require.NoError(t, db.QueryRowContext(context.Background(), `SELECT value FROM binding_marker`).Scan(&marker))
	require.Equal(t, "verified", marker, "inspection must read the file handle bound before the pathname swap")
}

func writeSQLiteBindingFixture(t *testing.T, path, marker string) {
	t.Helper()
	db, err := sql.Open(driverLibsql, path)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `CREATE TABLE binding_marker (value TEXT NOT NULL)`)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `INSERT INTO binding_marker (value) VALUES (?)`, marker)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}
