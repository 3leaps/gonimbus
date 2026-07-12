package cmd

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// openIndexGCTestDB keeps GC fixture lifecycle semantics independent of the
// optional general-purpose libsql driver selected by the build tag. GC's local
// snapshot contract is always exercised against SQLite's local WAL behavior.
func openIndexGCTestDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))

	uriPath := filepath.ToSlash(path)
	if filepath.VolumeName(path) != "" && !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath
	}
	dsn := (&url.URL{Scheme: "file", Path: uriPath}).String()
	db, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(context.Background()))
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	var busyTimeout int
	require.NoError(t, db.QueryRowContext(context.Background(), "PRAGMA busy_timeout=5000").Scan(&busyTimeout))
	var journalMode string
	require.NoError(t, db.QueryRowContext(context.Background(), "PRAGMA journal_mode=WAL").Scan(&journalMode))
	return db
}
