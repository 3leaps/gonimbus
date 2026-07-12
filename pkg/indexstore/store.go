package indexstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Config struct {
	// Path is a local filesystem path to the index database.
	// If set, it is converted into a libsql-compatible DSN (file:<path>).
	Path string

	// URL is a libsql/Turso URL, e.g. libsql://your-db.turso.io.
	URL string

	// AuthToken is appended to URL-based DSNs as authToken=... when not already present.
	AuthToken string
}

// OpenLocalReadOnly opens an existing local SQLite database without creating
// parent directories, changing journal mode, running migrations, or creating
// SQLite sidecars. The connection is bound to a no-follow file handle before
// SQLite sees it, so a pathname swap cannot substitute unverified metadata.
// Callers must reject transaction sidecars before and after this inspection.
func OpenLocalReadOnly(ctx context.Context, path string) (*sql.DB, error) {
	return openLocalReadOnly(ctx, path, nil)
}

func openLocalReadOnly(ctx context.Context, path string, afterBind func() error) (*sql.DB, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	path = strings.TrimSpace(path)
	if path == "" || path == ":memory:" || strings.HasPrefix(path, "file:") {
		return nil, errors.New("existing local index store path is required")
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("resolve index store path: %w", err)
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return nil, fmt.Errorf("inspect index store: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("index store must be an existing regular file")
	}
	bound, driverPath, err := openBoundSQLiteSnapshotFile(abs)
	if err != nil {
		return nil, fmt.Errorf("bind read-only index store: %w", err)
	}
	defer func() { _ = bound.Close() }()
	if afterBind != nil {
		if err := afterBind(); err != nil {
			return nil, fmt.Errorf("after binding read-only index store: %w", err)
		}
	}

	dsnURL := &url.URL{Scheme: "file", Path: filepath.ToSlash(driverPath)}
	query := dsnURL.Query()
	query.Set("mode", "ro")
	query.Set("immutable", "1")
	dsnURL.RawQuery = query.Encode()

	db, err := sql.Open(driverLibsql, dsnURL.String())
	if err != nil {
		return nil, fmt.Errorf("open read-only index store: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping read-only index store: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA query_only=ON"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enforce read-only index store: %w", err)
	}
	return db, nil
}

// ValidateCurrentSchemaReadOnly verifies that db has exactly the schema this
// binary understands. It never upgrades older schemas; callers must retain
// those artifacts until an explicit migration operation is authorized.
func ValidateCurrentSchemaReadOnly(ctx context.Context, db *sql.DB) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		return errors.New("db is nil")
	}
	var version int
	if err := db.QueryRowContext(ctx, `SELECT schema_version FROM schema_meta WHERE id=1`).Scan(&version); err != nil {
		return fmt.Errorf("read schema_version without migration: %w", err)
	}
	if version != SchemaVersion {
		return fmt.Errorf("index schema version %d is not current version %d", version, SchemaVersion)
	}
	return nil
}

func buildDSN(cfg Config) (string, error) {
	if u := strings.TrimSpace(cfg.URL); u != "" {
		return addAuthToken(u, cfg.AuthToken)
	}

	path := strings.TrimSpace(cfg.Path)
	if path == "" {
		return "", errors.New("index store path or url is required")
	}
	if path == ":memory:" {
		return path, nil
	}

	if strings.HasPrefix(path, "file:") || strings.HasPrefix(path, "libsql:") {
		if strings.HasPrefix(path, "file:") {
			localPath, err := extractFilePath(path)
			if err != nil {
				return "", err
			}
			if err := ensureStoreDir(localPath); err != nil {
				return "", err
			}
		}
		return path, nil
	}

	if err := ensureStoreDir(path); err != nil {
		return "", err
	}

	return "file:" + filepath.Clean(path), nil
}

func addAuthToken(dsn string, token string) (string, error) {
	if strings.TrimSpace(token) == "" {
		return dsn, nil
	}

	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("invalid store url: %w", err)
	}

	query := parsed.Query()
	if query.Get("authToken") == "" {
		query.Set("authToken", token)
		parsed.RawQuery = query.Encode()
	}

	return parsed.String(), nil
}

func extractFilePath(dsn string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("invalid store path: %w", err)
	}

	if parsed.Path != "" {
		return strings.TrimPrefix(parsed.Path, "//"), nil
	}

	return strings.TrimPrefix(parsed.Opaque, "//"), nil
}

func configureLocalSQLite(ctx context.Context, db *sql.DB, dsn string) error {
	if db == nil {
		return errors.New("store connection is nil")
	}
	if dsn == ":memory:" {
		return nil
	}
	if !strings.HasPrefix(dsn, "file:") {
		return nil
	}

	// Keep a single connection and use WAL to reduce lock contention.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var busyTimeout int
	if err := db.QueryRowContext(ctx, "PRAGMA busy_timeout=5000").Scan(&busyTimeout); err != nil {
		return fmt.Errorf("set busy timeout: %w", err)
	}
	var journalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journalMode); err != nil {
		return fmt.Errorf("enable WAL mode: %w", err)
	}

	return nil
}

func ensureStoreDir(path string) error {
	if strings.TrimSpace(path) == "" || path == ":memory:" {
		return nil
	}

	dir := filepath.Dir(filepath.Clean(path))
	if dir == "." || dir == string(filepath.Separator) {
		return nil
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create store directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil { // #nosec G302 -- directories require owner execute permission
		return fmt.Errorf("chmod store directory: %w", err)
	}
	return nil
}
