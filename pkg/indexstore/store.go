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

	_ "github.com/tursodatabase/go-libsql"
)

const driverLibsql = "libsql"

type Config struct {
	// Path is a local filesystem path to the index database.
	// If set, it is converted into a libsql-compatible DSN (file:<path>).
	Path string

	// URL is a libsql/Turso URL, e.g. libsql://your-db.turso.io.
	URL string

	// AuthToken is appended to URL-based DSNs as authToken=... when not already present.
	AuthToken string
}

// Open opens (and creates if needed) a libsql-backed index database.
//
// Notes:
// - Local file paths are created if parent directories do not exist.
// - For local DBs, WAL and busy_timeout are applied for predictable CLI behavior.
func Open(ctx context.Context, cfg Config) (*sql.DB, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(driverLibsql, dsn)
	if err != nil {
		return nil, fmt.Errorf("open index store: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping index store: %w", err)
	}

	if err := configureLocalSQLite(ctx, db, dsn); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
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

	// Best-effort: if PRAGMA is unsupported by a backend, treat it as non-fatal.
	var journalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journalMode); err != nil {
		return fmt.Errorf("enable WAL mode: %w", err)
	}
	var busyTimeout int
	if err := db.QueryRowContext(ctx, "PRAGMA busy_timeout=5000").Scan(&busyTimeout); err != nil {
		return fmt.Errorf("set busy timeout: %w", err)
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

	// #nosec G301 -- data directories use 0755 for multi-user access compatibility
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create store directory: %w", err)
	}
	return nil
}
