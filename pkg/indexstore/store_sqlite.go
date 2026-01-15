//go:build !cgo

package indexstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	sqlite "modernc.org/sqlite"
)

const driverLibsql = "libsql"

func init() {
	sql.Register(driverLibsql, &sqlite.Driver{})
}

// Open opens (and creates if needed) a SQLite-backed index database.
//
// Notes:
// - Local file paths are created if parent directories do not exist.
// - For local DBs, WAL and busy_timeout are applied for predictable CLI behavior.
// - Remote libsql URLs require a cgo-enabled build.
func Open(ctx context.Context, cfg Config) (*sql.DB, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(dsn, "libsql://") || strings.HasPrefix(dsn, "https://") {
		return nil, errors.New("libsql URL requires cgo-enabled build")
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
