//go:build cgo

package indexstore

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/tursodatabase/go-libsql"
)

const driverLibsql = "libsql"

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
