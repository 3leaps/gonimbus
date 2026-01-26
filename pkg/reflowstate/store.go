package reflowstate

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

const schemaVersion = 1

type Store struct {
	db *sql.DB
}

type Config struct {
	Path string
}

func Open(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("reflow state path is required")
	}
	// Reuse indexstore's local SQLite configuration (WAL, busy_timeout, single conn).
	db, err := indexstore.Open(ctx, indexstore.Config{Path: cfg.Path})
	if err != nil {
		return nil, err
	}

	s := &Store{db: db}
	if err := s.ensureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) ensureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS reflow_meta (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			schema_version INTEGER NOT NULL,
			created_at TEXT NOT NULL
		);`,
		`INSERT OR IGNORE INTO reflow_meta (id, schema_version, created_at) VALUES (1, ?, ?);`,
		`CREATE TABLE IF NOT EXISTS reflow_items (
			source_uri TEXT NOT NULL,
			dest_uri TEXT NOT NULL,
			source_key TEXT,
			dest_key TEXT,
			source_etag TEXT,
			source_size_bytes INTEGER,
			status TEXT NOT NULL,
			bytes INTEGER,
			reason TEXT,
			error_code TEXT,
			error_message TEXT,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (source_uri, dest_uri)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_reflow_items_dest_key ON reflow_items(dest_key);`,
		`CREATE TABLE IF NOT EXISTS reflow_dest_key_sources (
			dest_key TEXT NOT NULL,
			source_uri TEXT NOT NULL,
			source_etag TEXT,
			source_size_bytes INTEGER,
			first_seen_at TEXT NOT NULL,
			last_seen_at TEXT NOT NULL,
			seen_count INTEGER NOT NULL,
			PRIMARY KEY (dest_key, source_uri)
		);`,
		`CREATE TABLE IF NOT EXISTS reflow_collisions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			dest_key TEXT NOT NULL,
			kind TEXT NOT NULL,
			source_uri TEXT NOT NULL,
			source_etag TEXT,
			source_size_bytes INTEGER,
			dest_etag TEXT,
			dest_size_bytes INTEGER,
			noted_at TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_reflow_collisions_dest_key ON reflow_collisions(dest_key);`,
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for i, stmt := range stmts {
		if i == 1 {
			if _, err := s.db.ExecContext(ctx, stmt, schemaVersion, now); err != nil {
				return fmt.Errorf("init schema meta: %w", err)
			}
			continue
		}
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}
	return nil
}

func (s *Store) ItemDone(ctx context.Context, sourceURI, destURI string) (bool, string, error) {
	var status string
	err := s.db.QueryRowContext(ctx, `SELECT status FROM reflow_items WHERE source_uri = ? AND dest_uri = ?`, sourceURI, destURI).Scan(&status)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, "", nil
		}
		return false, "", err
	}
	switch status {
	case "complete", "skipped":
		return true, status, nil
	default:
		return false, status, nil
	}
}

type UpsertItemParams struct {
	SourceURI      string
	DestURI        string
	SourceKey      string
	DestKey        string
	SourceETag     string
	SourceSize     int64
	Status         string
	Bytes          int64
	Reason         string
	ErrorCode      string
	ErrorMessage   string
	UpdatedAtRFC33 string
}

func (s *Store) UpsertItem(ctx context.Context, p UpsertItemParams) error {
	if p.UpdatedAtRFC33 == "" {
		p.UpdatedAtRFC33 = time.Now().UTC().Format(time.RFC3339Nano)
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO reflow_items (
			source_uri, dest_uri, source_key, dest_key, source_etag, source_size_bytes, status, bytes, reason, error_code, error_message, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_uri, dest_uri) DO UPDATE SET
			source_key=excluded.source_key,
			dest_key=excluded.dest_key,
			source_etag=excluded.source_etag,
			source_size_bytes=excluded.source_size_bytes,
			status=excluded.status,
			bytes=excluded.bytes,
			reason=excluded.reason,
			error_code=excluded.error_code,
			error_message=excluded.error_message,
			updated_at=excluded.updated_at
	`,
		p.SourceURI, p.DestURI, p.SourceKey, p.DestKey, p.SourceETag, p.SourceSize, p.Status, p.Bytes, p.Reason, p.ErrorCode, p.ErrorMessage, p.UpdatedAtRFC33,
	)
	return err
}

func (s *Store) NoteDestKeySource(ctx context.Context, destKey, sourceURI, sourceETag string, sourceSize int64) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO reflow_dest_key_sources (dest_key, source_uri, source_etag, source_size_bytes, first_seen_at, last_seen_at, seen_count)
		VALUES (?, ?, ?, ?, ?, ?, 1)
		ON CONFLICT(dest_key, source_uri) DO UPDATE SET
			source_etag=excluded.source_etag,
			source_size_bytes=excluded.source_size_bytes,
			last_seen_at=excluded.last_seen_at,
			seen_count=reflow_dest_key_sources.seen_count + 1
	`, destKey, sourceURI, sourceETag, sourceSize, now, now)
	return err
}

type CollisionKind string

const (
	CollisionDuplicate CollisionKind = "duplicate"
	CollisionConflict  CollisionKind = "conflict"
	CollisionOverwrite CollisionKind = "overwrite"
)

func (s *Store) NoteCollision(ctx context.Context, destKey string, kind CollisionKind, sourceURI, sourceETag string, sourceSize int64, destETag string, destSize int64) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO reflow_collisions (dest_key, kind, source_uri, source_etag, source_size_bytes, dest_etag, dest_size_bytes, noted_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, destKey, string(kind), sourceURI, sourceETag, sourceSize, destETag, destSize, now)
	return err
}
