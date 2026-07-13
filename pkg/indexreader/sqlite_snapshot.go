package indexreader

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

// ErrSQLiteIdentityScope reports that an authority-held database does not
// contain exactly the single index set authorized by the caller.
var ErrSQLiteIdentityScope = errors.New("SQLite snapshot identity/scope mismatch")

// SQLiteSnapshotOptions identifies a strict immutable SQLite read. Canonical
// reads provide SegmentSetRoot and IndexSetID so stable whole-set authority is
// held for the snapshot lifetime. Omitting both is reserved for caller-owned,
// externally quiesced databases.
type SQLiteSnapshotOptions struct {
	Path           string
	SegmentSetRoot string
	IndexSetID     string
	Authority      *indexcoord.Lease
}

// SQLiteSnapshot holds a sidecar-free immutable database and, for canonical
// indexes, the same stable whole-set authority used by writers and GC.
type SQLiteSnapshot struct {
	db             *sql.DB
	path           string
	segmentSetRoot string
	indexSetID     string
	authority      *indexcoord.Lease
	authorityOwned bool
	closed         bool
}

// OpenSQLiteSnapshot opens a current-schema, non-mutating SQLite snapshot.
func OpenSQLiteSnapshot(ctx context.Context, opts SQLiteSnapshotOptions) (*SQLiteSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	opts.Path = strings.TrimSpace(opts.Path)
	opts.SegmentSetRoot = strings.TrimSpace(opts.SegmentSetRoot)
	opts.IndexSetID = strings.TrimSpace(opts.IndexSetID)
	if opts.Path == "" {
		return nil, fmt.Errorf("SQLite snapshot path is required")
	}
	if (opts.SegmentSetRoot == "") != (opts.IndexSetID == "") {
		return nil, fmt.Errorf("SQLite snapshot segment root and index_set_id must be supplied together")
	}
	if opts.Authority != nil && opts.SegmentSetRoot == "" {
		return nil, fmt.Errorf("SQLite snapshot authority requires a segment root and index_set_id")
	}
	if err := indexstore.RejectSQLiteTransactionSidecars(opts.Path); err != nil {
		return nil, err
	}

	authority := opts.Authority
	authorityOwned := false
	var err error
	if opts.SegmentSetRoot != "" {
		if authority == nil {
			authority, err = indexcoord.Acquire(ctx, opts.SegmentSetRoot, opts.IndexSetID, "sqlite-read")
			if err != nil {
				return nil, fmt.Errorf("acquire SQLite snapshot authority: %w", err)
			}
			authorityOwned = true
		} else if err := authority.AssertHeldFor(opts.IndexSetID, opts.SegmentSetRoot); err != nil {
			return nil, fmt.Errorf("validate SQLite snapshot authority: %w", err)
		}
	}
	releaseOnError := func() {
		if authorityOwned {
			_ = authority.Release()
		}
	}
	if err := indexstore.RejectSQLiteTransactionSidecars(opts.Path); err != nil {
		releaseOnError()
		return nil, err
	}
	db, err := indexstore.OpenLocalReadOnly(ctx, opts.Path)
	if err != nil {
		releaseOnError()
		return nil, err
	}
	if err := indexstore.ValidateCurrentSchemaReadOnly(ctx, db); err != nil {
		_ = db.Close()
		releaseOnError()
		return nil, fmt.Errorf("local index schema is not current; run a guarded build/init migration before read: %w", err)
	}
	s := &SQLiteSnapshot{
		db: db, path: opts.Path, segmentSetRoot: opts.SegmentSetRoot,
		indexSetID: opts.IndexSetID, authority: authority, authorityOwned: authorityOwned,
	}
	if err := s.Check(); err != nil {
		return nil, errors.Join(err, s.Close())
	}
	if opts.IndexSetID != "" {
		sets, err := indexstore.ListIndexSets(ctx, db, "")
		if err != nil {
			return nil, errors.Join(fmt.Errorf("bind SQLite snapshot to index_set_id: %w", err), s.Close())
		}
		if len(sets) != 1 {
			return nil, errors.Join(fmt.Errorf("%w: database contains %d index sets, expected exactly 1", ErrSQLiteIdentityScope, len(sets)), s.Close())
		}
		if sets[0].IndexSetID != opts.IndexSetID {
			return nil, errors.Join(fmt.Errorf("%w: database index_set_id %q does not match authority index_set_id %q", ErrSQLiteIdentityScope, sets[0].IndexSetID, opts.IndexSetID), s.Close())
		}
	}
	if err := s.Check(); err != nil {
		return nil, errors.Join(err, s.Close())
	}
	return s, nil
}

// DB returns the query-only connection owned by the snapshot.
func (s *SQLiteSnapshot) DB() *sql.DB {
	if s == nil || s.closed {
		return nil
	}
	return s.db
}

// Check revalidates authority and rejects transaction sidecars.
func (s *SQLiteSnapshot) Check() error {
	if s == nil || s.closed || s.db == nil {
		return fmt.Errorf("SQLite snapshot is closed")
	}
	if s.authority != nil {
		if err := s.authority.AssertHeldFor(s.indexSetID, s.segmentSetRoot); err != nil {
			return fmt.Errorf("SQLite snapshot authority: %w", err)
		}
	}
	return indexstore.RejectSQLiteTransactionSidecars(s.path)
}

// Close closes the connection, performs post-read checks, and releases only
// authority acquired by OpenSQLiteSnapshot itself.
func (s *SQLiteSnapshot) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	var errs []error
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			errs = append(errs, err)
		}
		s.db = nil
	}
	if s.authority != nil {
		if err := s.authority.AssertHeldFor(s.indexSetID, s.segmentSetRoot); err != nil {
			errs = append(errs, fmt.Errorf("SQLite snapshot authority at close: %w", err))
		}
	}
	if err := indexstore.RejectSQLiteTransactionSidecars(s.path); err != nil {
		errs = append(errs, err)
	}
	if s.authorityOwned && s.authority != nil {
		if err := s.authority.Release(); err != nil {
			errs = append(errs, err)
		}
	}
	s.authority = nil
	return errors.Join(errs...)
}
