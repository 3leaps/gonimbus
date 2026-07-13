package indexreader

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

type sqliteReader struct {
	meta     Meta
	db       *sql.DB
	snapshot *SQLiteSnapshot
}

func openSQLiteReader(ctx context.Context, opts ResolveOptions, c candidate) (*sqliteReader, error) {
	snapshot, err := OpenSQLiteSnapshot(ctx, SQLiteSnapshotOptions{
		Path:           c.dbPath,
		SegmentSetRoot: filepath.Join(opts.SegmentCacheRoot, c.meta.IndexSetID),
		IndexSetID:     c.meta.IndexSetID,
		Authority:      opts.Authority,
	})
	if err != nil {
		return nil, fmt.Errorf("open index database: %w", err)
	}
	return &sqliteReader{meta: c.meta, db: snapshot.DB(), snapshot: snapshot}, nil
}

func (r *sqliteReader) Meta() Meta { return r.meta }

func (r *sqliteReader) Close() error {
	if r == nil || r.snapshot == nil {
		return nil
	}
	err := r.snapshot.Close()
	r.snapshot = nil
	r.db = nil
	return err
}

func (r *sqliteReader) SQLiteDB() *sql.DB { return r.db }

func (r *sqliteReader) WalkObjects(ctx context.Context, params indexstore.QueryParams, visit VisitObject) (indexstore.QueryStats, error) {
	if err := r.snapshot.Check(); err != nil {
		return indexstore.QueryStats{}, err
	}
	if visit == nil {
		return indexstore.QueryStats{}, fmt.Errorf("visit callback is required")
	}
	params.IndexSetID = r.meta.IndexSetID
	results, stats, err := indexstore.QueryObjects(ctx, r.db, params)
	if err != nil {
		return stats, err
	}
	if err := r.snapshot.Check(); err != nil {
		return stats, err
	}
	for _, result := range results {
		if err := visit(result); err != nil {
			return stats, err
		}
	}
	if err := r.snapshot.Check(); err != nil {
		return stats, err
	}
	return stats, nil
}

func (r *sqliteReader) QueryObjects(ctx context.Context, params indexstore.QueryParams) ([]indexstore.QueryResult, indexstore.QueryStats, error) {
	if err := r.snapshot.Check(); err != nil {
		return nil, indexstore.QueryStats{}, err
	}
	params.IndexSetID = r.meta.IndexSetID
	results, stats, err := indexstore.QueryObjects(ctx, r.db, params)
	if err == nil {
		err = r.snapshot.Check()
	}
	return results, stats, err
}

func (r *sqliteReader) QueryObjectCount(ctx context.Context, params indexstore.QueryParams) (int64, error) {
	if err := r.snapshot.Check(); err != nil {
		return 0, err
	}
	params.IndexSetID = r.meta.IndexSetID
	count, err := indexstore.QueryObjectCount(ctx, r.db, params)
	if err == nil {
		err = r.snapshot.Check()
	}
	return count, err
}

func (r *sqliteReader) QueryCanonicalObjects(ctx context.Context, params indexstore.QueryParams) ([]indexstore.CanonicalOutputRecord, indexstore.CanonicalQueryStats, error) {
	if err := r.snapshot.Check(); err != nil {
		return nil, indexstore.CanonicalQueryStats{}, err
	}
	params.IndexSetID = r.meta.IndexSetID
	results, stats, err := indexstore.QueryCanonicalObjects(ctx, r.db, params)
	if err == nil {
		err = r.snapshot.Check()
	}
	return results, stats, err
}

func (r *sqliteReader) ResolveSinceRunFilter(ctx context.Context, runID string) (*indexstore.SinceRunFilter, error) {
	if err := r.snapshot.Check(); err != nil {
		return nil, err
	}
	filter, err := indexstore.ResolveSinceRunFilter(ctx, r.db, r.meta.IndexSetID, runID)
	if err == nil {
		err = r.snapshot.Check()
	}
	return filter, err
}
