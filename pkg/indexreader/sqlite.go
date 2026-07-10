package indexreader

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

type sqliteReader struct {
	meta Meta
	db   *sql.DB
}

func openSQLiteReader(ctx context.Context, c candidate) (*sqliteReader, error) {
	db, err := indexstore.Open(ctx, indexstore.Config{Path: c.dbPath})
	if err != nil {
		return nil, fmt.Errorf("open index database: %w", err)
	}
	if err := indexstore.Migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate index schema: %w", err)
	}
	return &sqliteReader{meta: c.meta, db: db}, nil
}

func (r *sqliteReader) Meta() Meta { return r.meta }

func (r *sqliteReader) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

func (r *sqliteReader) WalkObjects(ctx context.Context, params indexstore.QueryParams, visit VisitObject) (indexstore.QueryStats, error) {
	if visit == nil {
		return indexstore.QueryStats{}, fmt.Errorf("visit callback is required")
	}
	params.IndexSetID = r.meta.IndexSetID
	results, stats, err := indexstore.QueryObjects(ctx, r.db, params)
	if err != nil {
		return stats, err
	}
	for _, result := range results {
		if err := visit(result); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func (r *sqliteReader) QueryObjects(ctx context.Context, params indexstore.QueryParams) ([]indexstore.QueryResult, indexstore.QueryStats, error) {
	params.IndexSetID = r.meta.IndexSetID
	return indexstore.QueryObjects(ctx, r.db, params)
}

func (r *sqliteReader) QueryObjectCount(ctx context.Context, params indexstore.QueryParams) (int64, error) {
	params.IndexSetID = r.meta.IndexSetID
	return indexstore.QueryObjectCount(ctx, r.db, params)
}

func (r *sqliteReader) QueryCanonicalObjects(ctx context.Context, params indexstore.QueryParams) ([]indexstore.CanonicalOutputRecord, indexstore.CanonicalQueryStats, error) {
	params.IndexSetID = r.meta.IndexSetID
	return indexstore.QueryCanonicalObjects(ctx, r.db, params)
}

func (r *sqliteReader) ResolveSinceRunFilter(ctx context.Context, runID string) (*indexstore.SinceRunFilter, error) {
	return indexstore.ResolveSinceRunFilter(ctx, r.db, r.meta.IndexSetID, runID)
}
