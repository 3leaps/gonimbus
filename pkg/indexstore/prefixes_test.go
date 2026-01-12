package indexstore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertPrefixStat(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))

	params := IndexSetParams{
		BaseURI:  "s3://test-bucket/data/",
		Provider: "s3",
		BuildParams: BuildParams{
			SourceType:    "crawl",
			SchemaVersion: SchemaVersion,
		},
	}
	indexSet, _, err := FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)

	run, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	t.Run("insert prefix stat", func(t *testing.T) {
		stat := PrefixStatRow{
			IndexSetID:     indexSet.IndexSetID,
			RunID:          run.RunID,
			Prefix:         "data/",
			Depth:          0,
			ObjectsDirect:  100,
			BytesDirect:    1024000,
			CommonPrefixes: 5,
			Truncated:      false,
		}

		err := InsertPrefixStat(ctx, db, stat)
		require.NoError(t, err)

		stats, err := GetPrefixStats(ctx, db, indexSet.IndexSetID, run.RunID)
		require.NoError(t, err)
		require.Len(t, stats, 1)
		assert.Equal(t, "data/", stats[0].Prefix)
		assert.Equal(t, int64(100), stats[0].ObjectsDirect)
		assert.False(t, stats[0].Truncated)
	})

	t.Run("insert truncated prefix", func(t *testing.T) {
		stat := PrefixStatRow{
			IndexSetID:      indexSet.IndexSetID,
			RunID:           run.RunID,
			Prefix:          "data/large/",
			Depth:           1,
			ObjectsDirect:   1000,
			BytesDirect:     10240000,
			CommonPrefixes:  50,
			Truncated:       true,
			TruncatedReason: "max_objects",
		}

		err := InsertPrefixStat(ctx, db, stat)
		require.NoError(t, err)

		stats, err := GetPrefixStats(ctx, db, indexSet.IndexSetID, run.RunID)
		require.NoError(t, err)
		require.Len(t, stats, 2)

		// Find the truncated one
		var truncatedStat *PrefixStatRow
		for i := range stats {
			if stats[i].Truncated {
				truncatedStat = &stats[i]
				break
			}
		}
		require.NotNil(t, truncatedStat)
		assert.Equal(t, "max_objects", truncatedStat.TruncatedReason)
	})
}

func TestInsertPrefixStat_Validation(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))

	params := IndexSetParams{
		BaseURI:  "s3://test-bucket/data/",
		Provider: "s3",
		BuildParams: BuildParams{
			SourceType:    "crawl",
			SchemaVersion: SchemaVersion,
		},
	}
	indexSet, _, err := FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)

	run, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	t.Run("rejects negative depth", func(t *testing.T) {
		stat := PrefixStatRow{
			IndexSetID: indexSet.IndexSetID,
			RunID:      run.RunID,
			Prefix:     "bad/",
			Depth:      -1,
		}
		err := InsertPrefixStat(ctx, db, stat)
		assert.ErrorIs(t, err, ErrInvalidPrefixStat)
	})

	t.Run("rejects negative objects_direct", func(t *testing.T) {
		stat := PrefixStatRow{
			IndexSetID:    indexSet.IndexSetID,
			RunID:         run.RunID,
			Prefix:        "bad/",
			ObjectsDirect: -1,
		}
		err := InsertPrefixStat(ctx, db, stat)
		assert.ErrorIs(t, err, ErrInvalidPrefixStat)
	})
}

func TestBatchInsertPrefixStats(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))

	params := IndexSetParams{
		BaseURI:  "s3://test-bucket/data/",
		Provider: "s3",
		BuildParams: BuildParams{
			SourceType:    "crawl",
			SchemaVersion: SchemaVersion,
		},
	}
	indexSet, _, err := FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)

	run, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	stats := []PrefixStatRow{
		{IndexSetID: indexSet.IndexSetID, RunID: run.RunID, Prefix: "a/", Depth: 0, ObjectsDirect: 10, BytesDirect: 1000, CommonPrefixes: 2},
		{IndexSetID: indexSet.IndexSetID, RunID: run.RunID, Prefix: "b/", Depth: 0, ObjectsDirect: 20, BytesDirect: 2000, CommonPrefixes: 3},
		{IndexSetID: indexSet.IndexSetID, RunID: run.RunID, Prefix: "c/", Depth: 0, ObjectsDirect: 30, BytesDirect: 3000, CommonPrefixes: 4},
	}

	err = BatchInsertPrefixStats(ctx, db, stats)
	require.NoError(t, err)

	retrieved, err := GetPrefixStats(ctx, db, indexSet.IndexSetID, run.RunID)
	require.NoError(t, err)
	assert.Len(t, retrieved, 3)
}

func TestGetLatestPrefixStats(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, Migrate(ctx, db))

	params := IndexSetParams{
		BaseURI:  "s3://test-bucket/data/",
		Provider: "s3",
		BuildParams: BuildParams{
			SourceType:    "crawl",
			SchemaVersion: SchemaVersion,
		},
	}
	indexSet, _, err := FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)

	// Create first run and mark as success
	run1, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	stat1 := PrefixStatRow{
		IndexSetID:    indexSet.IndexSetID,
		RunID:         run1.RunID,
		Prefix:        "data/",
		ObjectsDirect: 100,
	}
	err = InsertPrefixStat(ctx, db, stat1)
	require.NoError(t, err)

	err = UpdateIndexRunStatus(ctx, db, run1.RunID, RunStatusSuccess, nil)
	require.NoError(t, err)

	// Create second run with different stats
	run2, err := CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	stat2 := PrefixStatRow{
		IndexSetID:    indexSet.IndexSetID,
		RunID:         run2.RunID,
		Prefix:        "data/",
		ObjectsDirect: 200, // different value
	}
	err = InsertPrefixStat(ctx, db, stat2)
	require.NoError(t, err)

	err = UpdateIndexRunStatus(ctx, db, run2.RunID, RunStatusSuccess, nil)
	require.NoError(t, err)

	// GetLatestPrefixStats should return run2's stats
	latest, err := GetLatestPrefixStats(ctx, db, indexSet.IndexSetID)
	require.NoError(t, err)
	require.Len(t, latest, 1)
	assert.Equal(t, int64(200), latest[0].ObjectsDirect)
}
