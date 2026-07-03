package cmd

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
)

func TestIndexBuildSinceFlagBareDefaultsToAuto(t *testing.T) {
	flag := indexBuildCmd.Flags().Lookup("since")
	require.NotNil(t, flag)
	require.Equal(t, "auto", flag.NoOptDefVal)
}

func TestPlanIndexBuildSince_NarrowsDatePartitionsWithoutMutatingManifest(t *testing.T) {
	m := testSinceManifestWithScope(&manifest.IndexScopeConfig{
		Type:       "date_partitions",
		BasePrefix: "site-a/",
		Date: &manifest.IndexScopeDateConfig{
			SegmentIndex: 0,
			Format:       "2006-01-02",
			Range: &manifest.IndexScopeDateRange{
				After:  "2026-06-01",
				Before: "2026-07-05",
			},
		},
	})
	originalScopeHash, err := computeScopeHash(m)
	require.NoError(t, err)

	plan, err := planIndexBuildSince(context.Background(), m, "2026-07-02T13:14:15Z", nil, time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.True(t, plan.Enabled)
	require.True(t, plan.EnumerationReductionApplied)
	require.NotNil(t, plan.RuntimeScope)
	require.Equal(t, "2026-07-02", plan.RuntimeScope.Date.Range.After)
	require.Equal(t, "2026-07-05", plan.RuntimeScope.Date.Range.Before)
	require.Equal(t, "2026-06-01", m.Build.Scope.Date.Range.After)

	afterScopeHash, err := computeScopeHash(m)
	require.NoError(t, err)
	require.Equal(t, originalScopeHash, afterScopeHash)
}

func TestPlanIndexBuildSince_NonDateScopeFallsBackToFullEnumerationFilter(t *testing.T) {
	m := testSinceManifestWithScope(&manifest.IndexScopeConfig{
		Type:     "prefix_list",
		Prefixes: []string{"current/"},
	})

	plan, err := planIndexBuildSince(context.Background(), m, "2026-07-02", nil, time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.True(t, plan.Enabled)
	require.False(t, plan.EnumerationReductionApplied)
	require.Nil(t, plan.RuntimeScope)
	require.NotNil(t, plan.Filter)
	require.Contains(t, plan.Warnings, "--since could not narrow provider listing scope; using full enumeration with last-modified filtering")
}

func TestPlanIndexBuildSince_MixedUnionReportsPartialReduction(t *testing.T) {
	m := testSinceManifestWithScope(&manifest.IndexScopeConfig{
		Type: "union",
		Scopes: []manifest.IndexScopeConfig{
			{
				Type:     "prefix_list",
				Prefixes: []string{"hot-data/"},
			},
			{
				Type:       "date_partitions",
				BasePrefix: "site-a/",
				Date: &manifest.IndexScopeDateConfig{
					SegmentIndex: 0,
					Format:       "2006-01-02",
					Range: &manifest.IndexScopeDateRange{
						After:  "2026-07-01",
						Before: "2026-07-04",
					},
				},
			},
		},
	})

	plan, err := planIndexBuildSince(context.Background(), m, "2026-07-02T09:30:00Z", nil, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.True(t, plan.Enabled)
	require.True(t, plan.EnumerationReductionApplied)
	require.True(t, plan.EnumerationReductionPartial)
	require.Equal(t, "partial", enumerationReductionStatus(plan))
	require.Equal(t, "union scope partially narrowed from --since watermark", plan.Reason)
	require.Contains(t, plan.Warnings, "--since partially narrowed union provider listing scope; non-date child scopes will use full enumeration with last-modified filtering")
	require.NotNil(t, plan.RuntimeScope)
	require.Len(t, plan.RuntimeScope.Scopes, 2)
	require.Equal(t, []string{"hot-data/"}, plan.RuntimeScope.Scopes[0].Prefixes)
	require.Equal(t, "2026-07-02", plan.RuntimeScope.Scopes[1].Date.Range.After)
	require.Equal(t, "2026-07-01", m.Build.Scope.Scopes[1].Date.Range.After)
}

func TestPlanIndexBuildSinceAuto_MissingWatermarkFailsClosed(t *testing.T) {
	m := testSinceManifestWithScope(nil)

	plan, err := planIndexBuildSince(context.Background(), m, "auto", nil, time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.True(t, plan.Enabled)
	require.Equal(t, "auto", plan.Mode)
	require.True(t, plan.AutoFallback)
	require.True(t, plan.Watermark.IsZero())
	require.Nil(t, plan.Filter)
	require.False(t, plan.EnumerationReductionApplied)
	require.Contains(t, plan.Warnings[0], "using full enumeration")
}

func TestPlanIndexBuildSinceAuto_FutureWatermarkFailsClosed(t *testing.T) {
	m := testSinceManifestWithScope(nil)
	future := time.Date(2026, 7, 3, 1, 0, 0, 0, time.UTC)

	plan, err := planIndexBuildSince(context.Background(), m, "auto", &future, time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.True(t, plan.AutoFallback)
	require.True(t, plan.Watermark.IsZero())
	require.Nil(t, plan.Filter)
}

func TestResolveIndexBuildSinceAuto_LatestNonSuccessFailsClosed(t *testing.T) {
	ctx := context.Background()
	db, err := indexstore.Open(ctx, indexstore.Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.NoError(t, indexstore.Migrate(ctx, db))

	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, indexstore.IndexSetParams{
		BaseURI:  "s3://bucket/data/",
		Provider: "s3",
		BuildParams: indexstore.BuildParams{
			SourceType:    "crawl",
			SchemaVersion: indexstore.SchemaVersion,
		},
	})
	require.NoError(t, err)

	successRun, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, successRun.RunID, indexstore.RunStatusSuccess, nil))
	partialRun, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, partialRun.RunID, indexstore.RunStatusPartial, nil))

	plan, err := resolveIndexBuildSince(ctx, db, indexSet.IndexSetID, testSinceManifestWithScope(nil), "auto", time.Now().UTC())
	require.NoError(t, err)
	require.True(t, plan.Enabled)
	require.True(t, plan.AutoFallback)
	require.True(t, plan.Watermark.IsZero())
	require.Nil(t, plan.Filter)
	require.Equal(t, "latest index run is not successful", plan.Reason)
	require.Contains(t, plan.Warnings[0], "status partial")
	require.Contains(t, plan.Warnings[0], "using full enumeration")
}

func TestResolveIndexBuildSinceAuto_DBReadErrorFailsClosedWithSpecificWarning(t *testing.T) {
	ctx := context.Background()
	db, err := indexstore.Open(ctx, indexstore.Config{Path: ":memory:"})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	require.NoError(t, db.Close())

	plan, err := resolveIndexBuildSince(ctx, db, "idx_unreadable", testSinceManifestWithScope(nil), "auto", time.Now().UTC())
	require.NoError(t, err)
	require.True(t, plan.Enabled)
	require.True(t, plan.AutoFallback)
	require.True(t, plan.Watermark.IsZero())
	require.Nil(t, plan.Filter)
	require.Equal(t, "auto watermark metadata unreadable", plan.Reason)
	require.Contains(t, plan.Warnings[0], "could not read prior run metadata")
	require.Contains(t, plan.Warnings[0], "using full enumeration")
}

func TestIndexBuildSincePlanFromCheckpoint_ReplaysResolvedWatermark(t *testing.T) {
	cfg := &indexBuildCheckpointConfig{
		SinceMode:      "auto",
		SinceWatermark: "2026-07-02T13:14:15Z",
		SinceRuntimeScope: &manifest.IndexScopeConfig{
			Type: "date_partitions",
			Date: &manifest.IndexScopeDateConfig{
				SegmentIndex: 0,
				Range: &manifest.IndexScopeDateRange{
					After:  "2026-07-02",
					Before: "2026-07-04",
				},
			},
		},
	}

	plan, err := indexBuildSincePlanFromCheckpoint(cfg)
	require.NoError(t, err)
	require.True(t, plan.Enabled)
	require.Equal(t, "auto", plan.Mode)
	require.Equal(t, "2026-07-02T13:14:15Z", plan.Watermark.Format(time.RFC3339))
	require.NotNil(t, plan.Filter)
	require.True(t, plan.EnumerationReductionApplied)
	require.NotNil(t, plan.RuntimeScope)
	require.Equal(t, "2026-07-02", plan.RuntimeScope.Date.Range.After)

	require.True(t, plan.Filter.Match(&provider.ObjectSummary{
		Key:          "data/new.json",
		LastModified: time.Date(2026, 7, 2, 13, 14, 15, 0, time.UTC),
	}))
	require.False(t, plan.Filter.Match(&provider.ObjectSummary{
		Key:          "data/old.json",
		LastModified: time.Date(2026, 7, 2, 13, 14, 14, 0, time.UTC),
	}))
}

func TestRunCrawlForIndexSinceUsesNarrowedPrefixesBeforeList(t *testing.T) {
	ctx, db, indexSet, run := newSinceIndexRunTestDB(t)
	fakeProvider := &sinceTestProvider{
		objects: []provider.ObjectSummary{
			{Key: "data/site-a/2026-07-02/new.json", Size: 10, ETag: "new", LastModified: time.Date(2026, 7, 2, 13, 0, 0, 0, time.UTC)},
			{Key: "data/site-a/2026-07-03/new.json", Size: 11, ETag: "newer", LastModified: time.Date(2026, 7, 3, 13, 0, 0, 0, time.UTC)},
		},
	}
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(context.Context, s3.Config) (provider.Provider, error) {
			return fakeProvider, nil
		},
	})
	t.Cleanup(restore)

	m := testSinceManifestWithScope(&manifest.IndexScopeConfig{
		Type:       "date_partitions",
		BasePrefix: "site-a/",
		Date: &manifest.IndexScopeDateConfig{
			SegmentIndex: 0,
			Format:       "2006-01-02",
			Range: &manifest.IndexScopeDateRange{
				After:  "2026-07-01",
				Before: "2026-07-04",
			},
		},
	})
	plan, err := planIndexBuildSince(ctx, m, "2026-07-02T09:30:00Z", nil, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	runtimeFilter := combineIndexBuildFilters(nil, plan.Filter)

	result, err := runCrawlForIndex(ctx, manifestForSincePlan(m, plan), db, indexSet.IndexSetID, run, runtimeFilter, nil, true)
	require.NoError(t, err)
	require.Equal(t, string(indexstore.RunStatusSuccess), string(result.FinalStatus))

	listPrefixes := fakeProvider.listPrefixSnapshot()
	require.ElementsMatch(t, []string{
		"data/site-a/2026-07-02/",
		"data/site-a/2026-07-03/",
	}, listPrefixes)
	require.NotContains(t, listPrefixes, "data/site-a/2026-07-01/")
	require.ElementsMatch(t, listPrefixes, result.CrawlPrefixes)
}

func TestIndexBuildSinceSuccessfulRunDoesNotSoftDeleteOutOfScopeRows(t *testing.T) {
	ctx, db, indexSet, run := newSinceIndexRunTestDB(t)
	fakeProvider := &sinceTestProvider{
		objects: []provider.ObjectSummary{
			{Key: "data/site-a/2026-07-02/new.json", Size: 10, ETag: "new", LastModified: time.Date(2026, 7, 2, 13, 0, 0, 0, time.UTC)},
		},
	}
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(context.Context, s3.Config) (provider.Provider, error) {
			return fakeProvider, nil
		},
	})
	t.Cleanup(restore)

	oldTime := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	previousRun, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.BatchUpsertObjects(ctx, db, []indexstore.ObjectRow{{
		IndexSetID:    indexSet.IndexSetID,
		RelKey:        "site-a/2026-07-01/old.json",
		SizeBytes:     100,
		LastModified:  &oldTime,
		ETag:          "old",
		LastSeenRunID: previousRun.RunID,
		LastSeenAt:    oldTime,
	}}))

	m := testSinceManifestWithScope(&manifest.IndexScopeConfig{
		Type:       "date_partitions",
		BasePrefix: "site-a/",
		Date: &manifest.IndexScopeDateConfig{
			SegmentIndex: 0,
			Format:       "2006-01-02",
			Range: &manifest.IndexScopeDateRange{
				After:  "2026-07-01",
				Before: "2026-07-03",
			},
		},
	})
	plan, err := planIndexBuildSince(ctx, m, "2026-07-02T00:00:00Z", nil, time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	result, err := runCrawlForIndex(ctx, manifestForSincePlan(m, plan), db, indexSet.IndexSetID, run, combineIndexBuildFilters(nil, plan.Filter), nil, true)
	require.NoError(t, err)
	require.Equal(t, string(indexstore.RunStatusSuccess), string(result.FinalStatus))
	require.NoError(t, finalizeIndexRun(ctx, db, indexSet.IndexSetID, run, result, false, indexBuildSinceEvents(run.RunID, plan, time.Now().UTC())))

	oldObject, err := indexstore.GetObject(ctx, db, indexSet.IndexSetID, "site-a/2026-07-01/old.json")
	require.NoError(t, err)
	require.NotNil(t, oldObject)
	require.Nil(t, oldObject.DeletedAt)
}

func TestIndexBuildSinceKeepsStableIndexSetIdentity(t *testing.T) {
	m := testSinceManifestWithScope(&manifest.IndexScopeConfig{
		Type:       "date_partitions",
		BasePrefix: "site-a/",
		Date: &manifest.IndexScopeDateConfig{
			SegmentIndex: 0,
			Range: &manifest.IndexScopeDateRange{
				After:  "2026-07-01",
				Before: "2026-07-04",
			},
		},
	})
	scopeHash, err := computeScopeHash(m)
	require.NoError(t, err)
	base := buildIndexSetParams(m, effectiveIdentity{}, "", scopeHash)
	baseID, err := indexstore.ComputeIndexSetID(base)
	require.NoError(t, err)

	plan, err := planIndexBuildSince(context.Background(), m, "2026-07-02T00:00:00Z", nil, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.True(t, plan.EnumerationReductionApplied)

	after := buildIndexSetParams(m, effectiveIdentity{}, "", scopeHash)
	afterID, err := indexstore.ComputeIndexSetID(after)
	require.NoError(t, err)
	require.Equal(t, baseID.IndexSetID, afterID.IndexSetID)
}

func TestIndexIngestWriterSinceDeltaReportClassifiesByPrefix(t *testing.T) {
	ctx := context.Background()
	db, err := indexstore.Open(ctx, indexstore.Config{Path: ":memory:"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.NoError(t, indexstore.Migrate(ctx, db))

	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, indexstore.IndexSetParams{
		BaseURI:  "s3://bucket/data/",
		Provider: "s3",
		BuildParams: indexstore.BuildParams{
			SourceType:    "crawl",
			SchemaVersion: indexstore.SchemaVersion,
		},
	})
	require.NoError(t, err)
	oldRun, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)

	oldTime := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, indexstore.BatchUpsertObjects(ctx, db, []indexstore.ObjectRow{
		{IndexSetID: indexSet.IndexSetID, RelKey: "site-a/2026-07-02/changed.json", SizeBytes: 100, LastModified: &oldTime, ETag: "old", LastSeenRunID: oldRun.RunID, LastSeenAt: oldTime},
		{IndexSetID: indexSet.IndexSetID, RelKey: "site-a/2026-07-02/same.json", SizeBytes: 200, LastModified: &oldTime, ETag: "same", LastSeenRunID: oldRun.RunID, LastSeenAt: oldTime},
	}))

	writer := newIndexIngestWriter(db, indexSet.IndexSetID, run, "s3://bucket/data/", "data/", indexIngestWriterConfig{
		ObjectBatchSize: 10,
		PrefixBatchSize: 10,
		DeltaReport:     true,
	})
	writer.setDeltaPrefixes([]string{"data/site-a/2026-07-02/"})

	require.NoError(t, writer.WriteObject(ctx, &output.ObjectRecord{Key: "data/site-a/2026-07-02/new.json", Size: 300, LastModified: oldTime, ETag: "new"}))
	require.NoError(t, writer.WriteObject(ctx, &output.ObjectRecord{Key: "data/site-a/2026-07-02/changed.json", Size: 101, LastModified: oldTime, ETag: "new-etag"}))
	require.NoError(t, writer.WriteObject(ctx, &output.ObjectRecord{Key: "data/site-a/2026-07-02/same.json", Size: 200, LastModified: oldTime, ETag: "same"}))
	require.NoError(t, writer.Close())

	result := writer.Result()
	counts := result.DeltaByPrefix["data/site-a/2026-07-02/"]
	require.Equal(t, int64(1), counts.Added)
	require.Equal(t, int64(1), counts.Changed)
	require.Equal(t, int64(1), counts.Unchanged)
}

func testSinceManifestWithScope(scopeCfg *manifest.IndexScopeConfig) *manifest.IndexManifest {
	m := &manifest.IndexManifest{
		Version: "1.0",
		Connection: manifest.IndexConnectionConfig{
			Provider: "s3",
			Bucket:   "bucket",
			BaseURI:  "s3://bucket/data/",
		},
		Build: &manifest.IndexBuildConfig{
			Source: "crawl",
			Scope:  scopeCfg,
			Match: &manifest.IndexMatchConfig{
				Includes: []string{"**"},
			},
		},
	}
	m.ApplyDefaults()
	return m
}

func newSinceIndexRunTestDB(t *testing.T) (context.Context, *sql.DB, *indexstore.IndexSet, *indexstore.IndexRun) {
	t.Helper()
	ctx := context.Background()
	db, err := indexstore.Open(ctx, indexstore.Config{Path: ":memory:"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, indexstore.Migrate(ctx, db))
	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, indexstore.IndexSetParams{
		BaseURI:  "s3://bucket/data/",
		Provider: "s3",
		BuildParams: indexstore.BuildParams{
			SourceType:    "crawl",
			SchemaVersion: indexstore.SchemaVersion,
		},
	})
	require.NoError(t, err)
	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	return ctx, db, indexSet, run
}

type sinceTestProvider struct {
	mu           sync.Mutex
	objects      []provider.ObjectSummary
	listPrefixes []string
}

func (p *sinceTestProvider) List(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	p.mu.Lock()
	p.listPrefixes = append(p.listPrefixes, opts.Prefix)
	p.mu.Unlock()

	var out []provider.ObjectSummary
	for _, obj := range p.objects {
		if strings.HasPrefix(obj.Key, opts.Prefix) {
			out = append(out, obj)
		}
	}
	return &provider.ListResult{Objects: out}, nil
}

func (p *sinceTestProvider) listPrefixSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.listPrefixes...)
}

func (p *sinceTestProvider) ListCommonPrefixes(context.Context, provider.ListCommonPrefixesOptions) (*provider.ListCommonPrefixesResult, error) {
	return &provider.ListCommonPrefixesResult{}, nil
}

func (p *sinceTestProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (p *sinceTestProvider) Close() error {
	return nil
}
