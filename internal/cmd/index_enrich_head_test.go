package cmd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/provider"
)

type fakeEnrichProvider struct {
	metas     map[string]*provider.ObjectMeta
	errs      map[string]error
	errSeq    map[string][]error
	counts    map[string]int
	headCalls []string
}

func (p *fakeEnrichProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return nil, nil
}

func (p *fakeEnrichProvider) Head(_ context.Context, key string) (*provider.ObjectMeta, error) {
	p.headCalls = append(p.headCalls, key)
	if p.counts == nil {
		p.counts = map[string]int{}
	}
	attempt := p.counts[key]
	p.counts[key] = attempt + 1
	if seq := p.errSeq[key]; attempt < len(seq) && seq[attempt] != nil {
		return nil, seq[attempt]
	}
	if err := p.errs[key]; err != nil {
		return nil, err
	}
	return p.metas[key], nil
}

func (p *fakeEnrichProvider) Close() error { return nil }

func TestExecuteEnrichHeadFiltersResumeAndUpdatesState(t *testing.T) {
	ctx, db, indexSet := setupEnrichHeadDB(t)
	defer func() { _ = db.Close() }()

	storageClass := "GLACIER"
	insertEnrichObject(t, ctx, db, indexSet.IndexSetID, "archive/enrich.xml", storageClass, nil)
	enrichedAt := time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC)
	insertEnrichObject(t, ctx, db, indexSet.IndexSetID, "archive/resume-skip.xml", storageClass, &enrichedAt)
	insertEnrichObject(t, ctx, db, indexSet.IndexSetID, "standard/filtered.xml", "STANDARD", nil)

	candidates, _, err := indexstore.QueryHeadEnrichmentCandidates(ctx, db, indexstore.QueryParams{
		IndexSetID:     indexSet.IndexSetID,
		Pattern:        "archive/**",
		StorageClasses: []string{"GLACIER"},
	})
	require.NoError(t, err)
	require.Len(t, candidates, 2)

	restoreExpiry := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	prov := &fakeEnrichProvider{
		metas: map[string]*provider.ObjectMeta{
			"prefix/archive/enrich.xml": {
				ObjectSummary: provider.ObjectSummary{Key: "prefix/archive/enrich.xml"},
				ContentType:   "application/xml",
				ArchiveStatus: "DEEP_ARCHIVE_ACCESS",
				RestoreState:  "completed",
				RestoreExpiry: &restoreExpiry,
			},
		},
		errs: map[string]error{},
	}

	statePath := filepath.Join(t.TempDir(), "state.jsonl")
	stateFile, err := os.OpenFile(statePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	require.NoError(t, err)
	cmd := indexEnrichWithHeadCmd
	cmd.Flags().Set("resume", "true")
	cmd.Flags().Set("parallel", "2")
	summary, err := executeEnrichHead(ctx, db, prov, indexSet, candidates, cmd, stateFile, true)
	require.NoError(t, err)
	require.NoError(t, stateFile.Close())

	require.Equal(t, int64(2), summary.Candidates)
	require.Equal(t, int64(1), summary.Enriched)
	require.Equal(t, int64(1), summary.ResumeSkipped)
	require.Equal(t, int64(0), summary.Failed)
	require.Equal(t, int64(1), summary.HeadCalls)
	require.Equal(t, []string{"prefix/archive/enrich.xml"}, prov.headCalls)

	got, err := indexstore.GetObject(ctx, db, indexSet.IndexSetID, "archive/enrich.xml")
	require.NoError(t, err)
	require.NotNil(t, got.HeadEnrichedAt)
	require.NotNil(t, got.ArchiveStatus)
	require.Equal(t, "DEEP_ARCHIVE_ACCESS", *got.ArchiveStatus)
	require.NotNil(t, got.RestoreState)
	require.Equal(t, "completed", *got.RestoreState)
	require.NotNil(t, got.RestoreExpiry)
	require.Equal(t, restoreExpiry, *got.RestoreExpiry)

	data, err := os.ReadFile(statePath)
	require.NoError(t, err)
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	require.Len(t, lines, 2)
	var records []enrichHeadStateRecord
	for _, line := range lines {
		var rec enrichHeadStateRecord
		require.NoError(t, json.Unmarshal(line, &rec))
		records = append(records, rec)
	}
	statuses := map[string]bool{}
	for _, rec := range records {
		statuses[rec.Data.Status] = true
		require.NotEqual(t, "standard/filtered.xml", rec.Data.RelKey)
	}
	require.True(t, statuses["success"])
	require.True(t, statuses["resume_skipped"])
}

func TestExecuteEnrichHeadFailureDoesNotMutateHeadEnrichedAt(t *testing.T) {
	ctx, db, indexSet := setupEnrichHeadDB(t)
	defer func() { _ = db.Close() }()

	insertEnrichObject(t, ctx, db, indexSet.IndexSetID, "archive/fail.xml", "GLACIER", nil)
	candidates, _, err := indexstore.QueryHeadEnrichmentCandidates(ctx, db, indexstore.QueryParams{IndexSetID: indexSet.IndexSetID})
	require.NoError(t, err)

	prov := &fakeEnrichProvider{
		metas: map[string]*provider.ObjectMeta{},
		errs: map[string]error{
			"prefix/archive/fail.xml": provider.ErrAccessDenied,
		},
	}
	cmd := indexEnrichWithHeadCmd
	cmd.Flags().Set("resume", "false")
	cmd.Flags().Set("parallel", "1")
	summary, err := executeEnrichHead(ctx, db, prov, indexSet, candidates, cmd, nil, false)
	require.NoError(t, err)
	require.Equal(t, int64(1), summary.Failed)
	require.Equal(t, int64(1), summary.HeadCalls)

	got, err := indexstore.GetObject(ctx, db, indexSet.IndexSetID, "archive/fail.xml")
	require.NoError(t, err)
	require.Nil(t, got.HeadEnrichedAt)
}

func TestExecuteEnrichHeadCountsRetryHeadCalls(t *testing.T) {
	ctx, db, indexSet := setupEnrichHeadDB(t)
	defer func() { _ = db.Close() }()

	insertEnrichObject(t, ctx, db, indexSet.IndexSetID, "archive/retry.xml", "GLACIER", nil)
	candidates, _, err := indexstore.QueryHeadEnrichmentCandidates(ctx, db, indexstore.QueryParams{IndexSetID: indexSet.IndexSetID})
	require.NoError(t, err)

	prov := &fakeEnrichProvider{
		metas: map[string]*provider.ObjectMeta{
			"prefix/archive/retry.xml": {
				ObjectSummary: provider.ObjectSummary{Key: "prefix/archive/retry.xml"},
				ContentType:   "application/xml",
			},
		},
		errs: map[string]error{},
		errSeq: map[string][]error{
			"prefix/archive/retry.xml": {provider.ErrThrottled, provider.ErrProviderUnavailable, nil},
		},
	}

	statePath := filepath.Join(t.TempDir(), "state.jsonl")
	stateFile, err := os.OpenFile(statePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	require.NoError(t, err)
	cmd := indexEnrichWithHeadCmd
	cmd.Flags().Set("resume", "false")
	cmd.Flags().Set("parallel", "1")
	summary, err := executeEnrichHead(ctx, db, prov, indexSet, candidates, cmd, stateFile, false)
	require.NoError(t, err)
	require.NoError(t, stateFile.Close())

	require.Equal(t, int64(1), summary.Enriched)
	require.Equal(t, int64(0), summary.Failed)
	require.Equal(t, int64(3), summary.HeadCalls)
	require.Equal(t, []string{"prefix/archive/retry.xml", "prefix/archive/retry.xml", "prefix/archive/retry.xml"}, prov.headCalls)

	data, err := os.ReadFile(statePath)
	require.NoError(t, err)
	var rec enrichHeadStateRecord
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(data), &rec))
	require.Equal(t, "success", rec.Data.Status)
	require.Equal(t, 3, rec.Data.Attempts)
}

func TestEnrichHeadRunStatusPartialForPerObjectFailures(t *testing.T) {
	require.Equal(t, indexstore.RunStatus(indexstore.RunStatusPartial), enrichHeadRunStatus(enrichHeadSummaryData{Failed: 1}, nil))
	require.Equal(t, indexstore.RunStatus(indexstore.RunStatusFailed), enrichHeadRunStatus(enrichHeadSummaryData{Failed: 1}, context.Canceled))
	require.Equal(t, indexstore.RunStatus(indexstore.RunStatusSuccess), enrichHeadRunStatus(enrichHeadSummaryData{}, nil))
}

func setupEnrichHeadDB(t *testing.T) (context.Context, *sql.DB, *indexstore.IndexSet) {
	t.Helper()
	ctx := context.Background()
	db, err := indexstore.Open(ctx, indexstore.Config{Path: ":memory:"})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, indexstore.IndexSetParams{
		BaseURI:  "s3://bucket/prefix/",
		Provider: "s3",
		BuildParams: indexstore.BuildParams{
			SourceType:    "crawl",
			SchemaVersion: indexstore.SchemaVersion,
		},
	})
	require.NoError(t, err)
	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusSuccess, nil))
	return ctx, db, indexSet
}

func insertEnrichObject(t *testing.T, ctx context.Context, db *sql.DB, indexSetID string, relKey string, storageClass string, headEnrichedAt *time.Time) {
	t.Helper()
	run, err := indexstore.CreateIndexRun(ctx, db, indexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpsertObject(ctx, db, indexstore.ObjectRow{
		IndexSetID:    indexSetID,
		RelKey:        relKey,
		SizeBytes:     10,
		StorageClass:  &storageClass,
		LastSeenRunID: run.RunID,
		LastSeenAt:    run.StartedAt,
	}))
	if headEnrichedAt != nil {
		require.NoError(t, indexstore.BatchUpdateHeadEnrichment(ctx, db, []indexstore.HeadEnrichmentUpdate{{
			IndexSetID:     indexSetID,
			RelKey:         relKey,
			HeadEnrichedAt: *headEnrichedAt,
		}}))
	}
}
