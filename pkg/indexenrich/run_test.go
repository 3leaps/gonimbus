package indexenrich

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexbuild"
	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
)

type fakeHeadProvider struct {
	mu          sync.Mutex
	metas       map[string]*provider.ObjectMeta
	errs        map[string]error
	entered     chan struct{} // closed on first Head entry (once)
	enteredOnce sync.Once
	block       chan struct{} // when non-nil, Head blocks until closed (or ctx done)
	headCalls   atomic.Int64
	listCalls   atomic.Int64
}

func (p *fakeHeadProvider) signalEntered() {
	p.enteredOnce.Do(func() {
		if p.entered != nil {
			close(p.entered)
		}
	})
}

func (p *fakeHeadProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	p.listCalls.Add(1)
	return &provider.ListResult{}, nil
}
func (p *fakeHeadProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	p.headCalls.Add(1)
	p.signalEntered()
	if p.block != nil {
		select {
		case <-p.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.errs[key]; err != nil {
		return nil, err
	}
	if m := p.metas[key]; m != nil {
		return m, nil
	}
	return &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key}, ContentType: "application/xml"}, nil
}
func (p *fakeHeadProvider) Close() error { return nil }

func seedDurableParent(t *testing.T, root, indexSetID string, rows []indexsubstrate.CurrentObjectRow) (segmentRoot, latestPath string) {
	t.Helper()
	segmentRoot = filepath.Join(root, "segments", indexSetID)
	require.NoError(t, os.MkdirAll(segmentRoot, 0o700))
	lease, err := indexsubstrate.AcquireWriteLease(segmentRoot, indexSetID, "seed", 0)
	require.NoError(t, err)
	defer func() { _ = lease.Release() }()

	runID := fmt.Sprintf("run_%d", time.Now().UnixNano())
	runDir := filepath.Join(segmentRoot, "runs", runID)
	require.NoError(t, os.MkdirAll(runDir, 0o700))
	jdir := filepath.Join(root, "journals", indexSetID, runID)
	require.NoError(t, os.MkdirAll(jdir, 0o700))
	jpath := filepath.Join(jdir, "observe.jsonl")
	started := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	jw, err := indexsubstrate.CreateJournal(jpath, indexsubstrate.JournalHeader{
		Type: indexsubstrate.JournalHeaderType, JournalID: "jrn_seed", IndexSetID: indexSetID,
		RunID: runID, Shard: "s", IndexSchemaVersion: indexsubstrate.IndexSchemaVersion, StartedAt: started,
	})
	require.NoError(t, err)
	prior := make([]indexsubstrate.CurrentObjectRow, len(rows))
	copy(prior, rows)
	for i := range prior {
		if prior[i].IndexSetID == "" {
			prior[i].IndexSetID = indexSetID
		}
		if prior[i].FirstSeenRunID == "" {
			prior[i].FirstSeenRunID = runID
		}
		if prior[i].FirstSeenAt.IsZero() {
			prior[i].FirstSeenAt = started
		}
		if prior[i].LastSeenRunID == "" {
			prior[i].LastSeenRunID = runID
		}
		if prior[i].LastSeenAt.IsZero() {
			prior[i].LastSeenAt = started
		}
		if prior[i].LastChangedRunID == "" {
			prior[i].LastChangedRunID = runID
		}
		if prior[i].LastChangedAt.IsZero() {
			prior[i].LastChangedAt = started
		}
	}
	for _, row := range prior {
		if row.DeletedAt != nil {
			continue
		}
		size := row.SizeBytes
		_, err = jw.Append(indexsubstrate.ObjectRecord{
			Type: indexsubstrate.ObjectRecordType, Op: indexsubstrate.ObjectRecordOpObserve,
			RelKey: row.RelKey, ObservedAt: started, SizeBytes: &size, ETag: row.ETag, StorageClass: row.StorageClass,
		})
		require.NoError(t, err)
	}
	require.NoError(t, jw.Seal(started.Add(time.Minute)))
	require.NoError(t, jw.Close())

	cov := []indexsubstrate.CoverageAttestation{{
		Scope: &indexsubstrate.Scope{Prefix: "hot/"}, Basis: indexsubstrate.CoverageBasisConfirmed, Complete: true,
	}}
	_, err = indexsubstrate.PublishSnapshot(indexsubstrate.PublishConfig{
		IndexSetID: indexSetID, RunID: runID, RunStartedAt: started, CreatedAt: started.Add(2 * time.Minute),
		PriorRows: prior, JournalPaths: []string{jpath}, Coverage: cov, SegmentDir: runDir,
		ManifestPath: filepath.Join(runDir, "manifest.json"), CompletePath: filepath.Join(runDir, "complete.json"),
		LatestPath: filepath.Join(segmentRoot, "latest.json"), WriteLease: lease, TargetRowsPerSegment: 100,
	})
	require.NoError(t, err)
	return segmentRoot, filepath.Join(segmentRoot, "latest.json")
}

func TestRunZeroCandidatesNoPublish(t *testing.T) {
	root := t.TempDir()
	id := "idx_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	standard := "STANDARD"
	seg, _ := seedDurableParent(t, root, id, []indexsubstrate.CurrentObjectRow{
		{IndexSetID: id, RelKey: "hot/a.xml", SizeBytes: 10, ETag: `"a"`, StorageClass: &standard},
	})
	prov := &fakeHeadProvider{}
	res, err := Run(context.Background(), Config{
		IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: prov,
		SegmentSetRoot: seg, JournalRoot: filepath.Join(root, "journals", id),
		Query: QueryOptions{Pattern: "missing/**"}, Parallel: 2,
	})
	require.NoError(t, err)
	require.False(t, res.Published)
	require.False(t, res.LatestAdvanced)
	require.Equal(t, int64(0), res.Candidates)
	require.Equal(t, int64(0), res.Committed)
	require.Equal(t, id, res.IndexSetID)
}

func TestRunSuccessPublishesAndPreservesNonCandidates(t *testing.T) {
	root := t.TempDir()
	id := "idx_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	standard := "STANDARD"
	seg, latest := seedDurableParent(t, root, id, []indexsubstrate.CurrentObjectRow{
		{IndexSetID: id, RelKey: "hot/a.xml", SizeBytes: 10, ETag: `"a"`, StorageClass: &standard},
		{IndexSetID: id, RelKey: "hot/b.xml", SizeBytes: 20, ETag: `"b"`, StorageClass: &standard},
	})
	parent, err := indexsubstrate.OpenLatestPublishedSnapshot(latest)
	require.NoError(t, err)
	prov := &fakeHeadProvider{metas: map[string]*provider.ObjectMeta{
		"data/hot/a.xml": {ObjectSummary: provider.ObjectSummary{Key: "data/hot/a.xml"}, ContentType: "application/xml"},
	}}
	res, err := Run(context.Background(), Config{
		IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: prov,
		SegmentSetRoot: seg, JournalRoot: filepath.Join(root, "journals", id),
		Query: QueryOptions{Pattern: "hot/a.xml"}, Parallel: 2,
	})
	require.NoError(t, err)
	require.True(t, res.Published)
	require.True(t, res.LatestAdvanced)
	require.Equal(t, int64(1), res.HeadSucceeded)
	require.Equal(t, int64(1), res.Committed)
	require.Equal(t, parent.Complete.RunID, res.ParentRunID)
	require.NotEqual(t, parent.Complete.RunID, res.RunID)

	_, rows, err := indexsubstrate.ReadLatestPublishedRows(latest)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	by := map[string]indexsubstrate.CurrentObjectRow{}
	for _, r := range rows {
		by[r.RelKey] = r
	}
	require.NotNil(t, by["hot/a.xml"].HeadEnrichedAt)
	require.NotNil(t, by["hot/a.xml"].ContentType)
	require.Equal(t, "application/xml", *by["hot/a.xml"].ContentType)
	require.Equal(t, int64(10), by["hot/a.xml"].SizeBytes)
	require.Nil(t, by["hot/b.xml"].HeadEnrichedAt)
	require.Nil(t, by["hot/b.xml"].DeletedAt)
}

func TestRunHeadFailureDoesNotAdvanceLatest(t *testing.T) {
	root := t.TempDir()
	id := "idx_cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	standard := "STANDARD"
	seg, latest := seedDurableParent(t, root, id, []indexsubstrate.CurrentObjectRow{
		{IndexSetID: id, RelKey: "hot/a.xml", SizeBytes: 10, ETag: `"a"`, StorageClass: &standard},
		{IndexSetID: id, RelKey: "hot/b.xml", SizeBytes: 20, ETag: `"b"`, StorageClass: &standard},
	})
	before, err := os.ReadFile(latest)
	require.NoError(t, err)
	prov := &fakeHeadProvider{
		metas: map[string]*provider.ObjectMeta{
			"data/hot/a.xml": {ObjectSummary: provider.ObjectSummary{Key: "data/hot/a.xml"}, ContentType: "application/xml"},
		},
		errs: map[string]error{"data/hot/b.xml": provider.ErrAccessDenied},
	}
	res, err := Run(context.Background(), Config{
		IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: prov,
		SegmentSetRoot: seg, JournalRoot: filepath.Join(root, "journals", id), Parallel: 2,
	})
	require.Error(t, err)
	require.False(t, res.Published)
	require.False(t, res.LatestAdvanced)
	require.Equal(t, int64(0), res.Committed)
	require.Equal(t, int64(1), res.HeadSucceeded, "mixed failure must report successful HEAD observations")
	require.Equal(t, int64(1), res.Failed)
	after, err := os.ReadFile(latest)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestRunContentionSecondEnrichRejectsDeterministic(t *testing.T) {
	root := t.TempDir()
	id := "idx_dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	standard := "STANDARD"
	seg, latest := seedDurableParent(t, root, id, []indexsubstrate.CurrentObjectRow{
		{IndexSetID: id, RelKey: "hot/a.xml", SizeBytes: 10, ETag: `"a"`, StorageClass: &standard},
	})
	parentRun := mustLatestRun(t, latest)
	entered := make(chan struct{})
	block := make(chan struct{})
	provSlow := &fakeHeadProvider{entered: entered, block: block}
	provLoser := &fakeHeadProvider{}

	var firstRes Result
	var firstErr error
	var firstDone sync.WaitGroup
	firstDone.Add(1)
	go func() {
		defer firstDone.Done()
		firstRes, firstErr = Run(context.Background(), Config{
			IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: provSlow,
			SegmentSetRoot: seg, JournalRoot: filepath.Join(root, "journals", id, "first"), Parallel: 1,
		})
	}()
	// Wait until first owns lease and has entered HEAD (no sleep).
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first enrich never entered HEAD")
	}

	_, secondErr := Run(context.Background(), Config{
		IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: provLoser,
		SegmentSetRoot: seg, JournalRoot: filepath.Join(root, "journals", id, "second"), Parallel: 1,
	})
	require.ErrorIs(t, secondErr, indexcoord.ErrHeld)
	require.Equal(t, int64(0), provLoser.headCalls.Load(), "loser must not perform HEAD")

	close(block)
	firstDone.Wait()
	require.NoError(t, firstErr)
	require.True(t, firstRes.LatestAdvanced)
	require.Equal(t, firstRes.RunID, mustLatestRun(t, latest))
	require.NotEqual(t, parentRun, mustLatestRun(t, latest))
}

func TestRunPostLatestHookFailureStillCommitted(t *testing.T) {
	root := t.TempDir()
	id := "idx_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	standard := "STANDARD"
	seg, latest := seedDurableParent(t, root, id, []indexsubstrate.CurrentObjectRow{
		{IndexSetID: id, RelKey: "hot/a.xml", SizeBytes: 10, ETag: `"a"`, StorageClass: &standard},
	})
	prov := &fakeHeadProvider{}
	res, err := runWithHooks(context.Background(), Config{
		IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: prov,
		SegmentSetRoot: seg, JournalRoot: filepath.Join(root, "journals", id), Parallel: 1,
	}, runHooks{
		afterPublishStep: func(step string) error {
			if step == string(indexsubstrate.PublishStepLatestAdvanced) {
				return fmt.Errorf("injected post-latest")
			}
			return nil
		},
	})
	require.Error(t, err)
	require.True(t, res.LatestAdvanced)
	require.True(t, res.Published)
	require.Equal(t, int64(1), res.Committed)
	_, _, readErr := indexsubstrate.ReadLatestPublishedRows(latest)
	require.NoError(t, readErr)
	require.Equal(t, res.RunID, mustLatestRun(t, latest))
}

func TestRunCancellationDuringHeadLeavesLatestUnchanged(t *testing.T) {
	root := t.TempDir()
	id := "idx_ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	standard := "STANDARD"
	seg, latest := seedDurableParent(t, root, id, []indexsubstrate.CurrentObjectRow{
		{IndexSetID: id, RelKey: "hot/a.xml", SizeBytes: 10, ETag: `"a"`, StorageClass: &standard},
	})
	before, err := os.ReadFile(latest)
	require.NoError(t, err)
	entered := make(chan struct{})
	block := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	var runErr error
	var res Result
	go func() {
		defer wg.Done()
		res, runErr = Run(ctx, Config{
			IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: &fakeHeadProvider{entered: entered, block: block},
			SegmentSetRoot: seg, JournalRoot: filepath.Join(root, "journals", id), Parallel: 1,
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("never entered HEAD")
	}
	cancel()
	close(block)
	wg.Wait()
	require.Error(t, runErr)
	require.False(t, res.LatestAdvanced)
	require.Equal(t, int64(0), res.Committed)
	after, err := os.ReadFile(latest)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestRunPreLatestPublishFailureLeavesLatestUnchanged(t *testing.T) {
	root := t.TempDir()
	id := "idx_1111111111111111111111111111111111111111111111111111111111111111"
	standard := "STANDARD"
	seg, latest := seedDurableParent(t, root, id, []indexsubstrate.CurrentObjectRow{
		{IndexSetID: id, RelKey: "hot/a.xml", SizeBytes: 10, ETag: `"a"`, StorageClass: &standard},
	})
	before, err := os.ReadFile(latest)
	require.NoError(t, err)
	parentRun := mustLatestRun(t, latest)
	for _, step := range []string{
		string(indexsubstrate.PublishStepSegmentsWritten),
		string(indexsubstrate.PublishStepManifestWritten),
		string(indexsubstrate.PublishStepCompleteWritten),
	} {
		step := step
		res, err := runWithHooks(context.Background(), Config{
			IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: &fakeHeadProvider{},
			SegmentSetRoot: seg, JournalRoot: filepath.Join(root, "journals", id, step), Parallel: 1,
		}, runHooks{
			afterPublishStep: func(got string) error {
				if got == step {
					return fmt.Errorf("injected pre-latest %s", step)
				}
				return nil
			},
		})
		require.Error(t, err, step)
		require.False(t, res.LatestAdvanced, step)
		require.Equal(t, int64(0), res.Committed, step)
		after, readErr := os.ReadFile(latest)
		require.NoError(t, readErr, step)
		require.Equal(t, before, after, step)
		require.Equal(t, parentRun, mustLatestRun(t, latest), step)
	}
}

func TestRunAllResumeSkippedNoPublish(t *testing.T) {
	root := t.TempDir()
	id := "idx_2222222222222222222222222222222222222222222222222222222222222222"
	standard := "STANDARD"
	enriched := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	seg, latest := seedDurableParent(t, root, id, []indexsubstrate.CurrentObjectRow{
		{IndexSetID: id, RelKey: "hot/a.xml", SizeBytes: 10, ETag: `"a"`, StorageClass: &standard, HeadEnrichedAt: &enriched},
	})
	before, err := os.ReadFile(latest)
	require.NoError(t, err)
	prov := &fakeHeadProvider{}
	res, err := Run(context.Background(), Config{
		IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: prov,
		SegmentSetRoot: seg, JournalRoot: filepath.Join(root, "journals", id),
		Resume: true, Parallel: 1,
	})
	require.NoError(t, err)
	require.False(t, res.Published)
	require.Equal(t, int64(1), res.ResumeSkipped)
	require.Equal(t, int64(0), res.Committed)
	require.Equal(t, int64(0), prov.headCalls.Load())
	after, err := os.ReadFile(latest)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestRunPreservesTombstonesAndNonHeadFields(t *testing.T) {
	root := t.TempDir()
	id := "idx_3333333333333333333333333333333333333333333333333333333333333333"
	standard := "STANDARD"
	deletedAt := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	seg, latest := seedDurableParent(t, root, id, []indexsubstrate.CurrentObjectRow{
		{IndexSetID: id, RelKey: "hot/alive.xml", SizeBytes: 10, ETag: `"alive"`, StorageClass: &standard},
		{IndexSetID: id, RelKey: "hot/gone.xml", SizeBytes: 20, ETag: `"gone"`, StorageClass: &standard, DeletedAt: &deletedAt},
	})
	res, err := Run(context.Background(), Config{
		IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: &fakeHeadProvider{},
		SegmentSetRoot: seg, JournalRoot: filepath.Join(root, "journals", id),
		Query: QueryOptions{Pattern: "hot/alive.xml"}, Parallel: 1,
	})
	require.NoError(t, err)
	require.True(t, res.Published)
	_, rows, err := indexsubstrate.ReadLatestPublishedRows(latest)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	by := map[string]indexsubstrate.CurrentObjectRow{}
	for _, r := range rows {
		by[r.RelKey] = r
	}
	require.NotNil(t, by["hot/alive.xml"].HeadEnrichedAt)
	require.Equal(t, int64(10), by["hot/alive.xml"].SizeBytes)
	require.Equal(t, `"alive"`, by["hot/alive.xml"].ETag)
	require.NotNil(t, by["hot/gone.xml"].DeletedAt)
	require.True(t, by["hot/gone.xml"].DeletedAt.Equal(deletedAt))
	require.Nil(t, by["hot/gone.xml"].HeadEnrichedAt)
	require.Equal(t, int64(20), by["hot/gone.xml"].SizeBytes)
}

func TestRunFailingStateSinkDoesNotAdvance(t *testing.T) {
	root := t.TempDir()
	id := "idx_4444444444444444444444444444444444444444444444444444444444444444"
	standard := "STANDARD"
	seg, latest := seedDurableParent(t, root, id, []indexsubstrate.CurrentObjectRow{
		{IndexSetID: id, RelKey: "hot/a.xml", SizeBytes: 10, ETag: `"a"`, StorageClass: &standard},
	})
	before, err := os.ReadFile(latest)
	require.NoError(t, err)
	res, err := Run(context.Background(), Config{
		IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: &fakeHeadProvider{},
		SegmentSetRoot: seg, JournalRoot: filepath.Join(root, "journals", id), Parallel: 1,
		StateSink: func(StateEvent) error { return fmt.Errorf("disk full") },
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "state sink")
	require.False(t, res.LatestAdvanced)
	require.Equal(t, int64(0), res.Committed)
	// HEAD succeeded before sink failure; observation-vs-commit truth is load-bearing.
	require.Equal(t, int64(1), res.HeadSucceeded)
	after, err := os.ReadFile(latest)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestRunThrottleCancelsRemainingCandidates(t *testing.T) {
	root := t.TempDir()
	id := "idx_7777777777777777777777777777777777777777777777777777777777777777"
	standard := "STANDARD"
	// Many candidates; first returns throttle (after retries), others would block.
	rows := make([]indexsubstrate.CurrentObjectRow, 0, 8)
	for i := 0; i < 8; i++ {
		rows = append(rows, indexsubstrate.CurrentObjectRow{
			IndexSetID: id, RelKey: fmt.Sprintf("hot/%d.xml", i), SizeBytes: 10,
			ETag: fmt.Sprintf(`"%d"`, i), StorageClass: &standard,
		})
	}
	seg, latest := seedDurableParent(t, root, id, rows)
	before, err := os.ReadFile(latest)
	require.NoError(t, err)

	prov := &countingThrottleProvider{throttleKey: "data/hot/0.xml"}
	res, err := Run(context.Background(), Config{
		IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: prov,
		SegmentSetRoot: seg, JournalRoot: filepath.Join(root, "journals", id),
		Parallel: 4,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, provider.ErrThrottled, "public error must preserve throttle sentinel")
	require.NotContains(t, err.Error(), "supersecret")
	require.False(t, res.LatestAdvanced)
	require.Equal(t, int64(0), res.Committed)
	// Circuit breaker: not all candidates should complete full HEAD work under cancel.
	// With 8 candidates and cancel after throttle exhaustion, calls are bounded.
	require.Less(t, prov.headCalls.Load(), int64(8*enrichMaxRetries),
		"cancel after throttle must not finish every candidate at max retries")
	after, err := os.ReadFile(latest)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

type countingThrottleProvider struct {
	throttleKey string
	headCalls   atomic.Int64
	mu          sync.Mutex
	seen        map[string]int
}

func (p *countingThrottleProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}
func (p *countingThrottleProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	p.headCalls.Add(1)
	p.mu.Lock()
	if p.seen == nil {
		p.seen = map[string]int{}
	}
	p.seen[key]++
	p.mu.Unlock()
	if key == p.throttleKey {
		return nil, fmt.Errorf("%w: retry later X-Amz-Signature=supersecret", provider.ErrThrottled)
	}
	// Other keys block until cancelled so fanout cancellation is observable.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Second):
		return &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key}, ContentType: "application/xml"}, nil
	}
}
func (p *countingThrottleProvider) Close() error { return nil }

func TestRunStateSinkSchemaFields(t *testing.T) {
	root := t.TempDir()
	id := "idx_5555555555555555555555555555555555555555555555555555555555555555"
	standard := "STANDARD"
	seg, _ := seedDurableParent(t, root, id, []indexsubstrate.CurrentObjectRow{
		{IndexSetID: id, RelKey: "hot/a.xml", SizeBytes: 10, ETag: `"a"`, StorageClass: &standard},
		{IndexSetID: id, RelKey: "hot/b.xml", SizeBytes: 20, ETag: `"b"`, StorageClass: &standard},
	})
	var events []StateEvent
	var mu sync.Mutex
	_, err := Run(context.Background(), Config{
		IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: &fakeHeadProvider{
			errs: map[string]error{"data/hot/b.xml": provider.ErrAccessDenied},
		},
		SegmentSetRoot: seg, JournalRoot: filepath.Join(root, "journals", id), Parallel: 1,
		StateSink: func(ev StateEvent) error {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
			return nil
		},
	})
	require.Error(t, err)
	require.Len(t, events, 2)
	by := map[string]StateEvent{}
	for _, e := range events {
		by[e.RelKey] = e
	}
	ok := by["hot/a.xml"]
	require.Equal(t, "success", ok.Status)
	require.Equal(t, id, ok.IndexSetID)
	require.Equal(t, "data/hot/a.xml", ok.FullKey)
	require.NotNil(t, ok.ContentType)
	require.GreaterOrEqual(t, ok.Attempts, 1)
	fail := by["hot/b.xml"]
	require.Equal(t, "failed", fail.Status)
	require.Equal(t, "access_denied", fail.ErrorCode)
	require.NotEmpty(t, fail.ErrorMessage)
}

func TestRunRedactsSecretsInErrors(t *testing.T) {
	root := t.TempDir()
	id := "idx_6666666666666666666666666666666666666666666666666666666666666666"
	standard := "STANDARD"
	seg, _ := seedDurableParent(t, root, id, []indexsubstrate.CurrentObjectRow{
		{IndexSetID: id, RelKey: "hot/a.xml", SizeBytes: 10, ETag: `"a"`, StorageClass: &standard},
	})
	secret := "https://example.com/obj?X-Amz-Signature=supersecret&token=abc"
	var sinkMsg string
	_, err := Run(context.Background(), Config{
		IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: &fakeHeadProvider{
			errs: map[string]error{"data/hot/a.xml": fmt.Errorf("head failed: %s", secret)},
		},
		SegmentSetRoot: seg, JournalRoot: filepath.Join(root, "journals", id), Parallel: 1,
		StateSink: func(ev StateEvent) error {
			sinkMsg = ev.ErrorMessage
			return nil
		},
	})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "supersecret")
	require.NotContains(t, sinkMsg, "supersecret")
	require.NotContains(t, sinkMsg, "token=abc")
}

func TestRunRequiresJournalRoot(t *testing.T) {
	_, err := Run(context.Background(), Config{
		IndexSetID: "idx_test", BaseURI: "s3://b/d/", Provider: &fakeHeadProvider{},
		SegmentSetRoot: t.TempDir(),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "journal root")
}

func TestPublicRunRejectsStableAuthorityAfterRootQuarantine(t *testing.T) {
	root := t.TempDir()
	id := "idx_7777777777777777777777777777777777777777777777777777777777777777"
	segmentRoot, _ := seedDurableParent(t, root, id, nil)
	quarantine := segmentRoot + ".quarantine"
	held, err := indexcoord.Acquire(context.Background(), segmentRoot, id, "gc")
	require.NoError(t, err)
	defer func() { _ = held.Release() }()
	require.NoError(t, os.Rename(segmentRoot, quarantine))

	_, err = Run(context.Background(), Config{
		IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: &fakeHeadProvider{},
		SegmentSetRoot: segmentRoot, JournalRoot: filepath.Join(root, "journals", id), Parallel: 1,
	})
	require.ErrorIs(t, err, indexcoord.ErrHeld)
	require.NoDirExists(t, segmentRoot)
	require.DirExists(t, quarantine)
}

func TestRunUsesAndPreservesCallerOwnedAuthority(t *testing.T) {
	root := t.TempDir()
	id := "idx_8888888888888888888888888888888888888888888888888888888888888888"
	segmentRoot, _ := seedDurableParent(t, root, id, nil)
	held, err := indexcoord.Acquire(context.Background(), segmentRoot, id, "embedder")
	require.NoError(t, err)
	defer func() { _ = held.Release() }()

	_, err = Run(context.Background(), Config{
		IndexSetID: id, BaseURI: "s3://bucket/data/", Provider: &fakeHeadProvider{},
		SegmentSetRoot: segmentRoot, JournalRoot: filepath.Join(root, "journals", id), Parallel: 1,
		Authority: held,
	})
	require.NoError(t, err)
	require.NoError(t, held.AssertHeldFor(id, segmentRoot))
	contender, err := indexcoord.Acquire(context.Background(), segmentRoot, id, "peer")
	require.ErrorIs(t, err, indexcoord.ErrHeld)
	require.Nil(t, contender)
}

func TestBuildRejectsWhileEnrichHoldsLease(t *testing.T) {
	root := t.TempDir()
	id := "idx_test"
	// Build uses LatestPath parent dir as segment root for the lease.
	segRoot := filepath.Join(root, "set")
	require.NoError(t, os.MkdirAll(segRoot, 0o700))
	// Hold set lease as enrich would.
	held, err := indexsubstrate.AcquireWriteLease(segRoot, id, "enrich-peer", 0)
	require.NoError(t, err)
	defer func() { _ = held.Release() }()

	base := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	listProv := &countingListProvider{objects: []provider.ObjectSummary{
		{Key: "data/a.xml", Size: 1, ETag: `"a"`, LastModified: base},
	}}
	sink := &countingObservationSink{}
	cfg := indexbuild.Config{
		IndexSetID: id,
		RunID:      "run_build1",
		BaseURI:    "s3://bucket/data/",
		Source:     indexbuild.Source{Provider: listProv, ProviderName: "s3"},
		Match:      indexbuild.MatchConfig{Includes: []string{"**"}},
		Paths: indexbuild.PathConfig{
			JournalDir:   filepath.Join(root, "journals"),
			SegmentDir:   filepath.Join(segRoot, "runs", "run_build1"),
			ManifestPath: filepath.Join(segRoot, "runs", "run_build1", "manifest.json"),
			CompletePath: filepath.Join(segRoot, "runs", "run_build1", "complete.json"),
			LatestPath:   filepath.Join(segRoot, "latest.json"),
			IndexDBDir:   filepath.Join(root, "indexes", id),
		},
		Coverage: []indexbuild.CoverageAttestation{{
			Scope: &indexbuild.Scope{Prefix: "data/"}, Basis: indexbuild.CoverageBasisConfirmed, Complete: true,
		}},
		ObservationSinks:     []output.Writer{sink},
		RunStartedAt:         base,
		CreatedAt:            base.Add(time.Minute),
		Clock:                func() time.Time { return base.Add(2 * time.Minute) },
		TargetRowsPerSegment: 100,
	}
	_, err = indexbuild.NewRunner(cfg).Build(context.Background())
	require.ErrorIs(t, err, indexsubstrate.ErrWriteLeaseHeld)
	require.Equal(t, int64(0), listProv.listCalls.Load(), "Build must not LIST under held lease")
	require.Equal(t, int64(0), sink.writes.Load(), "Build must not mutate observation sinks under held lease")
	require.NoFileExists(t, cfg.Paths.LatestPath)
}

func mustLatestRun(t *testing.T, latest string) string {
	t.Helper()
	snap, err := indexsubstrate.OpenLatestPublishedSnapshot(latest)
	require.NoError(t, err)
	return snap.Complete.RunID
}

type countingListProvider struct {
	objects   []provider.ObjectSummary
	listCalls atomic.Int64
}

func (p *countingListProvider) List(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	p.listCalls.Add(1)
	var out []provider.ObjectSummary
	for _, obj := range p.objects {
		if len(opts.Prefix) == 0 || len(obj.Key) >= len(opts.Prefix) && obj.Key[:len(opts.Prefix)] == opts.Prefix {
			out = append(out, obj)
		}
	}
	return &provider.ListResult{Objects: out}, nil
}
func (p *countingListProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}
func (p *countingListProvider) Close() error { return nil }

type countingObservationSink struct {
	writes atomic.Int64
}

func (s *countingObservationSink) WriteObject(context.Context, *output.ObjectRecord) error {
	s.writes.Add(1)
	return nil
}
func (s *countingObservationSink) WriteError(context.Context, *output.ErrorRecord) error { return nil }
func (s *countingObservationSink) WriteProgress(context.Context, *output.ProgressRecord) error {
	return nil
}
func (s *countingObservationSink) WriteSummary(context.Context, *output.SummaryRecord) error {
	return nil
}
func (s *countingObservationSink) WritePrefix(context.Context, *output.PrefixRecord) error {
	return nil
}
func (s *countingObservationSink) WritePreflight(context.Context, *output.PreflightRecord) error {
	return nil
}
func (s *countingObservationSink) WriteTransfer(context.Context, *output.TransferRecord) error {
	return nil
}
func (s *countingObservationSink) WriteSkip(context.Context, *output.SkipRecord) error { return nil }
func (s *countingObservationSink) Close() error                                        { return nil }
