package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
)

func TestAdvanceLatestPointerWritesWhenMissing(t *testing.T) {
	ctx := context.Background()
	hub, fp := setupLatestCASTestHub(t, []completeFixture{
		{runID: "run_100", completedAt: "2026-01-01T00:00:00Z"},
	}, "")

	var events bytes.Buffer
	outcome, err := advanceLatestPointer(ctx, hub, fp, fp, testFullIndexSetID, "run_100", latestPointerOptions{
		Mode:      latestWriteModeConditional,
		RetryMax:  defaultLatestRetryMax,
		RetryBase: 0,
		Events:    &events,
	})
	require.NoError(t, err)
	require.Equal(t, latestPointerUpdated, outcome)
	require.Empty(t, events.String())

	got := readLatestRunForTest(t, hub.BaseDir)
	require.Equal(t, "run_100", got)
}

func TestAdvanceLatestPointerAdvancesWhenCandidateNewer(t *testing.T) {
	ctx := context.Background()
	hub, fp := setupLatestCASTestHub(t, []completeFixture{
		{runID: "run_100", completedAt: "2026-01-01T00:00:00Z"},
		{runID: "run_200", completedAt: "2026-02-01T00:00:00Z"},
	}, "run_100")

	outcome, err := advanceLatestPointer(ctx, hub, fp, fp, testFullIndexSetID, "run_200", latestPointerOptions{
		Mode:      latestWriteModeConditional,
		RetryMax:  defaultLatestRetryMax,
		RetryBase: 0,
		Events:    ioDiscardForTest{},
	})
	require.NoError(t, err)
	require.Equal(t, latestPointerUpdated, outcome)
	require.Equal(t, "run_200", readLatestRunForTest(t, hub.BaseDir))
}

func TestAdvanceLatestPointerYieldsWhenCurrentNewer(t *testing.T) {
	ctx := context.Background()
	hub, fp := setupLatestCASTestHub(t, []completeFixture{
		{runID: "run_100", completedAt: "2026-01-01T00:00:00Z"},
		{runID: "run_200", completedAt: "2026-02-01T00:00:00Z"},
	}, "run_200")

	var events bytes.Buffer
	outcome, err := advanceLatestPointer(ctx, hub, fp, fp, testFullIndexSetID, "run_100", latestPointerOptions{
		Mode:      latestWriteModeConditional,
		RetryMax:  defaultLatestRetryMax,
		RetryBase: 0,
		Events:    &events,
	})
	require.NoError(t, err)
	require.Equal(t, latestPointerYielded, outcome)
	require.Equal(t, "run_200", readLatestRunForTest(t, hub.BaseDir))
	require.Contains(t, events.String(), casYieldRecordType)
}

func TestAdvanceLatestPointerRetryExhaustedFailsClosed(t *testing.T) {
	ctx := context.Background()
	hub, fp := setupLatestCASTestHub(t, []completeFixture{
		{runID: "run_100", completedAt: "2026-01-01T00:00:00Z"},
	}, "")
	putter := alwaysConflictPutter{ObjectPutter: fp}

	var events bytes.Buffer
	_, err := advanceLatestPointer(ctx, hub, fp, putter, testFullIndexSetID, "run_100", latestPointerOptions{
		Mode:      latestWriteModeConditional,
		RetryMax:  1,
		RetryBase: 0,
		Events:    &events,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "CAS conflict budget exhausted")
	require.Contains(t, events.String(), casRetryRecordType)
	require.Contains(t, events.String(), casFailRecordType)
}

func TestAdvanceLatestPointerRetriesThenAdvances(t *testing.T) {
	ctx := context.Background()
	hub, fp := setupLatestCASTestHub(t, []completeFixture{
		{runID: "run_100", completedAt: "2026-01-01T00:00:00Z"},
		{runID: "run_150", completedAt: "2026-01-15T00:00:00Z"},
		{runID: "run_200", completedAt: "2026-02-01T00:00:00Z"},
	}, "run_100")
	putter := conflictOncePutter{
		provider:   fp,
		hub:        hub,
		indexSetID: testFullIndexSetID,
		conflictID: "run_150",
	}

	var events bytes.Buffer
	outcome, err := advanceLatestPointer(ctx, hub, fp, &putter, testFullIndexSetID, "run_200", latestPointerOptions{
		Mode:      latestWriteModeConditional,
		RetryMax:  2,
		RetryBase: 0,
		Events:    &events,
	})
	require.NoError(t, err)
	require.Equal(t, latestPointerUpdated, outcome)
	require.Contains(t, events.String(), casRetryRecordType)
	require.Equal(t, "run_200", readLatestRunForTest(t, hub.BaseDir))
}

func TestCompareCompleteDocsUsesRunIDTieBreaker(t *testing.T) {
	a := hubCompleteDoc{RunID: "run_200", CompletedAt: "2026-01-01T00:00:00Z"}
	b := hubCompleteDoc{RunID: "run_100", CompletedAt: "2026-01-01T00:00:00Z"}
	require.Positive(t, compareCompleteDocs(a, b))
}

type completeFixture struct {
	runID       string
	completedAt string
}

func setupLatestCASTestHub(t *testing.T, runs []completeFixture, latestRun string) (*hubDestSpec, *providerfile.Provider) {
	t.Helper()
	baseDir := t.TempDir()
	hub := &hubDestSpec{Provider: string(provider.ProviderFile), BaseDir: baseDir}
	for _, run := range runs {
		runDir := filepath.Join(baseDir, "index-sets", testFullIndexSetID, "runs", run.runID)
		require.NoError(t, os.MkdirAll(runDir, 0o755))
		complete := map[string]any{
			"version":      "1.0",
			"index_set_id": testFullIndexSetID,
			"run_id":       run.runID,
			"completed_at": run.completedAt,
		}
		data, err := json.MarshalIndent(complete, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(runDir, "complete.json"), data, 0o644))
	}
	if latestRun != "" {
		latestJSON, err := buildLatestJSONForRun(testFullIndexSetID, latestRun)
		require.NoError(t, err)
		latestDir := filepath.Join(baseDir, "index-sets", testFullIndexSetID)
		require.NoError(t, os.WriteFile(filepath.Join(latestDir, "latest.json"), latestJSON, 0o644))
	}
	fp, err := providerfile.New(providerfile.Config{BaseDir: baseDir})
	require.NoError(t, err)
	return hub, fp
}

func readLatestRunForTest(t *testing.T, baseDir string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(baseDir, "index-sets", testFullIndexSetID, "latest.json"))
	require.NoError(t, err)
	var latest latestPointerDoc
	require.NoError(t, json.Unmarshal(data, &latest))
	return latest.RunID
}

type alwaysConflictPutter struct {
	provider.ObjectPutter
}

func (p alwaysConflictPutter) PutObjectConditional(context.Context, string, io.Reader, int64, provider.PutPrecondition) (provider.PutResult, error) {
	return provider.PutResult{}, &provider.ProviderError{Op: "PutObjectConditional", Provider: provider.ProviderFile, Err: provider.ErrAlreadyExists}
}

type conflictOncePutter struct {
	provider   *providerfile.Provider
	hub        *hubDestSpec
	indexSetID string
	conflictID string
	conflicted bool
}

func (p *conflictOncePutter) PutObject(ctx context.Context, key string, body io.Reader, contentLength int64) error {
	return p.provider.PutObject(ctx, key, body, contentLength)
}

func (p *conflictOncePutter) PutObjectConditional(ctx context.Context, key string, body io.Reader, contentLength int64, precond provider.PutPrecondition) (provider.PutResult, error) {
	if !p.conflicted {
		p.conflicted = true
		latestJSON, err := buildLatestJSONForRun(p.indexSetID, p.conflictID)
		if err != nil {
			return provider.PutResult{}, err
		}
		latestKey := hubArtifactKey(p.hub, "index-sets", p.indexSetID, "latest.json")
		if err := p.provider.PutObject(ctx, latestKey, bytes.NewReader(latestJSON), int64(len(latestJSON))); err != nil {
			return provider.PutResult{}, err
		}
		return provider.PutResult{}, &provider.ProviderError{Op: "PutObjectConditional", Provider: provider.ProviderFile, Key: key, Err: provider.ErrPreconditionFailed}
	}
	return p.provider.PutObjectConditional(ctx, key, body, contentLength, precond)
}

type ioDiscardForTest struct{}

func (ioDiscardForTest) Write(p []byte) (int, error) {
	return len(p), nil
}

var _ provider.ConditionalPutter = alwaysConflictPutter{}
var _ provider.ConditionalPutter = (*conflictOncePutter)(nil)
var _ provider.ObjectPutter = (*conflictOncePutter)(nil)
var _ interface{ Write([]byte) (int, error) } = ioDiscardForTest{}
