package indexbuild

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// pagedProvider serves a fixed sequence of List pages in call order and keeps
// returning the last page if asked for more.
type pagedProvider struct {
	mu    sync.Mutex
	pages []provider.ListResult
	calls int
}

func (p *pagedProvider) List(_ context.Context, _ provider.ListOptions) (*provider.ListResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	i := p.calls
	if i >= len(p.pages) {
		i = len(p.pages) - 1
	}
	page := p.pages[i]
	p.calls++
	return &page, nil
}

func (*pagedProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (*pagedProvider) Close() error { return nil }

func TestBuildRefusesIncompleteListingWithoutPublishingOrTombstoning(t *testing.T) {
	at := time.Now().UTC().Add(-2 * time.Hour)
	a := obj("data/a.xml", `"a"`, 1, at)
	b := obj("data/b.xml", `"b"`, 1, at)

	for _, tc := range []struct {
		name  string
		pages []provider.ListResult
	}{
		{name: "truncated without token", pages: []provider.ListResult{
			{Objects: []provider.ObjectSummary{a}, IsTruncated: true},
		}},
		// Only the empty-token check can stop this one: a listing that kept
		// going would fetch a final page and complete, not hit a repeat.
		{name: "truncated without token then a clean last page", pages: []provider.ListResult{
			{Objects: []provider.ObjectSummary{a}, IsTruncated: true},
			{},
		}},
		{name: "token repeats immediately", pages: []provider.ListResult{
			{Objects: []provider.ObjectSummary{a}, IsTruncated: true, ContinuationToken: "t1"},
			{IsTruncated: true, ContinuationToken: "t1"},
		}},
		{name: "multi-step token cycle", pages: []provider.ListResult{
			{Objects: []provider.ObjectSummary{a}, IsTruncated: true, ContinuationToken: "A"},
			{IsTruncated: true, ContinuationToken: "B"},
			{IsTruncated: true, ContinuationToken: "A"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			_, err := NewRunner(contConfig(root, "run1", []provider.ObjectSummary{a, b}, at)).Build(context.Background())
			require.NoError(t, err)
			latestPath := filepath.Join(root, "latest.json")
			before, err := os.ReadFile(latestPath)
			require.NoError(t, err)

			cfg := contConfig(root, "run2", nil, at.Add(time.Hour))
			prov := &pagedProvider{pages: tc.pages}
			cfg.Source.Provider = prov
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err = NewRunner(cfg).Build(ctx)
			require.ErrorIs(t, err, provider.ErrIncompleteListing)
			require.NotErrorIs(t, err, context.DeadlineExceeded)
			require.LessOrEqual(t, prov.calls, len(tc.pages)+1, "listing must stop at the first bad page")

			_, statErr := os.Stat(cfg.Paths.CompletePath)
			require.True(t, os.IsNotExist(statErr), "no complete marker for a refused run")
			after, err := os.ReadFile(latestPath)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after), "latest must still select run1")
			_, rows, err := ReadLatest(latestPath)
			require.NoError(t, err)
			require.Len(t, rows, 2)
			for _, row := range rows {
				require.Nil(t, row.DeletedAt, "parent row %s must not be tombstoned", row.RelKey)
			}
		})
	}
}

func TestBuildFollowsAdvancingPages(t *testing.T) {
	at := time.Now().UTC().Add(-time.Hour)
	a := obj("data/a.xml", `"a"`, 1, at)
	b := obj("data/b.xml", `"b"`, 1, at)
	root := t.TempDir()
	cfg := contConfig(root, "run1", nil, at)
	cfg.Source.Provider = &pagedProvider{pages: []provider.ListResult{
		{Objects: []provider.ObjectSummary{a}, IsTruncated: true, ContinuationToken: "A"},
		{Objects: []provider.ObjectSummary{b}, IsTruncated: true, ContinuationToken: "B"},
		{},
	}}
	_, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)
	_, rows, err := ReadLatest(filepath.Join(root, "latest.json"))
	require.NoError(t, err)
	require.Len(t, rows, 2)
}
