package cmd

import (
	"context"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

type mockDelimiterLister struct {
	results []*provider.ListWithDelimiterResult
	calls   int
}

func (m *mockDelimiterLister) ListWithDelimiter(ctx context.Context, opts provider.ListWithDelimiterOptions) (*provider.ListWithDelimiterResult, error) {
	idx := m.calls
	m.calls++
	if idx >= len(m.results) {
		return &provider.ListWithDelimiterResult{Objects: nil, CommonPrefixes: nil, IsTruncated: false}, nil
	}
	return m.results[idx], nil
}

func TestSummarizeDirectPrefix_MaxObjectsTruncates(t *testing.T) {
	l := &mockDelimiterLister{results: []*provider.ListWithDelimiterResult{{
		Objects:        []provider.ObjectSummary{{Key: "a", Size: 1}, {Key: "b", Size: 2}, {Key: "c", Size: 3}},
		CommonPrefixes: []string{"p1/", "p2/"},
		IsTruncated:    false,
	}}}

	rec, _, err := summarizeDirectPrefix(context.Background(), l, "data/", "/", 2, 10, false)
	require.NoError(t, err)
	require.Equal(t, int64(2), rec.ObjectsDirect)
	require.Equal(t, int64(3), rec.BytesDirect)
	require.Equal(t, int64(2), rec.CommonPrefixes)
	require.Equal(t, int64(1), rec.Pages)
	require.True(t, rec.Truncated)
	require.Equal(t, "max-objects", rec.TruncatedReason)
}

func TestSummarizeDirectPrefix_MaxPagesTruncates(t *testing.T) {
	l := &mockDelimiterLister{results: []*provider.ListWithDelimiterResult{
		{Objects: []provider.ObjectSummary{{Key: "a", Size: 1}}, CommonPrefixes: []string{"p1/"}, IsTruncated: true, ContinuationToken: "t1"},
		{Objects: []provider.ObjectSummary{{Key: "b", Size: 2}}, CommonPrefixes: []string{"p2/"}, IsTruncated: false},
	}}

	rec, _, err := summarizeDirectPrefix(context.Background(), l, "data/", "/", 100, 1, false)
	require.NoError(t, err)
	require.Equal(t, int64(1), rec.ObjectsDirect)
	require.Equal(t, int64(1), rec.BytesDirect)
	require.Equal(t, int64(1), rec.CommonPrefixes)
	require.Equal(t, int64(1), rec.Pages)
	require.True(t, rec.Truncated)
	require.Equal(t, "max-pages", rec.TruncatedReason)
}

func TestOutputTreeTable_DoesNotError(t *testing.T) {
	rec := &output.PrefixRecord{Prefix: "data/", Delimiter: "/", ObjectsDirect: 1, BytesDirect: 1024, CommonPrefixes: 2, Pages: 1}
	require.NoError(t, outputTreeTable(rec))
}

type mapDelimiterLister struct {
	results map[string]*provider.ListWithDelimiterResult
}

func (m *mapDelimiterLister) ListWithDelimiter(ctx context.Context, opts provider.ListWithDelimiterOptions) (*provider.ListWithDelimiterResult, error) {
	if res, ok := m.results[opts.Prefix]; ok {
		return res, nil
	}
	return &provider.ListWithDelimiterResult{Objects: nil, CommonPrefixes: nil, IsTruncated: false}, nil
}

func TestTraversePrefixes_DepthOne(t *testing.T) {
	l := &mapDelimiterLister{results: map[string]*provider.ListWithDelimiterResult{
		"root/": {CommonPrefixes: []string{"a/", "b/"}},
		"a/":    {Objects: []provider.ObjectSummary{{Key: "a/file", Size: 1}}},
		"b/":    {Objects: []provider.ObjectSummary{{Key: "b/file", Size: 2}}},
	}}

	var recs []*output.PrefixRecord
	err := traversePrefixes(
		context.Background(),
		l,
		"root/",
		"/",
		1,
		1000,
		1000,
		100,
		2,
		func(prefix string) bool { return true },
		func(rec *output.PrefixRecord) error {
			recs = append(recs, rec)
			return nil
		},
		func(n int) {},
		func(reason string) {},
	)
	require.NoError(t, err)
	require.Len(t, recs, 3)

	// Root is depth 0; children are depth 1.
	depths := map[string]int{}
	for _, r := range recs {
		depths[r.Prefix] = r.Depth
	}
	require.Equal(t, 0, depths["root/"])
	require.Equal(t, 1, depths["a/"])
	require.Equal(t, 1, depths["b/"])
}

func TestTraversePrefixes_MaxPrefixesStopsExpansion(t *testing.T) {
	l := &mapDelimiterLister{results: map[string]*provider.ListWithDelimiterResult{
		"root/": {CommonPrefixes: []string{"a/", "b/", "c/"}},
		"a/":    {Objects: []provider.ObjectSummary{{Key: "a/file", Size: 1}}},
		"b/":    {Objects: []provider.ObjectSummary{{Key: "b/file", Size: 2}}},
		"c/":    {Objects: []provider.ObjectSummary{{Key: "c/file", Size: 3}}},
	}}

	var (
		recs    []*output.PrefixRecord
		reasons []string
	)

	err := traversePrefixes(
		context.Background(),
		l,
		"root/",
		"/",
		1,
		1000,
		1000,
		2, // root + 1 child
		2,
		func(prefix string) bool { return true },
		func(rec *output.PrefixRecord) error {
			recs = append(recs, rec)
			return nil
		},
		func(n int) {},
		func(reason string) { reasons = append(reasons, reason) },
	)
	require.NoError(t, err)
	// Should have processed root and exactly one child.
	require.Len(t, recs, 2)
	require.Contains(t, reasons, "max-prefixes")
}

func TestTraversePrefixes_AllowPrefixFiltersChildren(t *testing.T) {
	l := &mapDelimiterLister{results: map[string]*provider.ListWithDelimiterResult{
		"root/": {CommonPrefixes: []string{"a/", "b/"}},
		"a/":    {Objects: []provider.ObjectSummary{{Key: "a/file", Size: 1}}},
		"b/":    {Objects: []provider.ObjectSummary{{Key: "b/file", Size: 2}}},
	}}

	var recs []*output.PrefixRecord
	err := traversePrefixes(
		context.Background(),
		l,
		"root/",
		"/",
		1,
		1000,
		1000,
		100,
		2,
		func(prefix string) bool { return strings.HasPrefix(prefix, "a/") || prefix == "root/" },
		func(rec *output.PrefixRecord) error {
			recs = append(recs, rec)
			return nil
		},
		func(n int) {},
		func(reason string) {},
	)
	require.NoError(t, err)
	require.Len(t, recs, 2)
}
