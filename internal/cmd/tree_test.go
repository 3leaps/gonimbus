package cmd

import (
	"context"
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

	rec, err := summarizeDirectPrefix(context.Background(), l, "data/", "/", 2, 10)
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

	rec, err := summarizeDirectPrefix(context.Background(), l, "data/", "/", 100, 1)
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
