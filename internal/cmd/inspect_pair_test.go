package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
)

type inspectPairMockProvider struct {
	meta      map[string]provider.ObjectMeta
	errs      map[string]error
	headCalls []string
}

func (p *inspectPairMockProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}

func (p *inspectPairMockProvider) Head(_ context.Context, key string) (*provider.ObjectMeta, error) {
	p.headCalls = append(p.headCalls, key)
	if err := p.errs[key]; err != nil {
		return nil, err
	}
	meta, ok := p.meta[key]
	if !ok {
		return nil, &provider.ProviderError{Op: "Head", Provider: provider.ProviderS3, Key: key, Err: provider.ErrNotFound}
	}
	return &meta, nil
}

func (p *inspectPairMockProvider) Close() error { return nil }

func TestInspectPairAuditVerdictsAndSummary(t *testing.T) {
	mock := &inspectPairMockProvider{
		meta: map[string]provider.ObjectMeta{
			"root/verified.txt":        {ObjectSummary: provider.ObjectSummary{Key: "root/verified.txt", Size: 10, ETag: "abc123"}},
			"root/etag-diff.txt":       {ObjectSummary: provider.ObjectSummary{Key: "root/etag-diff.txt", Size: 11, ETag: "other-2"}},
			"root/size-mismatch.txt":   {ObjectSummary: provider.ObjectSummary{Key: "root/size-mismatch.txt", Size: 12, ETag: "same"}},
			"root/overwritten.txt":     {ObjectSummary: provider.ObjectSummary{Key: "root/overwritten.txt", Size: 13, ETag: "over"}},
			"root/quarantined.txt":     {ObjectSummary: provider.ObjectSummary{Key: "root/quarantined.txt", Size: 14, ETag: "quar"}},
			"root/multipart-equal.txt": {ObjectSummary: provider.ObjectSummary{Key: "root/multipart-equal.txt", Size: 15, ETag: "abc-2"}},
		},
		errs: map[string]error{
			"root/error.txt": errors.New("transport unavailable"),
		},
	}
	input := strings.Join([]string{
		reflowLine(map[string]any{"status": "in_progress", "source_uri": "s3://src/a", "dest_uri": "s3://dst/root/ignored.txt", "source_size_bytes": 10}),
		reflowLine(map[string]any{"status": "planned", "source_uri": "s3://src/a", "dest_uri": "s3://dst/root/planned.txt", "source_size_bytes": 10}),
		reflowLine(map[string]any{"status": "complete", "source_uri": "s3://src/verified.txt", "dest_uri": "s3://dst/root/verified.txt", "source_size_bytes": 10, "source_etag": "abc123"}),
		reflowLine(map[string]any{"status": "complete", "source_uri": "s3://src/etag-diff.txt", "dest_uri": "s3://dst/root/etag-diff.txt", "source_size_bytes": 11, "source_etag": "source-2"}),
		reflowLine(map[string]any{"status": "complete", "source_uri": "s3://src/size-mismatch.txt", "dest_uri": "s3://dst/root/size-mismatch.txt", "source_size_bytes": 10, "source_etag": "same"}),
		reflowLine(map[string]any{"status": "complete", "source_uri": "s3://src/missing.txt", "dest_uri": "s3://dst/root/missing.txt", "source_size_bytes": 10}),
		reflowLine(map[string]any{"status": "complete", "source_uri": "s3://src/error.txt", "dest_uri": "s3://dst/root/error.txt", "source_size_bytes": 10}),
		reflowLine(map[string]any{"status": "complete", "source_uri": "s3://src/overwritten.txt", "dest_uri": "s3://dst/root/overwritten.txt", "source_size_bytes": 13, "source_etag": "over", "collision": map[string]any{"kind": "overwritten"}}),
		reflowLine(map[string]any{"status": "quarantined", "source_uri": "s3://src/quarantined.txt", "dest_uri": "s3://dst/root/quarantined.txt", "source_size_bytes": 14, "source_etag": "quar"}),
		reflowLine(map[string]any{"status": "complete", "source_uri": "s3://src/multipart-equal.txt", "dest_uri": "s3://dst/root/multipart-equal.txt", "source_size_bytes": 15, "source_etag": "abc-2"}),
		reflowLine(map[string]any{"status": "skipped", "source_uri": "s3://src/skipped.txt", "dest_uri": "s3://dst/root/skipped.txt", "source_size_bytes": 10, "reason": "collision.duplicate"}),
		otherLine("gonimbus.reflow.run.v1", map[string]any{"dest_uri": "s3://dst/root/"}),
	}, "\n") + "\n"

	var stdout bytes.Buffer
	err := runInspectPair(context.Background(), strings.NewReader(input), &stdout, inspectPairOptions{
		UseStdin:             true,
		ExpectedDestPrefixes: []string{"s3://dst/root/"},
		ProviderFactory: func(context.Context, inspectPairScope) (provider.Provider, error) {
			return mock, nil
		},
	})
	require.Error(t, err)

	records, summary := decodeInspectPairOutput(t, stdout.String())
	require.Len(t, records, 9)
	require.Equal(t, []string{
		"root/verified.txt",
		"root/etag-diff.txt",
		"root/size-mismatch.txt",
		"root/missing.txt",
		"root/error.txt",
		"root/overwritten.txt",
		"root/quarantined.txt",
		"root/multipart-equal.txt",
	}, mock.headCalls)

	byDest := map[string]inspectPairRecord{}
	for _, rec := range records {
		byDest[rec.DestURI] = rec
	}
	require.Equal(t, "verified", byDest["s3://dst/root/verified.txt"].Verdict)
	require.True(t, byDest["s3://dst/root/verified.txt"].ETagComparable)
	require.Equal(t, "verified_size_etag_differs", byDest["s3://dst/root/etag-diff.txt"].Verdict)
	require.False(t, byDest["s3://dst/root/etag-diff.txt"].ETagComparable)
	require.Equal(t, "size_mismatch", byDest["s3://dst/root/size-mismatch.txt"].Verdict)
	require.Equal(t, "missing", byDest["s3://dst/root/missing.txt"].Verdict)
	require.Equal(t, "error", byDest["s3://dst/root/error.txt"].Verdict)
	require.Equal(t, "verified", byDest["s3://dst/root/overwritten.txt"].Verdict)
	require.Equal(t, "verified", byDest["s3://dst/root/quarantined.txt"].Verdict)
	require.Equal(t, "verified", byDest["s3://dst/root/multipart-equal.txt"].Verdict)
	require.False(t, byDest["s3://dst/root/multipart-equal.txt"].ETagComparable)
	require.Equal(t, "not_verified", byDest["s3://dst/root/skipped.txt"].Verdict)
	require.Equal(t, "collision.duplicate", byDest["s3://dst/root/skipped.txt"].Reason)

	require.Equal(t, int64(9), summary.Total)
	require.Equal(t, int64(4), summary.Verified)
	require.Equal(t, int64(1), summary.VerifiedSizeETagDiffers)
	require.Equal(t, int64(1), summary.SizeMismatch)
	require.Equal(t, int64(1), summary.Missing)
	require.Equal(t, int64(1), summary.Error)
	require.Equal(t, int64(1), summary.NotVerified)
	require.Equal(t, int64(2), summary.IgnoredNonterminal)
}

func TestInspectPairRejectsOutOfScopeBeforeProviderConstruction(t *testing.T) {
	var factoryCalls int
	input := strings.Join([]string{
		reflowLine(map[string]any{"status": "complete", "source_uri": "s3://src/a", "dest_uri": "s3://other/root/a", "source_size_bytes": 1}),
		reflowLine(map[string]any{"status": "complete", "source_uri": "s3://src/b", "dest_uri": "s3://dst/other/b", "source_size_bytes": 1}),
		reflowLine(map[string]any{"status": "complete", "source_uri": "s3://src/c", "dest_uri": fileURI(filepath.Join(t.TempDir(), "c")), "source_size_bytes": 1}),
	}, "\n") + "\n"

	var stdout bytes.Buffer
	err := runInspectPair(context.Background(), strings.NewReader(input), &stdout, inspectPairOptions{
		UseStdin:             true,
		ExpectedDestPrefixes: []string{"s3://dst/root/"},
		ProviderFactory: func(context.Context, inspectPairScope) (provider.Provider, error) {
			factoryCalls++
			return &inspectPairMockProvider{}, nil
		},
	})
	require.Error(t, err)
	require.Equal(t, 0, factoryCalls)
	records, summary := decodeInspectPairOutput(t, stdout.String())
	require.Len(t, records, 3)
	for _, rec := range records {
		require.Equal(t, "invalid_dest", rec.Verdict)
	}
	require.Equal(t, int64(3), summary.InvalidDest)
}

func TestInspectPairAcceptsGCSExpectedDestScope(t *testing.T) {
	mock := &inspectPairMockProvider{
		meta: map[string]provider.ObjectMeta{
			"root/verified.txt": {ObjectSummary: provider.ObjectSummary{Key: "root/verified.txt", Size: 10, ETag: "abc123"}},
		},
		errs: map[string]error{},
	}
	input := reflowLine(map[string]any{
		"status":            "complete",
		"source_uri":        "s3://src/verified.txt",
		"dest_uri":          "gs://dst/root/verified.txt",
		"source_size_bytes": 10,
		"source_etag":       "abc123",
	}) + "\n"

	var gotScope inspectPairScope
	var stdout bytes.Buffer
	err := runInspectPair(context.Background(), strings.NewReader(input), &stdout, inspectPairOptions{
		UseStdin:             true,
		ExpectedDestPrefixes: []string{"gs://dst/root/"},
		ProviderFactory: func(_ context.Context, scope inspectPairScope) (provider.Provider, error) {
			gotScope = scope
			return mock, nil
		},
	})
	require.NoError(t, err)

	records, summary := decodeInspectPairOutput(t, stdout.String())
	require.Len(t, records, 1)
	require.Equal(t, "verified", records[0].Verdict)
	require.Equal(t, "gs://dst/root/", records[0].ExpectedDestPrefix)
	require.Equal(t, string(provider.ProviderGCS), gotScope.Provider)
	require.Equal(t, "dst", gotScope.Bucket)
	require.Equal(t, "root/", gotScope.Prefix)
	require.Equal(t, []string{"root/verified.txt"}, mock.headCalls)
	require.Equal(t, int64(1), summary.Verified)
}

func TestInspectPairFileSymlinkEscapeIsInvalidBeforeHead(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	require.NoError(t, osSymlinkOrSkip(t, outside, filepath.Join(root, "escape")))

	var factoryCalls int
	input := reflowLine(map[string]any{
		"status":            "complete",
		"source_uri":        "s3://src/secret.txt",
		"dest_uri":          fileURI(filepath.Join(root, "escape", "secret.txt")),
		"source_size_bytes": 1,
	}) + "\n"

	var stdout bytes.Buffer
	err := runInspectPair(context.Background(), strings.NewReader(input), &stdout, inspectPairOptions{
		UseStdin:             true,
		ExpectedDestPrefixes: []string{fileURI(root) + "/"},
		ProviderFactory: func(context.Context, inspectPairScope) (provider.Provider, error) {
			factoryCalls++
			return &inspectPairMockProvider{}, nil
		},
	})
	require.Error(t, err)
	require.Equal(t, 0, factoryCalls)
	records, summary := decodeInspectPairOutput(t, stdout.String())
	require.Len(t, records, 1)
	require.Equal(t, "invalid_dest", records[0].Verdict)
	require.Equal(t, int64(1), summary.InvalidDest)
}

func TestInspectPairFileBrokenSymlinkEscapeIsInvalidBeforeHead(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	require.NoError(t, osSymlinkOrSkip(t, filepath.Join(outside, "missing.txt"), filepath.Join(root, "link.txt")))

	var factoryCalls int
	input := reflowLine(map[string]any{
		"status":            "complete",
		"source_uri":        "s3://src/missing.txt",
		"dest_uri":          fileURI(filepath.Join(root, "link.txt")),
		"source_size_bytes": 1,
	}) + "\n"

	var stdout bytes.Buffer
	err := runInspectPair(context.Background(), strings.NewReader(input), &stdout, inspectPairOptions{
		UseStdin:             true,
		ExpectedDestPrefixes: []string{fileURI(root) + "/"},
		ProviderFactory: func(context.Context, inspectPairScope) (provider.Provider, error) {
			factoryCalls++
			return &inspectPairMockProvider{}, nil
		},
	})
	require.Error(t, err)
	require.Equal(t, 0, factoryCalls)
	records, summary := decodeInspectPairOutput(t, stdout.String())
	require.Len(t, records, 1)
	require.Equal(t, "invalid_dest", records[0].Verdict)
	require.Equal(t, int64(1), summary.InvalidDest)
}

func TestInspectPairFileOrdinaryMissingPathStaysInspectable(t *testing.T) {
	root := t.TempDir()
	mock := &inspectPairMockProvider{
		meta: map[string]provider.ObjectMeta{},
		errs: map[string]error{},
	}
	input := reflowLine(map[string]any{
		"status":            "complete",
		"source_uri":        "s3://src/missing.txt",
		"dest_uri":          fileURI(filepath.Join(root, "missing.txt")),
		"source_size_bytes": 1,
	}) + "\n"

	var stdout bytes.Buffer
	err := runInspectPair(context.Background(), strings.NewReader(input), &stdout, inspectPairOptions{
		UseStdin:             true,
		ExpectedDestPrefixes: []string{fileURI(root) + "/"},
		ProviderFactory: func(context.Context, inspectPairScope) (provider.Provider, error) {
			return mock, nil
		},
	})
	require.Error(t, err)
	require.Equal(t, []string{"missing.txt"}, mock.headCalls)
	records, summary := decodeInspectPairOutput(t, stdout.String())
	require.Len(t, records, 1)
	require.Equal(t, "missing", records[0].Verdict)
	require.Equal(t, int64(1), summary.Missing)
}

func TestInspectPairFlagValidation(t *testing.T) {
	tests := []struct {
		name string
		opts inspectPairOptions
	}{
		{name: "neither input", opts: inspectPairOptions{ExpectedDestPrefixes: []string{"s3://dst/root/"}}},
		{name: "both inputs", opts: inspectPairOptions{UseStdin: true, FromReflow: "audit.jsonl", ExpectedDestPrefixes: []string{"s3://dst/root/"}}},
		{name: "missing scope", opts: inspectPairOptions{UseStdin: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runInspectPair(context.Background(), strings.NewReader(""), ioDiscard{}, tt.opts)
			require.Error(t, err)
		})
	}
}

func reflowLine(data map[string]any) string {
	return otherLine(reflowpkg.RecordType, data)
}

func otherLine(recordType string, data map[string]any) string {
	payload, err := json.Marshal(map[string]any{"type": recordType, "data": data})
	if err != nil {
		panic(err)
	}
	return string(payload)
}

func decodeInspectPairOutput(t *testing.T, stdout string) ([]inspectPairRecord, inspectPairSummary) {
	t.Helper()
	var records []inspectPairRecord
	var summary inspectPairSummary
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var env output.Record
		require.NoError(t, json.Unmarshal([]byte(line), &env))
		switch env.Type {
		case inspectPairRecordType:
			var rec inspectPairRecord
			require.NoError(t, json.Unmarshal(env.Data, &rec))
			records = append(records, rec)
		case inspectPairSummaryType:
			require.NoError(t, json.Unmarshal(env.Data, &summary))
		default:
			t.Fatalf("unexpected record type %q", env.Type)
		}
	}
	return records, summary
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }

func osSymlinkOrSkip(t *testing.T, oldname, newname string) error {
	t.Helper()
	err := os.Symlink(oldname, newname)
	if err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	return nil
}
