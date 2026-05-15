package atlas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/stretchr/testify/require"
)

func TestBuildWritesAtlasRowsHeaderDiagnosticsAndStats(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	reader := fakeGetter{
		"prefix/a.json":   []byte(`{"event_date":"2026-05-01","tenant":"acme"}`),
		"prefix/b.json":   []byte(`{"event_date":"2026-05-01","tenant":"acme"}`),
		"prefix/c.json":   []byte(`{"event_date":"2026-05-02","tenant":"beta"}`),
		"prefix/bad.json": []byte(`{"tenant":"missing-date"}`),
	}
	out := filepath.Join(t.TempDir(), "atlas")

	result, err := Build(context.Background(), BuildOptions{
		Source: SourceRun{
			IndexSetID:  "idx_test",
			RunID:       "run_test",
			BaseURI:     "s3://bucket/prefix/",
			ScopeDigest: "scope123",
			Coverage:    CoverageFull,
			Objects: []SourceObject{
				{RelKey: "a.json", SizeBytes: int64(len(reader["prefix/a.json"])), LastSeenRunID: "run_test", LastSeenAt: now},
				{RelKey: "b.json", SizeBytes: int64(len(reader["prefix/b.json"])), LastSeenRunID: "run_test", LastSeenAt: now},
				{RelKey: "c.json", SizeBytes: int64(len(reader["prefix/c.json"])), LastSeenRunID: "run_test", LastSeenAt: now},
				{RelKey: "missing.json", LastSeenRunID: "run_test", LastSeenAt: now},
				{RelKey: "bad.json", SizeBytes: int64(len(reader["prefix/bad.json"])), LastSeenRunID: "run_test", LastSeenAt: now},
			},
		},
		Recipe:    testRecipe(),
		Reader:    reader,
		OutputDir: out,
		Now: func() time.Time {
			return now
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(5), result.Header.Counts.ObjectsScanned)
	require.Equal(t, int64(3), result.Header.Counts.RowsWritten)
	require.Equal(t, int64(2), result.Header.Counts.Diagnostics)
	require.Equal(t, CoverageFull, result.Header.Coverage)
	require.Equal(t, "scope123", result.Header.ScopeDigest)

	header, err := ReadHeader(out)
	require.NoError(t, err)
	require.Equal(t, result.Header.AtlasID, header.AtlasID)
	require.Equal(t, SchemaVersion, header.SchemaVersion)
	require.Equal(t, []string{"event_date"}, header.ShardBy)

	rows := readRows(t, filepath.Join(out, ShardsDir, "2026-05-01.jsonl"))
	require.Len(t, rows, 2)
	require.Equal(t, "prefix/a.json", rows[0].StorageKey)
	require.Equal(t, shaHex(reader["prefix/a.json"]), rows[0].ContentHash)
	require.Equal(t, "2026-05-01", rows[0].Dimensions["event_date"])
	require.Equal(t, "acme", rows[0].Dimensions["tenant"])

	stats, err := ComputeStats(out)
	require.NoError(t, err)
	require.Equal(t, int64(3), stats.Tier1Keys)
	require.Equal(t, int64(2), stats.Tier2Content)
	require.Equal(t, int64(2), stats.Tier3Shards)
	require.Equal(t, int64(2), stats.Diagnostics)
	require.Equal(t, 2, stats.ShardFiles)

	diagnostics := readDiagnostics(t, filepath.Join(out, DiagnosticsFile))
	require.Len(t, diagnostics, 2)
	require.Equal(t, "get_object_failed", diagnostics[0].Code)
	require.Equal(t, "required_dimension_unresolved", diagnostics[1].Code)
}

func TestBuildRejectsNonEmptyOutputDir(t *testing.T) {
	out := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(out, "existing"), []byte("x"), 0644))

	_, err := Build(context.Background(), BuildOptions{
		Source:    SourceRun{IndexSetID: "idx", RunID: "run", BaseURI: "s3://bucket/", Objects: []SourceObject{{RelKey: "a.json"}}},
		Recipe:    testRecipe(),
		Reader:    fakeGetter{"a.json": []byte(`{"event_date":"2026-05-01","tenant":"acme"}`)},
		OutputDir: out,
	})
	require.ErrorContains(t, err, "must be empty")
}

type fakeGetter map[string][]byte

func (g fakeGetter) GetObject(_ context.Context, key string) (io.ReadCloser, int64, error) {
	data, ok := g[key]
	if !ok {
		return nil, 0, fmt.Errorf("not found: %s", key)
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

func testRecipe() Recipe {
	return Recipe{
		Coverage: CoverageFull,
		Dimensions: []DimensionRecipe{
			{
				Name:           "event_date",
				Kind:           DimensionTemporalDay,
				Classification: ClassificationConfidential,
				Extractor:      probe.ExtractorConfig{Type: "json_path", JSONPath: "$.event_date"},
			},
			{
				Name:           "tenant",
				Kind:           DimensionCategorical,
				Classification: ClassificationProprietary,
				Extractor:      probe.ExtractorConfig{Type: "json_path", JSONPath: "$.tenant"},
			},
		},
		ShardBy: []string{"event_date"},
	}
}

func readRows(t *testing.T, path string) []ObjectRow {
	t.Helper()
	return readEnvelopeRows[ObjectRow](t, path, "gonimbus.atlas.object.v1")
}

func readDiagnostics(t *testing.T, path string) []DiagnosticRow {
	t.Helper()
	return readEnvelopeRows[DiagnosticRow](t, path, "gonimbus.atlas.diagnostic.v1")
}

func readEnvelopeRows[T any](t *testing.T, path, wantType string) []T {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	out := make([]T, 0, len(lines))
	for _, line := range lines {
		var env recordEnvelope
		require.NoError(t, json.Unmarshal(line, &env))
		require.Equal(t, wantType, env.Type)
		var row T
		require.NoError(t, json.Unmarshal(env.Data, &row))
		out = append(out, row)
	}
	return out
}

func shaHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
