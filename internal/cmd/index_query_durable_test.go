package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

// TestIndexQuery_DurableOnlyCLI proves the streaming CLI adapter can query a
// durable-only index with no index.db present.
func TestIndexQuery_DurableOnlyCLI(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)

	env := seedDurableOnlyAppData(t, dataRoot, []indexsubstrate.CurrentObjectRow{
		durableCLIRow("data/one.json", 10, "e1", time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)),
		durableCLIRow("data/two.xml", 20, "e2", time.Date(2025, 4, 2, 0, 0, 0, 0, time.UTC)),
		durableCLIRow("other/three.txt", 30, "e3", time.Date(2025, 4, 3, 0, 0, 0, 0, time.UTC)),
	})
	require.NoFileExists(t, filepath.Join(env.identityDir, "index.db"))

	stdout, stderr, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--pattern", "data/**",
	)
	require.NoError(t, err, "stderr=%q", stderr)

	lines := nonEmptyLines(stdout)
	require.Len(t, lines, 2, "stdout=%q stderr=%q", stdout, stderr)
	require.Contains(t, stderr, "Matched 2 objects")

	var first indexQueryRecord
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &first))
	require.Equal(t, "gonimbus.index.object.v1", first.Type)
	require.Equal(t, "data/one.json", first.Data.RelKey)
	require.Equal(t, env.baseURI, first.Data.BaseURI)
	require.EqualValues(t, 10, first.Data.SizeBytes)
}

func executeIndexQueryCommand(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := newIndexQueryCommandForTest()
	cmd.SetArgs(args)
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	oldStdout := os.Stdout
	oldStderr := os.Stderr
	stdoutR, stdoutW, err := os.Pipe()
	require.NoError(t, err)
	stderrR, stderrW, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = stdoutW
	os.Stderr = stderrW

	execErr := cmd.Execute()
	require.NoError(t, stdoutW.Close())
	require.NoError(t, stderrW.Close())
	os.Stdout = oldStdout
	os.Stderr = oldStderr

	var stdoutBuf, stderrBuf bytes.Buffer
	_, _ = stdoutBuf.ReadFrom(stdoutR)
	_, _ = stderrBuf.ReadFrom(stderrR)
	return stdoutBuf.String(), stderrBuf.String(), execErr
}

func newIndexQueryCommandForTest() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "query [base-uri]",
		Args: cobra.MaximumNArgs(1),
		RunE: runIndexQuery,
	}
	cmd.Flags().StringP("pattern", "p", "", "Doublestar glob pattern to match keys")
	cmd.Flags().String("key-regex", "", "Regex pattern to match keys")
	cmd.Flags().String("min-size", "", "Minimum object size")
	cmd.Flags().String("max-size", "", "Maximum object size")
	cmd.Flags().String("after", "", "Objects modified after this date")
	cmd.Flags().String("before", "", "Objects modified before this date")
	cmd.Flags().StringArray("storage-class", nil, "Storage class filter")
	cmd.Flags().String("enriched-after", "", "Objects HEAD-enriched after this date")
	cmd.Flags().Int("limit", 0, "Maximum number of results")
	cmd.Flags().Bool("include-deleted", false, "Include soft-deleted objects")
	cmd.Flags().Bool("count", false, "Only output count of matching objects")
	cmd.Flags().Bool("canonical-by-etag", false, "Emit one canonical record per non-empty ETag group")
	cmd.Flags().String("canonical-tie-break", string(indexstore.CanonicalTieBreakMinKey), "Canonical selection rule")
	cmd.Flags().Bool("include-alternates", false, "Populate alternates[] on canonical ETag records")
	cmd.Flags().String("since-run", "", "Only emit current objects first seen or changed after this successful run")
	cmd.Flags().String("index-set", "", "Explicit index set ID")
	cmd.Flags().String("output", "", "Output destination URI")
	cmd.Flags().String("output-profile", "", "AWS profile for output destination")
	cmd.Flags().String("output-region", "", "AWS region for output destination")
	cmd.Flags().String("output-endpoint", "", "Custom endpoint for output destination")
	return cmd
}

// TestIndexQuery_StreamingJSONLParity compares timestamp-normalized JSONL from
// sqlite and durable readers through the same record encoder used by the CLI.
func TestIndexQuery_StreamingJSONLParity(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)

	mod := time.Date(2025, 6, 1, 15, 30, 0, 0, time.UTC)
	sc := "STANDARD"
	ctype := "text/plain"
	enriched := time.Date(2025, 6, 2, 0, 0, 0, 0, time.UTC)

	env := seedDurableOnlyAppData(t, dataRoot, []indexsubstrate.CurrentObjectRow{
		func() indexsubstrate.CurrentObjectRow {
			r := durableCLIRow("x/a.txt", 42, "etag-x", mod)
			r.StorageClass = &sc
			r.ContentType = &ctype
			r.HeadEnrichedAt = &enriched
			return r
		}(),
		durableCLIRow("x/b.txt", 99, "etag-y", mod.Add(time.Hour)),
	})

	// Add sqlite sibling with matching LIST/HEAD fields.
	ctx := context.Background()
	dbPath := filepath.Join(env.identityDir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, env.params)
	require.NoError(t, err)
	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusSuccess, nil))
	require.NoError(t, indexstore.BatchUpsertObjects(ctx, db, []indexstore.ObjectRow{
		{
			IndexSetID: indexSet.IndexSetID, RelKey: "x/a.txt", SizeBytes: 42,
			LastModified: &mod, ETag: "etag-x", StorageClass: &sc,
			LastSeenRunID: run.RunID, LastSeenAt: mod,
			FirstSeenRunID: run.RunID, FirstSeenAt: mod, LastChangedRunID: run.RunID, LastChangedAt: mod,
		},
		{
			IndexSetID: indexSet.IndexSetID, RelKey: "x/b.txt", SizeBytes: 99,
			LastModified: timePtrCmd(mod.Add(time.Hour)), ETag: "etag-y",
			LastSeenRunID: run.RunID, LastSeenAt: mod.Add(time.Hour),
			FirstSeenRunID: run.RunID, FirstSeenAt: mod.Add(time.Hour),
			LastChangedRunID: run.RunID, LastChangedAt: mod.Add(time.Hour),
		},
	}))
	require.NoError(t, indexstore.BatchUpdateHeadEnrichment(ctx, db, []indexstore.HeadEnrichmentUpdate{{
		IndexSetID:     indexSet.IndexSetID,
		RelKey:         "x/a.txt",
		ContentType:    &ctype,
		HeadEnrichedAt: enriched,
	}}))
	require.NoError(t, db.Close())

	opts := indexreader.ResolveOptions{
		IndexesRoot:      filepath.Join(dataRoot, "indexes"),
		SegmentCacheRoot: filepath.Join(dataRoot, "cache", "segments"),
	}
	sqliteReader, err := indexreader.ResolveIndexReader(ctx, opts, indexreader.ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	require.Equal(t, indexreader.FormatSQLiteV1, sqliteReader.Meta().Format)
	defer func() { _ = sqliteReader.Close() }()

	require.NoError(t, os.Rename(dbPath, dbPath+".bak"))
	t.Cleanup(func() { _ = os.Rename(dbPath+".bak", dbPath) })
	durableReader, err := indexreader.ResolveIndexReader(ctx, opts, indexreader.ResolveTarget{IndexSetID: env.indexSetID})
	require.NoError(t, err)
	require.Equal(t, indexreader.FormatDurableV2, durableReader.Meta().Format)
	defer func() { _ = durableReader.Close() }()

	fixedTS := "2025-01-01T00:00:00Z"
	params := indexstore.QueryParams{IndexSetID: env.indexSetID}
	sqlJSONL := collectJSONL(t, ctx, sqliteReader, params, env.baseURI, fixedTS)
	durJSONL := collectJSONL(t, ctx, durableReader, params, env.baseURI, fixedTS)
	require.Equal(t, sqlJSONL, durJSONL)
}

func collectJSONL(t *testing.T, ctx context.Context, reader indexreader.Reader, params indexstore.QueryParams, baseURI, ts string) []string {
	t.Helper()
	var out []string
	_, err := reader.WalkObjects(ctx, params, func(r indexstore.QueryResult) error {
		rec := newIndexQueryRecord(baseURI, ts, r)
		// Zero run-id variance between backends for stable JSONL comparison.
		rec.Data.FirstSeenRunID = ""
		rec.Data.LastChangedRunID = ""
		data, err := json.Marshal(rec)
		require.NoError(t, err)
		out = append(out, string(data))
		return nil
	})
	require.NoError(t, err)
	return out
}

type durableCLIEnv struct {
	baseURI     string
	indexSetID  string
	identityDir string
	params      indexstore.IndexSetParams
}

func seedDurableOnlyAppData(t *testing.T, dataRoot string, rows []indexsubstrate.CurrentObjectRow) durableCLIEnv {
	t.Helper()
	indexesRoot := filepath.Join(dataRoot, "indexes")
	segmentCacheRoot := filepath.Join(dataRoot, "cache", "segments")
	baseURI := "s3://test-bucket/data/"
	params := indexstore.IndexSetParams{
		BaseURI:         baseURI,
		Provider:        "s3",
		StorageProvider: "aws_s3",
		CloudProvider:   "aws",
		RegionKind:      "aws",
		Region:          "us-east-1",
		BuildParams: indexstore.BuildParams{
			SourceType:      "crawl",
			SchemaVersion:   indexstore.SchemaVersion,
			GonimbusVersion: "test",
			Includes:        []string{"**"},
		},
	}
	identity, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)

	identityDir := filepath.Join(indexesRoot, identity.DirName)
	require.NoError(t, os.MkdirAll(identityDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(identityDir, "identity.json"), []byte(identity.CanonicalJSON+"\n"), 0o600))

	runID := "run_cli_1"
	segmentRoot := filepath.Join(segmentCacheRoot, identity.IndexSetID)
	runDir := filepath.Join(segmentRoot, "runs", runID)
	require.NoError(t, os.MkdirAll(runDir, 0o755))
	for i := range rows {
		rows[i].IndexSetID = identity.IndexSetID
		if rows[i].FirstSeenRunID == "" {
			rows[i].FirstSeenRunID = runID
		}
		if rows[i].LastChangedRunID == "" {
			rows[i].LastChangedRunID = runID
		}
		if rows[i].LastSeenRunID == "" {
			rows[i].LastSeenRunID = runID
		}
	}
	createdAt := time.Date(2025, 4, 10, 12, 0, 0, 0, time.UTC)
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir:                  runDir,
		IndexSetID:           identity.IndexSetID,
		RunID:                runID,
		CreatedAt:            createdAt,
		TargetRowsPerSegment: 100,
	}, rows)
	require.NoError(t, err)
	manifestPath := filepath.Join(runDir, "manifest.json")
	require.NoError(t, indexsubstrate.WriteInternalManifestFile(manifestPath, manifest))
	manifestSHA, err := fileSHA256Hex(manifestPath)
	require.NoError(t, err)
	completePath := filepath.Join(runDir, "complete.json")
	writeJSONFile(t, completePath, map[string]any{
		"type":            "gonimbus.index.complete.v1",
		"index_set_id":    identity.IndexSetID,
		"run_id":          runID,
		"completed_at":    createdAt.Format(time.RFC3339Nano),
		"manifest_path":   manifestPath,
		"manifest_sha256": manifestSHA,
		"segment_dir":     runDir,
		"segments":        len(manifest.Segments),
	})
	writeJSONFile(t, filepath.Join(segmentRoot, "latest.json"), map[string]any{
		"type":          "gonimbus.index.latest.v1",
		"index_set_id":  identity.IndexSetID,
		"run_id":        runID,
		"updated_at":    createdAt.Format(time.RFC3339Nano),
		"complete_path": completePath,
	})
	lease, err := indexsubstrate.AcquireWriteLease(segmentRoot, identity.IndexSetID, "fixture-publish", 0)
	require.NoError(t, err)
	require.NoError(t, lease.Release())
	return durableCLIEnv{
		baseURI:     baseURI,
		indexSetID:  identity.IndexSetID,
		identityDir: identityDir,
		params:      params,
	}
}

func durableCLIRow(relKey string, size int64, etag string, mod time.Time) indexsubstrate.CurrentObjectRow {
	return indexsubstrate.CurrentObjectRow{
		RelKey:           relKey,
		SizeBytes:        size,
		LastModified:     &mod,
		ETag:             etag,
		FirstSeenRunID:   "run_cli_1",
		FirstSeenAt:      mod,
		LastChangedRunID: "run_cli_1",
		LastChangedAt:    mod,
		LastSeenRunID:    "run_cli_1",
		LastSeenAt:       mod,
	}
}

func writeJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))
}

func fileSHA256Hex(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256SumCmd(data)
	return sum, nil
}

func nonEmptyLines(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

func timePtrCmd(t time.Time) *time.Time {
	v := t.UTC()
	return &v
}
