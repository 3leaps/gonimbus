//go:build cloudintegration

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/test/cloudtest"
)

const (
	releaseStressS3SizeEnv        = "GONIMBUS_S3_RELEASE_STRESS_SIZE_BYTES"
	releaseStressS3ParallelEnv    = "GONIMBUS_S3_RELEASE_STRESS_PARALLEL"
	releaseStressS3MinBackoffsEnv = "GONIMBUS_S3_RELEASE_STRESS_MIN_BACKOFFS"
	releaseStressS3TimeoutEnv     = "GONIMBUS_S3_RELEASE_STRESS_TIMEOUT"

	releaseStressS3DefaultSize     = int64(5<<30 + 64<<20)
	releaseStressS3DefaultParallel = 128
)

func TestReleaseStressS3LargeMultipart_CloudIntegration(t *testing.T) {
	withTransferReflowTestState(t)

	size := releaseStressInt64Env(t, releaseStressS3SizeEnv, releaseStressS3DefaultSize)
	require.Greater(t, size, int64(5<<30), "%s must cross the S3 single-PUT wall", releaseStressS3SizeEnv)
	parallel := releaseStressIntEnv(t, releaseStressS3ParallelEnv, releaseStressS3DefaultParallel)
	require.Greater(t, parallel, 1, "%s must request a high-parallel reflow", releaseStressS3ParallelEnv)
	minBackoffs := releaseStressInt64Env(t, releaseStressS3MinBackoffsEnv, 0)
	timeout := releaseStressDurationEnv(t, releaseStressS3TimeoutEnv, 4*time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cfg, prefix := cloudtest.CreateS3ObjectPrefix(t, ctx)
	prov := cloudtest.RealS3Provider(t, ctx, cfg)
	defer func() { _ = prov.Close() }()

	workDir, err := os.MkdirTemp("", "gonimbus-s3-release-stress-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(workDir) })

	dbPath, identityPath, indexSetID, runID := createReleaseStressIndexDB(t, ctx, workDir, cfg, prefix, size)
	require.FileExists(t, identityPath)

	hubPrefix := prefix + "index-hub/"
	runPrefix := hubPrefix + "index-sets/" + indexSetID + "/runs/" + runID + "/"
	hubURI := cfg.ObjectURI(hubPrefix)
	runIndexExportToRealS3(t, ctx, cfg, hubURI, indexSetID, runID, dbPath)

	indexDBKey := runPrefix + "index.db"
	indexMeta, err := prov.Head(ctx, indexDBKey)
	require.NoError(t, err)
	require.Equal(t, size, indexMeta.Size)

	latestKey := hubPrefix + "index-sets/" + indexSetID + "/latest.json"
	reflowStdout := runReflowExportedRunPrefix(t, ctx, cfg, runPrefix, latestKey, prefix+"reflowed/", parallel)
	summary := requireReflowSummary(t, reflowStdout)
	require.Equal(t, int64(4), summary.Statuses["complete"])
	require.Zero(t, summary.Errors)
	require.Zero(t, summary.InvalidInputs)
	require.GreaterOrEqual(t, summary.ConcurrencyThrottleBackoffs, minBackoffs)

	reflowedIndexKey := prefix + "reflowed/index.db"
	reflowedIndexMeta, err := prov.Head(ctx, reflowedIndexKey)
	require.NoError(t, err)
	require.Equal(t, size, reflowedIndexMeta.Size)
}

func createReleaseStressIndexDB(t *testing.T, ctx context.Context, dir string, cfg cloudtest.RealS3Config, prefix string, size int64) (dbPath string, identityPath string, indexSetID string, runID string) {
	t.Helper()

	dbPath = filepath.Join(dir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))

	params := testIndexSetParams(cfg.ObjectURI(prefix + "source/"))
	params.Region = cfg.Region
	params.Endpoint = cfg.Endpoint
	indexSet, _, err := indexstore.FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, "crawl")
	require.NoError(t, err)
	require.NoError(t, indexstore.UpdateIndexRunStatus(ctx, db, run.RunID, indexstore.RunStatusSuccess, nil))

	identityPath = filepath.Join(dir, "identity.json")
	identityContent := []byte(`{"test":"release-stress"}`)
	require.NoError(t, os.WriteFile(identityPath, identityContent, 0o600))

	_, _ = db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, db.Close())
	requireSQLiteReadableAfterSparseExtend(t, dbPath, indexSet.IndexSetID, size)

	return dbPath, identityPath, indexSet.IndexSetID, run.RunID
}

func requireSQLiteReadableAfterSparseExtend(t *testing.T, dbPath string, indexSetID string, size int64) {
	t.Helper()

	f, err := os.OpenFile(dbPath, os.O_RDWR, 0)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(size))
	require.NoError(t, f.Close())

	info, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.Equal(t, size, info.Size())

	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	_, err = indexstore.GetIndexSet(context.Background(), db, indexSetID)
	require.NoError(t, err)
}

func runIndexExportToRealS3(t *testing.T, ctx context.Context, cfg cloudtest.RealS3Config, hubURI, indexSetID, runID, dbPath string) {
	t.Helper()

	cmd := &cobra.Command{Use: "export", RunE: runIndexExport}
	cmd.Flags().String("hub", "", "")
	cmd.Flags().String("index-set", "", "")
	cmd.Flags().String("run-id", "", "")
	cmd.Flags().String("db", "", "")
	cmd.Flags().String("hub-profile", "", "")
	cmd.Flags().String("hub-region", "", "")
	cmd.Flags().String("hub-endpoint", "", "")
	cmd.Flags().String("hub-gcp-project", "", "")
	addLatestPointerFlags(cmd)
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{
		"--hub", hubURI,
		"--index-set", indexSetID,
		"--run-id", runID,
		"--db", dbPath,
		"--hub-profile", cfg.Profile,
		"--hub-region", cfg.Region,
		"--hub-endpoint", cfg.Endpoint,
	})

	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	require.NoError(t, cmd.Execute(), "stderr: %s", stderr.String())
}

func runReflowExportedRunPrefix(t *testing.T, ctx context.Context, cfg cloudtest.RealS3Config, runPrefix string, latestKey string, destPrefix string, parallel int) string {
	t.Helper()

	prov := cloudtest.RealS3Provider(t, ctx, cfg)
	defer func() { _ = prov.Close() }()

	keys := []string{
		runPrefix + "index.db",
		runPrefix + "identity.json",
		runPrefix + "complete.json",
		latestKey,
	}

	var input strings.Builder
	enc := json.NewEncoder(&input)
	for _, key := range keys {
		meta, err := prov.Head(ctx, key)
		require.NoError(t, err)
		destRel := path.Base(key)
		if strings.HasSuffix(key, "latest.json") {
			destRel = "latest.json"
		}
		rec := map[string]any{
			"type": reflowpkg.ReflowInputRecordType,
			"data": map[string]any{
				"source_uri":           cfg.ObjectURI(key),
				"source_key":           key,
				"source_etag":          meta.ETag,
				"source_size_bytes":    meta.Size,
				"source_last_modified": meta.LastModified,
				"dest_rel_key":         destRel,
			},
		}
		require.NoError(t, enc.Encode(rec))
	}

	cmd := newTransferReflowTestCommand()
	cmd.SetContext(ctx)
	cmd.SetIn(strings.NewReader(input.String()))
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	args := []string{
		"--stdin",
		"--dest", cfg.ObjectURI(destPrefix),
		"--parallel", strconv.Itoa(parallel),
		"--src-region", cfg.Region,
		"--src-profile", cfg.Profile,
		"--src-endpoint", cfg.Endpoint,
		"--dest-region", cfg.Region,
		"--dest-profile", cfg.Profile,
		"--dest-endpoint", cfg.Endpoint,
	}
	cmd.SetArgs(args)
	require.NoError(t, cmd.Execute(), "stderr: %s\nstdout: %s", stderr.String(), stdout.String())

	return stdout.String()
}

func requireReflowSummary(t *testing.T, output string) reflowpkg.SummaryRecord {
	t.Helper()

	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var env struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &env))
		if env.Type != reflowpkg.SummaryRecordType {
			continue
		}
		var summary reflowpkg.SummaryRecord
		require.NoError(t, json.Unmarshal(env.Data, &summary))
		return summary
	}
	t.Fatalf("missing %s in output: %s", reflowpkg.SummaryRecordType, output)
	return reflowpkg.SummaryRecord{}
}

func releaseStressIntEnv(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := releaseStressInt64Env(t, name, int64(fallback))
	require.LessOrEqual(t, value, int64(^uint(0)>>1), "%s overflows int", name)
	return int(value)
}

func releaseStressInt64Env(t *testing.T, name string, fallback int64) int64 {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	require.NoError(t, err, "%s must be an integer byte/count value", name)
	return value
}

func releaseStressDurationEnv(t *testing.T, name string, fallback time.Duration) time.Duration {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	require.NoError(t, err, "%s must be a Go duration such as 4h", name)
	return value
}
