package cmd

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/fulmenhq/gofulmen/schema"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

func TestIndexQueryReceiptJSONL_TerminalSuccessAndCounters(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, []indexsubstrate.CurrentObjectRow{
		durableCLIRow("data/one.json", 10, "e1", time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)),
		durableCLIRow("data/two.json", 20, "e2", time.Date(2025, 4, 2, 0, 0, 0, 0, time.UTC)),
		durableCLIRow("data/three.json", 30, "e3", time.Date(2025, 4, 3, 0, 0, 0, 0, time.UTC)),
	})

	stdout, stderr, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--pattern", "data/**",
		"--limit", "1",
		"--output-format", indexQueryReceiptOutputFormat,
	)
	require.NoError(t, err, "stderr=%q", stderr)
	lines := nonEmptyLines(stdout)
	require.Len(t, lines, 2, "stdout=%q stderr=%q", stdout, stderr)

	var object indexQueryRecord
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &object))
	require.Equal(t, "gonimbus.index.object.v1", object.Type)

	var receipt indexQueryReceiptRecord
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &receipt))
	require.Equal(t, indexQueryReceiptType, receipt.Type)
	require.Equal(t, "success", receipt.Outcome)
	require.Equal(t, "local_published", receipt.SourceKind)
	require.Equal(t, env.indexSetID, receipt.IndexSetID)
	require.Equal(t, env.runID, receipt.RunID)
	require.Equal(t, env.identitySHA256, receipt.SourceIdentitySHA256)
	require.Equal(t, indexreader.SourceIdentitySchemaV1, receipt.SourceIdentitySchema)
	require.Equal(t, indexreader.SourceIdentityProfileV1, receipt.SourceIdentityProfile)
	require.EqualValues(t, 2, receipt.Results.Examined)
	require.EqualValues(t, 2, receipt.Results.Matched)
	require.EqualValues(t, 1, receipt.Results.Emitted)
	require.EqualValues(t, 1, receipt.Results.LogicalResults)
	require.True(t, receipt.Results.Truncated)
	require.Equal(t, 1, receipt.Segments.Declared)
	require.Equal(t, 1, receipt.Segments.Walked)
	require.Equal(t, 1, receipt.Segments.Verified)
	require.Zero(t, receipt.Segments.ManifestPruned)
	require.Equal(t, "objects", receipt.Query.ResultMode)
	require.Equal(t, []string{"pattern"}, receipt.Query.FilterKinds)
	require.Equal(t, 1, receipt.Query.EffectiveLimit)
	require.Len(t, receipt.Query.SpecSHA256, 64)
	require.NotContains(t, lines[1], "base_uri")
	require.NotContains(t, lines[1], "manifest_path")
	require.NotContains(t, lines[1], "segment_dir")
	validateQueryReceiptAgainstSchema(t, receipt)
}

func TestIndexQueryReceiptSchema_RequiresSourceIdentityForAllSourceKinds(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	rawSchema, err := os.ReadFile(filepath.Join(root, "schemas", "gonimbus", "v1.0.0", "index-query-receipt.v1.schema.json"))
	require.NoError(t, err)

	var document struct {
		Required   []string `json:"required"`
		Properties map[string]struct {
			Const string   `json:"const"`
			Enum  []string `json:"enum"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(rawSchema, &document))
	require.Contains(t, document.Required, "source_identity_sha256")
	require.Contains(t, document.Required, "source_identity_schema")
	require.Contains(t, document.Required, "source_identity_profile")
	require.Equal(t, indexreader.SourceIdentitySchemaV1, document.Properties["source_identity_schema"].Const)
	require.Equal(t, indexreader.SourceIdentityProfileV1, document.Properties["source_identity_profile"].Const)
	require.ElementsMatch(t,
		[]string{string(indexreader.SnapshotSourceLocalPublished), string(indexreader.SnapshotSourceAcquiredHub)},
		document.Properties["source_kind"].Enum,
	)
}

func TestIndexQueryReceiptJSONL_ZeroCountEmitsReceiptOnly(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, []indexsubstrate.CurrentObjectRow{
		durableCLIRow("data/one.json", 10, "e1", time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)),
	})

	literal := "private-literal-that-must-not-appear/**"
	stdout, stderr, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--pattern", literal,
		"--count",
		"--output-format", indexQueryReceiptOutputFormat,
	)
	require.NoError(t, err, "stderr=%q", stderr)
	lines := nonEmptyLines(stdout)
	require.Len(t, lines, 1, "stdout=%q stderr=%q", stdout, stderr)
	require.NotContains(t, lines[0], literal)

	var receipt indexQueryReceiptRecord
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &receipt))
	require.Equal(t, "count", receipt.Query.ResultMode)
	require.Zero(t, receipt.Query.EffectiveLimit)
	require.Zero(t, receipt.Results.Matched)
	require.Zero(t, receipt.Results.Emitted)
	require.Zero(t, receipt.Results.LogicalResults)
	require.False(t, receipt.Results.Truncated)
	validateQueryReceiptAgainstSchema(t, receipt)
}

func TestIndexQueryReceiptJSONL_RequiresExactPin(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)

	_, _, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID[:20],
		"--run-id", env.runID,
		"--output-format", indexQueryReceiptOutputFormat,
	)
	require.ErrorContains(t, err, "full lowercase --index-set")

	_, _, err = executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--output-format", indexQueryReceiptOutputFormat,
	)
	require.ErrorContains(t, err, "requires --run-id")

	_, _, err = executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--output-format", "receipt-jsonl-v2",
	)
	require.ErrorContains(t, err, "unsupported --output-format")
}

func TestIndexQueryReceiptJSONL_RequiresCanonicalSourceIdentity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, env durableCLIEnv)
	}{
		{
			name: "missing",
			mutate: func(t *testing.T, env durableCLIEnv) {
				require.NoError(t, os.Remove(filepath.Join(env.identityDir, "identity.json")))
			},
		},
		{
			name: "noncanonical whitespace",
			mutate: func(t *testing.T, env durableCLIEnv) {
				path := filepath.Join(env.identityDir, "identity.json")
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, append([]byte(" "), data...), 0o600))
			},
		},
		{
			name: "missing final LF",
			mutate: func(t *testing.T, env durableCLIEnv) {
				path := filepath.Join(env.identityDir, "identity.json")
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, data[:len(data)-1], 0o600))
			},
		},
		{
			name: "extra trailing LF",
			mutate: func(t *testing.T, env durableCLIEnv) {
				path := filepath.Join(env.identityDir, "identity.json")
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))
			},
		},
		{
			name: "mismatched set derivation",
			mutate: func(t *testing.T, env durableCLIEnv) {
				params := env.params
				params.BaseURI = "s3://different-source/data/"
				identity, err := indexstore.ComputeIndexSetID(params)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(
					filepath.Join(env.identityDir, "identity.json"),
					[]byte(identity.CanonicalJSON+"\n"),
					0o600,
				))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resetAppDataRootTestState(t)
			dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
			t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
			env := seedDurableOnlyAppData(t, dataRoot, nil)
			test.mutate(t, env)

			stdout, stderr, err := executeIndexQueryCommand(t,
				"--index-set", env.indexSetID,
				"--run-id", env.runID,
				"--output-format", indexQueryReceiptOutputFormat,
			)
			require.ErrorContains(t, err, "exact canonical source identity is required")
			require.Empty(t, stdout, "stdout=%q stderr=%q", stdout, stderr)
			require.NotContains(t, err.Error(), env.identityDir)
		})
	}
}

func TestIndexQueryReceiptJSONL_RequiresReadableSourceIdentity(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("owner read permission semantics differ on Windows")
	}
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	identityPath := filepath.Join(env.identityDir, "identity.json")
	require.NoError(t, os.Chmod(identityPath, 0))
	t.Cleanup(func() { _ = os.Chmod(identityPath, 0o600) })

	stdout, stderr, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--output-format", indexQueryReceiptOutputFormat,
	)
	require.ErrorContains(t, err, "exact canonical source identity is required")
	require.Empty(t, stdout, "stdout=%q stderr=%q", stdout, stderr)
	require.NotContains(t, err.Error(), env.identityDir)
}

func TestIndexQueryReceiptJSONL_OutputPublishesCompleteStream(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, []indexsubstrate.CurrentObjectRow{
		durableCLIRow("data/one.json", 10, "e1", time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)),
	})
	outputPath := filepath.Join(t.TempDir(), "receipt-stream.jsonl")

	stdout, stderr, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--output-format", indexQueryReceiptOutputFormat,
		"--output", "file://"+outputPath,
	)
	require.NoError(t, err, "stderr=%q", stderr)
	require.Empty(t, stdout)
	require.Contains(t, stderr, "plus terminal receipt")

	data, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	lines := nonEmptyLines(string(data))
	require.Len(t, lines, 2)
	var receipt indexQueryReceiptRecord
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &receipt))
	require.Equal(t, indexQueryReceiptType, receipt.Type)
	require.Equal(t, "success", receipt.Outcome)
}

func TestIndexQueryReceiptJSONL_OutputIsNoReplace(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	outputPath := filepath.Join(t.TempDir(), "existing.jsonl")
	require.NoError(t, os.WriteFile(outputPath, []byte("existing\n"), 0o600))

	stdout, stderr, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--output-format", indexQueryReceiptOutputFormat,
		"--output", "file://"+outputPath,
	)
	require.ErrorContains(t, err, "refusing to replace")
	require.Empty(t, stdout, "stdout=%q stderr=%q", stdout, stderr)
	data, readErr := os.ReadFile(outputPath)
	require.NoError(t, readErr)
	require.Equal(t, "existing\n", string(data))
}

func TestIndexQueryReceiptJSONL_OutputRefusesSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require elevated privileges")
	}
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	outputDir := t.TempDir()
	targetPath := filepath.Join(outputDir, "target.jsonl")
	outputPath := filepath.Join(outputDir, "output.jsonl")
	require.NoError(t, os.WriteFile(targetPath, []byte("target\n"), 0o600))
	require.NoError(t, os.Symlink(targetPath, outputPath))

	_, _, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--output-format", indexQueryReceiptOutputFormat,
		"--output", "file://"+outputPath,
	)
	require.ErrorContains(t, err, "output path is a symlink")
	data, readErr := os.ReadFile(targetPath)
	require.NoError(t, readErr)
	require.Equal(t, "target\n", string(data))
}

func TestIndexQueryReceiptJSONL_OutputRefusesSymlinkDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require elevated privileges")
	}
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	root := t.TempDir()
	realDir := filepath.Join(root, "real")
	linkDir := filepath.Join(root, "link")
	require.NoError(t, os.Mkdir(realDir, 0o750))
	require.NoError(t, os.Symlink(realDir, linkDir))
	outputPath := filepath.Join(linkDir, "output.jsonl")

	_, _, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--output-format", indexQueryReceiptOutputFormat,
		"--output", "file://"+outputPath,
	)
	require.ErrorContains(t, err, "output directory uses a symlink")
	require.NoFileExists(t, filepath.Join(realDir, "output.jsonl"))
}

func TestIndexQueryReceiptJSONL_OutputRequiresExistingDirectory(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	root := t.TempDir()
	outputDir := filepath.Join(root, "missing")
	outputPath := filepath.Join(outputDir, "output.jsonl")

	_, _, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--output-format", indexQueryReceiptOutputFormat,
		"--output", "file://"+outputPath,
	)
	require.ErrorContains(t, err, "receipt output directory must already exist")
	require.NoDirExists(t, outputDir)
}

func TestIndexQueryReceiptJSONL_OutputRefusesSpecialFile(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	outputPath := filepath.Join(t.TempDir(), "existing-directory")
	require.NoError(t, os.Mkdir(outputPath, 0o750))

	_, _, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--output-format", indexQueryReceiptOutputFormat,
		"--output", "file://"+outputPath,
	)
	require.ErrorContains(t, err, "output path is not a regular file")
}

func TestIndexQueryReceiptJSONL_OutputStagesAsRestrictedSibling(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	outputDir := t.TempDir()
	outputPath := filepath.Join(outputDir, "output.jsonl")
	indexQueryReceiptBeforeLocalPublish = func(tempPath, finalPath string) error {
		require.Equal(t, outputDir, filepath.Dir(tempPath))
		require.Equal(t, outputPath, finalPath)
		info, err := os.Lstat(tempPath)
		require.NoError(t, err)
		require.True(t, info.Mode().IsRegular())
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		return errors.New("stop before publish")
	}
	t.Cleanup(func() { indexQueryReceiptBeforeLocalPublish = nil })

	_, _, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--output-format", indexQueryReceiptOutputFormat,
		"--output", "file://"+outputPath,
	)
	require.ErrorContains(t, err, "stop before publish")
	require.NoFileExists(t, outputPath)
	entries, readErr := os.ReadDir(outputDir)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestIndexQueryReceiptJSONL_OutputRefusesParentSwapBeforePublish(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	root := t.TempDir()
	outputDir := filepath.Join(root, "bound")
	movedDir := filepath.Join(root, "moved")
	require.NoError(t, os.Mkdir(outputDir, 0o750))
	outputPath := filepath.Join(outputDir, "output.jsonl")

	indexQueryReceiptBeforeLocalPublish = func(_, _ string) error {
		require.NoError(t, os.Rename(outputDir, movedDir))
		require.NoError(t, os.Mkdir(outputDir, 0o750))
		return nil
	}
	t.Cleanup(func() { indexQueryReceiptBeforeLocalPublish = nil })

	_, _, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--output-format", indexQueryReceiptOutputFormat,
		"--output", "file://"+outputPath,
	)
	require.ErrorContains(t, err, "output directory binding changed")
	require.NoFileExists(t, outputPath)
	require.NoFileExists(t, filepath.Join(movedDir, "output.jsonl"))
	entries, readErr := os.ReadDir(movedDir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "retained directory handle should clean its own staging file")
}

func TestIndexQueryReceiptJSONL_OutputRefusesAncestorRedirectDuringBind(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require elevated privileges")
	}
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, nil)
	root := t.TempDir()
	parentDir := filepath.Join(root, "parent")
	movedParentDir := filepath.Join(root, "moved-parent")
	alternateParentDir := filepath.Join(root, "alternate-parent")
	outputDir := filepath.Join(parentDir, "output")
	movedOutputDir := filepath.Join(movedParentDir, "output")
	alternateOutputDir := filepath.Join(alternateParentDir, "output")
	require.NoError(t, os.MkdirAll(outputDir, 0o750))
	require.NoError(t, os.MkdirAll(alternateOutputDir, 0o750))
	outputPath := filepath.Join(outputDir, "output.jsonl")

	indexQueryReceiptBeforeLocalBind = func(directory string) error {
		require.Equal(t, outputDir, directory)
		require.NoError(t, os.Rename(parentDir, movedParentDir))
		require.NoError(t, os.Symlink(alternateParentDir, parentDir))
		return nil
	}
	t.Cleanup(func() { indexQueryReceiptBeforeLocalBind = nil })

	_, _, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--output-format", indexQueryReceiptOutputFormat,
		"--output", "file://"+outputPath,
	)
	require.ErrorContains(t, err, "output directory uses a symlink")
	require.NoFileExists(t, filepath.Join(movedOutputDir, "output.jsonl"))
	require.NoFileExists(t, filepath.Join(alternateOutputDir, "output.jsonl"))
	movedEntries, readErr := os.ReadDir(movedOutputDir)
	require.NoError(t, readErr)
	require.Empty(t, movedEntries)
	alternateEntries, readErr := os.ReadDir(alternateOutputDir)
	require.NoError(t, readErr)
	require.Empty(t, alternateEntries)
}

func TestIndexQueryReceiptJSONL_OutputCleanupFailureAfterCommitIsWarning(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, []indexsubstrate.CurrentObjectRow{
		durableCLIRow("data/one.json", 10, "e1", time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)),
	})
	outputDir := t.TempDir()
	outputPath := filepath.Join(outputDir, "output.jsonl")

	indexQueryReceiptBeforeLocalCleanup = func(string) error {
		return errors.New("injected cleanup failure")
	}
	t.Cleanup(func() { indexQueryReceiptBeforeLocalCleanup = nil })

	stdout, stderr, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--output-format", indexQueryReceiptOutputFormat,
		"--output", "file://"+outputPath,
	)
	require.NoError(t, err, "stderr=%q", stderr)
	require.Empty(t, stdout)
	require.Contains(t, stderr, "warning: committed output staging cleanup deferred: injected cleanup failure")
	require.Contains(t, stderr, "plus terminal receipt")

	data, readErr := os.ReadFile(outputPath)
	require.NoError(t, readErr)
	lines := nonEmptyLines(string(data))
	require.Len(t, lines, 2)
	var receipt indexQueryReceiptRecord
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &receipt))
	require.Equal(t, "success", receipt.Outcome)
	entries, readErr := os.ReadDir(outputDir)
	require.NoError(t, readErr)
	require.Len(t, entries, 1)
	require.Equal(t, filepath.Base(outputPath), entries[0].Name())
}

func TestIndexQueryReceiptJSONL_CorruptSegmentEmitsNoSuccessReceipt(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, []indexsubstrate.CurrentObjectRow{
		durableCLIRow("data/one.json", 10, "e1", time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)),
	})
	segmentPaths, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", env.indexSetID, "runs", env.runID, "*.parquet"))
	require.NoError(t, err)
	require.Len(t, segmentPaths, 1)
	require.NoError(t, os.WriteFile(segmentPaths[0], []byte("corrupt"), 0o600))

	outputPath := filepath.Join(t.TempDir(), "must-not-publish.jsonl")
	stdout, stderr, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--output-format", indexQueryReceiptOutputFormat,
		"--output", "file://"+outputPath,
	)
	require.ErrorContains(t, err, "segment digest mismatch")
	require.Empty(t, stdout, "stdout=%q stderr=%q", stdout, stderr)
	require.NoFileExists(t, outputPath)
}

func TestIndexQueryReceiptJSONL_CanonicalModes(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	modified := time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)
	env := seedDurableOnlyAppData(t, dataRoot, []indexsubstrate.CurrentObjectRow{
		durableCLIRow("data/a.json", 10, "shared", modified),
		durableCLIRow("data/b.json", 20, "shared", modified),
	})

	stdout, stderr, err := executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--canonical-by-etag",
		"--output-format", indexQueryReceiptOutputFormat,
	)
	require.NoError(t, err, "stderr=%q", stderr)
	lines := nonEmptyLines(stdout)
	require.Len(t, lines, 2)
	require.Contains(t, lines[0], "gonimbus.index.object.canonical.v1")
	require.NotContains(t, lines[0], "gonimbus.index.object.v1")
	var receipt indexQueryReceiptRecord
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &receipt))
	require.Equal(t, "canonical_objects", receipt.Query.ResultMode)
	require.Equal(t, "min-key", receipt.Query.CanonicalTieBreak)
	require.EqualValues(t, 2, receipt.Results.Matched)
	require.EqualValues(t, 1, receipt.Results.Emitted)

	stdout, stderr, err = executeIndexQueryCommand(t,
		"--index-set", env.indexSetID,
		"--run-id", env.runID,
		"--canonical-by-etag",
		"--count",
		"--output-format", indexQueryReceiptOutputFormat,
	)
	require.NoError(t, err, "stderr=%q", stderr)
	lines = nonEmptyLines(stdout)
	require.Len(t, lines, 1)
	receipt = indexQueryReceiptRecord{}
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &receipt))
	require.Equal(t, "canonical_count", receipt.Query.ResultMode)
	require.Empty(t, receipt.Query.CanonicalTieBreak)
	require.EqualValues(t, 2, receipt.Results.Matched)
	require.Zero(t, receipt.Results.Emitted)
	require.EqualValues(t, 1, receipt.Results.LogicalResults)
}

func TestIndexQueryReceiptJSONL_CanonicalEmptyETagFailsClosed(t *testing.T) {
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	modified := time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)
	env := seedDurableOnlyAppData(t, dataRoot, []indexsubstrate.CurrentObjectRow{
		durableCLIRow("data/a.json", 10, "shared", modified),
		durableCLIRow("data/no-etag.json", 30, "", modified),
	})

	for _, extra := range [][]string{nil, {"--count"}, {"--limit", "1"}} {
		args := []string{
			"--index-set", env.indexSetID,
			"--run-id", env.runID,
			"--canonical-by-etag",
			"--output-format", indexQueryReceiptOutputFormat,
		}
		args = append(args, extra...)
		stdout, stderr, err := executeIndexQueryCommand(t, args...)
		require.ErrorContains(t, err, "canonical receipt query requires non-empty ETags")
		require.Empty(t, stdout, "stdout=%q stderr=%q", stdout, stderr)
		require.NotContains(t, err.Error(), "data/no-etag.json")
	}
}

func TestIndexQueryReceiptQuerySpecNormalization(t *testing.T) {
	meta := indexreader.VerifiedSnapshotMetadata{
		IndexSetID: "idx_" + strings.Repeat("a", 64),
		RunID:      "run_1",
	}
	base := indexQueryReceiptRunOptions{
		Params: indexstore.QueryParams{
			Pattern:        "雪/**",
			StorageClasses: []string{"STANDARD", "GLACIER", "STANDARD"},
			ModifiedAfter:  time.Date(2026, 9, 8, 12, 34, 56, 12, time.FixedZone("offset", -4*60*60)),
		},
	}
	first, err := buildIndexQueryReceiptQuery(meta, base)
	require.NoError(t, err)
	require.Equal(t, "7b2d10192775d5505de98deca94ac5bb66b469d0fe5c3c2acfbf4ced910ccd43", first.SpecSHA256)
	base.Params.StorageClasses = []string{"GLACIER", "STANDARD"}
	second, err := buildIndexQueryReceiptQuery(meta, base)
	require.NoError(t, err)
	require.Equal(t, first.SpecSHA256, second.SpecSHA256)
	require.Equal(t, 2, first.StorageClassCount)
	require.Equal(t, []string{"modified_after", "pattern", "storage_class"}, first.FilterKinds)

	base.BaseURI = "s3://different-authority/"
	withDifferentBaseURI, err := buildIndexQueryReceiptQuery(meta, base)
	require.NoError(t, err)
	require.NotEqual(t, first.SpecSHA256, withDifferentBaseURI.SpecSHA256)

	canonical, err := marshalJCSSubset(map[string]any{
		"b": "line\n",
		"a": "\u2028\u2029<>&",
	})
	require.NoError(t, err)
	// RFC 8785 section 3.2.2.2 preserves non-control Unicode code points,
	// including U+2028 and U+2029, as UTF-8 rather than escaping them.
	require.Equal(t, "{\"a\":\"\u2028\u2029<>&\",\"b\":\"line\\n\"}", string(canonical))

	_, err = marshalJCSSubset(int64(1 << 53))
	require.ErrorContains(t, err, "exceeds the RFC 8785 interoperable range")
}

func validateQueryReceiptAgainstSchema(t *testing.T, receipt indexQueryReceiptRecord) {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	rawSchema, err := os.ReadFile(filepath.Join(root, "schemas", "gonimbus", "v1.0.0", "index-query-receipt.v1.schema.json"))
	require.NoError(t, err)
	validator, err := schema.NewValidator(rawSchema)
	require.NoError(t, err)
	data, err := json.Marshal(receipt)
	require.NoError(t, err)
	diagnostics, err := validator.ValidateJSON(data)
	require.NoError(t, err)
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == schema.SeverityError {
			t.Fatalf("receipt failed schema validation: %s: %s", diagnostic.Pointer, diagnostic.Message)
		}
	}
}
