package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

type custodyE2EArtifact struct {
	Path      string `json:"path"`
	Role      string `json:"role"`
	Required  bool   `json:"required"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

type custodyE2EFixture struct {
	env          durableCLIEnv
	sourceRoot   string
	targetRoot   string
	evidencePath string
	legacyPath   string
	legacyBytes  []byte
}

func TestCustodyBridgeE2E_ProductionPathCountAndFindReceipts(t *testing.T) {
	tests := []struct {
		name      string
		exactTime bool
		wantBasis string
	}{
		{
			name:      "legacy unavailable",
			wantBasis: indexreader.BridgeSnapshotTimeLegacyUnavailable,
		},
		{
			name:      "exact commit",
			exactTime: true,
			wantBasis: indexreader.BridgeSnapshotTimeExactCommit,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newCustodyE2EFixture(t)
			evidencePath := ""
			if tc.exactTime {
				evidencePath = fixture.evidencePath
			}
			bridgeReceipt, output := runCustodyE2EBridge(
				t, context.Background(), fixture, evidencePath,
			)
			require.Equal(t, tc.wantBasis, bridgeReceipt.SnapshotTime.Basis)
			require.NotEqual(t, bridgeReceipt.SourceHandleRef, bridgeReceipt.TargetHandleRef)
			require.NotContains(t, output, "legacy-source")
			require.NotContains(t, output, "custody-target")
			require.NotContains(t, output, fixture.sourceRoot)
			require.NotContains(t, output, fixture.targetRoot)
			require.NotContains(t, output, fixture.evidencePath)
			require.Equal(t, fixture.legacyBytes, mustReadCustodyE2EFile(t, fixture.legacyPath))
			validateBridgeReceiptAgainstSchema(t, []byte(output))
			targetComplete := filepath.Join(
				fixture.targetRoot, "index-sets", fixture.env.indexSetID,
				"runs", fixture.env.runID, "complete.json",
			)
			var targetMarker map[string]any
			require.NoError(t, json.Unmarshal(mustReadCustodyE2EFile(t, targetComplete), &targetMarker))
			require.Equal(t, indexreader.HubMarkerSchemaV3, targetMarker["marker_schema_version"])
			require.NotContains(t, targetMarker, "completed_at")
			require.NotContains(t, targetMarker, "snapshot_completed_at")
			require.NotContains(t, targetMarker, "snapshot_completion_semantics")
			sourceFiles := mustListCustodyE2EFiles(t, fixture.sourceRoot)
			require.Len(t, sourceFiles, 3)
			require.Contains(t, sourceFiles, filepath.ToSlash(filepath.Join(
				"index-sets", fixture.env.indexSetID, "runs", fixture.env.runID, "complete.json",
			)))
			targetFiles := mustListCustodyE2EFiles(t, fixture.targetRoot)
			require.Len(t, targetFiles, 4)
			require.Contains(t, targetFiles, filepath.ToSlash(filepath.Join(
				"index-sets", fixture.env.indexSetID, "identity.json",
			)))
			require.Contains(t, targetFiles, filepath.ToSlash(filepath.Join(
				"index-sets", fixture.env.indexSetID, "runs", fixture.env.runID, "complete.json",
			)))
			require.NoFileExists(t, filepath.Join(fixture.targetRoot, "latest.json"))
			require.NoFileExists(t, filepath.Join(
				fixture.targetRoot, "index-sets", fixture.env.indexSetID, "latest.json",
			))

			bundleDir := filepath.Join(filepath.Dir(fixture.sourceRoot), "acquired")
			acquireCmd := newIndexAcquireCommandForTest()
			acquireCmd.SetArgs([]string{
				"--hub-read-handle", "custody-read",
				"--index-set", fixture.env.indexSetID,
				"--run-id", fixture.env.runID,
				"--dest", bundleDir,
			})
			require.NoError(t, acquireCmd.Execute())

			stdout, stderr, err := executeIndexQueryCommand(t,
				"--snapshot-dir", bundleDir,
				"--count",
				"--output-format", indexQueryReceiptOutputFormatV2,
			)
			require.NoError(t, err, "stderr=%q", stderr)
			lines := nonEmptyLines(stdout)
			require.Len(t, lines, 1)
			var countReceipt indexQueryReceiptV2Record
			require.NoError(t, json.Unmarshal([]byte(lines[0]), &countReceipt))
			require.Equal(t, tc.wantBasis, countReceipt.SnapshotTime.Basis)
			require.Equal(t, bridgeReceipt.SourceIdentitySHA256, countReceipt.SourceIdentitySHA256)
			require.Equal(t, bridgeReceipt.ManifestSHA256, countReceipt.ManifestSHA256)
			require.Equal(t, bridgeReceipt.Declared.Rows, countReceipt.Declared.Rows)
			require.Equal(t, bridgeReceipt.Declared.Segments, countReceipt.Declared.Segments)
			require.EqualValues(t, 1, countReceipt.Results.LogicalResults)
			validateQueryReceiptV2AgainstSchema(t, countReceipt)

			stdout, stderr, err = executeIndexQueryCommand(t,
				"--snapshot-dir", bundleDir,
				"--pattern", "objects/evidence.txt",
				"--output-format", indexQueryReceiptOutputFormatV2,
			)
			require.NoError(t, err, "stderr=%q", stderr)
			lines = nonEmptyLines(stdout)
			require.Len(t, lines, 2)
			var object indexQueryRecord
			require.NoError(t, json.Unmarshal([]byte(lines[0]), &object))
			require.Equal(t, "objects/evidence.txt", object.Data.RelKey)
			var findReceipt indexQueryReceiptV2Record
			require.NoError(t, json.Unmarshal([]byte(lines[1]), &findReceipt))
			require.Equal(t, tc.wantBasis, findReceipt.SnapshotTime.Basis)
			require.EqualValues(t, 1, findReceipt.Results.Emitted)
			require.NotContains(t, lines[1], "objects/evidence.txt")
			validateQueryReceiptV2AgainstSchema(t, findReceipt)

			if !tc.exactTime {
				stdout, _, err = executeIndexQueryCommand(t,
					"--snapshot-dir", bundleDir,
					"--count",
					"--output-format", indexQueryReceiptOutputFormat,
				)
				require.ErrorContains(t, err, "requires exact snapshot time")
				require.Empty(t, stdout)
			}
		})
	}
}

func TestCustodyBridgeE2E_CancellationLeavesNoPublishedAuthority(t *testing.T) {
	fixture := newCustodyE2EFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := newCustodyE2EBridgeCommand(fixture, "")
	cmd.SetContext(ctx)
	var output bytes.Buffer
	cmd.SetOut(&output)
	require.Error(t, cmd.Execute())
	require.Empty(t, output.String())
	require.Empty(t, mustReadCustodyE2EDirectory(t, fixture.targetRoot))

	resetCustodyE2EClock()
	_, _ = runCustodyE2EBridge(t, context.Background(), fixture, "")
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	bundleDir := filepath.Join(filepath.Dir(fixture.sourceRoot), "cancelled-acquire")
	acquireCmd := newIndexAcquireCommandForTest()
	acquireCmd.SetContext(ctx)
	acquireCmd.SetArgs([]string{
		"--hub-read-handle", "custody-read",
		"--index-set", fixture.env.indexSetID,
		"--run-id", fixture.env.runID,
		"--dest", bundleDir,
	})
	require.ErrorIs(t, acquireCmd.Execute(), context.Canceled)
	require.NoDirExists(t, bundleDir)
}

func TestCustodyBridgeE2E_AdversarialLocalCommitCannotReplaceForeignMarker(t *testing.T) {
	fixture := newCustodyE2EFixture(t)
	targetComplete := filepath.Join(
		fixture.targetRoot, "index-sets", fixture.env.indexSetID,
		"runs", fixture.env.runID, "complete.json",
	)
	require.NoError(t, os.MkdirAll(filepath.Dir(targetComplete), 0o700))
	foreign := []byte("foreign-commit-marker\n")
	require.NoError(t, os.WriteFile(targetComplete, foreign, 0o600))

	cmd := newCustodyE2EBridgeCommand(fixture, "")
	var output bytes.Buffer
	cmd.SetOut(&output)
	err := cmd.Execute()
	require.ErrorContains(t, err, indexreader.BridgeErrorTargetConflict)
	require.NotContains(t, err.Error(), targetComplete)
	require.NotContains(t, err.Error(), fixture.targetRoot)
	require.Empty(t, output.String())
	require.Equal(t, foreign, mustReadCustodyE2EFile(t, targetComplete))

	require.NoError(t, os.Remove(targetComplete))
	resetCustodyE2EClock()
	receipt, _ := runCustodyE2EBridge(t, context.Background(), fixture, "")
	require.Equal(t, indexreader.BridgeDispositionCreated, receipt.Disposition)
	var published map[string]any
	require.NoError(t, json.Unmarshal(mustReadCustodyE2EFile(t, targetComplete), &published))
	require.Equal(t, indexreader.HubMarkerSchemaV3, published["marker_schema_version"])
}

func newCustodyE2EFixture(t *testing.T) custodyE2EFixture {
	t.Helper()
	restoreBridgeCommandGlobals(t)
	oldAcquireResolver := configuredHubReadHandleResolver
	t.Cleanup(func() { configuredHubReadHandleResolver = oldAcquireResolver })
	configuredBridgeHubReadHandleResolver = resolveConfiguredBridgeHubReadHandle
	configuredHubPublishHandleResolver = resolveConfiguredHubPublishHandle
	configuredHubReadHandleResolver = resolveConfiguredHubReadHandle
	runLegacyDurableBridge = indexreader.BridgeLegacyDurableRun

	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("GONIMBUS_READONLY", "false")
	viper.Set("readonly", false)
	resetAppDataRootTestState(t)
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	dataRoot := filepath.Join(root, "data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	env := seedDurableOnlyAppData(t, dataRoot, []indexsubstrate.CurrentObjectRow{
		durableCLIRow(
			"objects/evidence.txt", 17, "evidence-etag",
			time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC),
		),
	})
	sourceRoot := filepath.Join(root, "legacy-source")
	targetRoot := filepath.Join(root, "custody-target")
	require.NoError(t, os.Mkdir(sourceRoot, 0o700))
	require.NoError(t, os.Mkdir(targetRoot, 0o700))
	legacyPath, legacyBytes := writeCustodyE2ELegacyHub(t, env, sourceRoot)

	viper.Set("hub_read_handles.legacy-source.uri", "file://"+sourceRoot+"/")
	viper.Set("hub_publish_handles.custody-target.uri", "file://"+targetRoot+"/")
	viper.Set("hub_read_handles.custody-read.uri", "file://"+targetRoot+"/")
	resetCustodyE2EClock()
	return custodyE2EFixture{
		env: env, sourceRoot: sourceRoot, targetRoot: targetRoot,
		evidencePath: filepath.Join(env.segmentRoot, "runs", env.runID, "complete.json"),
		legacyPath:   legacyPath, legacyBytes: legacyBytes,
	}
}

func writeCustodyE2ELegacyHub(
	t *testing.T,
	env durableCLIEnv,
	sourceRoot string,
) (string, []byte) {
	t.Helper()
	localRun := filepath.Join(env.segmentRoot, "runs", env.runID)
	manifestPath := filepath.Join(localRun, "manifest.json")
	manifest, err := indexsubstrate.ReadInternalManifestFile(manifestPath)
	require.NoError(t, err)
	hubRun := filepath.Join(sourceRoot, "index-sets", env.indexSetID, "runs", env.runID)
	require.NoError(t, os.MkdirAll(filepath.Join(hubRun, "segments"), 0o700))
	hubManifest := filepath.Join(hubRun, "manifest.json")
	manifestBytes := mustReadCustodyE2EFile(t, manifestPath)
	require.NoError(t, os.WriteFile(hubManifest, manifestBytes, 0o600))
	manifestSHA, err := fileSHA256Hex(hubManifest)
	require.NoError(t, err)

	segments := make([]custodyE2EArtifact, 0, len(manifest.Segments))
	for _, segment := range manifest.Segments {
		source := filepath.Join(localRun, segment.Path)
		target := filepath.Join(hubRun, "segments", segment.Path)
		data := mustReadCustodyE2EFile(t, source)
		require.NoError(t, os.WriteFile(target, data, 0o600))
		segments = append(segments, custodyE2EArtifact{
			Path: "segments/" + segment.Path, Role: "segment", Required: true,
			SizeBytes: segment.SizeBytes, SHA256: segment.Digest.Hex,
		})
	}
	legacy := map[string]any{
		"version": "1.0", "marker_schema_version": indexreader.HubMarkerSchemaV1,
		"format": "durable-v2", "format_version": "2",
		"index_set_id": env.indexSetID, "run_id": env.runID,
		"completed_at": manifest.RunStartedAt.Add(2 * time.Minute).Format(time.RFC3339Nano),
		"exported_by":  "gonimbus/e2e-fixture",
		"artifacts": map[string]any{
			"manifest": custodyE2EArtifact{
				Path: "manifest.json", Role: "manifest", Required: true,
				SizeBytes: int64(len(manifestBytes)), SHA256: manifestSHA,
			},
			"segments": segments,
		},
		"durable": map[string]any{
			"manifest_type": manifest.Type, "manifest_render": manifest.Render,
			"index_schema_version": manifest.IndexSchemaVersion,
			"segment_namespace":    manifest.Reachability.SegmentNamespace,
			"segments":             len(manifest.Segments), "rows": manifest.Counts.Rows,
		},
	}
	legacyPath := filepath.Join(hubRun, "complete.json")
	writeJSONFile(t, legacyPath, legacy)
	return legacyPath, mustReadCustodyE2EFile(t, legacyPath)
}

func newCustodyE2EBridgeCommand(fixture custodyE2EFixture, evidencePath string) *cobra.Command {
	cmd := newIndexHubBridgeCommandForTest()
	args := []string{
		"--source-hub-read-handle", "legacy-source",
		"--target-hub-publish-handle", "custody-target",
		"--index-set", fixture.env.indexSetID,
		"--run-id", fixture.env.runID,
		"--identity-file", filepath.Join(fixture.env.identityDir, "identity.json"),
		"--output-format", custodyBridgeOutputFormat,
	}
	if evidencePath != "" {
		args = append(args, "--snapshot-completion-evidence", evidencePath)
	}
	cmd.SetArgs(args)
	return cmd
}

func runCustodyE2EBridge(
	t *testing.T,
	ctx context.Context,
	fixture custodyE2EFixture,
	evidencePath string,
) (custodyBridgeReceipt, string) {
	t.Helper()
	cmd := newCustodyE2EBridgeCommand(fixture, evidencePath)
	cmd.SetContext(ctx)
	var output bytes.Buffer
	cmd.SetOut(&output)
	require.NoError(t, cmd.Execute())
	var receipt custodyBridgeReceipt
	require.NoError(t, json.Unmarshal(output.Bytes(), &receipt))
	return receipt, output.String()
}

func resetCustodyE2EClock() {
	times := []time.Time{
		time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 10, 12, 3, 0, 0, time.UTC),
		time.Date(2026, 9, 10, 12, 4, 0, 0, time.UTC),
	}
	custodyBridgeClock = func() time.Time {
		next := times[0]
		times = times[1:]
		return next
	}
}

func mustReadCustodyE2EFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func mustReadCustodyE2EDirectory(t *testing.T, path string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	return entries
}

func mustListCustodyE2EFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			files = append(files, filepath.ToSlash(rel))
		}
		return nil
	}))
	sort.Strings(files)
	return files
}

func TestCustodyBridgeE2E_LegacyDirectoryCoverageBridges(t *testing.T) {
	fixture := newCustodyE2ELegacyPrefixFixture(t, []indexsubstrate.CoverageAttestation{
		{
			Scope:    &indexsubstrate.Scope{Prefix: "shard-a/2026-02-01/"},
			Basis:    indexsubstrate.CoverageBasisConfirmed,
			Complete: true,
		},
		{
			Scope:    &indexsubstrate.Scope{Prefix: "shard-b/2026-02-01/"},
			Basis:    indexsubstrate.CoverageBasisConfirmed,
			Complete: true,
		},
	})
	bridgeReceipt, output := runCustodyE2EBridge(t, context.Background(), fixture, "")
	require.Equal(t, indexreader.BridgeDispositionCreated, bridgeReceipt.Disposition)
	require.Equal(t, indexreader.BridgeSnapshotTimeLegacyUnavailable, bridgeReceipt.SnapshotTime.Basis)
	require.Equal(t, fixture.env.indexSetID, bridgeReceipt.IndexSetID)
	require.Equal(t, fixture.env.runID, bridgeReceipt.RunID)
	require.Equal(t, 2, bridgeReceipt.Declared.Rows)
	require.Equal(t, 2, bridgeReceipt.Declared.Segments)
	require.Equal(t, fixture.legacyBytes, mustReadCustodyE2EFile(t, fixture.legacyPath))
	validateBridgeReceiptAgainstSchema(t, []byte(output))

	sourceFiles := mustListCustodyE2EFiles(t, fixture.sourceRoot)
	require.Len(t, sourceFiles, 4)
	targetFiles := mustListCustodyE2EFiles(t, fixture.targetRoot)
	require.Len(t, targetFiles, 5)
	require.NoFileExists(t, filepath.Join(fixture.targetRoot, "latest.json"))

	hubManifestPath := filepath.Join(
		fixture.sourceRoot, "index-sets", fixture.env.indexSetID,
		"runs", fixture.env.runID, "manifest.json",
	)
	publishedManifestPath := filepath.Join(
		fixture.targetRoot, "index-sets", fixture.env.indexSetID,
		"runs", fixture.env.runID, "manifest.json",
	)
	hubManifestBytes := mustReadCustodyE2EFile(t, hubManifestPath)
	require.Equal(t, hubManifestBytes, mustReadCustodyE2EFile(t, publishedManifestPath))
	published, err := indexsubstrate.ReadInternalManifestFile(publishedManifestPath)
	require.NoError(t, err)
	require.Len(t, published.Coverage, 2)
	require.Equal(t, "shard-a/2026-02-01/", published.Coverage[0].Scope.Prefix)
	require.Equal(t, "shard-b/2026-02-01/", published.Coverage[1].Scope.Prefix)
	expectedCoverageSHA256, err := indexsubstrate.CoverageSHA256(published.Coverage)
	require.NoError(t, err)

	targetComplete := filepath.Join(
		fixture.targetRoot, "index-sets", fixture.env.indexSetID,
		"runs", fixture.env.runID, "complete.json",
	)
	var targetMarker map[string]any
	require.NoError(t, json.Unmarshal(mustReadCustodyE2EFile(t, targetComplete), &targetMarker))
	require.Equal(t, indexreader.HubMarkerSchemaV3, targetMarker["marker_schema_version"])

	bundleDir := filepath.Join(filepath.Dir(fixture.sourceRoot), "acquired")
	acquireCmd := newIndexAcquireCommandForTest()
	acquireCmd.SetArgs([]string{
		"--hub-read-handle", "custody-read",
		"--index-set", fixture.env.indexSetID,
		"--run-id", fixture.env.runID,
		"--dest", bundleDir,
	})
	require.NoError(t, acquireCmd.Execute())

	doctorStdout, _, err := executeIndexDoctorCommand(t, bundleDir)
	require.NoError(t, err)
	require.NotEmpty(t, doctorStdout)

	stdout, stderr, err := executeIndexQueryCommand(t,
		"--snapshot-dir", bundleDir,
		"--count",
		"--output-format", indexQueryReceiptOutputFormatV2,
	)
	require.NoError(t, err, "stderr=%q", stderr)
	lines := nonEmptyLines(stdout)
	require.Len(t, lines, 1)
	var countReceipt indexQueryReceiptV2Record
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &countReceipt))
	require.Equal(t, indexreader.BridgeSnapshotTimeLegacyUnavailable, countReceipt.SnapshotTime.Basis)
	require.Equal(t, bridgeReceipt.SourceIdentitySHA256, countReceipt.SourceIdentitySHA256)
	require.Equal(t, bridgeReceipt.ManifestSHA256, countReceipt.ManifestSHA256)
	require.Equal(t, bridgeReceipt.Declared.Rows, countReceipt.Declared.Rows)
	require.Equal(t, bridgeReceipt.Declared.Segments, countReceipt.Declared.Segments)
	require.EqualValues(t, 2, countReceipt.Results.LogicalResults)
	require.Equal(t, expectedCoverageSHA256, countReceipt.CoverageSHA256)
	require.Equal(t, 2, countReceipt.Coverage.Entries)
	require.Equal(t, 2, countReceipt.Coverage.CompleteEntries)
	require.Equal(t, 2, countReceipt.Coverage.ConfirmedEntries)
	require.Equal(t, 0, countReceipt.Coverage.InferredEntries)
	require.Equal(t, 0, countReceipt.Coverage.GapCount)
	validateQueryReceiptV2AgainstSchema(t, countReceipt)

	stdout, stderr, err = executeIndexQueryCommand(t,
		"--snapshot-dir", bundleDir,
		"--pattern", "shard-a/2026-02-01/object-a.txt",
		"--output-format", indexQueryReceiptOutputFormatV2,
	)
	require.NoError(t, err, "stderr=%q", stderr)
	lines = nonEmptyLines(stdout)
	require.Len(t, lines, 2)
	var object indexQueryRecord
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &object))
	require.Equal(t, "shard-a/2026-02-01/object-a.txt", object.Data.RelKey)
	var findReceipt indexQueryReceiptV2Record
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &findReceipt))
	require.Equal(t, indexreader.BridgeSnapshotTimeLegacyUnavailable, findReceipt.SnapshotTime.Basis)
	require.EqualValues(t, 1, findReceipt.Results.Emitted)
	require.Equal(t, expectedCoverageSHA256, findReceipt.CoverageSHA256)
	require.Equal(t, 2, findReceipt.Coverage.Entries)
	require.Equal(t, 2, findReceipt.Coverage.CompleteEntries)
	require.Equal(t, 2, findReceipt.Coverage.ConfirmedEntries)
	require.Equal(t, 0, findReceipt.Coverage.InferredEntries)
	require.Equal(t, 0, findReceipt.Coverage.GapCount)
	validateQueryReceiptV2AgainstSchema(t, findReceipt)

	stdout, _, err = executeIndexQueryCommand(t,
		"--snapshot-dir", bundleDir,
		"--count",
		"--output-format", indexQueryReceiptOutputFormat,
	)
	require.ErrorContains(t, err, "requires exact snapshot time")
	require.Empty(t, stdout)

	resetCustodyE2EClock()
	replayReceipt, _ := runCustodyE2EBridge(t, context.Background(), fixture, "")
	require.Equal(t, indexreader.BridgeDispositionAlreadyIdentical, replayReceipt.Disposition)
}

func TestCustodyBridgeE2E_InvalidCoverageScopeFailsClosed(t *testing.T) {
	fixture := newCustodyE2ELegacyPrefixFixture(t, []indexsubstrate.CoverageAttestation{{
		Scope:    &indexsubstrate.Scope{Prefix: "./"},
		Basis:    indexsubstrate.CoverageBasisConfirmed,
		Complete: true,
	}})
	cmd := newCustodyE2EBridgeCommand(fixture, "")
	var output bytes.Buffer
	cmd.SetOut(&output)
	err := cmd.Execute()
	require.ErrorContains(t, err, indexreader.BridgeErrorManifestInvalid)
	require.Empty(t, output.String())
	require.Empty(t, mustReadCustodyE2EDirectory(t, fixture.targetRoot))
}

func TestCustodyBridgeE2E_InvalidCoverageGapFailsClosed(t *testing.T) {
	fixture := newCustodyE2ELegacyPrefixFixture(t, []indexsubstrate.CoverageAttestation{{
		Scope:    &indexsubstrate.Scope{Prefix: "shard-a/2026-02-01"},
		Basis:    indexsubstrate.CoverageBasisConfirmed,
		Complete: true,
		Gaps:     []indexsubstrate.Scope{{Prefix: "./"}},
	}})
	cmd := newCustodyE2EBridgeCommand(fixture, "")
	var output bytes.Buffer
	cmd.SetOut(&output)
	err := cmd.Execute()
	require.ErrorContains(t, err, indexreader.BridgeErrorManifestInvalid)
	require.Empty(t, output.String())
	require.Empty(t, mustReadCustodyE2EDirectory(t, fixture.targetRoot))
}

func TestCustodyBridgeE2E_RootSentinelCoverageStillBridges(t *testing.T) {
	fixture := newCustodyE2EFixture(t)
	receipt, _ := runCustodyE2EBridge(t, context.Background(), fixture, "")
	require.Equal(t, indexreader.BridgeDispositionCreated, receipt.Disposition)
	published, err := indexsubstrate.ReadInternalManifestFile(filepath.Join(
		fixture.targetRoot, "index-sets", fixture.env.indexSetID,
		"runs", fixture.env.runID, "manifest.json",
	))
	require.NoError(t, err)
	require.Len(t, published.Coverage, 1)
	require.Equal(t, indexsubstrate.RelativeRootScopePrefix, published.Coverage[0].Scope.Prefix)
}

func newCustodyE2ELegacyPrefixFixture(
	t *testing.T,
	coverage []indexsubstrate.CoverageAttestation,
) custodyE2EFixture {
	t.Helper()
	restoreBridgeCommandGlobals(t)
	oldAcquireResolver := configuredHubReadHandleResolver
	t.Cleanup(func() { configuredHubReadHandleResolver = oldAcquireResolver })
	configuredBridgeHubReadHandleResolver = resolveConfiguredBridgeHubReadHandle
	configuredHubPublishHandleResolver = resolveConfiguredHubPublishHandle
	configuredHubReadHandleResolver = resolveConfiguredHubReadHandle
	runLegacyDurableBridge = indexreader.BridgeLegacyDurableRun

	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("GONIMBUS_READONLY", "false")
	viper.Set("readonly", false)
	resetAppDataRootTestState(t)
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	dataRoot := filepath.Join(root, "data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)

	params := indexstore.IndexSetParams{
		BaseURI:         "s3://test-bucket/data/",
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
	identityDir := filepath.Join(dataRoot, "indexes", identity.DirName)
	require.NoError(t, os.MkdirAll(identityDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(identityDir, "identity.json"),
		[]byte(identity.CanonicalJSON+"\n"), 0o600,
	))

	runID := "run_1783100000000000000"
	segmentRoot := filepath.Join(dataRoot, "cache", "segments", identity.IndexSetID)
	runDir := filepath.Join(segmentRoot, "runs", runID)
	require.NoError(t, os.MkdirAll(runDir, 0o755))
	createdAt := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	rows := []indexsubstrate.CurrentObjectRow{
		durableCLIRow("shard-a/2026-02-01/object-a.txt", 11, "etag-a", createdAt),
		durableCLIRow("shard-b/2026-02-01/object-b.txt", 13, "etag-b", createdAt),
	}
	for i := range rows {
		rows[i].IndexSetID = identity.IndexSetID
		rows[i].FirstSeenRunID = runID
		rows[i].LastChangedRunID = runID
		rows[i].LastSeenRunID = runID
	}
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir: runDir, IndexSetID: identity.IndexSetID, RunID: runID,
		CreatedAt: createdAt, RunStartedAt: &createdAt,
		TargetRowsPerSegment: 1, Coverage: coverage,
	}, rows)
	require.NoError(t, err)
	require.Len(t, manifest.Segments, 2)
	require.NoError(t, indexsubstrate.WriteInternalManifestFile(
		filepath.Join(runDir, "manifest.json"), manifest,
	))

	env := durableCLIEnv{
		baseURI:     "s3://test-bucket/data/",
		indexSetID:  identity.IndexSetID,
		identityDir: identityDir,
		segmentRoot: segmentRoot,
		runID:       runID,
		params:      params,
	}
	sourceRoot := filepath.Join(root, "legacy-source")
	targetRoot := filepath.Join(root, "custody-target")
	require.NoError(t, os.Mkdir(sourceRoot, 0o700))
	require.NoError(t, os.Mkdir(targetRoot, 0o700))
	legacyPath, legacyBytes := writeCustodyE2ELegacyHub(t, env, sourceRoot)

	viper.Set("hub_read_handles.legacy-source.uri", "file://"+sourceRoot+"/")
	viper.Set("hub_publish_handles.custody-target.uri", "file://"+targetRoot+"/")
	viper.Set("hub_read_handles.custody-read.uri", "file://"+targetRoot+"/")
	resetCustodyE2EClock()
	return custodyE2EFixture{
		env: env, sourceRoot: sourceRoot, targetRoot: targetRoot,
		legacyPath: legacyPath, legacyBytes: legacyBytes,
	}
}
