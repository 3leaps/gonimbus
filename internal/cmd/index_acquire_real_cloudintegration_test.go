//go:build cloudintegration

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/provider"
	providers3 "github.com/3leaps/gonimbus/pkg/provider/s3"
)

const (
	hubAcquireRealS3ConfigEnv             = "GONIMBUS_HUB_ACQUIRE_REAL_S3_CONFIG"
	hubAcquireRealS3SelectedHandleEnv     = "GONIMBUS_HUB_ACQUIRE_REAL_S3_SELECTED_HANDLE"
	hubAcquireRealS3UnselectedHandleEnv   = "GONIMBUS_HUB_ACQUIRE_REAL_S3_UNSELECTED_HANDLE"
	hubAcquireRealS3FixtureScope          = "gonimbus-hub-acquire-evidence"
	hubAcquireRealS3FixtureCleanupTimeout = 2 * time.Minute
)

type observedHubExactReader struct {
	source indexreader.HubExactObjectReader

	mu    sync.Mutex
	keys  []string
	reads int
}

func (r *observedHubExactReader) OpenExact(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	r.mu.Lock()
	r.reads++
	r.keys = append(r.keys, key)
	r.mu.Unlock()
	return r.source.OpenExact(ctx, key)
}

func (r *observedHubExactReader) snapshot() (int, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads, append([]string(nil), r.keys...)
}

// TestIndexAcquireTwoAccountIsolation_RealS3 is an opt-in evidence lane. Its
// config stays outside the repository and binds two logical handles to two
// distinct real S3 account profiles. Fixture export and the unselected-handle
// probe happen before the measured operation so a skipped/broken credential
// cannot masquerade as isolation; only the selected handle is then available
// to acquisition.
func TestIndexAcquireTwoAccountIsolation_RealS3(t *testing.T) {
	configPath := strings.TrimSpace(os.Getenv(hubAcquireRealS3ConfigEnv))
	if configPath == "" {
		t.Skipf("%s not set; skipping two-account hub acquire evidence", hubAcquireRealS3ConfigEnv)
	}
	configPath = requireExternalLiveConfig(t, configPath)

	selectedName := requireLiveEnv(t, hubAcquireRealS3SelectedHandleEnv)
	unselectedName := requireLiveEnv(t, hubAcquireRealS3UnselectedHandleEnv)
	if selectedName == unselectedName {
		t.Fatal("selected and unselected hub read handles must differ")
	}

	liveConfig := viper.New()
	liveConfig.SetConfigFile(configPath)
	if err := liveConfig.ReadInConfig(); err != nil {
		t.Fatal("read external two-account hub configuration")
	}
	requireDistinctLiveS3Handles(t, liveConfig, selectedName, unselectedName)
	scope := fmt.Sprintf("%s/%d", hubAcquireRealS3FixtureScope, time.Now().UnixNano())
	scopeLiveHubHandle(t, liveConfig, selectedName, scope)
	scopeLiveHubHandle(t, liveConfig, unselectedName, scope)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	fixture := seedDurableOnlyAppData(t, dataRoot, []indexsubstrate.CurrentObjectRow{
		durableCLIRow("objects/evidence.txt", 17, "evidence-etag", time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)),
	})
	registerLiveHubCleanup(t, liveConfig, selectedName)
	registerLiveHubCleanup(t, liveConfig, unselectedName)
	runLiveDurableHubExport(t, ctx, liveConfig, selectedName, fixture.indexSetID, fixture.runID)
	runLiveDurableHubExport(t, ctx, liveConfig, unselectedName, fixture.indexSetID, fixture.runID)

	selected, err := resolveConfiguredHubReadHandleFrom(ctx, liveConfig, selectedName)
	if err != nil {
		t.Fatal("resolve selected live hub read handle; check credentials and IP allowlist")
	}
	t.Cleanup(func() {
		if selected.closer != nil {
			_ = selected.closer.Close()
		}
	})
	unselected, err := resolveConfiguredHubReadHandleFrom(ctx, liveConfig, unselectedName)
	if err != nil {
		t.Fatal("resolve unselected live hub read handle; check credentials and IP allowlist")
	}
	t.Cleanup(func() {
		if unselected.closer != nil {
			_ = unselected.closer.Close()
		}
	})

	unselectedProbeKey := path.Join("index-sets", fixture.indexSetID, "runs", fixture.runID, "complete.json")
	probeBody, probeSize, err := unselected.reader.OpenExact(ctx, unselectedProbeKey)
	if err != nil {
		t.Fatal("unselected live account probe failed; check fixture access and IP allowlist")
	}
	_, copyErr := io.Copy(io.Discard, io.LimitReader(probeBody, 1))
	closeErr := probeBody.Close()
	require.NoError(t, copyErr)
	require.NoError(t, closeErr)
	require.Positive(t, probeSize)

	selectedObserved := &observedHubExactReader{source: selected.reader}
	unselectedObserved := &observedHubExactReader{source: unselected.reader}
	oldResolver := configuredHubReadHandleResolver
	t.Cleanup(func() { configuredHubReadHandleResolver = oldResolver })
	var selectedResolutions, unselectedResolutions int
	configuredHubReadHandleResolver = func(_ context.Context, name string) (hubReadHandleResolution, error) {
		switch name {
		case selectedName:
			selectedResolutions++
			return hubReadHandleResolution{reader: selectedObserved}, nil
		case unselectedName:
			unselectedResolutions++
			return hubReadHandleResolution{reader: unselectedObserved}, nil
		default:
			return hubReadHandleResolution{}, fmt.Errorf("unexpected hub_read_handle")
		}
	}

	parent, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	bundleDir := filepath.Join(parent, "acquired")
	acquireCmd := newIndexAcquireCommandForTest()
	acquireCmd.SetContext(ctx)
	acquireCmd.SetArgs([]string{
		"--hub-read-handle", selectedName,
		"--index-set", fixture.indexSetID,
		"--run-id", fixture.runID,
		"--dest", bundleDir,
	})
	require.NoError(t, acquireCmd.Execute())

	selectedReads, selectedKeys := selectedObserved.snapshot()
	unselectedReads, _ := unselectedObserved.snapshot()
	require.Equal(t, 1, selectedResolutions)
	require.Zero(t, unselectedResolutions)
	require.Positive(t, selectedReads)
	require.Zero(t, unselectedReads)
	for _, key := range selectedKeys {
		require.NotEqual(t, "latest.json", path.Base(key))
	}

	stdout, stderr, err := executeIndexQueryCommand(t,
		"--snapshot-dir", bundleDir,
		"--count",
		"--output-format", indexQueryReceiptOutputFormat,
	)
	require.NoError(t, err, "stderr=%q", stderr)
	lines := nonEmptyLines(stdout)
	require.Len(t, lines, 1, "stdout=%q stderr=%q", stdout, stderr)
	var receipt indexQueryReceiptRecord
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &receipt))
	require.Equal(t, indexQueryReceiptType, receipt.Type)
	require.Equal(t, "success", receipt.Outcome)
	require.Equal(t, string(indexreader.SnapshotSourceAcquiredHub), receipt.SourceKind)
	require.Equal(t, fixture.indexSetID, receipt.IndexSetID)
	require.Equal(t, fixture.runID, receipt.RunID)
	validateQueryReceiptAgainstSchema(t, receipt)

	for _, forbidden := range []string{
		selectedName,
		unselectedName,
		liveConfig.GetString("hub_read_handles." + selectedName + ".profile"),
		liveConfig.GetString("hub_read_handles." + unselectedName + ".profile"),
		liveConfig.GetString("hub_read_handles." + selectedName + ".uri"),
		liveConfig.GetString("hub_read_handles." + unselectedName + ".uri"),
	} {
		if forbidden != "" && strings.Contains(stdout, forbidden) {
			t.Fatal("query receipt exposed configured authority detail")
		}
	}
}

func scopeLiveHubHandle(t *testing.T, config *viper.Viper, name, scope string) {
	t.Helper()
	key := "hub_read_handles." + name
	handle := config.Sub(key)
	if handle == nil {
		t.Fatal("live hub handle is not configured")
	}
	base := strings.TrimSpace(handle.GetString("uri"))
	hub, err := parseHubURI(base)
	if err != nil || hub.Provider != "s3" {
		t.Fatal("two-account real S3 evidence requires a valid S3 hub URI")
	}
	config.Set(key+".uri", strings.TrimSuffix(base, "/")+"/"+scope+"/")
	for _, field := range []string{"profile", "region", "endpoint", "gcp_project"} {
		config.Set(key+"."+field, strings.TrimSpace(handle.GetString(field)))
	}
}

func runLiveDurableHubExport(
	t *testing.T,
	ctx context.Context,
	config *viper.Viper,
	name, indexSetID, runID string,
) {
	t.Helper()
	handle := config.Sub("hub_read_handles." + name)
	if handle == nil {
		t.Fatal("live hub handle is not configured")
	}
	cmd := &cobra.Command{Use: "export", RunE: runIndexExport}
	cmd.Flags().String("hub", "", "")
	cmd.Flags().String("index-set", "", "")
	cmd.Flags().String("run-id", "", "")
	cmd.Flags().String("db", "", "")
	cmd.Flags().String("format", "", "")
	cmd.Flags().String("hub-profile", "", "")
	cmd.Flags().String("hub-region", "", "")
	cmd.Flags().String("hub-endpoint", "", "")
	cmd.Flags().String("hub-gcp-project", "", "")
	addLatestPointerFlags(cmd)
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{
		"--hub", strings.TrimSpace(handle.GetString("uri")),
		"--index-set", indexSetID,
		"--run-id", runID,
		"--format", "durable",
		"--hub-profile", strings.TrimSpace(handle.GetString("profile")),
		"--hub-region", strings.TrimSpace(handle.GetString("region")),
		"--hub-endpoint", strings.TrimSpace(handle.GetString("endpoint")),
	})
	if err := executeWithDiscardedStderr(t, cmd); err != nil {
		t.Fatal("provision live durable hub fixture; check credentials and IP allowlist")
	}
}

func executeWithDiscardedStderr(t *testing.T, cmd *cobra.Command) error {
	t.Helper()
	oldStderr := os.Stderr
	stderrR, stderrW, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = stderrW
	drained := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, stderrR)
		close(drained)
	}()
	execErr := cmd.Execute()
	os.Stderr = oldStderr
	require.NoError(t, stderrW.Close())
	<-drained
	require.NoError(t, stderrR.Close())
	return execErr
}

func registerLiveHubCleanup(t *testing.T, config *viper.Viper, name string) {
	t.Helper()
	handle := config.Sub("hub_read_handles." + name)
	if handle == nil {
		t.Fatal("live hub handle is not configured")
	}
	hub, err := parseHubURI(strings.TrimSpace(handle.GetString("uri")))
	if err != nil || hub.Provider != "s3" {
		t.Fatal("live hub cleanup requires a valid S3 hub URI")
	}
	cfg := providers3.Config{
		Bucket:         hub.Bucket,
		Region:         strings.TrimSpace(handle.GetString("region")),
		Endpoint:       strings.TrimSpace(handle.GetString("endpoint")),
		Profile:        strings.TrimSpace(handle.GetString("profile")),
		ForcePathStyle: strings.TrimSpace(handle.GetString("endpoint")) != "",
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), hubAcquireRealS3FixtureCleanupTimeout)
		defer cancel()
		p, err := providers3.New(ctx, cfg)
		if err != nil {
			t.Log("warning: live hub fixture cleanup provider unavailable")
			return
		}
		defer func() { _ = p.Close() }()
		token := ""
		for {
			page, err := p.List(ctx, provider.ListOptions{Prefix: hub.Prefix, ContinuationToken: token})
			if err != nil {
				t.Log("warning: live hub fixture cleanup list failed")
				return
			}
			for _, object := range page.Objects {
				if err := p.DeleteObject(ctx, object.Key); err != nil {
					t.Log("warning: live hub fixture cleanup delete failed")
				}
			}
			if !page.IsTruncated || page.ContinuationToken == "" {
				return
			}
			token = page.ContinuationToken
		}
	})
}

func requireLiveEnv(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required when %s is set", name, hubAcquireRealS3ConfigEnv)
	}
	return value
}

func requireExternalLiveConfig(t *testing.T, configPath string) string {
	t.Helper()
	if !filepath.IsAbs(configPath) {
		t.Fatalf("%s must be an absolute path outside the repository", hubAcquireRealS3ConfigEnv)
	}
	resolved, err := filepath.EvalSymlinks(filepath.Clean(configPath))
	if err != nil {
		t.Fatal("resolve external two-account hub configuration")
	}
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	rel, err := filepath.Rel(repoRoot, resolved)
	require.NoError(t, err)
	if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
		t.Fatalf("%s must resolve outside the repository", hubAcquireRealS3ConfigEnv)
	}
	return resolved
}

func requireDistinctLiveS3Handles(t *testing.T, config *viper.Viper, selectedName, unselectedName string) {
	t.Helper()
	selected := config.Sub("hub_read_handles." + selectedName)
	unselected := config.Sub("hub_read_handles." + unselectedName)
	if selected == nil || unselected == nil {
		t.Fatal("external configuration must define both hub read handles")
	}
	selectedHub, selectedErr := parseHubURI(strings.TrimSpace(selected.GetString("uri")))
	unselectedHub, unselectedErr := parseHubURI(strings.TrimSpace(unselected.GetString("uri")))
	if selectedErr != nil || unselectedErr != nil ||
		selectedHub.Provider != "s3" || unselectedHub.Provider != "s3" {
		t.Fatal("two-account real S3 evidence requires two valid S3 hub URIs")
	}
	selectedProfile := strings.TrimSpace(selected.GetString("profile"))
	unselectedProfile := strings.TrimSpace(unselected.GetString("profile"))
	if selectedProfile == "" || unselectedProfile == "" || selectedProfile == unselectedProfile {
		t.Fatal("two-account real S3 evidence requires two distinct non-empty profiles")
	}
}
