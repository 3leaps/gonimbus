package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/output"
)

func withCrawlTestState(t *testing.T) {
	t.Helper()
	oldEmit := crawlEmit
	oldSelectionSummary := crawlSelectionSum
	oldMinObjects := crawlMinObjects
	oldMaxBytes := crawlMaxBytes
	t.Cleanup(func() {
		crawlEmit = oldEmit
		crawlSelectionSum = oldSelectionSummary
		crawlMinObjects = oldMinObjects
		crawlMaxBytes = oldMaxBytes
	})
}

func TestShowCrawlPlan(t *testing.T) {
	tests := []struct {
		name     string
		manifest *manifest.Manifest
		contains []string
	}{
		{
			name: "basic manifest",
			manifest: &manifest.Manifest{
				Connection: manifest.ConnectionConfig{
					Provider: "s3",
					Bucket:   "test-bucket",
					Region:   "us-east-1",
				},
				Match: manifest.MatchConfig{
					Includes: []string{"**/*"},
				},
				Crawl: manifest.CrawlConfig{
					Concurrency: 10,
				},
				Output: manifest.OutputConfig{
					Destination: "stdout",
				},
			},
			contains: []string{
				"Crawl Plan (dry-run)",
				"Provider:    s3",
				"Bucket:      test-bucket",
				"Region:      us-east-1",
				"**/*",
				"Concurrency: 10",
				"Output:      stdout",
			},
		},
		{
			name: "with endpoint and excludes",
			manifest: &manifest.Manifest{
				Connection: manifest.ConnectionConfig{
					Provider: "s3",
					Bucket:   "test-bucket",
					Endpoint: "https://custom.endpoint.com",
				},
				Match: manifest.MatchConfig{
					Includes: []string{"data/**/*.parquet"},
					Excludes: []string{"**/.DS_Store", "**/tmp/*"},
				},
				Crawl: manifest.CrawlConfig{
					Concurrency: 5,
					RateLimit:   100.0,
				},
				Output: manifest.OutputConfig{
					Destination: "results.jsonl",
				},
			},
			contains: []string{
				"Endpoint:    https://custom.endpoint.com",
				"data/**/*.parquet",
				"Exclude:",
				"**/.DS_Store",
				"**/tmp/*",
				"Rate Limit:  100.0 req/s",
				"Output:      results.jsonl",
			},
		},
		{
			name: "with filters",
			manifest: &manifest.Manifest{
				Connection: manifest.ConnectionConfig{
					Provider: "s3",
					Bucket:   "test-bucket",
				},
				Match: manifest.MatchConfig{
					Includes: []string{"data/**/*.parquet"},
					Filters: &manifest.FilterConfig{
						Size:     &manifest.SizeFilterConfig{Min: "1KB", Max: "100MB"},
						Modified: &manifest.DateFilterConfig{After: "2024-01-01", Before: "2024-12-31"},
						KeyRegex: "\\.parquet$",
					},
				},
				Crawl:  manifest.CrawlConfig{Concurrency: 5},
				Output: manifest.OutputConfig{Destination: "stdout"},
			},
			contains: []string{
				"Filters:",
				"Size:      min=1KB max=100MB",
				"Modified:  after=2024-01-01 before=2024-12-31",
				"Key Regex: \\.parquet$",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture stdout
			old := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			err := showCrawlPlan(tt.manifest)
			require.NoError(t, err)

			require.NoError(t, w.Close())
			os.Stdout = old

			var buf bytes.Buffer
			_, _ = buf.ReadFrom(r)
			output := buf.String()

			for _, want := range tt.contains {
				assert.Contains(t, output, want, "output should contain %q", want)
			}
		})
	}
}

func TestCreateWriter_Stdout(t *testing.T) {
	m := &manifest.Manifest{
		Connection: manifest.ConnectionConfig{Provider: "s3"},
		Output:     manifest.OutputConfig{Destination: "stdout"},
	}

	writer, cleanup, err := createWriter(m, "test-job-id")
	require.NoError(t, err)
	require.NotNil(t, writer)
	require.NotNil(t, cleanup)

	// Cleanup shouldn't panic
	cleanup()
}

func TestCreateWriter_EmptyDestination(t *testing.T) {
	m := &manifest.Manifest{
		Connection: manifest.ConnectionConfig{Provider: "s3"},
		Output:     manifest.OutputConfig{Destination: ""},
	}

	writer, cleanup, err := createWriter(m, "test-job-id")
	require.NoError(t, err)
	require.NotNil(t, writer)
	require.NotNil(t, cleanup)

	cleanup()
}

func TestCreateWriter_FileDestination(t *testing.T) {
	tmpDir := t.TempDir()
	outPath := filepath.Join(tmpDir, "output.jsonl")

	m := &manifest.Manifest{
		Connection: manifest.ConnectionConfig{Provider: "s3"},
		Output:     manifest.OutputConfig{Destination: outPath},
	}

	writer, cleanup, err := createWriter(m, "test-job-id")
	require.NoError(t, err)
	require.NotNil(t, writer)
	require.NotNil(t, cleanup)

	// File should exist
	_, err = os.Stat(outPath)
	require.NoError(t, err)

	cleanup()
}

func TestCreateWriter_FilePrefix(t *testing.T) {
	tmpDir := t.TempDir()
	outPath := filepath.Join(tmpDir, "output.jsonl")

	m := &manifest.Manifest{
		Connection: manifest.ConnectionConfig{Provider: "s3"},
		Output:     manifest.OutputConfig{Destination: "file:" + outPath},
	}

	writer, cleanup, err := createWriter(m, "test-job-id")
	require.NoError(t, err)
	require.NotNil(t, writer)

	// File should exist
	_, err = os.Stat(outPath)
	require.NoError(t, err)

	cleanup()
}

func TestCreateWriter_InvalidPath(t *testing.T) {
	m := &manifest.Manifest{
		Connection: manifest.ConnectionConfig{Provider: "s3"},
		Output:     manifest.OutputConfig{Destination: "/nonexistent/deeply/nested/path/output.jsonl"},
	}

	_, _, err := createWriter(m, "test-job-id")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create output file")
}

func TestCrawlFileReflowInputSelectionSummaryAndGuard(t *testing.T) {
	withCrawlTestState(t)

	srcDir := createCrawlFileFixture(t)
	summaryPath := filepath.Join(t.TempDir(), "selection.jsonl")
	crawlEmit = crawlEmitReflowInput
	crawlSelectionSum = summaryPath
	crawlMinObjects = 2
	crawlMaxBytes = 100

	stdout, err := runCrawlManifestForTest(t, crawlFileManifest(srcDir))
	require.NoError(t, err)

	records := parseCrawlReflowInputRecords(t, stdout)
	require.Len(t, records, 2)
	require.Equal(t, []string{".secret/token", "keep.txt"}, []string{records[0].SourceKey, records[1].SourceKey})
	require.Contains(t, records[0].SourceURI, srcDir)
	require.Contains(t, records[1].SourceURI, srcDir)
	require.NotContains(t, stdout, "link.txt")
	require.NotContains(t, stdout, "skip.tmp")
	requireNoRecordType(t, stdout, output.TypeSummary)
	requireNoRecordType(t, stdout, output.TypePreflight)

	summary := readCrawlSelectionSummary(t, summaryPath)
	require.Equal(t, int64(2), summary.ObjectsSelected)
	require.Equal(t, int64(len("secret")+len("keep")), summary.BytesTotal)
	require.Equal(t, "ok", summary.Status)

	crawlSelectionSum = filepath.Join(t.TempDir(), "selection-failed.jsonl")
	crawlMaxBytes = 1
	stdout, err = runCrawlManifestForTest(t, crawlFileManifest(srcDir))
	require.Error(t, err)
	require.Contains(t, err.Error(), "maximum byte threshold")
	require.Empty(t, strings.TrimSpace(stdout))
	failed := readCrawlSelectionSummary(t, crawlSelectionSum)
	require.Equal(t, "failed", failed.Status)
	require.Equal(t, "max_bytes", failed.Reason)
}

func TestCrawlFileReflowInputPipesToTransferReflowWithRedactedSidecar(t *testing.T) {
	withCrawlTestState(t)

	srcDir := createCrawlFileFixture(t)
	crawlEmit = crawlEmitReflowInput
	crawlSelectionSum = filepath.Join(t.TempDir(), "selection.jsonl")
	crawlOut, err := runCrawlManifestForTest(t, crawlFileManifest(srcDir))
	require.NoError(t, err)

	withTransferReflowTestState(t)
	destDir := t.TempDir()
	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader(crawlOut))
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"--stdin",
		"--dest", fileURI(destDir) + "/",
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--parallel", "1",
		"--provenance", "sidecar",
	})

	require.NoError(t, cmd.Execute(), stdout.String())
	require.Equal(t, "keep", string(mustReadFile(t, filepath.Join(destDir, "keep.txt"))))
	require.Equal(t, "secret", string(mustReadFile(t, filepath.Join(destDir, ".secret", "token"))))
	require.NoFileExists(t, filepath.Join(destDir, "link.txt"))
	require.NoFileExists(t, filepath.Join(destDir, "skip.tmp"))
	require.Contains(t, stdout.String(), `"source_uri":"file://local/.secret/token"`)
	require.NotContains(t, stdout.String(), srcDir)

	sidecarRaw := mustReadFile(t, filepath.Join(destDir, "keep.txt"+provenanceSuffix))
	require.NotContains(t, string(sidecarRaw), srcDir)
	var sidecar map[string]any
	require.NoError(t, json.Unmarshal(sidecarRaw, &sidecar))
	source, ok := sidecar["source"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "file://local/keep.txt", source["uri"])
}

func TestExitError(t *testing.T) {
	tests := []struct {
		name    string
		code    int
		message string
		err     error
		want    string
	}{
		{
			name:    "basic error",
			code:    1,
			message: "Something failed",
			err:     assert.AnError,
			want:    "Something failed",
		},
		{
			name:    "includes exit code",
			code:    32,
			message: "Auth failed",
			err:     assert.AnError,
			want:    "exit code 32",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := exitError(tt.code, tt.message, tt.err)
			require.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), tt.want))
		})
	}
}

func createCrawlFileFixture(t *testing.T) string {
	t.Helper()
	srcDir := mustEvalSymlinks(t, t.TempDir())
	outsideDir := mustEvalSymlinks(t, t.TempDir())
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, ".secret"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "keep.txt"), []byte("keep"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, ".secret", "token"), []byte("secret"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "skip.tmp"), []byte("skip"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(outsideDir, "outside.txt"), []byte("outside"), 0o600))
	require.NoError(t, os.Symlink(filepath.Join(outsideDir, "outside.txt"), filepath.Join(srcDir, "link.txt")))
	return srcDir
}

func crawlFileManifest(srcDir string) *manifest.Manifest {
	return &manifest.Manifest{
		Version: "1.0",
		Connection: manifest.ConnectionConfig{
			Provider: "file",
			BaseDir:  srcDir,
		},
		Match: manifest.MatchConfig{
			Includes:      []string{"**"},
			Excludes:      []string{"skip.tmp"},
			IncludeHidden: true,
		},
		Crawl: manifest.CrawlConfig{Concurrency: 1},
		Output: manifest.OutputConfig{
			Destination: "stdout",
		},
	}
}

func runCrawlManifestForTest(t *testing.T, m *manifest.Manifest) (string, error) {
	t.Helper()
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	defer func() {
		os.Stdout = oldStdout
		_ = r.Close()
	}()

	runErr := executeCrawl(context.Background(), m)
	require.NoError(t, w.Close())

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	return buf.String(), runErr
}

func parseCrawlReflowInputRecords(t *testing.T, stdout string) []reflowInputRecord {
	t.Helper()
	var out []reflowInputRecord
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var env testRecordEnvelope
		require.NoError(t, json.Unmarshal([]byte(line), &env))
		require.Equal(t, crawlReflowInputType, env.Type)
		var rec reflowInputRecord
		require.NoError(t, json.Unmarshal(env.Data, &rec))
		out = append(out, rec)
	}
	return out
}

func readCrawlSelectionSummary(t *testing.T, path string) crawlSelectionSummary {
	t.Helper()
	raw := mustReadFile(t, path)
	record := requireRecord(t, string(raw), crawlSelectionSumType, "")
	var summary crawlSelectionSummary
	require.NoError(t, json.Unmarshal(record.Data, &summary))
	return summary
}
