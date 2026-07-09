package cmd

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/indexbuild"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errors.New("broken stderr") }

func TestStderrProgressWriterBestEffortOnWriteError(t *testing.T) {
	w := newStderrProgressWriter(errorWriter{})
	err := w.WriteProgress(context.Background(), &output.ProgressRecord{
		Phase:          "listing",
		Prefix:         "data/",
		ObjectsFound:   10,
		ObjectsMatched: 8,
		BytesTotal:     100,
	})
	require.NoError(t, err, "progress sink must never fail the build on stderr write errors")
}

func TestStderrProgressWriterRendersCrawlShape(t *testing.T) {
	var buf bytes.Buffer
	w := newStderrProgressWriter(&buf)
	require.NoError(t, w.WriteProgress(context.Background(), &output.ProgressRecord{
		Phase:          "listing",
		Prefix:         "hot/",
		ObjectsFound:   3,
		ObjectsMatched: 2,
		BytesTotal:     42,
	}))
	require.Equal(t, "progress: phase=listing prefix=hot/ objects_found=3 objects_matched=2 bytes=42\n", buf.String())
	require.NoError(t, w.WriteObject(context.Background(), &output.ObjectRecord{}))
	require.NoError(t, w.Close())
}

func TestStderrSegmentProgressBestEffortAndShape(t *testing.T) {
	var buf bytes.Buffer
	fn := newStderrSegmentProgress(&buf)
	fn(indexbuild.SegmentProgress{Segment: 1, Total: 2, Rows: 100, RowsDone: 100})
	fn(indexbuild.SegmentProgress{Segment: 2, Total: 2, Rows: 50, RowsDone: 150})
	require.Equal(t,
		"progress: phase=segmenting segment=1/2 rows=100\nprogress: phase=segmenting segment=2/2 rows=50\n",
		buf.String(),
	)

	require.NotPanics(t, func() {
		newStderrSegmentProgress(errorWriter{})(indexbuild.SegmentProgress{Segment: 1, Total: 1, Rows: 1})
	})
}

func TestIndexBuildDurableSucceedsWithProgressSinkWired(t *testing.T) {
	// Durable-only path wires ObservationSinks + OnSegmentProgress; ensure a
	// small build still succeeds (progress is best-effort and stderr-bound).
	resetAppDataRootTestState(t)
	dataRoot := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	base := time.Date(2026, 7, 9, 15, 0, 0, 0, time.UTC)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`
version: "1.0"
connection:
  provider: s3
  bucket: bucket
  base_uri: s3://bucket/data/
identity:
  storage_provider: aws_s3
build:
  source: crawl
  match:
    includes: ["**"]
  crawl:
    concurrency: 1
    progress_every: 1
`), 0o600))

	restore := withIndexBuildExperimentalEngineTestState(t)
	restore()
	indexBuildJobPath = manifestPath
	indexBuildFormat = "durable"

	oldSource := newIndexBuildEngineSource
	newIndexBuildEngineSource = func(context.Context, *uri.ObjectURI, providerdispatch.SourceOptions) (provider.Provider, error) {
		return indexBuildEngineFakeProvider{objects: indexBuildEngineTestObjects(base)}, nil
	}
	t.Cleanup(func() { newIndexBuildEngineSource = oldSource })

	cmd := &cobra.Command{Use: "build"}
	cmd.SetContext(context.Background())
	var stdout strings.Builder
	cmd.SetOut(&stdout)
	require.NoError(t, runIndexBuild(cmd, nil))
	require.Empty(t, strings.TrimSpace(stdout.String()), "durable build must not emit progress on stdout")

	latestFiles, err := filepath.Glob(filepath.Join(dataRoot, "cache", "segments", "*", "latest.json"))
	require.NoError(t, err)
	require.Len(t, latestFiles, 1)
}
