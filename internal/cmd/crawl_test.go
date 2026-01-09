package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/manifest"
)

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
