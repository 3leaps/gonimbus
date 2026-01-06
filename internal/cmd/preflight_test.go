package cmd

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreflightCrawl_PlanOnly_WritesRecord(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	preflightMode = "plan-only"
	preflightProbeStrategy = "multipart-abort"
	preflightProbePrefix = "_gonimbus/probe/"

	rootCmd.SetArgs([]string{"preflight", "crawl", "s3://bucket/data/**/*.parquet", "--mode", "plan-only"})
	rootCmd.SetContext(context.Background())

	require.NoError(t, rootCmd.Execute())
	rootCmd.SetArgs(nil)

	require.NoError(t, w.Close())

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	out := buf.String()

	require.Contains(t, out, "gonimbus.preflight.v1")
	require.Contains(t, out, "\"mode\":\"plan-only\"")
}
