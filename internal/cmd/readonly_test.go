package cmd

import (
	"context"
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func resetReadOnly(t *testing.T) {
	t.Helper()
	readOnly = false
	viper.Set("readonly", false)
	require.NoError(t, rootCmd.PersistentFlags().Set("readonly", "false"))
}

func TestPreflightCrawl_ReadOnly_BlocksWriteProbe(t *testing.T) {
	resetReadOnly(t)

	rootCmd.SetArgs([]string{"--readonly", "preflight", "crawl", "s3://bucket/data/**/*.parquet", "--mode", "write-probe"})
	rootCmd.SetContext(context.Background())

	err := rootCmd.Execute()
	rootCmd.SetArgs(nil)
	resetReadOnly(t)

	require.Error(t, err)
	require.Contains(t, err.Error(), "readonly")
}

func TestPreflightWrite_ReadOnly_BlocksWriteProbe(t *testing.T) {
	resetReadOnly(t)

	rootCmd.SetArgs([]string{"--readonly", "preflight", "write", "s3://bucket/", "--mode", "write-probe"})
	rootCmd.SetContext(context.Background())

	err := rootCmd.Execute()
	rootCmd.SetArgs(nil)
	resetReadOnly(t)

	require.Error(t, err)
	require.Contains(t, err.Error(), "readonly")
}

func TestCrawl_ReadOnly_BlocksWriteProbe(t *testing.T) {
	resetReadOnly(t)

	f, err := os.CreateTemp("", "gonimbus-crawl-*.yaml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(f.Name()) }()
	defer func() { _ = f.Close() }()

	_, err = f.WriteString(`version: "1.0"
connection:
  provider: s3
  bucket: test-bucket
  region: us-east-1

match:
  includes:
    - "data/**"

crawl:
  preflight:
    mode: write-probe

output:
  destination: stdout
`)
	require.NoError(t, err)

	rootCmd.SetArgs([]string{"--readonly", "crawl", "--job", f.Name()})
	rootCmd.SetContext(context.Background())

	err = rootCmd.Execute()
	rootCmd.SetArgs(nil)
	resetReadOnly(t)

	require.Error(t, err)
	require.Contains(t, err.Error(), "readonly")
}

func TestTransfer_ReadOnly_BlocksExecution(t *testing.T) {
	resetReadOnly(t)

	f, err := os.CreateTemp("", "gonimbus-transfer-*.yaml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(f.Name()) }()
	defer func() { _ = f.Close() }()

	_, err = f.WriteString(`version: "1.0"
source:
  provider: s3
  bucket: source-bucket
  region: us-east-1

target:
  provider: s3
  bucket: target-bucket
  region: us-east-1

match:
  includes:
    - "data/**"

transfer:
  mode: copy
  preflight:
    mode: read-safe

output:
  destination: stdout
`)
	require.NoError(t, err)

	rootCmd.SetArgs([]string{"--readonly", "transfer", "--job", f.Name()})
	rootCmd.SetContext(context.Background())

	err = rootCmd.Execute()
	rootCmd.SetArgs(nil)
	resetReadOnly(t)

	require.Error(t, err)
	require.Contains(t, err.Error(), "readonly")
}
