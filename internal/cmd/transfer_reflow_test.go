package cmd

import (
	"context"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

func TestValidateTransferReflowArgs(t *testing.T) {
	makeCmd := func(stdin bool) *cobra.Command {
		c := &cobra.Command{}
		c.Flags().Bool("stdin", stdin, "")
		return c
	}

	t.Run("without stdin requires source uri", func(t *testing.T) {
		err := validateTransferReflowArgs(makeCmd(false), []string{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires exactly 1 argument")
	})

	t.Run("without stdin accepts one source uri", func(t *testing.T) {
		err := validateTransferReflowArgs(makeCmd(false), []string{"s3://bucket/source/file.xml"})
		require.NoError(t, err)
	})

	t.Run("without stdin rejects too many source uris", func(t *testing.T) {
		err := validateTransferReflowArgs(makeCmd(false), []string{"s3://bucket/source/a.xml", "s3://bucket/source/b.xml"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires exactly 1 argument")
	})

	t.Run("stdin accepts no positional args", func(t *testing.T) {
		err := validateTransferReflowArgs(makeCmd(true), []string{})
		require.NoError(t, err)
	})

	t.Run("stdin rejects positional source uri", func(t *testing.T) {
		err := validateTransferReflowArgs(makeCmd(true), []string{"s3://bucket/source/file.xml"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "when using --stdin")
		require.Contains(t, err.Error(), "do not provide source-uri arguments")
	})
}

func TestEnqueueReflowLine_ReflowInputRecord(t *testing.T) {
	out := make(chan reflowTask, 1)
	var providerBuckets []string
	getProviders := func(bucket string) (provider.Provider, provider.Provider, error) {
		providerBuckets = append(providerBuckets, bucket)
		return &mockProvider{}, &mockProvider{}, nil
	}

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://bucket/source/file.xml","source_key":"source/file.xml","source_etag":"abc123","source_size_bytes":42,"vars":{"site":"001"},"dest_rel_key":"dest/file.xml"}}`
	srcBucket, err := enqueueReflowLine(context.Background(), line, "", getProviders, out)
	require.NoError(t, err)
	require.Equal(t, "bucket", srcBucket)
	require.Equal(t, []string{"bucket"}, providerBuckets)

	task := <-out
	require.Equal(t, "bucket", task.SourceBucket)
	require.Equal(t, "s3://bucket/source/file.xml", task.SourceURI)
	require.Equal(t, "source/file.xml", task.SourceKey)
	require.Equal(t, "abc123", task.SourceETag)
	require.Equal(t, int64(42), task.SourceSize)
	require.Equal(t, map[string]string{"site": "001"}, task.Vars)
	require.Equal(t, "dest/file.xml", task.DestRelKey)
}

func TestEnqueueReflowLine_ReflowInputRecordRejectsPrefixURI(t *testing.T) {
	out := make(chan reflowTask, 1)
	getProviders := func(bucket string) (provider.Provider, provider.Provider, error) {
		t.Fatalf("getProviders should not be called for invalid exact-object input")
		return nil, nil, nil
	}

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://bucket/source/","source_key":"source/"}}`
	_, err := enqueueReflowLine(context.Background(), line, "", getProviders, out)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be an exact object URI")
	require.Empty(t, out)
}
