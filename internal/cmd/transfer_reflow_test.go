package cmd

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/fulmenhq/gofulmen/appidentity"
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

func TestTransferReflowCommand_StdinPipeConsumesInput(t *testing.T) {
	withTransferReflowTestState(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader("{\"type\":\"unsupported\",\"data\":{}}\n"))
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--stdin",
		"--dest", fileURI(t.TempDir()),
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--dry-run",
		"--parallel", "1",
	})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid_inputs=1")
	require.NotContains(t, err.Error(), "source-uri")
	require.NotContains(t, err.Error(), "requires exactly 1 argument")
	require.Contains(t, stdout.String(), "unsupported json record type")
	require.Contains(t, stdout.String(), "transfer_reflow")
}

func TestTransferReflowCommand_StdinRejectsExplicitSourceArg(t *testing.T) {
	withTransferReflowTestState(t)

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader("{\"type\":\"unsupported\",\"data\":{}}\n"))
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"--stdin",
		"--dest", fileURI(t.TempDir()),
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"s3://bucket/source/file.xml",
	})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "when using --stdin")
	require.Contains(t, err.Error(), "do not provide source-uri arguments")
	require.Empty(t, stdout.String())
}

func newTransferReflowTestCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "reflow [source-uri]",
		Args:         validateTransferReflowArgs,
		RunE:         runTransferReflow,
		SilenceUsage: true,
	}
	cmd.Flags().BoolVar(&reflowStdin, "stdin", false, "")
	cmd.Flags().StringVar(&reflowDest, "dest", "", "")
	cmd.Flags().StringVar(&reflowRewriteFrom, "rewrite-from", "", "")
	cmd.Flags().StringVar(&reflowRewriteTo, "rewrite-to", "", "")
	cmd.Flags().IntVar(&reflowParallel, "parallel", 16, "")
	cmd.Flags().BoolVar(&reflowDryRun, "dry-run", false, "")
	cmd.Flags().BoolVar(&reflowResume, "resume", false, "")
	cmd.Flags().StringVar(&reflowCheckpoint, "checkpoint", "", "")
	cmd.Flags().BoolVar(&reflowOverwrite, "overwrite", false, "")
	cmd.Flags().StringVar(&reflowOnCollision, "on-collision", "log", "")
	cmd.Flags().StringVar(&reflowSrcRegion, "src-region", "", "")
	cmd.Flags().StringVar(&reflowSrcProfile, "src-profile", "", "")
	cmd.Flags().StringVar(&reflowSrcEndpoint, "src-endpoint", "", "")
	cmd.Flags().StringVar(&reflowDstRegion, "dest-region", "", "")
	cmd.Flags().StringVar(&reflowDstProfile, "dest-profile", "", "")
	cmd.Flags().StringVar(&reflowDstEndpoint, "dest-endpoint", "", "")
	return cmd
}

func withTransferReflowTestState(t *testing.T) {
	t.Helper()

	oldIdentity := appIdentity
	oldStdin := reflowStdin
	oldDest := reflowDest
	oldRewriteFrom := reflowRewriteFrom
	oldRewriteTo := reflowRewriteTo
	oldParallel := reflowParallel
	oldDryRun := reflowDryRun
	oldResume := reflowResume
	oldCheckpoint := reflowCheckpoint
	oldOverwrite := reflowOverwrite
	oldOnCollision := reflowOnCollision
	oldSrcRegion := reflowSrcRegion
	oldSrcProfile := reflowSrcProfile
	oldSrcEndpoint := reflowSrcEndpoint
	oldDstRegion := reflowDstRegion
	oldDstProfile := reflowDstProfile
	oldDstEndpoint := reflowDstEndpoint

	t.Setenv("XDG_DATA_HOME", t.TempDir())
	appIdentity = &appidentity.Identity{
		BinaryName: "gonimbus",
		ConfigName: "gonimbus",
	}
	reflowStdin = false
	reflowDest = ""
	reflowRewriteFrom = ""
	reflowRewriteTo = ""
	reflowParallel = 16
	reflowDryRun = false
	reflowResume = false
	reflowCheckpoint = ""
	reflowOverwrite = false
	reflowOnCollision = "log"
	reflowSrcRegion = ""
	reflowSrcProfile = ""
	reflowSrcEndpoint = ""
	reflowDstRegion = ""
	reflowDstProfile = ""
	reflowDstEndpoint = ""

	t.Cleanup(func() {
		appIdentity = oldIdentity
		reflowStdin = oldStdin
		reflowDest = oldDest
		reflowRewriteFrom = oldRewriteFrom
		reflowRewriteTo = oldRewriteTo
		reflowParallel = oldParallel
		reflowDryRun = oldDryRun
		reflowResume = oldResume
		reflowCheckpoint = oldCheckpoint
		reflowOverwrite = oldOverwrite
		reflowOnCollision = oldOnCollision
		reflowSrcRegion = oldSrcRegion
		reflowSrcProfile = oldSrcProfile
		reflowSrcEndpoint = oldSrcEndpoint
		reflowDstRegion = oldDstRegion
		reflowDstProfile = oldDstProfile
		reflowDstEndpoint = oldDstEndpoint
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
	require.Equal(t, "normal", task.RoutingClass)
}

func TestEnqueueReflowLine_ReflowInputRecordQuarantine(t *testing.T) {
	out := make(chan reflowTask, 1)
	var providerBuckets []string
	getProviders := func(bucket string) (provider.Provider, provider.Provider, error) {
		providerBuckets = append(providerBuckets, bucket)
		return &mockProvider{}, &mockProvider{}, nil
	}

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://bucket/source/file.xml","source_key":"source/file.xml","routing_class":"quarantine","quarantine_prefix":"_unresolved/","vars":{"date":"_unresolved"}}}`
	srcBucket, err := enqueueReflowLine(context.Background(), line, "", getProviders, out)
	require.NoError(t, err)
	require.Equal(t, "bucket", srcBucket)
	require.Equal(t, []string{"bucket"}, providerBuckets)

	task := <-out
	require.Equal(t, "quarantine", task.RoutingClass)
	require.Equal(t, "_unresolved", task.QuarantinePrefix)
	require.Equal(t, "source/file.xml", task.SourceKey)
	require.Equal(t, "_unresolved/source/file.xml", buildQuarantineDestRel(task.QuarantinePrefix, task.SourceKey))
}

func TestEnqueueReflowLine_ReflowInputRecordQuarantineRequiresPrefix(t *testing.T) {
	out := make(chan reflowTask, 1)
	getProviders := func(bucket string) (provider.Provider, provider.Provider, error) {
		return &mockProvider{}, &mockProvider{}, nil
	}

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://bucket/source/file.xml","source_key":"source/file.xml","routing_class":"quarantine"}}`
	_, err := enqueueReflowLine(context.Background(), line, "", getProviders, out)

	require.Error(t, err)
	require.Contains(t, err.Error(), "quarantine_prefix is required")
	require.Empty(t, out)
}

func TestEnqueueReflowLine_ReflowInputRecordQuarantineRejectsAbsolutePrefix(t *testing.T) {
	out := make(chan reflowTask, 1)
	getProviders := func(bucket string) (provider.Provider, provider.Provider, error) {
		return &mockProvider{}, &mockProvider{}, nil
	}

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://bucket/source/file.xml","source_key":"source/file.xml","routing_class":"quarantine","quarantine_prefix":"s3://other/prefix"}}`
	_, err := enqueueReflowLine(context.Background(), line, "", getProviders, out)

	require.Error(t, err)
	require.Contains(t, err.Error(), "quarantine_prefix must be a relative destination prefix")
	require.Empty(t, out)
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
