package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fulmenhq/gofulmen/appidentity"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
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

func TestTransferReflowCommand_ProvenanceDefaultOffWritesNoSidecar(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
	dst := newReflowMemoryProvider()

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""))
	require.NoError(t, err)
	require.False(t, dst.hasObject("source/file.xml.gnb.json"))

	complete := requireRecord(t, stdout, reflowRecordType, "complete")
	require.NotContains(t, string(complete.Data), `"provenance"`)
	require.Equal(t, []string{"source/file.xml"}, dst.headCallsSnapshot())
}

func TestTransferReflowCommand_ProvenanceLandedWritesSidecarAndRef(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
	dst := newReflowMemoryProvider()

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""), "--provenance", "sidecar")
	require.NoError(t, err)
	require.Equal(t, []string{"source/file.xml"}, dst.headCallsSnapshot())

	sidecar := readSidecar(t, dst, "source/file.xml.gnb.json")
	require.Equal(t, "landed", sidecar["action"])
	require.Equal(t, "gonimbus.provenance.v1", sidecar["schema"])
	require.Contains(t, string(dst.mustObject("source/file.xml.gnb.json")), `"last_modified":"2026-01-15T20:53:44Z"`)
	require.NotContains(t, string(dst.mustObject("source/file.xml.gnb.json")), `"etag":"dest`)

	complete := requireRecord(t, stdout, reflowRecordType, "complete")
	require.Contains(t, string(complete.Data), `"provenance":{"written":true,"key":"source/file.xml.gnb.json"}`)
}

func TestTransferReflowCommand_ProvenanceDuplicateSkipOverwritesSidecar(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "same-etag", time.Time{})
	dst := newReflowMemoryProvider()
	dst.putFixture("source/file.xml", "payload", "same-etag", time.Time{})
	dst.putFixture("source/file.xml.gnb.json", `{"action":"old"}`, "old-sidecar", time.Time{})

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "same-etag", int64(len("payload")), "", ""), "--provenance", "sidecar")
	require.NoError(t, err)
	require.Equal(t, []string{"source/file.xml"}, dst.headCallsSnapshot())
	require.Equal(t, []string{"source/file.xml.gnb.json"}, dst.putCallsSnapshot())

	sidecar := readSidecar(t, dst, "source/file.xml.gnb.json")
	require.Equal(t, "skipped.duplicate", sidecar["action"])
	require.Contains(t, string(dst.mustObject("source/file.xml.gnb.json")), `"etag":"same-etag"`)

	skipped := requireRecord(t, stdout, reflowRecordType, "skipped")
	require.Contains(t, string(skipped.Data), `"reason":"collision.duplicate"`)
	require.Contains(t, string(skipped.Data), `"provenance":{"written":true,"key":"source/file.xml.gnb.json"}`)
}

func TestTransferReflowCommand_ProvenanceQuarantineWritesSidecarUnderPrefix(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Time{})
	dst := newReflowMemoryProvider()

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "quarantine", "_unresolved"), "--provenance", "sidecar")
	require.NoError(t, err)

	sidecar := readSidecar(t, dst, "_unresolved/source/file.xml.gnb.json")
	require.Equal(t, "quarantined", sidecar["action"])
	require.Contains(t, string(dst.mustObject("_unresolved/source/file.xml.gnb.json")), `"routing_class":"quarantine"`)
	require.Contains(t, string(dst.mustObject("_unresolved/source/file.xml.gnb.json")), `"quarantine_prefix":"_unresolved"`)

	complete := requireRecord(t, stdout, reflowRecordType, "complete")
	require.Contains(t, string(complete.Data), `"_unresolved/source/file.xml.gnb.json"`)
}

func TestTransferReflowCommand_ProvenanceWriteFailureWarnsAndCompletes(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Time{})
	dst := newReflowMemoryProvider()
	dst.failSidecars = true

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""), "--provenance", "sidecar", "--provenance-on-write-error", "warn")
	require.NoError(t, err)
	require.True(t, dst.hasObject("source/file.xml"))
	require.False(t, dst.hasObject("source/file.xml.gnb.json"))

	warn := requireRecord(t, stdout, reflowWarningRecord, "")
	require.Contains(t, string(warn.Data), "PROVENANCE_WRITE_FAILED")
	complete := requireRecord(t, stdout, reflowRecordType, "complete")
	require.Contains(t, string(complete.Data), `"provenance":{"written":false,"key":"source/file.xml.gnb.json"}`)
}

func TestTransferReflowCommand_ProvenanceWriteFailureFailsRecord(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Time{})
	dst := newReflowMemoryProvider()
	dst.failSidecars = true

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""), "--provenance", "sidecar", "--provenance-on-write-error", "fail")
	require.Error(t, err)
	require.Contains(t, err.Error(), "reflow completed with errors")
	require.True(t, dst.hasObject("source/file.xml"))
	require.False(t, dst.hasObject("source/file.xml.gnb.json"))

	errRecord := requireRecord(t, stdout, output.TypeError, "")
	require.Contains(t, string(errRecord.Data), "provenance sidecar write failed")
	failed := requireRecord(t, stdout, reflowRecordType, "failed")
	require.Contains(t, string(failed.Data), `"reason":"provenance.write_failed"`)
	require.Contains(t, string(failed.Data), `"provenance":{"written":false,"key":"source/file.xml.gnb.json"}`)
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
	cmd.Flags().StringVar(&reflowProvenance, "provenance", provenanceModeNone, "")
	cmd.Flags().StringVar(&reflowProvSuffix, "provenance-suffix", provenanceSuffix, "")
	cmd.Flags().StringVar(&reflowProvOnError, "provenance-on-write-error", provenanceErrorWarn, "")
	cmd.Flags().BoolVar(&reflowProvUnsafe, "allow-unsafe-suffix", false, "")
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
	oldProvenance := reflowProvenance
	oldProvSuffix := reflowProvSuffix
	oldProvOnError := reflowProvOnError
	oldProvUnsafe := reflowProvUnsafe
	oldSrcRegion := reflowSrcRegion
	oldSrcProfile := reflowSrcProfile
	oldSrcEndpoint := reflowSrcEndpoint
	oldDstRegion := reflowDstRegion
	oldDstProfile := reflowDstProfile
	oldDstEndpoint := reflowDstEndpoint
	oldS3Provider := newReflowS3Provider
	oldFileProvider := newReflowFileProvider

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
	reflowProvenance = provenanceModeNone
	reflowProvSuffix = provenanceSuffix
	reflowProvOnError = provenanceErrorWarn
	reflowProvUnsafe = false
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
		reflowProvenance = oldProvenance
		reflowProvSuffix = oldProvSuffix
		reflowProvOnError = oldProvOnError
		reflowProvUnsafe = oldProvUnsafe
		reflowSrcRegion = oldSrcRegion
		reflowSrcProfile = oldSrcProfile
		reflowSrcEndpoint = oldSrcEndpoint
		reflowDstRegion = oldDstRegion
		reflowDstProfile = oldDstProfile
		reflowDstEndpoint = oldDstEndpoint
		newReflowS3Provider = oldS3Provider
		newReflowFileProvider = oldFileProvider
	})
}

func TestEnqueueReflowLine_ReflowInputRecord(t *testing.T) {
	out := make(chan reflowTask, 1)
	var providerBuckets []string
	getProviders := func(bucket string) (provider.Provider, provider.Provider, error) {
		providerBuckets = append(providerBuckets, bucket)
		return &mockProvider{}, &mockProvider{}, nil
	}

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://bucket/source/file.xml","source_key":"source/file.xml","source_etag":"abc123","source_size_bytes":42,"source_last_modified":"2026-01-15T20:53:44Z","vars":{"site":"001"},"probe":{"extractors":[{"name":"site","type":"regex","resolved":true,"required":true,"bytes_at_resolution":128}],"bytes_read":128,"termination_reason":"all_required_resolved"},"dest_rel_key":"dest/file.xml"}}`
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
	require.Equal(t, time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC), task.SourceLastMod)
	require.Equal(t, map[string]string{"site": "001"}, task.Vars)
	require.NotNil(t, task.Probe)
	require.Equal(t, int64(128), task.Probe.BytesRead)
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

func TestResolveProvenanceConfig(t *testing.T) {
	withTransferReflowTestState(t)

	cmd := newTransferReflowTestCommand()
	cfg, err := resolveProvenanceConfig(cmd)
	require.NoError(t, err)
	require.Equal(t, provenanceModeNone, cfg.Mode)

	require.NoError(t, cmd.Flags().Parse([]string{"--provenance", "sidecar", "--provenance-suffix", ".audit.json", "--provenance-on-write-error", "fail"}))
	cfg, err = resolveProvenanceConfig(cmd)
	require.NoError(t, err)
	require.Equal(t, provenanceConfig{Mode: provenanceModeSidecar, Suffix: ".audit.json", OnWriteError: provenanceErrorFail, AllowUnsafeSuffix: false}, cfg)
}

func TestValidateProvenanceConfigRejectsUnsafeSuffix(t *testing.T) {
	err := validateProvenanceConfig(provenanceConfig{Mode: provenanceModeSidecar, Suffix: ".xml", OnWriteError: provenanceErrorWarn})
	require.Error(t, err)
	require.Contains(t, err.Error(), "collides with common data extensions")

	err = validateProvenanceConfig(provenanceConfig{Mode: provenanceModeSidecar, Suffix: ".xml", OnWriteError: provenanceErrorWarn, AllowUnsafeSuffix: true})
	require.NoError(t, err)

	err = validateProvenanceConfig(provenanceConfig{Mode: provenanceModeSidecar, Suffix: "gnb.json", OnWriteError: provenanceErrorWarn})
	require.Error(t, err)
	require.Contains(t, err.Error(), "leading dot")

	err = validateProvenanceConfig(provenanceConfig{Mode: provenanceModeSidecar, Suffix: ".*.json", OnWriteError: provenanceErrorWarn})
	require.Error(t, err)
	require.Contains(t, err.Error(), "glob")
}

func TestWriteProvenanceSidecarWritesJSON(t *testing.T) {
	withTransferReflowTestState(t)
	SetVersionInfo("0.2.0-test", "abc123", "2026-05-16T12:00:00Z")

	destDir := t.TempDir()
	dst, err := providerfile.New(providerfile.Config{BaseDir: destDir})
	require.NoError(t, err)

	resolvedAt := int64(128)
	var stdout bytes.Buffer
	w := output.NewJSONLWriter(&stdout, "job-123", "file")
	defer func() { _ = w.Close() }()

	srcLastMod := time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC)
	ref, fatal := writeProvenanceSidecar(context.Background(), w, dst, provenanceConfig{Mode: provenanceModeSidecar, Suffix: provenanceSuffix, OnWriteError: provenanceErrorWarn}, reflowTask{
		SourceURI:     "s3://source-bucket/source/file.xml",
		SourceKey:     "source/file.xml",
		SourceETag:    "src-etag",
		SourceSize:    42,
		SourceLastMod: srcLastMod,
		Vars:          map[string]string{"site": "001"},
		Probe: &probe.ProbeAudit{
			Extractors:        []probe.ExtractorAudit{{Name: "site", Type: "regex", Resolved: true, Required: true, BytesAtResolution: &resolvedAt}},
			BytesRead:         128,
			TerminationReason: "all_required_resolved",
		},
	}, "landing/file.xml", "file://"+filepath.Join(destDir, "landing/file.xml"), &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: "landing/file.xml", ETag: "dest-etag", Size: 42}}, "{site}/{file}", "landed", "job-123")

	require.False(t, fatal)
	require.NotNil(t, ref)
	require.True(t, ref.Written)
	require.Equal(t, "landing/file.xml.gnb.json", ref.Key)

	raw, err := os.ReadFile(filepath.Join(destDir, "landing", "file.xml.gnb.json"))
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, provenanceSchema, got["schema"])
	require.Equal(t, provenanceSchemaVer, got["schema_version"])
	require.Equal(t, "landed", got["action"])
	require.Contains(t, string(raw), `"source"`)
	require.Contains(t, string(raw), `"last_modified":"2026-01-15T20:53:44Z"`)
	require.Contains(t, string(raw), `"probe"`)
	require.Empty(t, stdout.String())
}

func TestWriteProvenanceSidecarWarnsOnFailure(t *testing.T) {
	var stdout bytes.Buffer
	w := output.NewJSONLWriter(&stdout, "job-123", "file")
	defer func() { _ = w.Close() }()

	ref, fatal := writeProvenanceSidecar(context.Background(), w, failingPutter{err: errors.New("boom")}, provenanceConfig{Mode: provenanceModeSidecar, Suffix: provenanceSuffix, OnWriteError: provenanceErrorWarn}, reflowTask{SourceURI: "s3://source/key", SourceKey: "key"}, "dest/key", "s3://dest/key", nil, "{key}", "landed", "job-123")

	require.False(t, fatal)
	require.Equal(t, &provenanceRef{Written: false, Key: "dest/key.gnb.json"}, ref)
	require.Contains(t, stdout.String(), reflowWarningRecord)
	require.Contains(t, stdout.String(), "PROVENANCE_WRITE_FAILED")
}

func TestWriteProvenanceSidecarFailsOnFailure(t *testing.T) {
	var stdout bytes.Buffer
	w := output.NewJSONLWriter(&stdout, "job-123", "file")
	defer func() { _ = w.Close() }()

	ref, fatal := writeProvenanceSidecar(context.Background(), w, failingPutter{err: errors.New("boom")}, provenanceConfig{Mode: provenanceModeSidecar, Suffix: provenanceSuffix, OnWriteError: provenanceErrorFail}, reflowTask{SourceURI: "s3://source/key", SourceKey: "key"}, "dest/key", "s3://dest/key", nil, "{key}", "landed", "job-123")

	require.True(t, fatal)
	require.Equal(t, &provenanceRef{Written: false, Key: "dest/key.gnb.json"}, ref)
	require.Contains(t, stdout.String(), output.TypeError)
	require.Contains(t, stdout.String(), "provenance sidecar write failed")
}

type failingPutter struct {
	err error
}

func (p failingPutter) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return nil, nil
}

func (p failingPutter) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (p failingPutter) PutObject(context.Context, string, io.Reader, int64) error {
	return p.err
}

func (p failingPutter) Close() error {
	return nil
}

func runTransferReflowWithProviders(t *testing.T, src *reflowMemoryProvider, dst *reflowMemoryProvider, input string, extraArgs ...string) (string, error) {
	t.Helper()
	withTransferReflowTestState(t)

	newReflowS3Provider = func(context.Context, s3.Config) (provider.Provider, error) {
		return src, nil
	}
	newReflowFileProvider = func(providerfile.Config) (provider.Provider, error) {
		return dst, nil
	}

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader(input + "\n"))
	cmd.SetOut(&stdout)
	args := []string{
		"--stdin",
		"--dest", fileURI(t.TempDir()),
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--parallel", "1",
	}
	args = append(args, extraArgs...)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return stdout.String(), err
}

func reflowInputLine(key string, etag string, size int64, routingClass string, quarantinePrefix string) string {
	data := map[string]any{
		"source_uri":           "s3://source-bucket/" + key,
		"source_key":           key,
		"source_etag":          etag,
		"source_size_bytes":    size,
		"dest_rel_key":         key,
		"source_last_modified": "2026-01-15T20:53:44Z",
		"vars": map[string]string{
			"key": key,
		},
		"probe": map[string]any{
			"extractors": []map[string]any{{
				"name":                "key",
				"type":                "regex",
				"resolved":            true,
				"required":            true,
				"bytes_at_resolution": int64(64),
			}},
			"bytes_read":         int64(64),
			"termination_reason": "all_required_resolved",
		},
	}
	if routingClass != "" {
		data["routing_class"] = routingClass
	}
	if quarantinePrefix != "" {
		data["quarantine_prefix"] = quarantinePrefix
	}
	line, err := json.Marshal(map[string]any{"type": "gonimbus.reflow.input.v1", "data": data})
	if err != nil {
		panic(err)
	}
	return string(line)
}

type testRecordEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func requireRecord(t *testing.T, stdout string, recordType string, status string) testRecordEnvelope {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record testRecordEnvelope
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if record.Type != recordType {
			continue
		}
		if status != "" {
			var data struct {
				Status string `json:"status"`
			}
			require.NoError(t, json.Unmarshal(record.Data, &data))
			if data.Status != status {
				continue
			}
		}
		return record
	}
	t.Fatalf("record type %s status %s not found in stdout:\n%s", recordType, status, stdout)
	return testRecordEnvelope{}
}

func readSidecar(t *testing.T, p *reflowMemoryProvider, key string) map[string]any {
	t.Helper()
	raw := p.mustObject(key)
	var out map[string]any
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

type reflowMemoryProvider struct {
	mu           sync.Mutex
	objects      map[string][]byte
	meta         map[string]provider.ObjectMeta
	headCalls    []string
	putCalls     []string
	failSidecars bool
}

func newReflowMemoryProvider() *reflowMemoryProvider {
	return &reflowMemoryProvider{
		objects: map[string][]byte{},
		meta:    map[string]provider.ObjectMeta{},
	}
}

func (p *reflowMemoryProvider) putFixture(key string, body string, etag string, lastModified time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.objects[key] = []byte(body)
	p.meta[key] = provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key, Size: int64(len(body)), ETag: etag, LastModified: lastModified}}
}

func (p *reflowMemoryProvider) hasObject(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.objects[key]
	return ok
}

func (p *reflowMemoryProvider) mustObject(key string) []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	body, ok := p.objects[key]
	if !ok {
		panic("missing object " + key)
	}
	out := make([]byte, len(body))
	copy(out, body)
	return out
}

func (p *reflowMemoryProvider) headCallsSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.headCalls...)
}

func (p *reflowMemoryProvider) putCallsSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.putCalls...)
}

func (p *reflowMemoryProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}

func (p *reflowMemoryProvider) Head(_ context.Context, key string) (*provider.ObjectMeta, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.headCalls = append(p.headCalls, key)
	meta, ok := p.meta[key]
	if !ok {
		return nil, &provider.ProviderError{Op: "Head", Provider: provider.ProviderFile, Key: key, Err: provider.ErrNotFound}
	}
	return &meta, nil
}

func (p *reflowMemoryProvider) GetObject(_ context.Context, key string) (io.ReadCloser, int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	body, ok := p.objects[key]
	if !ok {
		return nil, 0, &provider.ProviderError{Op: "GetObject", Provider: provider.ProviderFile, Key: key, Err: provider.ErrNotFound}
	}
	return io.NopCloser(bytes.NewReader(body)), int64(len(body)), nil
}

func (p *reflowMemoryProvider) PutObject(_ context.Context, key string, body io.Reader, contentLength int64) error {
	if p.failSidecars && strings.HasSuffix(key, provenanceSuffix) {
		return errors.New("sidecar write failed")
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if contentLength >= 0 && int64(len(data)) != contentLength {
		return fmt.Errorf("content length mismatch")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.putCalls = append(p.putCalls, key)
	p.objects[key] = data
	p.meta[key] = provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key, Size: int64(len(data)), ETag: "dest-" + key}}
	return nil
}

func (p *reflowMemoryProvider) Close() error {
	return nil
}
