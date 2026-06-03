package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fulmenhq/gofulmen/appidentity"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/reflowstate"
	"github.com/3leaps/gonimbus/pkg/uri"
)

func TestValidateTransferReflowArgs(t *testing.T) {
	makeCmd := func(stdin bool, resumeRun string) *cobra.Command {
		c := &cobra.Command{}
		c.Flags().Bool("stdin", stdin, "")
		c.Flags().String("resume-run", resumeRun, "")
		return c
	}

	t.Run("without stdin requires source uri", func(t *testing.T) {
		err := validateTransferReflowArgs(makeCmd(false, ""), []string{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires exactly 1 argument")
	})

	t.Run("without stdin accepts one source uri", func(t *testing.T) {
		err := validateTransferReflowArgs(makeCmd(false, ""), []string{"s3://bucket/source/file.xml"})
		require.NoError(t, err)
	})

	t.Run("without stdin rejects too many source uris", func(t *testing.T) {
		err := validateTransferReflowArgs(makeCmd(false, ""), []string{"s3://bucket/source/a.xml", "s3://bucket/source/b.xml"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires exactly 1 argument")
	})

	t.Run("stdin accepts no positional args", func(t *testing.T) {
		err := validateTransferReflowArgs(makeCmd(true, ""), []string{})
		require.NoError(t, err)
	})

	t.Run("stdin rejects positional source uri", func(t *testing.T) {
		err := validateTransferReflowArgs(makeCmd(true, ""), []string{"s3://bucket/source/file.xml"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "when using --stdin")
		require.Contains(t, err.Error(), "do not provide source-uri arguments")
	})

	t.Run("resume-run accepts no positional args", func(t *testing.T) {
		err := validateTransferReflowArgs(makeCmd(false, "run_123"), []string{})
		require.NoError(t, err)
	})

	t.Run("resume-run rejects positional source uri", func(t *testing.T) {
		err := validateTransferReflowArgs(makeCmd(false, "run_123"), []string{"s3://bucket/source/file.xml"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "do not provide source-uri arguments with --resume-run")
	})
}

func TestTransferReflowResumeRunRejectsForegroundFlags(t *testing.T) {
	withTransferReflowTestState(t)

	cmd := newTransferReflowTestCommand()
	cmd.SetArgs([]string{"--resume-run", "run_123", "--dest", fileURI(t.TempDir())})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--dest")
	require.Contains(t, err.Error(), "not accepted with --resume-run")
}

func TestTransferReflowResumeRunRejectsSuccessfulCheckpointBeforeWork(t *testing.T) {
	withTransferReflowTestState(t)

	ctx := context.Background()
	statePath := filepath.Join(t.TempDir(), "state.db")
	state, err := newReflowStateStore(ctx, reflowstate.Config{Path: statePath})
	require.NoError(t, err)
	defer func() { _ = state.Close() }()

	cfg := transferReflowCheckpointConfig{
		SourceURI:               "s3://source-bucket/a.txt",
		Dest:                    fileURI(t.TempDir()),
		RewriteFrom:             "{key}",
		RewriteTo:               "{key}",
		Parallel:                1,
		CheckpointPath:          statePath,
		OnCollision:             reflowCollisionSkip,
		Provenance:              provenanceModeNone,
		ProvenanceSuffix:        provenanceSuffix,
		ProvenanceOnWriteError:  provenanceErrorWarn,
		MetadataPolicy:          metadataPolicyClear,
		MetadataOnMissingSource: metadataMissingSkip,
		MetadataSidecarSuffix:   providerfile.DefaultMetadataSidecarSuffix,
		Symlinks:                reflowSymlinkSkip,
		Hidden:                  reflowHiddenSkip,
		OnSourceFailure:         reflowSourceFailSkip,
	}
	fingerprint, err := checkpointFingerprint(cfg)
	require.NoError(t, err)
	require.NoError(t, state.SetOperationCheckpointIdentity(ctx, operationTransferReflow, fingerprint))
	payload, err := json.Marshal(transferReflowCheckpointPayload{Config: cfg})
	require.NoError(t, err)

	opStore, err := openDefaultOperationCheckpointStore(ctx)
	require.NoError(t, err)
	require.NoError(t, opStore.WriteCheckpoint(ctx, opcheckpoint.Envelope{
		SchemaVersion:     opcheckpoint.SchemaVersion,
		Operation:         operationTransferReflow,
		RunID:             "run_success",
		ConfigFingerprint: fingerprint,
		Status:            opcheckpoint.StatusSuccess,
		CreatedAt:         time.Now().UTC(),
		Payload:           payload,
	}))

	var providerCalled bool
	newReflowS3Provider = func(context.Context, s3.Config) (provider.Provider, error) {
		providerCalled = true
		return newReflowMemoryProvider(), nil
	}

	cmd := newTransferReflowTestCommand()
	cmd.SetArgs([]string{"--resume-run", "run_success"})
	err = cmd.Execute()
	require.ErrorIs(t, err, opcheckpoint.ErrIdentityMismatch)
	require.False(t, providerCalled)
}

func TestTransferReflowCheckpointEligibility(t *testing.T) {
	cfg := transferReflowCheckpointConfig{SourceURI: "s3://bucket/key", CheckpointPath: filepath.Join(t.TempDir(), "state.db")}
	require.True(t, transferReflowCheckpointEligible(cfg))

	cfg.Stdin = true
	require.False(t, transferReflowCheckpointEligible(cfg))
	cfg.Stdin = false

	cfg.DryRun = true
	require.False(t, transferReflowCheckpointEligible(cfg))
	cfg.DryRun = false

	cfg.SourceURI = ""
	require.False(t, transferReflowCheckpointEligible(cfg))
}

func TestTransferReflowCheckpointIdentityRejectsTamperedConfig(t *testing.T) {
	withTransferReflowTestState(t)

	ctx := context.Background()
	statePath := filepath.Join(t.TempDir(), "state.db")
	state, err := newReflowStateStore(ctx, reflowstate.Config{Path: statePath})
	require.NoError(t, err)
	defer func() { _ = state.Close() }()

	cfg := transferReflowCheckpointConfig{
		SourceURI:               "s3://source-bucket/a.txt",
		Dest:                    fileURI(t.TempDir()),
		RewriteFrom:             "{key}",
		RewriteTo:               "{key}",
		Parallel:                1,
		CheckpointPath:          statePath,
		OnCollision:             reflowCollisionSkip,
		Provenance:              provenanceModeNone,
		ProvenanceSuffix:        provenanceSuffix,
		ProvenanceOnWriteError:  provenanceErrorWarn,
		MetadataPolicy:          metadataPolicyClear,
		MetadataOnMissingSource: metadataMissingSkip,
		MetadataSidecarSuffix:   providerfile.DefaultMetadataSidecarSuffix,
		Symlinks:                reflowSymlinkSkip,
		Hidden:                  reflowHiddenSkip,
		OnSourceFailure:         reflowSourceFailSkip,
	}
	fingerprint, err := checkpointFingerprint(cfg)
	require.NoError(t, err)
	require.NoError(t, state.SetOperationCheckpointIdentity(ctx, operationTransferReflow, fingerprint))

	opStore, err := openDefaultOperationCheckpointStore(ctx)
	require.NoError(t, err)
	env := &opcheckpoint.Envelope{
		SchemaVersion:     opcheckpoint.SchemaVersion,
		Operation:         operationTransferReflow,
		RunID:             "run_123",
		ConfigFingerprint: fingerprint,
		Status:            opcheckpoint.StatusFailedResumable,
		CreatedAt:         time.Now().UTC(),
	}
	_, err = validateTransferReflowCheckpointIdentity(ctx, opStore, state, env, cfg)
	require.NoError(t, err)

	completedEnv := *env
	completedEnv.Status = opcheckpoint.StatusSuccess
	_, err = validateTransferReflowCheckpointIdentity(ctx, opStore, state, &completedEnv, cfg)
	require.ErrorIs(t, err, opcheckpoint.ErrIdentityMismatch)

	tampered := cfg
	tampered.RewriteTo = "changed/{key}"
	_, err = validateTransferReflowCheckpointIdentity(ctx, opStore, state, env, tampered)
	require.ErrorIs(t, err, opcheckpoint.ErrIdentityMismatch)

	tamperedFingerprint, err := checkpointFingerprint(tampered)
	require.NoError(t, err)
	tamperedEnv := *env
	tamperedEnv.ConfigFingerprint = tamperedFingerprint
	_, err = validateTransferReflowCheckpointIdentity(ctx, opStore, state, &tamperedEnv, tampered)
	require.ErrorIs(t, err, opcheckpoint.ErrIdentityMismatch)
}

func TestTransferReflowFailedResumableCheckpointBindsStateIdentity(t *testing.T) {
	withTransferReflowTestState(t)

	ctx := context.Background()
	statePath := filepath.Join(t.TempDir(), "state.db")
	state, err := newReflowStateStore(ctx, reflowstate.Config{Path: statePath})
	require.NoError(t, err)
	defer func() { _ = state.Close() }()

	cfg := transferReflowCheckpointConfig{
		SourceURI:               "s3://source-bucket/a.txt",
		Dest:                    fileURI(t.TempDir()),
		RewriteFrom:             "{key}",
		RewriteTo:               "{key}",
		Parallel:                1,
		CheckpointPath:          statePath,
		OnCollision:             reflowCollisionSkip,
		Provenance:              provenanceModeNone,
		ProvenanceSuffix:        provenanceSuffix,
		ProvenanceOnWriteError:  provenanceErrorWarn,
		MetadataPolicy:          metadataPolicyClear,
		MetadataOnMissingSource: metadataMissingSkip,
		MetadataSidecarSuffix:   providerfile.DefaultMetadataSidecarSuffix,
		Symlinks:                reflowSymlinkSkip,
		Hidden:                  reflowHiddenSkip,
		OnSourceFailure:         reflowSourceFailSkip,
	}
	progress := map[string]int64{"errors": 1}
	require.NoError(t, writeFailedResumableTransferReflowCheckpoint(ctx, state, "run_reflow_123", cfg, opcheckpoint.ErrorClassInterrupted, progress))

	wantFingerprint, err := checkpointFingerprint(cfg)
	require.NoError(t, err)
	gotFingerprint, err := state.OperationCheckpointFingerprint(ctx, operationTransferReflow)
	require.NoError(t, err)
	require.Equal(t, wantFingerprint, gotFingerprint)

	opStore, err := openDefaultOperationCheckpointStore(ctx)
	require.NoError(t, err)
	env, err := opStore.ReadCheckpoint(ctx, operationTransferReflow, "run_reflow_123")
	require.NoError(t, err)
	require.Equal(t, opcheckpoint.StatusFailedResumable, env.Status)
	require.Equal(t, opcheckpoint.ErrorClassInterrupted, env.ErrorClass)
	require.Equal(t, wantFingerprint, env.ConfigFingerprint)
	require.Equal(t, progress, env.Progress)
}

func TestTransferReflowCancelledPositionalRunWritesOperationCheckpoint(t *testing.T) {
	withTransferReflowTestState(t)

	src := newReflowMemoryProvider()
	src.putFixture("a.txt", "payload", "etag-a", time.Now().UTC())
	ctx, cancel := context.WithCancel(context.Background())
	newReflowS3Provider = func(context.Context, s3.Config) (provider.Provider, error) {
		return cancelingGetProvider{Provider: src, cancel: cancel}, nil
	}

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetContext(ctx)
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"--dest", fileURI(t.TempDir()),
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--parallel", "1",
		"s3://source-bucket/a.txt",
	})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "reflow cancelled")

	record := requireRecord(t, stdout.String(), opcheckpoint.ErrorRecordType, "")
	var opErr opcheckpoint.ErrorRecordData
	require.NoError(t, json.Unmarshal(record.Data, &opErr))
	require.Equal(t, operationTransferReflow, opErr.Operation)
	require.Equal(t, opcheckpoint.ErrorClassInterrupted, opErr.ErrorClass)
	require.Equal(t, "gonimbus transfer reflow --resume-run "+opErr.RunID, opErr.ResumeCommand)

	opStore, err := openDefaultOperationCheckpointStore(context.Background())
	require.NoError(t, err)
	env, err := opStore.ReadCheckpoint(context.Background(), operationTransferReflow, opErr.RunID)
	require.NoError(t, err)
	require.Equal(t, opcheckpoint.StatusFailedResumable, env.Status)

	var payload transferReflowCheckpointPayload
	require.NoError(t, json.Unmarshal(env.Payload, &payload))
	state, err := newReflowStateStore(context.Background(), reflowstate.Config{Path: payload.Config.CheckpointPath})
	require.NoError(t, err)
	defer func() { _ = state.Close() }()
	fingerprint, err := state.OperationCheckpointFingerprint(context.Background(), operationTransferReflow)
	require.NoError(t, err)
	require.Equal(t, env.ConfigFingerprint, fingerprint)
}

func TestTransferReflowCredentialRefreshFailureWritesOperationCheckpoint(t *testing.T) {
	withTransferReflowTestState(t)

	src := newReflowMemoryProvider()
	src.putFixture("a.txt", "payload", "etag-a", time.Now().UTC())
	newReflowS3Provider = func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
		require.Equal(t, "refreshable-profile", cfg.Profile)
		return refreshFailingGetProvider{Provider: src}, nil
	}

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"--dest", fileURI(t.TempDir()),
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--parallel", "1",
		"--src-profile", "refreshable-profile",
		"s3://source-bucket/a.txt",
	})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "reflow failed resumable")

	record := requireRecord(t, stdout.String(), opcheckpoint.ErrorRecordType, "")
	var opErr opcheckpoint.ErrorRecordData
	require.NoError(t, json.Unmarshal(record.Data, &opErr))
	require.Equal(t, operationTransferReflow, opErr.Operation)
	require.Equal(t, opcheckpoint.ErrorClassCredentialsRefreshFailed, opErr.ErrorClass)
	require.Equal(t, "gonimbus transfer reflow --resume-run "+opErr.RunID, opErr.ResumeCommand)

	opStore, err := openDefaultOperationCheckpointStore(context.Background())
	require.NoError(t, err)
	env, err := opStore.ReadCheckpoint(context.Background(), operationTransferReflow, opErr.RunID)
	require.NoError(t, err)
	require.Equal(t, opcheckpoint.StatusFailedResumable, env.Status)
	require.Equal(t, opcheckpoint.ErrorClassCredentialsRefreshFailed, env.ErrorClass)
}

func TestTransferReflowCredentialRefreshListFailureWritesOperationCheckpoint(t *testing.T) {
	withTransferReflowTestState(t)

	src := newReflowMemoryProvider()
	src.putFixture("prefix/a.txt", "payload", "etag-a", time.Now().UTC())
	newReflowS3Provider = func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
		require.Equal(t, "refreshable-profile", cfg.Profile)
		return &refreshFailingListProvider{Provider: src}, nil
	}

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"--dest", fileURI(t.TempDir()),
		"--rewrite-from", "prefix/{name}",
		"--rewrite-to", "{name}",
		"--parallel", "1",
		"--src-profile", "refreshable-profile",
		"s3://source-bucket/prefix/",
	})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "reflow failed resumable")

	record := requireRecord(t, stdout.String(), opcheckpoint.ErrorRecordType, "")
	var opErr opcheckpoint.ErrorRecordData
	require.NoError(t, json.Unmarshal(record.Data, &opErr))
	require.Equal(t, operationTransferReflow, opErr.Operation)
	require.Equal(t, opcheckpoint.ErrorClassCredentialsRefreshFailed, opErr.ErrorClass)
	require.Equal(t, "gonimbus transfer reflow --resume-run "+opErr.RunID, opErr.ResumeCommand)

	opStore, err := openDefaultOperationCheckpointStore(context.Background())
	require.NoError(t, err)
	env, err := opStore.ReadCheckpoint(context.Background(), operationTransferReflow, opErr.RunID)
	require.NoError(t, err)
	require.Equal(t, opcheckpoint.StatusFailedResumable, env.Status)
	require.Equal(t, opcheckpoint.ErrorClassCredentialsRefreshFailed, env.ErrorClass)
}

func TestTransferReflowCredentialRefreshWordingWithoutProfileIsNotResumable(t *testing.T) {
	withTransferReflowTestState(t)

	src := newReflowMemoryProvider()
	src.putFixture("a.txt", "payload", "etag-a", time.Now().UTC())
	newReflowS3Provider = func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
		require.Empty(t, cfg.Profile)
		return legacyRefreshTextGetProvider{Provider: src}, nil
	}

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"--dest", fileURI(t.TempDir()),
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--parallel", "1",
		"s3://source-bucket/a.txt",
	})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "reflow completed with errors")
	require.NotContains(t, stdout.String(), opcheckpoint.ErrorRecordType)
}

func TestTransferReflowLegacyCredentialRefreshTextWithProfileIsNotResumable(t *testing.T) {
	withTransferReflowTestState(t)

	src := newReflowMemoryProvider()
	src.putFixture("a.txt", "payload", "etag-a", time.Now().UTC())
	newReflowS3Provider = func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
		require.Equal(t, "refreshable-profile", cfg.Profile)
		return legacyRefreshTextGetProvider{Provider: src}, nil
	}

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"--dest", fileURI(t.TempDir()),
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--parallel", "1",
		"--src-profile", "refreshable-profile",
		"s3://source-bucket/a.txt",
	})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "reflow completed with errors")
	require.NotContains(t, err.Error(), "reflow failed resumable")
	require.NotContains(t, stdout.String(), opcheckpoint.ErrorRecordType)
}

func TestTransferReflowStdinCredentialRefreshFailureIsNotAdvertisedResumable(t *testing.T) {
	withTransferReflowTestState(t)

	src := newReflowMemoryProvider()
	src.putFixture("a.txt", "payload", "etag-a", time.Now().UTC())
	newReflowS3Provider = func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
		require.Equal(t, "refreshable-profile", cfg.Profile)
		return refreshFailingGetProvider{Provider: src}, nil
	}

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader("s3://source-bucket/a.txt\n"))
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"--stdin",
		"--dest", fileURI(t.TempDir()),
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--parallel", "1",
		"--src-profile", "refreshable-profile",
	})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "reflow failed")
	require.NotContains(t, err.Error(), "reflow failed resumable")
	require.NotContains(t, stdout.String(), opcheckpoint.ErrorRecordType)
}

type cancelingGetProvider struct {
	provider.Provider
	cancel func()
}

func (p cancelingGetProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	if p.cancel != nil {
		p.cancel()
	}
	return nil, 0, context.Canceled
}

type refreshFailingGetProvider struct {
	provider.Provider
}

func (p refreshFailingGetProvider) GetObject(context.Context, string) (io.ReadCloser, int64, error) {
	return nil, 0, fmt.Errorf("s3 GetObject: failed to refresh cached credentials: %w", opcheckpoint.ErrCredentialsRefreshFailed)
}

type legacyRefreshTextGetProvider struct {
	provider.Provider
}

func (p legacyRefreshTextGetProvider) GetObject(context.Context, string) (io.ReadCloser, int64, error) {
	return nil, 0, errors.New("s3 GetObject: failed to refresh cached credentials: invalid_grant")
}

type refreshFailingListProvider struct {
	provider.Provider
	listCalls int
}

func (p *refreshFailingListProvider) List(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	p.listCalls++
	if opts.ContinuationToken == "" {
		return &provider.ListResult{
			Objects: []provider.ObjectSummary{{
				Key:  "prefix/a.txt",
				Size: int64(len("payload")),
				ETag: "etag-a",
			}},
			IsTruncated:       true,
			ContinuationToken: "next-page",
		}, nil
	}
	return nil, fmt.Errorf("s3 ListObjectsV2: failed to refresh cached credentials: %w", opcheckpoint.ErrCredentialsRefreshFailed)
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
	require.NotContains(t, string(complete.Data), `"collision"`)
	require.Empty(t, dst.headCallsSnapshot())
}

func TestTransferReflowCommand_ProvenanceLandedWritesSidecarAndRef(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
	dst := newReflowMemoryProvider()

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""), "--provenance", "sidecar")
	require.NoError(t, err)
	require.Empty(t, dst.headCallsSnapshot())

	sidecar := readSidecar(t, dst, "source/file.xml.gnb.json")
	require.Equal(t, "landed", sidecar["action"])
	require.Equal(t, "gonimbus.provenance.v1", sidecar["schema"])
	require.Contains(t, string(dst.mustObject("source/file.xml.gnb.json")), `"last_modified":"2026-01-15T20:53:44Z"`)
	require.Contains(t, string(dst.mustObject("source/file.xml.gnb.json")), `"etag":"dest-source/file.xml"`)

	complete := requireRecord(t, stdout, reflowRecordType, "complete")
	require.Contains(t, string(complete.Data), `"provenance":{"written":true,"key":"source/file.xml.gnb.json","uri":`)
	require.NotContains(t, string(complete.Data), `"collision"`)
}

func TestTransferReflowCommand_ProvenanceMirroredRootFileWritesSidecarAndURI(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Time{})
	dst := newReflowMemoryProvider()
	sidecarRoot := fileURI(t.TempDir()) + "/"

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""), "--provenance", "sidecar", "--provenance-sidecar-root", sidecarRoot)
	require.NoError(t, err)

	sidecar := readSidecar(t, dst, "source/file.xml.gnb.json")
	require.Equal(t, "landed", sidecar["action"])

	complete := requireRecord(t, stdout, reflowRecordType, "complete")
	require.Contains(t, string(complete.Data), `"provenance":{"written":true,"key":"source/file.xml.gnb.json","uri":"`+sidecarRoot+`source/file.xml.gnb.json"}`)
	run := requireRecord(t, stdout, reflowRunRecordType, "")
	require.Contains(t, string(run.Data), `"placement":{"mode":"mirrored-root","sidecar_root":"`+sidecarRoot+`"}`)
}

func TestTransferReflowCommand_ProvenanceMirroredRootS3WritesUnderRootPrefix(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Time{})
	dst := newReflowMemoryProvider()

	stdout, _, err := runTransferReflowWithProviderFactory(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""),
		"--stdin",
		"--dest", "s3://dest-bucket/data/",
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--parallel", "1",
		"--provenance", "sidecar",
		"--provenance-sidecar-root", "s3://dest-bucket/runs/run-001/sidecars/",
	)
	require.NoError(t, err)
	require.True(t, dst.hasObject("data/source/file.xml"))

	sidecar := readSidecar(t, dst, "runs/run-001/sidecars/source/file.xml.gnb.json")
	require.Equal(t, "landed", sidecar["action"])

	complete := requireRecord(t, stdout, reflowRecordType, "complete")
	require.Contains(t, string(complete.Data), `"key":"runs/run-001/sidecars/source/file.xml.gnb.json"`)
	require.Contains(t, string(complete.Data), `"uri":"s3://dest-bucket/runs/run-001/sidecars/source/file.xml.gnb.json"`)
}

func TestTransferReflowCommand_ProvenanceMirroredRootUsesMixedSegmentRenderedKey(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("tenant-a/2026-01-15/subj-9/file.xml", "payload", "src-etag", time.Time{})
	dst := newReflowMemoryProvider()

	stdout, _, err := runTransferReflowWithProviderFactory(t, src, dst, reflowInputLineNoDestRel("tenant-a/2026-01-15/subj-9/file.xml", "src-etag", int64(len("payload"))),
		"--stdin",
		"--dest", "s3://dest-bucket/data/",
		"--rewrite-from", "{tenant}/{partition}/{subject}/{file}",
		"--rewrite-to", "tenant={tenant}/partition={partition}/{subject}/{file}",
		"--parallel", "1",
		"--provenance", "sidecar",
		"--provenance-sidecar-root", "s3://dest-bucket/runs/run-001/sidecars/",
	)
	require.NoError(t, err)

	renderedKey := "tenant=tenant-a/partition=2026-01-15/subj-9/file.xml"
	require.True(t, dst.hasObject("data/"+renderedKey))
	sidecar := readSidecar(t, dst, "runs/run-001/sidecars/"+renderedKey+".gnb.json")
	require.Equal(t, "landed", sidecar["action"])

	complete := requireRecord(t, stdout, reflowRecordType, "complete")
	require.Contains(t, string(complete.Data), `"dest_key":"data/`+renderedKey+`"`)
	require.Contains(t, string(complete.Data), `"key":"runs/run-001/sidecars/`+renderedKey+`.gnb.json"`)
	require.Contains(t, string(complete.Data), `"uri":"s3://dest-bucket/runs/run-001/sidecars/`+renderedKey+`.gnb.json"`)
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
	require.Contains(t, string(skipped.Data), `"provenance":{"written":true,"key":"source/file.xml.gnb.json","uri":`)
}

func TestTransferReflowCommand_ProvenanceMirroredRootDuplicateSkipOverwritesSidecar(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "same-etag", time.Time{})
	dst := newReflowMemoryProvider()
	dst.putFixture("data/source/file.xml", "payload", "same-etag", time.Time{})
	dst.putFixture("runs/run-001/sidecars/source/file.xml.gnb.json", `{"action":"old"}`, "old-sidecar", time.Time{})

	stdout, _, err := runTransferReflowWithProviderFactory(t, src, dst, reflowInputLine("source/file.xml", "same-etag", int64(len("payload")), "", ""),
		"--stdin",
		"--dest", "s3://dest-bucket/data/",
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--parallel", "1",
		"--provenance", "sidecar",
		"--provenance-sidecar-root", "s3://dest-bucket/runs/run-001/sidecars/",
	)
	require.NoError(t, err)
	require.Equal(t, []string{"data/source/file.xml"}, dst.headCallsSnapshot())
	require.Equal(t, []string{"runs/run-001/sidecars/source/file.xml.gnb.json"}, dst.putCallsSnapshot())

	sidecar := readSidecar(t, dst, "runs/run-001/sidecars/source/file.xml.gnb.json")
	require.Equal(t, "skipped.duplicate", sidecar["action"])
	require.Contains(t, string(dst.mustObject("runs/run-001/sidecars/source/file.xml.gnb.json")), `"etag":"same-etag"`)

	skipped := requireRecord(t, stdout, reflowRecordType, "skipped")
	require.Contains(t, string(skipped.Data), `"reason":"collision.duplicate"`)
	require.Contains(t, string(skipped.Data), `"key":"runs/run-001/sidecars/source/file.xml.gnb.json"`)
	require.Contains(t, string(skipped.Data), `"uri":"s3://dest-bucket/runs/run-001/sidecars/source/file.xml.gnb.json"`)
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
	require.Contains(t, string(complete.Data), `"provenance":{"written":false,"key":"source/file.xml.gnb.json","uri":`)
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
	require.Contains(t, string(failed.Data), `"provenance":{"written":false,"key":"source/file.xml.gnb.json","uri":`)
}

func TestTransferReflowCommand_CollisionHappyPathUsesIfAbsentWithoutHead(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Time{})
	dst := newReflowMemoryProvider()

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""))
	require.NoError(t, err)
	require.True(t, dst.hasObject("source/file.xml"))
	require.Equal(t, []string{"source/file.xml"}, dst.conditionalPutCallsSnapshot())
	require.Empty(t, dst.headCallsSnapshot())

	complete := requireReflowData(t, stdout, "complete")
	require.Nil(t, complete.Collision)
}

func TestTransferReflowCommand_CollisionSkipDuplicateEmitsNestedCollision(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "same-etag", time.Time{})
	dst := newReflowMemoryProvider()
	dst.putFixture("source/file.xml", "payload", "same-etag", time.Time{})

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "same-etag", int64(len("payload")), "", ""))
	require.NoError(t, err)
	require.Equal(t, []string{"source/file.xml"}, dst.conditionalPutCallsSnapshot())
	require.Equal(t, []string{"source/file.xml"}, dst.headCallsSnapshot())

	skipped := requireReflowData(t, stdout, "skipped")
	require.Equal(t, "collision.duplicate", skipped.Reason)
	requireCollisionEqual(t, skipped, collisionDuplicate, decisionIfAbsentHead, "same-etag", int64(len("payload")))
	requireNoLegacyCollisionKeys(t, requireRecord(t, stdout, reflowRecordType, "skipped").Data)
}

func TestTransferReflowCommand_CollisionZeroByteDuplicatePreservesObservedSize(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/empty.xml", "", "empty-etag", time.Time{})
	dst := newReflowMemoryProvider()
	dst.putFixture("source/empty.xml", "", "empty-etag", time.Time{})

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/empty.xml", "empty-etag", 0, "", ""))
	require.NoError(t, err)

	skipped := requireReflowData(t, stdout, "skipped")
	requireCollisionEqual(t, skipped, collisionDuplicate, decisionIfAbsentHead, "empty-etag", 0)
	record := requireRecord(t, stdout, reflowRecordType, "skipped")
	require.Contains(t, string(record.Data), `"dest_size_observed":0`)
	requireNoLegacyCollisionKeys(t, record.Data)
}

func TestTransferReflowCommand_CollisionQuarantineSkipsDuplicate(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "same-etag", time.Time{})
	dst := newReflowMemoryProvider()
	dst.putFixture("source/file.xml", "payload", "same-etag", time.Time{})

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "same-etag", int64(len("payload")), "", ""), "--on-collision", "quarantine", "--collision-quarantine-prefix", "_conflict")
	require.NoError(t, err)
	require.False(t, dst.hasObject("_conflict/source/file.xml"))

	skipped := requireReflowData(t, stdout, "skipped")
	require.Equal(t, "collision.duplicate", skipped.Reason)
	requireCollisionEqual(t, skipped, collisionDuplicate, decisionIfAbsentHead, "same-etag", int64(len("payload")))
}

func TestTransferReflowCommand_CollisionUsesPreconditionFailedHelper(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "same-etag", time.Time{})
	dst := newReflowMemoryProvider()
	dst.ifAbsentErr = provider.ErrPreconditionFailed
	dst.putFixture("source/file.xml", "payload", "same-etag", time.Time{})

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "same-etag", int64(len("payload")), "", ""))
	require.NoError(t, err)

	skipped := requireReflowData(t, stdout, "skipped")
	requireCollisionEqual(t, skipped, collisionDuplicate, decisionIfAbsentHead, "same-etag", int64(len("payload")))
}

func TestTransferReflowCommand_ParallelDuplicateRaceUsesInProcessArbiter(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	dst.ignoreIfAbsent = true

	destRel := "shared/file.xml"
	destETag := "dest-" + destRel
	input := makeRaceInput(src, destRel, []raceSource{
		{key: "source/a.xml", body: "payload", etag: destETag},
		{key: "source/b.xml", body: "payload", etag: destETag},
		{key: "source/c.xml", body: "payload", etag: destETag},
		{key: "source/d.xml", body: "payload", etag: destETag},
	})

	stdout, err := runTransferReflowWithProviders(t, src, dst, input, "--parallel", "4")
	require.NoError(t, err)
	require.Equal(t, []string{destRel}, dst.conditionalPutCallsSnapshot())
	require.Len(t, dst.headCallsSnapshot(), 3)

	records := requireReflowRecords(t, stdout)
	requireReflowStatusReasonCount(t, records, "complete", "", 1)
	requireReflowStatusReasonCount(t, records, "skipped", "collision.duplicate", 3)
	for _, rec := range records {
		if rec.Status == "skipped" {
			requireCollisionEqual(t, rec, collisionDuplicate, decisionIfAbsentHead, destETag, int64(len("payload")))
		}
	}
}

func TestReflowDestKeyArbiterCleansUpInactiveGates(t *testing.T) {
	arbiter := newReflowDestKeyArbiter()
	_, releaseA := arbiter.acquire("shared/file.xml")
	require.Equal(t, 1, arbiter.activeCount())
	releaseA()
	require.Equal(t, 0, arbiter.activeCount())

	_, releaseB := arbiter.acquire("shared/file.xml")
	require.Equal(t, 1, arbiter.activeCount())
	releaseB()
	require.Equal(t, 0, arbiter.activeCount())
}

func TestObjectBodiesEqualIgnoresReadChunkBoundaries(t *testing.T) {
	src := chunkedObjectProvider{body: []byte("byte-identical payload"), chunkSize: 3}
	dst := chunkedObjectProvider{body: []byte("byte-identical payload"), chunkSize: 7}

	equal, err := objectBodiesEqual(context.Background(), src, dst, "src", "dst")
	require.NoError(t, err)
	require.True(t, equal)

	dst.body = []byte("different payload")
	equal, err = objectBodiesEqual(context.Background(), src, dst, "src", "dst")
	require.NoError(t, err)
	require.False(t, equal)
}

func TestTransferReflowCommand_ParallelRaceUsesRealFileProviderConditionalPath(t *testing.T) {
	src := newReflowMemoryProvider()
	destDir := t.TempDir()
	destRel := "shared/file.xml"
	input := makeRaceInput(src, destRel, []raceSource{
		{key: "source/a.xml", body: "payload-a", etag: "src-a"},
		{key: "source/b.xml", body: "payload-b", etag: "src-b"},
		{key: "source/c.xml", body: "payload-c", etag: "src-c"},
		{key: "source/d.xml", body: "payload-d", etag: "src-d"},
	})

	stdout, _, err := runTransferReflowWithMemorySourceAndRealFileDest(t, src, destDir, input, "--parallel", "4")
	require.Error(t, err)
	require.Contains(t, err.Error(), "reflow completed with errors")
	require.FileExists(t, filepath.Join(destDir, destRel))
	require.Contains(t, []string{"payload-a", "payload-b", "payload-c", "payload-d"}, string(mustReadFile(t, filepath.Join(destDir, destRel))))

	records := requireReflowRecords(t, stdout)
	requireReflowStatusReasonCount(t, records, "complete", "", 1)
	requireReflowStatusReasonCount(t, records, "failed", "collision.conflict", 3)
	for _, rec := range records {
		if rec.Status == "failed" {
			require.NotNil(t, rec.Collision)
			require.Equal(t, collisionConflict, rec.Collision.Kind)
			require.Equal(t, decisionIfAbsentHead, rec.Collision.DecisionPath)
		}
	}
}

func TestTransferReflowCommand_ParallelDuplicateRaceWithRealFileProviderAndProvenance(t *testing.T) {
	src := newReflowMemoryProvider()
	destDir := t.TempDir()
	destRel := "shared/file.xml"
	input := makeRaceInput(src, destRel, []raceSource{
		{key: "source/a.xml", body: "payload", etag: "src-a"},
		{key: "source/b.xml", body: "payload", etag: "src-b"},
		{key: "source/c.xml", body: "payload", etag: "src-c"},
		{key: "source/d.xml", body: "payload", etag: "src-d"},
	})

	stdout, _, err := runTransferReflowWithMemorySourceAndRealFileDest(t, src, destDir, input, "--parallel", "4", "--provenance", "sidecar")
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(destDir, destRel))
	require.Equal(t, "payload", string(mustReadFile(t, filepath.Join(destDir, destRel))))
	sidecarPath := filepath.Join(destDir, destRel+provenanceSuffix)
	require.FileExists(t, sidecarPath)

	records := requireReflowRecords(t, stdout)
	requireReflowStatusReasonCount(t, records, "complete", "", 1)
	requireReflowStatusReasonCount(t, records, "skipped", "collision.duplicate", 3)
	for _, rec := range records {
		if rec.Status == "complete" || rec.Status == "skipped" {
			require.NotNil(t, rec.Provenance)
			require.True(t, rec.Provenance.Written)
			require.Equal(t, destRel+provenanceSuffix, rec.Provenance.Key)
		}
		if rec.Status == "skipped" {
			require.NotNil(t, rec.Collision)
			require.Equal(t, collisionDuplicate, rec.Collision.Kind)
			require.Equal(t, decisionIfAbsentHead, rec.Collision.DecisionPath)
			require.NotNil(t, rec.Collision.DestSizeObserved)
			require.Equal(t, int64(len("payload")), *rec.Collision.DestSizeObserved)
		}
	}

	var sidecar map[string]any
	require.NoError(t, json.Unmarshal(mustReadFile(t, sidecarPath), &sidecar))
	require.Contains(t, []string{"landed", "skipped.duplicate"}, sidecar["action"])
}

func TestTransferReflowCommand_FileSourceToFileDestCopiesTreeWithRedactedSourceURI(t *testing.T) {
	withTransferReflowTestState(t)

	srcDir := t.TempDir()
	destDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "nested", "file.txt"), []byte("payload"), 0o644))

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		fileURI(srcDir) + "/",
		"--dest", fileURI(destDir) + "/",
		"--rewrite-from", "nested/{file}",
		"--rewrite-to", "nested/{file}",
		"--parallel", "1",
	})

	err := cmd.Execute()
	require.NoError(t, err, stderr.String())
	require.Equal(t, "payload", string(mustReadFile(t, filepath.Join(destDir, "nested", "file.txt"))))

	complete := requireRecord(t, stdout.String(), reflowRecordType, "complete")
	require.Contains(t, string(complete.Data), `"source_uri":"file://local/nested/file.txt"`)
	require.Contains(t, string(complete.Data), `"source_bucket":"local"`)
	require.NotContains(t, string(complete.Data), srcDir)

	source := requireRecord(t, stdout.String(), reflowSourceRecordType, "")
	require.Contains(t, string(source.Data), `"source_root":"`+srcDir+`"`)
	preflight := requireRecord(t, stdout.String(), output.TypePreflight, "")
	require.Contains(t, string(preflight.Data), `"capability":"source.file.enumerate"`)
	require.Contains(t, string(preflight.Data), `"files=1 bytes=7"`)
}

func TestTransferReflowCommand_FileSourceVerboseEmitsAbsoluteSourceRootOnlyWhenOptedIn(t *testing.T) {
	withTransferReflowTestState(t)
	oldVerbose := verbose
	verbose = true
	t.Cleanup(func() { verbose = oldVerbose })

	srcDir := t.TempDir()
	destDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "file.txt"), []byte("payload"), 0o644))

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		fileURI(srcDir) + "/",
		"--dest", fileURI(destDir) + "/",
		"--rewrite-from", "{file}",
		"--rewrite-to", "{file}",
		"--parallel", "1",
	})

	require.NoError(t, cmd.Execute())
	complete := requireReflowData(t, stdout.String(), "complete")
	require.Equal(t, fileURI(filepath.Join(srcDir, "file.txt")), complete.SourceURI)
	require.Equal(t, srcDir, complete.SourceRoot)
}

func TestTransferReflowCommand_FileReflowInputAcceptsMultipleExactRecordsFromSameRoot(t *testing.T) {
	withTransferReflowTestState(t)

	srcDir := t.TempDir()
	destDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("alpha"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "b.txt"), []byte("bravo"), 0o644))

	line := func(name string, size int64) string {
		data := map[string]any{
			"source_uri":           fileURI(filepath.Join(srcDir, name)),
			"source_size_bytes":    size,
			"source_last_modified": "2026-01-15T20:53:44Z",
		}
		b, err := json.Marshal(map[string]any{"type": "gonimbus.reflow.input.v1", "data": data})
		require.NoError(t, err)
		return string(b)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader(line("a.txt", 5) + "\n" + line("b.txt", 5) + "\n"))
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--stdin",
		"--dest", fileURI(destDir) + "/",
		"--rewrite-from", "{file}",
		"--rewrite-to", "{file}",
		"--parallel", "1",
	})

	err := cmd.Execute()
	require.NoError(t, err, stderr.String())
	require.Equal(t, "alpha", string(mustReadFile(t, filepath.Join(destDir, "a.txt"))))
	require.Equal(t, "bravo", string(mustReadFile(t, filepath.Join(destDir, "b.txt"))))
	requireReflowStatusReasonCount(t, requireReflowRecords(t, stdout.String()), "complete", "", 2)
	require.Contains(t, stdout.String(), `"source_uri":"file://local/a.txt"`)
	require.Contains(t, stdout.String(), `"source_uri":"file://local/b.txt"`)
	require.NotContains(t, stdout.String(), srcDir)
}

func TestTransferReflowCommand_FileSourceResumeUsesRelativeCheckpointIdentityAcrossVerboseAndMovedRoot(t *testing.T) {
	withTransferReflowTestState(t)

	srcRootA := t.TempDir()
	srcRootB := t.TempDir()
	destDir := t.TempDir()
	checkpoint := filepath.Join(t.TempDir(), "state.db")
	require.NoError(t, os.WriteFile(filepath.Join(srcRootA, "file.txt"), []byte("payload"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(srcRootB, "file.txt"), []byte("payload"), 0o644))

	run := func(srcDir string, verboseMode bool, resume bool) (string, error) {
		oldVerbose := verbose
		verbose = verboseMode
		defer func() { verbose = oldVerbose }()

		var stdout bytes.Buffer
		cmd := newTransferReflowTestCommand()
		cmd.SetOut(&stdout)
		args := []string{
			fileURI(srcDir) + "/",
			"--dest", fileURI(destDir) + "/",
			"--rewrite-from", "{file}",
			"--rewrite-to", "{file}",
			"--checkpoint", checkpoint,
			"--parallel", "1",
		}
		if resume {
			args = append(args, "--resume")
		}
		cmd.SetArgs(args)
		err := cmd.Execute()
		return stdout.String(), err
	}

	_, err := run(srcRootA, false, false)
	require.NoError(t, err)
	stdout, err := run(srcRootB, true, true)
	require.NoError(t, err)
	skipped := requireReflowData(t, stdout, "skipped")
	require.Equal(t, "resume.complete", skipped.Reason)
	require.Equal(t, fileURI(filepath.Join(srcRootB, "file.txt")), skipped.SourceURI)
}

func TestTransferReflowCommand_FileSourceSkipsHiddenPathsByDefault(t *testing.T) {
	withTransferReflowTestState(t)

	srcDir := t.TempDir()
	destDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, ".secret"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, "__pycache__"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "keep.txt"), []byte("keep"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, ".DS_Store"), []byte("junk"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, ".secret", "token"), []byte("secret"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "__pycache__", "foo.pyc"), []byte("junk"), 0o644))

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		fileURI(srcDir) + "/",
		"--dest", fileURI(destDir) + "/",
		"--rewrite-from", "{file}",
		"--rewrite-to", "{file}",
		"--exclude", "__pycache__/*",
		"--parallel", "1",
	})

	require.NoError(t, cmd.Execute())
	require.Equal(t, "keep", string(mustReadFile(t, filepath.Join(destDir, "keep.txt"))))
	require.NoFileExists(t, filepath.Join(destDir, ".DS_Store"))
	require.NoFileExists(t, filepath.Join(destDir, ".secret", "token"))
	require.NoFileExists(t, filepath.Join(destDir, "__pycache__", "foo.pyc"))
	requireReflowStatusReasonCount(t, requireReflowRecords(t, stdout.String()), "complete", "", 1)
	preflight := requireRecord(t, stdout.String(), output.TypePreflight, "")
	require.Contains(t, string(preflight.Data), "files=1")
}

func TestTransferReflowCommand_FileSourceHiddenIncludeCopiesHiddenPaths(t *testing.T) {
	withTransferReflowTestState(t)

	srcDir := t.TempDir()
	destDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, ".secret"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, ".secret", "token"), []byte("secret"), 0o600))

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		fileURI(srcDir) + "/",
		"--dest", fileURI(destDir) + "/",
		"--rewrite-from", ".secret/{file}",
		"--rewrite-to", ".secret/{file}",
		"--hidden", "include",
		"--parallel", "1",
	})

	require.NoError(t, cmd.Execute())
	require.Equal(t, "secret", string(mustReadFile(t, filepath.Join(destDir, ".secret", "token"))))
	requireReflowStatusReasonCount(t, requireReflowRecords(t, stdout.String()), "complete", "", 1)
	require.Contains(t, stdout.String(), `"source_uri":"file://local/.secret/token"`)
}

func TestTransferReflowCommand_FileSourcePreflightRejectsSelfCopy(t *testing.T) {
	withTransferReflowTestState(t)

	srcDir := t.TempDir()
	destDir := filepath.Join(srcDir, "out")
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "file.txt"), []byte("payload"), 0o644))

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		fileURI(srcDir) + "/",
		"--dest", fileURI(destDir) + "/",
		"--rewrite-from", "{file}",
		"--rewrite-to", "{file}",
		"--parallel", "1",
	})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "File source preflight failed")
	require.NotContains(t, err.Error(), srcDir)
	require.NotContains(t, err.Error(), destDir)
	preflight := requireRecord(t, stdout.String(), output.TypePreflight, "")
	require.Contains(t, string(preflight.Data), `"capability":"destination.file.self_copy"`)
	require.Contains(t, string(preflight.Data), `"allowed":false`)
	require.NotContains(t, string(preflight.Data), srcDir)
	require.NotContains(t, string(preflight.Data), destDir)
	require.NotContains(t, stdout.String(), `"status":"in_progress"`)
}

func TestTransferReflowCommand_FileSourceSymlinkSkipEmitsSkippedRecord(t *testing.T) {
	withTransferReflowTestState(t)

	srcDir := t.TempDir()
	destDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "target.txt"), []byte("target"), 0o644))
	require.NoError(t, os.Symlink(filepath.Join(srcDir, "target.txt"), filepath.Join(srcDir, "link.txt")))

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		fileURI(srcDir) + "/",
		"--dest", fileURI(destDir) + "/",
		"--rewrite-from", "{file}",
		"--rewrite-to", "{file}",
		"--parallel", "1",
	})

	require.NoError(t, cmd.Execute())
	records := requireReflowRecords(t, stdout.String())
	requireReflowStatusReasonCount(t, records, "complete", "", 1)
	requireReflowStatusReasonCount(t, records, "skipped", "source.symlink.skipped", 1)
	require.NoFileExists(t, filepath.Join(destDir, "link.txt"))
}

func TestTransferReflowCommand_FileSourceSymlinkFollowTraversesDirectory(t *testing.T) {
	withTransferReflowTestState(t)

	srcDir := t.TempDir()
	destDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, "real"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "real", "inside.txt"), []byte("target"), 0o644))
	require.NoError(t, os.Symlink(filepath.Join(srcDir, "real"), filepath.Join(srcDir, "alias")))

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		fileURI(srcDir) + "/",
		"--dest", fileURI(destDir) + "/",
		"--rewrite-from", "{dir}/{file}",
		"--rewrite-to", "{dir}/{file}",
		"--symlinks", "follow",
		"--parallel", "1",
	})

	require.NoError(t, cmd.Execute())
	require.Equal(t, "target", string(mustReadFile(t, filepath.Join(destDir, "alias", "inside.txt"))))
	require.Equal(t, "target", string(mustReadFile(t, filepath.Join(destDir, "real", "inside.txt"))))
	records := requireReflowRecords(t, stdout.String())
	requireReflowStatusReasonCount(t, records, "complete", "", 2)
	requireReflowStatusReasonCount(t, records, "skipped", "source.symlink.skipped", 0)
}

func TestTransferReflowCommand_FileSourcePreserveModeCopiesPermissions(t *testing.T) {
	withTransferReflowTestState(t)

	srcDir := t.TempDir()
	destDir := t.TempDir()
	sourcePath := filepath.Join(srcDir, "script.sh")
	require.NoError(t, os.WriteFile(sourcePath, []byte("#!/bin/sh\n"), 0o755))

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		fileURI(srcDir) + "/",
		"--dest", fileURI(destDir) + "/",
		"--rewrite-from", "{file}",
		"--rewrite-to", "{file}",
		"--preserve-mode",
		"--parallel", "1",
	})

	require.NoError(t, cmd.Execute())
	info, err := os.Stat(filepath.Join(destDir, "script.sh"))
	require.NoError(t, err)
	require.Equal(t, fs.FileMode(0o755), info.Mode().Perm())
	requireReflowStatusReasonCount(t, requireReflowRecords(t, stdout.String()), "complete", "", 1)
}

func TestTransferReflowCommand_PreserveModeWarnsForNonFileToFileCells(t *testing.T) {
	t.Run("s3 to file", func(t *testing.T) {
		src := newReflowMemoryProvider()
		dst := newReflowMemoryProvider()
		src.putFixture("source/file.txt", "payload", "etag", time.Time{})
		_, stderr, err := runTransferReflowWithProvidersAndErr(t, src, dst, reflowInputLine("source/file.txt", "etag", int64(len("payload")), "", ""), "--preserve-mode")
		require.NoError(t, err)
		require.Contains(t, stderr, "--preserve-mode has no effect unless the source is file://")
	})

	t.Run("s3 to s3", func(t *testing.T) {
		src := newReflowMemoryProvider()
		dst := newReflowMemoryProvider()
		src.putFixture("source/file.txt", "payload", "etag", time.Time{})
		_, stderr, err := runTransferReflowWithProviderFactory(t, src, dst, reflowInputLine("source/file.txt", "etag", int64(len("payload")), "", ""),
			"--stdin",
			"--dest", "s3://dest-bucket/data/",
			"--rewrite-from", "{key}",
			"--rewrite-to", "{key}",
			"--parallel", "1",
			"--preserve-mode",
		)
		require.NoError(t, err)
		require.Contains(t, stderr, "--preserve-mode has no effect unless both source and destination are file://")
		require.Contains(t, stderr, "S3 has no Unix mode bits to read or preserve")
	})

	t.Run("file to s3", func(t *testing.T) {
		withTransferReflowTestState(t)
		srcDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(srcDir, "file.txt"), []byte("payload"), 0o644))
		dst := newReflowMemoryProvider()
		newReflowS3Provider = func(context.Context, s3.Config) (provider.Provider, error) {
			return dst, nil
		}

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		cmd := newTransferReflowTestCommand()
		cmd.SetOut(&stdout)
		cmd.SetErr(&stderr)
		cmd.SetArgs([]string{
			fileURI(srcDir) + "/",
			"--dest", "s3://dest-bucket/data/",
			"--rewrite-from", "{file}",
			"--rewrite-to", "{file}",
			"--parallel", "1",
			"--preserve-mode",
		})
		require.NoError(t, cmd.Execute())
		require.Contains(t, stderr.String(), "--preserve-mode has no effect unless the destination is file://")
	})
}

func TestTransferReflowCommand_ParallelDuplicateRaceWithProvenanceWritesSidecarEvents(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	dst.ignoreIfAbsent = true

	destRel := "shared/file.xml"
	destETag := "dest-" + destRel
	input := makeRaceInput(src, destRel, []raceSource{
		{key: "source/a.xml", body: "payload", etag: destETag},
		{key: "source/b.xml", body: "payload", etag: destETag},
		{key: "source/c.xml", body: "payload", etag: destETag},
		{key: "source/d.xml", body: "payload", etag: destETag},
	})

	stdout, err := runTransferReflowWithProviders(t, src, dst, input, "--parallel", "4", "--provenance", "sidecar")
	require.NoError(t, err)
	require.Equal(t, []string{destRel}, dst.conditionalPutCallsSnapshot())
	require.Equal(t, []string{destRel + provenanceSuffix, destRel + provenanceSuffix, destRel + provenanceSuffix, destRel + provenanceSuffix}, dst.putCallsSnapshot())

	records := requireReflowRecords(t, stdout)
	requireReflowStatusReasonCount(t, records, "complete", "", 1)
	requireReflowStatusReasonCount(t, records, "skipped", "collision.duplicate", 3)
	for _, rec := range records {
		if rec.Status == "complete" || rec.Status == "skipped" {
			require.NotNil(t, rec.Provenance)
			require.True(t, rec.Provenance.Written)
			require.Equal(t, destRel+provenanceSuffix, rec.Provenance.Key)
		}
	}
}

func TestTransferReflowCommand_MarkDestKeyObservedFailureStillEmitsCompleteAndProvenance(t *testing.T) {
	oldStateStore := newReflowStateStore
	newReflowStateStore = func(ctx context.Context, cfg reflowstate.Config) (reflowStateStore, error) {
		store, err := oldStateStore(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return markFailingReflowState{reflowStateStore: store, err: errors.New("mark failed")}, nil
	}
	t.Cleanup(func() { newReflowStateStore = oldStateStore })

	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Time{})
	dst := newReflowMemoryProvider()

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""), "--provenance", "sidecar")
	require.NoError(t, err)
	require.Equal(t, []string{"source/file.xml"}, dst.conditionalPutCallsSnapshot())
	require.True(t, dst.hasObject("source/file.xml"))
	require.True(t, dst.hasObject("source/file.xml"+provenanceSuffix))

	records := requireReflowRecords(t, stdout)
	requireReflowStatusReasonCount(t, records, "complete", "", 1)
	var complete testReflowData
	for _, rec := range records {
		if rec.Status == "complete" {
			complete = rec
			break
		}
	}
	require.NotNil(t, complete.Provenance)
	require.True(t, complete.Provenance.Written)

	require.Contains(t, stdout, reflowWarningRecord)
	require.Contains(t, stdout, "REFLOW_ARBITRATION_STATE_WRITE_FAILED")
	requireNoRecordType(t, stdout, output.TypeError)
}

func TestTransferReflowCommand_DefaultAndExplicitSkipUseConditionalPath(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "default"},
		{name: "explicit", args: []string{"--on-collision", "skip-if-duplicate"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := newReflowMemoryProvider()
			src.putFixture("source/file.xml", "payload", "src-etag", time.Time{})
			dst := newReflowMemoryProvider()

			_, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""), tc.args...)
			require.NoError(t, err)
			require.Equal(t, []string{"source/file.xml"}, dst.conditionalPutCallsSnapshot())
			require.Empty(t, dst.putCallsSnapshot())
			require.Empty(t, dst.headCallsSnapshot())
		})
	}
}

func TestTransferReflowCommand_CollisionFailDuplicateIsFailure(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "same-etag", time.Time{})
	dst := newReflowMemoryProvider()
	dst.putFixture("source/file.xml", "payload", "same-etag", time.Time{})

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "same-etag", int64(len("payload")), "", ""), "--on-collision", "fail")
	require.Error(t, err)
	require.Contains(t, err.Error(), "reflow completed with errors")

	failed := requireReflowData(t, stdout, "failed")
	require.Equal(t, "collision.exists.duplicate", failed.Reason)
	requireCollisionEqual(t, failed, collisionDuplicate, decisionIfAbsentHead, "same-etag", int64(len("payload")))
	requireRecord(t, stdout, output.TypeError, "")
}

func TestTransferReflowCommand_ParallelDuplicateRaceFailModeFailsWaiters(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	dst.ignoreIfAbsent = true

	destRel := "shared/file.xml"
	destETag := "dest-" + destRel
	input := makeRaceInput(src, destRel, []raceSource{
		{key: "source/a.xml", body: "payload", etag: destETag},
		{key: "source/b.xml", body: "payload", etag: destETag},
		{key: "source/c.xml", body: "payload", etag: destETag},
		{key: "source/d.xml", body: "payload", etag: destETag},
	})

	stdout, err := runTransferReflowWithProviders(t, src, dst, input, "--parallel", "4", "--on-collision", "fail")
	require.Error(t, err)
	require.Contains(t, err.Error(), "reflow completed with errors")
	require.Equal(t, []string{destRel}, dst.conditionalPutCallsSnapshot())

	records := requireReflowRecords(t, stdout)
	requireReflowStatusReasonCount(t, records, "complete", "", 1)
	requireReflowStatusReasonCount(t, records, "failed", "collision.exists.duplicate", 3)
	for _, rec := range records {
		if rec.Status == "failed" {
			requireCollisionEqual(t, rec, collisionDuplicate, decisionIfAbsentHead, destETag, int64(len("payload")))
		}
	}
}

func TestTransferReflowCommand_CollisionSkipConflictFails(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "new payload", "src-etag", time.Time{})
	dst := newReflowMemoryProvider()
	dst.putFixture("source/file.xml", "old payload", "dest-etag", time.Time{})

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("new payload")), "", ""))
	require.Error(t, err)
	require.Equal(t, "old payload", string(dst.mustObject("source/file.xml")))

	failed := requireReflowData(t, stdout, "failed")
	require.Equal(t, "collision.conflict", failed.Reason)
	requireCollisionEqual(t, failed, collisionConflict, decisionIfAbsentHead, "dest-etag", int64(len("old payload")))
	errRecord := requireErrorData(t, stdout)
	require.NotNil(t, errRecord.Collision)
	require.Equal(t, collisionConflict, errRecord.Collision.Kind)
	require.NotContains(t, fmt.Sprint(errRecord.Details), "collision")
}

func TestTransferReflowCommand_ParallelConflictRaceSkipModeFailsWaiters(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	dst.ignoreIfAbsent = true

	destRel := "shared/file.xml"
	input := makeRaceInput(src, destRel, []raceSource{
		{key: "source/a.xml", body: "payload-a", etag: "src-a"},
		{key: "source/b.xml", body: "payload-b", etag: "src-b"},
		{key: "source/c.xml", body: "payload-c", etag: "src-c"},
		{key: "source/d.xml", body: "payload-d", etag: "src-d"},
	})

	stdout, err := runTransferReflowWithProviders(t, src, dst, input, "--parallel", "4")
	require.Error(t, err)
	require.Contains(t, err.Error(), "reflow completed with errors")
	require.Equal(t, []string{destRel}, dst.conditionalPutCallsSnapshot())

	records := requireReflowRecords(t, stdout)
	requireReflowStatusReasonCount(t, records, "complete", "", 1)
	requireReflowStatusReasonCount(t, records, "failed", "collision.conflict", 3)
	for _, rec := range records {
		if rec.Status == "failed" {
			requireCollisionEqual(t, rec, collisionConflict, decisionIfAbsentHead, "dest-"+destRel, int64(len("payload-a")))
		}
	}
}

func TestTransferReflowCommand_CollisionConflictFailureWritesNoSidecar(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "new payload", "src-etag", time.Time{})
	dst := newReflowMemoryProvider()
	dst.putFixture("source/file.xml", "old payload", "dest-etag", time.Time{})

	_, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("new payload")), "", ""), "--provenance", "sidecar")
	require.Error(t, err)
	require.False(t, dst.hasObject("source/file.xml.gnb.json"))
}

func TestTransferReflowCommand_CollisionQuarantineRoutesConflictAndSidecar(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "new payload", "src-etag", time.Time{})
	dst := newReflowMemoryProvider()
	dst.putFixture("source/file.xml", "old payload", "dest-etag", time.Time{})

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("new payload")), "", ""), "--on-collision", "quarantine", "--collision-quarantine-prefix", "_conflict", "--provenance", "sidecar")
	require.NoError(t, err)
	require.Equal(t, "old payload", string(dst.mustObject("source/file.xml")))
	require.Equal(t, "new payload", string(dst.mustObject("_conflict/source/file.xml")))

	quarantined := requireReflowData(t, stdout, "quarantined")
	require.Equal(t, "collision.conflict.quarantined", quarantined.Reason)
	require.Equal(t, "quarantine", quarantined.RoutingClass)
	requireCollisionEqual(t, quarantined, collisionQuarantined, decisionQuarantine, "dest-etag", int64(len("old payload")))
	require.Contains(t, string(quarantined.Provenance.Key), "_conflict/source/file.xml.gnb.json")

	sidecar := readSidecar(t, dst, "_conflict/source/file.xml.gnb.json")
	collision, ok := sidecar["collision"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, collisionQuarantined, collision["kind"])
	require.Equal(t, decisionQuarantine, collision["decision_path"])
}

func TestTransferReflowCommand_ParallelDuplicateRaceQuarantineModeSkipsWaiters(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	dst.ignoreIfAbsent = true

	destRel := "shared/file.xml"
	destETag := "dest-" + destRel
	input := makeRaceInput(src, destRel, []raceSource{
		{key: "source/a.xml", body: "payload", etag: destETag},
		{key: "source/b.xml", body: "payload", etag: destETag},
		{key: "source/c.xml", body: "payload", etag: destETag},
		{key: "source/d.xml", body: "payload", etag: destETag},
	})

	stdout, err := runTransferReflowWithProviders(t, src, dst, input, "--parallel", "4", "--on-collision", "quarantine", "--collision-quarantine-prefix", "_conflict")
	require.NoError(t, err)
	require.Equal(t, []string{destRel}, dst.conditionalPutCallsSnapshot())
	require.False(t, dst.hasObject("_conflict/source/b.xml"))
	require.False(t, dst.hasObject("_conflict/source/c.xml"))
	require.False(t, dst.hasObject("_conflict/source/d.xml"))

	records := requireReflowRecords(t, stdout)
	requireReflowStatusReasonCount(t, records, "complete", "", 1)
	requireReflowStatusReasonCount(t, records, "skipped", "collision.duplicate", 3)
	for _, rec := range records {
		if rec.Status == "skipped" {
			requireCollisionEqual(t, rec, collisionDuplicate, decisionIfAbsentHead, destETag, int64(len("payload")))
		}
	}
}

func TestTransferReflowCommand_ParallelConflictRaceQuarantineModeQuarantinesWaiters(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	dst.ignoreIfAbsent = true

	destRel := "shared/file.xml"
	input := makeRaceInput(src, destRel, []raceSource{
		{key: "source/a.xml", body: "payload-a", etag: "src-a"},
		{key: "source/b.xml", body: "payload-b", etag: "src-b"},
		{key: "source/c.xml", body: "payload-c", etag: "src-c"},
		{key: "source/d.xml", body: "payload-d", etag: "src-d"},
	})

	stdout, err := runTransferReflowWithProviders(t, src, dst, input, "--parallel", "4", "--on-collision", "quarantine", "--collision-quarantine-prefix", "_conflict")
	require.NoError(t, err)
	require.Equal(t, []string{destRel}, dst.conditionalPutCallsSnapshot())

	records := requireReflowRecords(t, stdout)
	requireReflowStatusReasonCount(t, records, "complete", "", 1)
	requireReflowStatusReasonCount(t, records, "quarantined", "collision.conflict.quarantined", 3)
	for _, rec := range records {
		if rec.Status == "quarantined" {
			require.Equal(t, "quarantine", rec.RoutingClass)
			requireCollisionEqual(t, rec, collisionQuarantined, decisionQuarantine, "dest-"+destRel, int64(len("payload-a")))
		}
	}
}

func TestTransferReflowCommand_CollisionOverwriteEmitsNestedCollision(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "new payload", "src-etag", time.Time{})
	dst := newReflowMemoryProvider()
	dst.putFixture("source/file.xml", "old payload", "dest-etag", time.Time{})

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("new payload")), "", ""), "--on-collision", "overwrite", "--overwrite")
	require.NoError(t, err)
	require.Equal(t, "new payload", string(dst.mustObject("source/file.xml")))

	complete := requireReflowData(t, stdout, "complete")
	requireCollisionEqual(t, complete, collisionConflict, decisionOverwrite, "dest-etag", int64(len("old payload")))
}

func TestTransferReflowCommand_CollisionOverwriteIfSourceNewerReplacesWithIfMatch(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "new payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
	dst := newReflowMemoryProvider()
	dst.putFixture("source/file.xml", "old payload", "dest-etag", time.Date(2026, 1, 14, 20, 53, 44, 0, time.UTC))

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("new payload")), "", ""), "--on-collision", "overwrite-if-source-newer")
	require.NoError(t, err)
	require.Equal(t, "new payload", string(dst.mustObject("source/file.xml")))
	require.Equal(t, []string{"source/file.xml", "source/file.xml"}, dst.conditionalPutCallsSnapshot())
	preconds := dst.conditionalPutPreconditionsSnapshot()
	require.Len(t, preconds, 2)
	require.True(t, preconds[0].IfAbsent)
	require.NotNil(t, preconds[1].IfMatchETag)
	require.Equal(t, "dest-etag", *preconds[1].IfMatchETag)

	complete := requireReflowData(t, stdout, "complete")
	requireCollisionEqual(t, complete, collisionOverwritten, decisionHeadCompare, "dest-etag", int64(len("old payload")))
	requireSourceNewerCollisionEqual(t, complete, reasonSrcNewer, "2026-01-15T20:53:44Z", "2026-01-14T20:53:44Z")
}

func TestTransferReflowCommand_CollisionOverwriteIfSourceNewerFileDestUsesHeadToken(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("file.xml", "new payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "file.xml")
	require.NoError(t, os.WriteFile(destPath, []byte("old payload"), 0o600))
	destLastMod := time.Date(2026, 1, 14, 20, 53, 44, 0, time.UTC)
	require.NoError(t, os.Chtimes(destPath, destLastMod, destLastMod))

	stdout, stderr, err := runTransferReflowWithMemorySourceAndRealFileDest(t, src, destDir, reflowInputLine("file.xml", "src-etag", int64(len("new payload")), "", ""), "--on-collision", "overwrite-if-source-newer")
	require.NoError(t, err, stdout+stderr)
	got, err := os.ReadFile(destPath)
	require.NoError(t, err)
	require.Equal(t, "new payload", string(got))

	complete := requireReflowData(t, stdout, "complete")
	require.NotNil(t, complete.Collision)
	require.Equal(t, collisionOverwritten, complete.Collision.Kind)
	require.Equal(t, decisionHeadCompare, complete.Collision.DecisionPath)
	require.NotEmpty(t, complete.Collision.DestETagObserved)
	require.NotNil(t, complete.Collision.DestSizeObserved)
	require.Equal(t, int64(len("old payload")), *complete.Collision.DestSizeObserved)
	requireSourceNewerCollisionEqual(t, complete, reasonSrcNewer, "2026-01-15T20:53:44Z", "2026-01-14T20:53:44Z")
}

func TestTransferReflowCommand_CollisionOverwriteIfSourceNewerUsesSourceHeadWhenInputLastModifiedMissing(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "new payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
	dst := newReflowMemoryProvider()
	dst.putFixture("source/file.xml", "old payload", "dest-etag", time.Date(2026, 1, 14, 20, 53, 44, 0, time.UTC))

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLineWithoutLastModified("source/file.xml", "src-etag", int64(len("new payload"))), "--on-collision", "overwrite-if-source-newer")
	require.NoError(t, err)
	require.Equal(t, []string{"source/file.xml"}, src.headCallsSnapshot())

	complete := requireReflowData(t, stdout, "complete")
	requireCollisionEqual(t, complete, collisionOverwritten, decisionHeadCompare, "dest-etag", int64(len("old payload")))
	requireSourceNewerCollisionEqual(t, complete, reasonSrcNewer, "2026-01-15T20:53:44Z", "2026-01-14T20:53:44Z")
}

func TestTransferReflowCommand_CollisionOverwriteIfSourceNewerUsesIndexLastModifiedWithoutSourceHead(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("file.xml", "new payload", "src-etag", time.Time{})
	dst := newReflowMemoryProvider()
	dst.putFixture("file.xml", "old payload", "dest-etag", time.Date(2026, 1, 14, 20, 53, 44, 0, time.UTC))

	stdout, stderr, err := runTransferReflowWithProvidersAndErr(t, src, dst, reflowIndexObjectInputLine("file.xml", "src-etag", int64(len("new payload")), "2026-01-15T20:53:44Z"), "--on-collision", "overwrite-if-source-newer")
	require.NoError(t, err, stdout+stderr)
	require.Empty(t, src.headCallsSnapshot())
	require.Equal(t, "new payload", string(dst.mustObject("file.xml")))

	complete := requireReflowData(t, stdout, "complete")
	requireCollisionEqual(t, complete, collisionOverwritten, decisionHeadCompare, "dest-etag", int64(len("old payload")))
	requireSourceNewerCollisionEqual(t, complete, reasonSrcNewer, "2026-01-15T20:53:44Z", "2026-01-14T20:53:44Z")
}

func TestTransferReflowCommand_CollisionOverwriteIfSourceNewerEqualTimeSizeDiffers(t *testing.T) {
	lm := time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC)
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "new payload", "src-etag", lm)
	dst := newReflowMemoryProvider()
	dst.putFixture("source/file.xml", "old", "dest-etag", lm)

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("new payload")), "", ""), "--on-collision", "overwrite-if-source-newer")
	require.NoError(t, err)
	require.Equal(t, "new payload", string(dst.mustObject("source/file.xml")))

	complete := requireReflowData(t, stdout, "complete")
	requireCollisionEqual(t, complete, collisionOverwritten, decisionHeadCompare, "dest-etag", int64(len("old")))
	requireSourceNewerCollisionEqual(t, complete, reasonEqualSizeDiffers, "2026-01-15T20:53:44Z", "2026-01-15T20:53:44Z")
}

func TestTransferReflowCommand_CollisionOverwriteIfSourceNewerSkipsOlderSource(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "new payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
	dst := newReflowMemoryProvider()
	dst.putFixture("source/file.xml", "old payload", "dest-etag", time.Date(2026, 1, 16, 20, 53, 44, 0, time.UTC))

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("new payload")), "", ""), "--on-collision", "overwrite-if-source-newer")
	require.NoError(t, err)
	require.Equal(t, "old payload", string(dst.mustObject("source/file.xml")))
	require.Equal(t, []string{"source/file.xml"}, dst.conditionalPutCallsSnapshot())

	skipped := requireReflowData(t, stdout, "skipped")
	require.Equal(t, "collision.skipped_src_older", skipped.Reason)
	requireCollisionEqual(t, skipped, collisionSrcOlder, decisionHeadCompare, "dest-etag", int64(len("old payload")))
	requireSourceNewerCollisionEqual(t, skipped, reasonSrcOlder, "2026-01-15T20:53:44Z", "2026-01-16T20:53:44Z")
}

func TestTransferReflowCommand_CollisionOverwriteIfSourceNewerSkipsConcurrentMutation(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "new payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
	dst := newReflowMemoryProvider()
	dst.putFixture("source/file.xml", "old payload", "dest-etag", time.Date(2026, 1, 14, 20, 53, 44, 0, time.UTC))
	dst.mutateBeforeIfMatch = true

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("new payload")), "", ""), "--on-collision", "overwrite-if-source-newer")
	require.NoError(t, err)
	require.Equal(t, "concurrent mutation", string(dst.mustObject("source/file.xml")))

	skipped := requireReflowData(t, stdout, "skipped")
	require.Equal(t, "collision.skipped_concurrent_mutation", skipped.Reason)
	requireCollisionEqual(t, skipped, collisionConcurrentMut, decisionHeadCompare, "dest-etag", int64(len("old payload")))
	requireSourceNewerCollisionEqual(t, skipped, reasonConcurrentMut, "2026-01-15T20:53:44Z", "2026-01-14T20:53:44Z")
}

func TestTransferReflowCommand_CollisionOverwriteIfSourceNewerRequiresConditionalPutter(t *testing.T) {
	withTransferReflowTestState(t)

	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
	dst := newReflowMemoryProvider()
	newReflowS3Provider = func(context.Context, s3.Config) (provider.Provider, error) {
		return src, nil
	}
	newReflowFileProvider = func(providerfile.Config) (provider.Provider, error) {
		return &reflowNoConditionalProvider{p: dst}, nil
	}

	var stdout bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader(reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", "") + "\n"))
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"--stdin",
		"--dest", fileURI(t.TempDir()),
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--parallel", "1",
		"--on-collision", "overwrite-if-source-newer",
	})
	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "IfMatchETag")
	require.Contains(t, stdout.String(), `"missing_capability":"ConditionalPutter.IfMatchETag"`)
	require.False(t, dst.hasObject("source/file.xml"))
}

func TestTransferReflowCommand_CollisionLogAliasWarns(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Time{})
	dst := newReflowMemoryProvider()

	_, stderr, err := runTransferReflowWithProvidersAndErr(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""), "--on-collision", "log")
	require.NoError(t, err)
	require.Contains(t, stderr, "deprecated")
}

func TestTransferReflowCommand_CollisionQuarantineRequiresPrefix(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()

	_, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""), "--on-collision", "quarantine")
	require.Error(t, err)
	require.Contains(t, err.Error(), "collision_quarantine_prefix is required")
}

func TestTransferReflowCommand_CollisionQuarantineRejectsAbsolutePrefix(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()

	_, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""), "--on-collision", "quarantine", "--collision-quarantine-prefix", "s3://other/prefix")
	require.Error(t, err)
	require.Contains(t, err.Error(), "collision_quarantine_prefix must be a relative destination prefix")

	_, err = runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""), "--on-collision", "quarantine", "--collision-quarantine-prefix", "/absolute")
	require.Error(t, err)
	require.Contains(t, err.Error(), "collision_quarantine_prefix must be a relative destination prefix")
}

func TestTransferReflowCommand_MetadataSetUsesConditionalPutWithoutSourceHead(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Time{})

	_, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""),
		"--metadata-set", "Owner=first",
		"--metadata-set", "owner=final",
	)
	require.NoError(t, err)
	require.Empty(t, src.headCallsSnapshot())
	require.Equal(t, []string{"source/file.xml"}, dst.conditionalPutCallsSnapshot())
	meta := dst.metaSnapshot("source/file.xml")
	require.Equal(t, map[string]string{"owner": "final"}, meta.Metadata)
	require.Empty(t, meta.ContentType)
	require.Empty(t, meta.StorageClass)
}

func TestTransferReflowCommand_MetadataMergePreservesContentTypeAndPropagatesStorage(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{
		ContentType:  "application/xml",
		Metadata:     map[string]string{"Foo": "old", "Bar": "keep"},
		StorageClass: "standard_ia",
	})

	_, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""),
		"--metadata-policy", "merge",
		"--metadata-set", "foo=new",
		"--preserve-content-type",
		"--destination-storage-class", "propagate",
	)
	require.NoError(t, err)
	require.Equal(t, []string{"source/file.xml"}, src.headCallsSnapshot())
	meta := dst.metaSnapshot("source/file.xml")
	require.Equal(t, map[string]string{"bar": "keep", "foo": "new"}, meta.Metadata)
	require.Equal(t, "application/xml", meta.ContentType)
	require.Equal(t, "STANDARD_IA", meta.StorageClass)
}

func TestTransferReflowCommand_PerObjectMetadataDerivation(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	lmA := time.Date(2026, 5, 22, 10, 11, 12, 123456789, time.FixedZone("source", -4*60*60))
	lmB := time.Date(2026, 5, 22, 11, 12, 13, 987654321, time.UTC)
	src.putFixtureWithMeta("source/a.xml", "payload-a", "etag-a", lmA, provider.ObjectMeta{
		ContentType:  "application/xml",
		StorageClass: "standard_ia",
		Metadata: map[string]string{
			"Src":     "copy-a",
			"blob":    `{"subfield":"json-a","big":99999999999999999999,"flag":true}`,
			"encoded": `%7B%22subfield%22%3A%22url-a%22%7D`,
		},
	})
	src.putFixtureWithMeta("source/b.xml", "payload-b", "etag-b", lmB, provider.ObjectMeta{
		ContentType: "text/plain",
		Metadata: map[string]string{
			"src":     "copy-b",
			"blob":    `{"subfield":"json-b","big":12345,"flag":false}`,
			"encoded": `%7B%22subfield%22%3A%22url-b%22%7D`,
		},
	})
	input := strings.Join([]string{
		reflowInputLine("source/a.xml", "etag-a", int64(len("payload-a")), "", ""),
		reflowInputLine("source/b.xml", "etag-b", int64(len("payload-b")), "", ""),
	}, "\n")

	_, err := runTransferReflowWithProviders(t, src, dst, input,
		"--metadata-set-from-source-key", "source-copy=src",
		"--metadata-set-from-source-derived", "json-field=meta.blob.subfield",
		"--metadata-set-from-source-derived", "url-field=urldecode(meta.encoded).subfield",
		"--metadata-set-from-source-derived", "source-etag=system.etag",
		"--metadata-set-from-source-derived", "etag-tag=system.etag + \"-src\"",
		"--metadata-set-from-source-derived", "source-modified=system.last_modified",
		"--metadata-set-from-source-derived", "source-size=system.content_length",
		"--metadata-set-from-source-derived", "source-content-type=system.content_type",
		"--metadata-set-from-source-derived", "source-storage-class=system.storage_class",
		"--metadata-set-from-source-derived", "big=meta.blob.big",
		"--metadata-set-from-source-derived", "flag=meta.blob.flag",
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"source/a.xml", "source/b.xml"}, src.headCallsSnapshot())
	require.Equal(t, []string{"source/a.xml", "source/b.xml"}, dst.conditionalPutCallsSnapshot())

	metaA := dst.metaSnapshot("source/a.xml")
	require.Equal(t, "copy-a", metaA.Metadata["source-copy"])
	require.Equal(t, "json-a", metaA.Metadata["json-field"])
	require.Equal(t, "url-a", metaA.Metadata["url-field"])
	require.Equal(t, "etag-a", metaA.Metadata["source-etag"])
	require.Equal(t, "etag-a-src", metaA.Metadata["etag-tag"])
	require.Equal(t, lmA.UTC().Format(time.RFC3339Nano), metaA.Metadata["source-modified"])
	require.Equal(t, strconv.Itoa(len("payload-a")), metaA.Metadata["source-size"])
	require.Equal(t, "application/xml", metaA.Metadata["source-content-type"])
	require.Equal(t, "standard_ia", metaA.Metadata["source-storage-class"])
	require.Equal(t, "99999999999999999999", metaA.Metadata["big"])
	require.Equal(t, "true", metaA.Metadata["flag"])

	metaB := dst.metaSnapshot("source/b.xml")
	require.Equal(t, "copy-b", metaB.Metadata["source-copy"])
	require.Equal(t, "json-b", metaB.Metadata["json-field"])
	require.Equal(t, "url-b", metaB.Metadata["url-field"])
	require.Equal(t, "etag-b", metaB.Metadata["source-etag"])
	require.Equal(t, "STANDARD", metaB.Metadata["source-storage-class"])
	require.Equal(t, "false", metaB.Metadata["flag"])
}

func TestTransferReflowCommand_PerInvocationMetadataWinsOverPerObject(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{Metadata: map[string]string{"src": "per-object"}})

	_, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""),
		"--metadata-set-from-source-key", "foo=src",
		"--metadata-set", "foo=invocation",
	)
	require.NoError(t, err)
	require.Equal(t, "invocation", dst.metaSnapshot("source/file.xml").Metadata["foo"])
}

func TestTransferReflowCommand_MetadataOnMissingSourceModes(t *testing.T) {
	t.Run("skip", func(t *testing.T) {
		src := newReflowMemoryProvider()
		dst := newReflowMemoryProvider()
		src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{Metadata: map[string]string{"present": "value", "blob": `{"array":["x"]}`}})
		_, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""),
			"--metadata-set-from-source-key", "present=present",
			"--metadata-set-from-source-key", "missing=missing",
			"--metadata-set-from-source-derived", "array=meta.blob.array",
		)
		require.NoError(t, err)
		meta := dst.metaSnapshot("source/file.xml").Metadata
		require.Equal(t, "value", meta["present"])
		require.NotContains(t, meta, "missing")
		require.NotContains(t, meta, "array")
	})

	t.Run("empty", func(t *testing.T) {
		src := newReflowMemoryProvider()
		dst := newReflowMemoryProvider()
		src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{Metadata: map[string]string{"blob": `{"object":{"nested":"x"}` + `}`}})
		_, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""),
			"--metadata-set-from-source-key", "missing=missing",
			"--metadata-set-from-source-derived", "object=meta.blob.object",
			"--metadata-on-missing-source", "empty",
		)
		require.NoError(t, err)
		meta := dst.metaSnapshot("source/file.xml").Metadata
		require.Equal(t, "", meta["missing"])
		require.Equal(t, "", meta["object"])
	})

	t.Run("fail redacts source value", func(t *testing.T) {
		src := newReflowMemoryProvider()
		dst := newReflowMemoryProvider()
		checkpoint := filepath.Join(t.TempDir(), "reflow-state.db")
		sentinel := "DERIVED_SENTINEL_VALUE"
		src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{Metadata: map[string]string{"blob": sentinel + "%ZZ"}})
		stdout, stderr, err := runTransferReflowWithProvidersAndErr(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""),
			"--checkpoint", checkpoint,
			"--metadata-set-from-source-derived", "secret=urldecode(meta.blob).token",
			"--metadata-on-missing-source", "fail",
		)
		require.Error(t, err)
		require.NotContains(t, err.Error(), sentinel)
		require.NotContains(t, stdout, sentinel)
		require.NotContains(t, stderr, sentinel)
		require.NotContains(t, readCheckpointErrorMessage(t, checkpoint), sentinel)
		errData := requireErrorData(t, stdout)
		require.Contains(t, errData.Message, "metadata derivation failed")
		require.Equal(t, "secret", errData.Details["metadata_dest_key"])
		require.Equal(t, "url decode failed", errData.Details["metadata_reason"])
	})

	t.Run("fail reports non-scalar kind", func(t *testing.T) {
		src := newReflowMemoryProvider()
		dst := newReflowMemoryProvider()
		src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{Metadata: map[string]string{"blob": `{"array":["x"]}`}})
		stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""),
			"--metadata-set-from-source-derived", "array=meta.blob.array",
			"--metadata-on-missing-source", "fail",
		)
		require.Error(t, err)
		errData := requireErrorData(t, stdout)
		require.Equal(t, "array", errData.Details["metadata_result_kind"])
	})
}

func TestTransferReflowCommand_MetadataDerivationFailQuarantinesWhenRequested(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	checkpoint := filepath.Join(t.TempDir(), "reflow-state.db")
	sentinel := "DERIVED_SENTINEL_VALUE"
	src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{
		Metadata: map[string]string{"blob": sentinel + "%ZZ"},
	})

	stdout, stderr, err := runTransferReflowWithProvidersAndErr(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""),
		"--checkpoint", checkpoint,
		"--metadata-set-from-source-derived", "secret=urldecode(meta.blob).token",
		"--metadata-on-missing-source", "fail",
		"--on-collision", "quarantine",
		"--collision-quarantine-prefix", "_metadata",
		"--provenance", "sidecar",
	)
	require.NoError(t, err)
	require.False(t, dst.hasObject("source/file.xml"))
	require.Equal(t, "payload", string(dst.mustObject("_metadata/source/file.xml")))
	require.NotContains(t, stdout, sentinel)
	require.NotContains(t, stderr, sentinel)

	errData := requireErrorData(t, stdout)
	require.Contains(t, errData.Message, "metadata derivation failed")
	require.Equal(t, "secret", errData.Details["metadata_dest_key"])
	require.Equal(t, "url decode failed", errData.Details["metadata_reason"])

	quarantined := requireReflowData(t, stdout, "quarantined")
	require.Equal(t, "metadata.derivation.quarantined", quarantined.Reason)
	require.Equal(t, "quarantine", quarantined.RoutingClass)
	require.Equal(t, "_metadata/source/file.xml", quarantined.DestKey)
	require.Contains(t, string(quarantined.Provenance.Key), "_metadata/source/file.xml.gnb.json")

	checkpointItem := readCheckpointItem(t, checkpoint)
	require.Equal(t, "quarantined", checkpointItem.Status)
	require.Equal(t, "metadata.derivation.quarantined", checkpointItem.Reason)
	require.NotContains(t, checkpointItem.ErrorMessage, sentinel)
}

func TestTransferReflowMetadataConfigRejectsPerObjectDuplicatesAndParseErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "duplicate source key rules", args: []string{"--metadata-set-from-source-key", "foo=a", "--metadata-set-from-source-key", "foo=b"}, want: "duplicate per-object metadata destination key"},
		{name: "duplicate mixed rules", args: []string{"--metadata-set-from-source-key", "foo=a", "--metadata-set-from-source-derived", "foo=system.etag"}, want: "duplicate per-object metadata destination key"},
		{name: "unknown function", args: []string{"--metadata-set-from-source-derived", "foo=base64decode(meta.blob).x"}, want: "unknown function"},
		{name: "unknown system field", args: []string{"--metadata-set-from-source-derived", "foo=system.bogus_field"}, want: "unknown system field"},
		{name: "dangling plus", args: []string{"--metadata-set-from-source-derived", "foo=meta.blob.x +"}, want: "dangling + operator"},
		{name: "unclosed call", args: []string{"--metadata-set-from-source-derived", "foo=urldecode(meta.blob.x"}, want: "unclosed urldecode call"},
		{name: "wildcard", args: []string{"--metadata-set-from-source-derived", "foo=urldecode(meta.blob).*"}, want: "wildcard"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withTransferReflowTestState(t)
			cmd := newTransferReflowTestCommand()
			require.NoError(t, cmd.Flags().Parse(tt.args))
			_, err := resolveMetadataConfig(cmd)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestTransferReflowCommand_MetadataPreserveRejectsCanonicalSourceCollision(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{
		Metadata: map[string]string{"Foo": "secret-one", "foo": "secret-two"},
	})

	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""),
		"--metadata-policy", "preserve",
	)
	require.Error(t, err)
	require.False(t, dst.hasObject("source/file.xml"))
	errData := requireErrorData(t, stdout)
	require.Contains(t, errData.Message, "destination metadata options failed")
	require.NotContains(t, stdout, "secret-one")
	require.NotContains(t, stdout, "secret-two")
}

func TestTransferReflowCommand_PerObjectMetadataCanonicalCollisionRedactsValues(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	checkpoint := filepath.Join(t.TempDir(), "reflow-state.db")
	src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{
		Metadata: map[string]string{"Foo": "secret-one", "foo": "secret-two"},
	})

	stdout, stderr, err := runTransferReflowWithProvidersAndErr(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""),
		"--checkpoint", checkpoint,
		"--metadata-set-from-source-key", "foo-copy=foo",
		"--metadata-on-missing-source", "fail",
	)
	require.Error(t, err)
	require.False(t, dst.hasObject("source/file.xml"))
	for _, text := range []string{err.Error(), stdout, stderr, readCheckpointErrorMessage(t, checkpoint)} {
		require.NotContains(t, text, "secret-one")
		require.NotContains(t, text, "secret-two")
	}
	errData := requireErrorData(t, stdout)
	require.Equal(t, "foo-copy", errData.Details["metadata_dest_key"])
	require.Equal(t, "source metadata canonical collision", errData.Details["metadata_reason"])
	require.Contains(t, errData.Details["metadata_colliding_keys"], "Foo")
	require.Contains(t, errData.Details["metadata_colliding_keys"], "foo")
}

func TestTransferReflowCommand_MetadataBudgetErrorRedactsValuesEverywhere(t *testing.T) {
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Time{})
	sentinel := "sentinel-secret-value-" + strings.Repeat("x", metadataMaxTotalBytes+1)
	checkpoint := filepath.Join(t.TempDir(), "reflow-state.db")

	stdout, stderr, err := runTransferReflowWithProviderFactory(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""),
		"--stdin",
		"--dest", "s3://dest-bucket/",
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--parallel", "1",
		"--checkpoint", checkpoint,
		"--metadata-set", "secret_token="+sentinel,
	)
	require.Error(t, err)
	require.NotContains(t, err.Error(), sentinel)
	require.NotContains(t, stdout, sentinel)
	require.NotContains(t, stderr, sentinel)

	errData := requireErrorData(t, stdout)
	require.Equal(t, output.ErrCodeInvalidInput, errData.Code)
	require.Contains(t, errData.Message, "user metadata exceeds S3 metadata budget")
	require.NotContains(t, errData.Message, sentinel)
	require.Contains(t, errData.Details["metadata_keys"], "secret_token")
	require.EqualValues(t, 1, errData.Details["metadata_count"])
	require.NotContains(t, readCheckpointErrorMessage(t, checkpoint), sentinel)
}

func TestTransferReflowCommand_FileMetadataSidecarDoesNotUseS3Budget(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "src-etag", time.Time{})
	destDir := t.TempDir()
	oversized := "file-sidecar-value-" + strings.Repeat("x", metadataMaxTotalBytes+1)

	_, stderr, err := runTransferReflowWithMemorySourceAndRealFileDest(t, src, destDir, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""),
		"--metadata-set", "oversized="+oversized,
	)
	require.NoError(t, err, stderr)

	sidecarPath := filepath.Join(destDir, "source", "file.xml"+providerfile.DefaultMetadataSidecarSuffix)
	raw := mustReadFile(t, sidecarPath)
	var sidecar map[string]any
	require.NoError(t, json.Unmarshal(raw, &sidecar))
	require.Equal(t, "gonimbus.reflow.meta.v1", sidecar["schema"])
	userMeta, ok := sidecar["user_metadata"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, oversized, userMeta["oversized"])
}

func TestTransferReflowCommand_FileMetadataSidecarIncludesDerivedMetadata(t *testing.T) {
	src := newReflowMemoryProvider()
	src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{
		ContentType: "application/xml",
		Metadata: map[string]string{
			"src":  "copy-me",
			"blob": `{"site":"001","flag":true}`,
		},
	})
	destDir := t.TempDir()

	_, stderr, err := runTransferReflowWithMemorySourceAndRealFileDest(t, src, destDir, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", ""),
		"--metadata-set-from-source-key", "source-copy=src",
		"--metadata-set-from-source-derived", "site=meta.blob.site",
		"--metadata-set-from-source-derived", "flag=meta.blob.flag",
		"--preserve-content-type",
	)
	require.NoError(t, err, stderr)

	sidecarPath := filepath.Join(destDir, "source", "file.xml"+providerfile.DefaultMetadataSidecarSuffix)
	raw := mustReadFile(t, sidecarPath)
	var sidecar map[string]any
	require.NoError(t, json.Unmarshal(raw, &sidecar))
	require.Equal(t, "application/xml", sidecar["content_type"])
	userMeta, ok := sidecar["user_metadata"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "copy-me", userMeta["source-copy"])
	require.Equal(t, "001", userMeta["site"])
	require.Equal(t, "true", userMeta["flag"])
}

func TestTransferReflowCommand_MetadataCapabilityFailureEmitsConfigJSONLError(t *testing.T) {
	withTransferReflowTestState(t)
	src := newReflowMemoryProvider()
	dst := &reflowBareProvider{p: newReflowMemoryProvider()}
	newReflowS3Provider = func(context.Context, s3.Config) (provider.Provider, error) {
		return src, nil
	}
	newReflowFileProvider = func(providerfile.Config) (provider.Provider, error) {
		return dst, nil
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader(reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", "") + "\n"))
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--stdin",
		"--dest", fileURI(t.TempDir()),
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--metadata-set", "owner=team",
		"--metadata-set-from-source-derived", "source-etag=system.etag",
	})

	err := cmd.Execute()
	require.Error(t, err)
	errData := requireErrorData(t, stdout.String())
	require.Equal(t, output.ErrCodeInvalidInput, errData.Code)
	require.Contains(t, errData.Message, "metadata-aware PUT")
	require.Equal(t, "MetadataAwarePutter", errData.Details["missing_capability"])
	require.Contains(t, errData.Details["flags"], "--metadata-set")
	require.Contains(t, errData.Details["flags"], "--metadata-set-from-source-derived")
}

func TestTransferReflowMetadataConfigValidation(t *testing.T) {
	require.NoError(t, validateMetadataConfig(reflowMetadataConfig{Policy: metadataPolicyClear, MetadataSidecarSuffix: providerfile.DefaultMetadataSidecarSuffix}))
	require.ErrorContains(t, validateMetadataConfig(reflowMetadataConfig{Policy: "copy", MetadataSidecarSuffix: providerfile.DefaultMetadataSidecarSuffix}), "metadata-policy")
	require.ErrorContains(t, validateMetadataConfig(reflowMetadataConfig{Policy: metadataPolicyClear, MetadataSidecarSuffix: providerfile.DefaultMetadataSidecarSuffix, DestinationStorageClass: "GLACIER"}), "not a valid PUT target")
	require.NoError(t, validateMetadataConfig(reflowMetadataConfig{Policy: metadataPolicyClear, MetadataSidecarSuffix: providerfile.DefaultMetadataSidecarSuffix, DestinationStorageClass: storageClassPropagate}))
}

func TestTransferReflowSourceConfigValidation(t *testing.T) {
	require.NoError(t, validateSourceConfig(reflowSourceConfig{Symlinks: reflowSymlinkSkip, OnSourceFailure: reflowSourceFailSkip}))
	require.NoError(t, validateSourceConfig(reflowSourceConfig{Symlinks: reflowSymlinkFollow, OnSourceFailure: reflowSourceFailFail}))
	require.NoError(t, validateSourceConfig(reflowSourceConfig{Symlinks: reflowSymlinkSkip, Hidden: reflowHiddenInclude, OnSourceFailure: reflowSourceFailSkip}))
	require.ErrorContains(t, validateSourceConfig(reflowSourceConfig{Symlinks: "preserve", OnSourceFailure: reflowSourceFailSkip}), "--symlinks=preserve is not supported in v1")
	require.ErrorContains(t, validateSourceConfig(reflowSourceConfig{Symlinks: reflowSymlinkSkip, Hidden: "warn", OnSourceFailure: reflowSourceFailSkip}), "hidden must be one of")
	require.ErrorContains(t, validateSourceConfig(reflowSourceConfig{Symlinks: reflowSymlinkSkip, OnSourceFailure: "quarantine"}), "--on-source-failure=quarantine is not supported in v1")
	require.ErrorContains(t, validateSourceConfig(reflowSourceConfig{Symlinks: reflowSymlinkSkip, OnSourceFailure: reflowSourceFailSkip, Excludes: []string{"["}}), "invalid exclude glob")
}

func TestTransferReflowMetadataCapabilityRequiredOnlyForOptionedWrites(t *testing.T) {
	require.NoError(t, ensureMetadataCapability(&mockProvider{}, reflowMetadataConfig{Policy: metadataPolicyClear}))
	require.ErrorContains(t, ensureMetadataCapability(&mockProvider{}, reflowMetadataConfig{Policy: metadataPolicyClear, Set: map[string]string{"owner": "team"}}), "metadata-aware PUT")
	require.ErrorContains(t, ensureMetadataCapability(&mockProvider{}, reflowMetadataConfig{Policy: metadataPolicyPreserve}), "--metadata-policy")
	require.ErrorContains(t, ensureMetadataCapability(&mockProvider{}, reflowMetadataConfig{Policy: metadataPolicyClear, SourceKeyRules: []metadataSourceKeyRule{{DestKey: "foo", SourceKey: "bar"}}}), "--metadata-set-from-source-key")
}

func TestTransferReflowHelpWarnsAboutDurableMetadata(t *testing.T) {
	require.Contains(t, transferReflowCmd.Long, "durable destination metadata")
	require.Contains(t, transferReflowCmd.Long, "--metadata-set-from-source-key")
	require.Contains(t, transferReflowCmd.Long, "--metadata-set-from-source-derived")
	require.Contains(t, transferReflowCmd.Long, "not redacted at destination")
	require.Contains(t, transferReflowCmd.Long, "explicit allow-list")
	require.Contains(t, transferReflowCmd.Long, "Audit derivation expressions")
	require.Contains(t, transferReflowCmd.Long, "cleartext JSON sidecars")
	require.Contains(t, transferReflowCmd.Long, "--metadata-sidecar-suffix")
	require.Contains(t, transferReflowCmd.Long, "Local-tree reflow skips hidden files")
	require.Contains(t, transferReflowCmd.Long, "--hidden=include")
	require.Contains(t, transferReflowCmd.Long, "not gitignore-aware")
	require.Contains(t, transferReflowCmd.Long, "node_modules/*")
}

func TestLocalTreeMigrationRunbookWarnsAboutHiddenFiles(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "user-guide", "local-tree-migration.md"))
	require.NoError(t, err)
	text := string(raw)
	require.Contains(t, text, "Local-tree reflow skips hidden files")
	require.Contains(t, text, "--dry-run")
	require.Contains(t, text, "--hidden=include")
	require.Contains(t, text, "not gitignore-aware")
	require.Contains(t, text, "--exclude 'node_modules/*'")
	require.Contains(t, text, "absolute source root")
	require.Contains(t, text, "aws s3 sync")
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
	cmd.Flags().StringVar(&reflowResumeRun, "resume-run", "", "")
	cmd.Flags().StringVar(&reflowCheckpoint, "checkpoint", "", "")
	cmd.Flags().BoolVar(&reflowOverwrite, "overwrite", false, "")
	cmd.Flags().StringVar(&reflowOnCollision, "on-collision", reflowCollisionSkip, "")
	cmd.Flags().StringVar(&reflowCollQuar, "collision-quarantine-prefix", "", "")
	cmd.Flags().StringVar(&reflowProvenance, "provenance", provenanceModeNone, "")
	cmd.Flags().StringVar(&reflowProvRoot, "provenance-sidecar-root", "", "")
	cmd.Flags().StringVar(&reflowProvSuffix, "provenance-suffix", provenanceSuffix, "")
	cmd.Flags().StringVar(&reflowProvOnError, "provenance-on-write-error", provenanceErrorWarn, "")
	cmd.Flags().BoolVar(&reflowProvUnsafe, "allow-unsafe-suffix", false, "")
	cmd.Flags().StringVar(&reflowMetaPolicy, "metadata-policy", metadataPolicyClear, "")
	cmd.Flags().StringArrayVar(&reflowMetaSets, "metadata-set", nil, "")
	cmd.Flags().StringArrayVar(&reflowMetaSrcKeys, "metadata-set-from-source-key", nil, "")
	cmd.Flags().StringArrayVar(&reflowMetaDerived, "metadata-set-from-source-derived", nil, "")
	cmd.Flags().StringVar(&reflowMetaMissing, "metadata-on-missing-source", metadataMissingSkip, "")
	cmd.Flags().BoolVar(&reflowMetaContent, "preserve-content-type", false, "")
	cmd.Flags().StringVar(&reflowMetaStorage, "destination-storage-class", "", "")
	cmd.Flags().StringVar(&reflowMetaSuffix, "metadata-sidecar-suffix", providerfile.DefaultMetadataSidecarSuffix, "")
	cmd.Flags().StringVar(&reflowSymlinks, "symlinks", reflowSymlinkSkip, "")
	cmd.Flags().StringVar(&reflowHidden, "hidden", reflowHiddenSkip, "")
	cmd.Flags().StringArrayVar(&reflowExcludes, "exclude", nil, "")
	cmd.Flags().BoolVar(&reflowPreserve, "preserve-mode", false, "")
	cmd.Flags().StringVar(&reflowSrcFailure, "on-source-failure", reflowSourceFailSkip, "")
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
	oldResumeRun := reflowResumeRun
	oldCheckpoint := reflowCheckpoint
	oldOverwrite := reflowOverwrite
	oldOnCollision := reflowOnCollision
	oldCollQuar := reflowCollQuar
	oldProvenance := reflowProvenance
	oldProvRoot := reflowProvRoot
	oldProvSuffix := reflowProvSuffix
	oldProvOnError := reflowProvOnError
	oldProvUnsafe := reflowProvUnsafe
	oldMetaPolicy := reflowMetaPolicy
	oldMetaSets := reflowMetaSets
	oldMetaSrcKeys := reflowMetaSrcKeys
	oldMetaDerived := reflowMetaDerived
	oldMetaMissing := reflowMetaMissing
	oldMetaContent := reflowMetaContent
	oldMetaStorage := reflowMetaStorage
	oldMetaSuffix := reflowMetaSuffix
	oldSymlinks := reflowSymlinks
	oldHidden := reflowHidden
	oldExcludes := reflowExcludes
	oldPreserve := reflowPreserve
	oldSrcFailure := reflowSrcFailure
	oldSrcRegion := reflowSrcRegion
	oldSrcProfile := reflowSrcProfile
	oldSrcEndpoint := reflowSrcEndpoint
	oldDstRegion := reflowDstRegion
	oldDstProfile := reflowDstProfile
	oldDstEndpoint := reflowDstEndpoint
	oldS3Provider := newReflowS3Provider
	oldFileProvider := newReflowFileProvider
	oldStateStore := newReflowStateStore

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
	reflowResumeRun = ""
	reflowCheckpoint = ""
	reflowOverwrite = false
	reflowOnCollision = reflowCollisionSkip
	reflowCollQuar = ""
	reflowProvenance = provenanceModeNone
	reflowProvRoot = ""
	reflowProvSuffix = provenanceSuffix
	reflowProvOnError = provenanceErrorWarn
	reflowProvUnsafe = false
	reflowMetaPolicy = metadataPolicyClear
	reflowMetaSets = nil
	reflowMetaSrcKeys = nil
	reflowMetaDerived = nil
	reflowMetaMissing = metadataMissingSkip
	reflowMetaContent = false
	reflowMetaStorage = ""
	reflowMetaSuffix = providerfile.DefaultMetadataSidecarSuffix
	reflowSymlinks = reflowSymlinkSkip
	reflowHidden = reflowHiddenSkip
	reflowExcludes = nil
	reflowPreserve = false
	reflowSrcFailure = reflowSourceFailSkip
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
		reflowResumeRun = oldResumeRun
		reflowCheckpoint = oldCheckpoint
		reflowOverwrite = oldOverwrite
		reflowOnCollision = oldOnCollision
		reflowCollQuar = oldCollQuar
		reflowProvenance = oldProvenance
		reflowProvRoot = oldProvRoot
		reflowProvSuffix = oldProvSuffix
		reflowProvOnError = oldProvOnError
		reflowProvUnsafe = oldProvUnsafe
		reflowMetaPolicy = oldMetaPolicy
		reflowMetaSets = oldMetaSets
		reflowMetaSrcKeys = oldMetaSrcKeys
		reflowMetaDerived = oldMetaDerived
		reflowMetaMissing = oldMetaMissing
		reflowMetaContent = oldMetaContent
		reflowMetaStorage = oldMetaStorage
		reflowMetaSuffix = oldMetaSuffix
		reflowSymlinks = oldSymlinks
		reflowHidden = oldHidden
		reflowExcludes = oldExcludes
		reflowPreserve = oldPreserve
		reflowSrcFailure = oldSrcFailure
		reflowSrcRegion = oldSrcRegion
		reflowSrcProfile = oldSrcProfile
		reflowSrcEndpoint = oldSrcEndpoint
		reflowDstRegion = oldDstRegion
		reflowDstProfile = oldDstProfile
		reflowDstEndpoint = oldDstEndpoint
		newReflowS3Provider = oldS3Provider
		newReflowFileProvider = oldFileProvider
		newReflowStateStore = oldStateStore
	})
}

func TestEnqueueReflowLine_ReflowInputRecord(t *testing.T) {
	out := make(chan reflowTask, 1)
	var providerBuckets []string
	getProviders := func(srcURI *uri.ObjectURI) (provider.Provider, provider.Provider, error) {
		providerBuckets = append(providerBuckets, srcURI.Bucket)
		return &mockProvider{}, &mockProvider{}, nil
	}

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://bucket/source/file.xml","source_key":"source/file.xml","source_etag":"abc123","source_size_bytes":42,"source_last_modified":"2026-01-15T20:53:44Z","vars":{"site":"001"},"probe":{"extractors":[{"name":"site","type":"regex","resolved":true,"required":true,"bytes_at_resolution":128}],"bytes_read":128,"termination_reason":"all_required_resolved"},"dest_rel_key":"dest/file.xml"}}`
	srcBucket, err := enqueueReflowLine(context.Background(), line, "", reflowSourceConfig{Symlinks: reflowSymlinkSkip, OnSourceFailure: reflowSourceFailSkip}, getProviders, out)
	require.NoError(t, err)
	require.Equal(t, "s3:bucket", srcBucket)
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

func TestEnqueueReflowLine_IndexObjectRecordPreservesLastModified(t *testing.T) {
	out := make(chan reflowTask, 1)
	var providerBuckets []string
	getProviders := func(srcURI *uri.ObjectURI) (provider.Provider, provider.Provider, error) {
		providerBuckets = append(providerBuckets, srcURI.Bucket)
		return &mockProvider{}, &mockProvider{}, nil
	}

	srcBucket, err := enqueueReflowLine(context.Background(), reflowIndexObjectInputLine("source/file.xml", "abc123", 42, "2026-01-15T20:53:44Z"), "", reflowSourceConfig{Symlinks: reflowSymlinkSkip, OnSourceFailure: reflowSourceFailSkip}, getProviders, out)
	require.NoError(t, err)
	require.Equal(t, "s3:source-bucket", srcBucket)
	require.Equal(t, []string{"source-bucket"}, providerBuckets)

	task := <-out
	require.Equal(t, "source-bucket", task.SourceBucket)
	require.Equal(t, "s3://source-bucket/source/file.xml", task.SourceURI)
	require.Equal(t, "source/file.xml", task.SourceKey)
	require.Equal(t, "abc123", task.SourceETag)
	require.Equal(t, int64(42), task.SourceSize)
	require.Equal(t, time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC), task.SourceLastMod)
}

func TestEnqueueReflowLine_ReflowInputRecordQuarantine(t *testing.T) {
	out := make(chan reflowTask, 1)
	var providerBuckets []string
	getProviders := func(srcURI *uri.ObjectURI) (provider.Provider, provider.Provider, error) {
		providerBuckets = append(providerBuckets, srcURI.Bucket)
		return &mockProvider{}, &mockProvider{}, nil
	}

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://bucket/source/file.xml","source_key":"source/file.xml","routing_class":"quarantine","quarantine_prefix":"_unresolved/","vars":{"date":"_unresolved"}}}`
	srcBucket, err := enqueueReflowLine(context.Background(), line, "", reflowSourceConfig{Symlinks: reflowSymlinkSkip, OnSourceFailure: reflowSourceFailSkip}, getProviders, out)
	require.NoError(t, err)
	require.Equal(t, "s3:bucket", srcBucket)
	require.Equal(t, []string{"bucket"}, providerBuckets)

	task := <-out
	require.Equal(t, "quarantine", task.RoutingClass)
	require.Equal(t, "_unresolved", task.QuarantinePrefix)
	require.Equal(t, "source/file.xml", task.SourceKey)
	require.Equal(t, "_unresolved/source/file.xml", buildQuarantineDestRel(task.QuarantinePrefix, task.SourceKey))
}

func TestEnqueueReflowLine_ReflowInputRecordQuarantineRequiresPrefix(t *testing.T) {
	out := make(chan reflowTask, 1)
	getProviders := func(srcURI *uri.ObjectURI) (provider.Provider, provider.Provider, error) {
		return &mockProvider{}, &mockProvider{}, nil
	}

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://bucket/source/file.xml","source_key":"source/file.xml","routing_class":"quarantine"}}`
	_, err := enqueueReflowLine(context.Background(), line, "", reflowSourceConfig{Symlinks: reflowSymlinkSkip, OnSourceFailure: reflowSourceFailSkip}, getProviders, out)

	require.Error(t, err)
	require.Contains(t, err.Error(), "quarantine_prefix is required")
	require.Empty(t, out)
}

func TestEnqueueReflowLine_ReflowInputRecordQuarantineRejectsAbsolutePrefix(t *testing.T) {
	out := make(chan reflowTask, 1)
	getProviders := func(srcURI *uri.ObjectURI) (provider.Provider, provider.Provider, error) {
		return &mockProvider{}, &mockProvider{}, nil
	}

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://bucket/source/file.xml","source_key":"source/file.xml","routing_class":"quarantine","quarantine_prefix":"s3://other/prefix"}}`
	_, err := enqueueReflowLine(context.Background(), line, "", reflowSourceConfig{Symlinks: reflowSymlinkSkip, OnSourceFailure: reflowSourceFailSkip}, getProviders, out)

	require.Error(t, err)
	require.Contains(t, err.Error(), "quarantine_prefix must be a relative destination prefix")
	require.Empty(t, out)
}

func TestEnqueueReflowLine_ReflowInputRecordRejectsPrefixURI(t *testing.T) {
	out := make(chan reflowTask, 1)
	getProviders := func(srcURI *uri.ObjectURI) (provider.Provider, provider.Provider, error) {
		t.Fatalf("getProviders should not be called for invalid exact-object input")
		return nil, nil, nil
	}

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://bucket/source/","source_key":"source/"}}`
	_, err := enqueueReflowLine(context.Background(), line, "", reflowSourceConfig{Symlinks: reflowSymlinkSkip, OnSourceFailure: reflowSourceFailSkip}, getProviders, out)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be an exact object URI")
	require.Empty(t, out)
}

func TestResolveProvenanceConfig(t *testing.T) {
	withTransferReflowTestState(t)

	cmd := newTransferReflowTestCommand()
	dest, err := parseReflowDest("file://" + t.TempDir() + "/")
	require.NoError(t, err)
	cfg, err := resolveProvenanceConfig(cmd, dest)
	require.NoError(t, err)
	require.Equal(t, provenanceModeNone, cfg.Mode)

	require.NoError(t, cmd.Flags().Parse([]string{"--provenance", "sidecar", "--provenance-suffix", ".audit.json", "--provenance-on-write-error", "fail"}))
	cfg, err = resolveProvenanceConfig(cmd, dest)
	require.NoError(t, err)
	require.Equal(t, provenanceModeSidecar, cfg.Mode)
	require.Equal(t, provenancePlaceSibling, cfg.PlacementMode)
	require.Equal(t, ".audit.json", cfg.Suffix)
	require.Equal(t, provenanceErrorFail, cfg.OnWriteError)
	require.False(t, cfg.AllowUnsafeSuffix)
}

func TestParseReflowDestRejectsOutputOnlyFileLocalURI(t *testing.T) {
	_, err := parseReflowDest("file://local/path")
	require.Error(t, err)
	require.ErrorIs(t, err, uri.ErrInvalidFileURI)
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

	err = validateProvenanceConfig(provenanceConfig{Mode: provenanceModeNone, SidecarRootRaw: "s3://b/sidecars/"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires --provenance sidecar")
}

func TestParseProvenanceSidecarRootValidation(t *testing.T) {
	s3Dest, err := parseReflowDest("s3://b1/data/")
	require.NoError(t, err)
	fileDest, err := parseReflowDest("file://" + t.TempDir() + "/")
	require.NoError(t, err)

	tests := []struct {
		name    string
		raw     string
		dest    *reflowDestSpec
		wantErr string
	}{
		{name: "missing trailing slash", raw: "s3://b1/sidecars", dest: s3Dest, wantErr: "must end in '/'"},
		{name: "different provider", raw: "file:///tmp/sidecars/", dest: s3Dest, wantErr: "different-provider-scheme"},
		{name: "different bucket", raw: "s3://b2/sidecars/", dest: s3Dest, wantErr: "different-bucket"},
		{name: "same bucket", raw: "s3://b1/sidecars/", dest: s3Dest},
		{name: "file same scheme", raw: "file://" + t.TempDir() + "/", dest: fileDest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseProvenanceSidecarRoot(tt.raw, tt.dest)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, got)
		})
	}
}

func TestEmitProvenancePlacementWarningsForNestedRoots(t *testing.T) {
	dest, err := parseReflowDest("s3://b/data/")
	require.NoError(t, err)
	sidecar, err := parseReflowDest("s3://b/data/sidecars/")
	require.NoError(t, err)
	var stderr bytes.Buffer

	emitProvenancePlacementWarnings(&stderr, dest, provenanceConfig{
		Mode:          provenanceModeSidecar,
		PlacementMode: provenancePlaceMirror,
		SidecarRoot:   sidecar,
	})

	require.Contains(t, stderr.String(), "sidecar root is a descendant of dest root")
}

func TestWriteProvenanceSidecarWritesJSON(t *testing.T) {
	withTransferReflowTestState(t)
	SetVersionInfo("0.2.0-test", "abc123", "2026-05-16T12:00:00Z")

	destDir := t.TempDir()
	dst, err := providerfile.New(providerfile.Config{BaseDir: destDir})
	require.NoError(t, err)
	destSpec, err := parseReflowDest(fileURI(destDir) + "/")
	require.NoError(t, err)

	resolvedAt := int64(128)
	var stdout bytes.Buffer
	w := output.NewJSONLWriter(&stdout, "job-123", "file")
	defer func() { _ = w.Close() }()

	srcLastMod := time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC)
	ref, fatal := writeProvenanceSidecar(context.Background(), w, dst, provenanceConfig{Mode: provenanceModeSidecar, Suffix: provenanceSuffix, OnWriteError: provenanceErrorWarn, PlacementMode: provenancePlaceSibling}, destSpec, reflowTask{
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
	}, "landing/file.xml", "landing/file.xml", "file://"+filepath.Join(destDir, "landing/file.xml"), &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: "landing/file.xml", ETag: "dest-etag", Size: 42}}, "{site}/{file}", "landed", "job-123", nil)

	require.False(t, fatal)
	require.NotNil(t, ref)
	require.True(t, ref.Written)
	require.Equal(t, "landing/file.xml.gnb.json", ref.Key)
	require.Equal(t, fileURI(filepath.Join(destDir, "landing", "file.xml.gnb.json")), ref.URI)

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

func TestWriteProvenanceSidecarMirroredRootFileWritesOutsideDestRoot(t *testing.T) {
	destDir := t.TempDir()
	sidecarDir := t.TempDir()
	sidecarDst, err := providerfile.New(providerfile.Config{BaseDir: sidecarDir})
	require.NoError(t, err)
	destSpec, err := parseReflowDest(fileURI(destDir) + "/")
	require.NoError(t, err)
	sidecarRoot, err := parseReflowDest(fileURI(sidecarDir) + "/")
	require.NoError(t, err)

	var stdout bytes.Buffer
	w := output.NewJSONLWriter(&stdout, "job-123", "file")
	defer func() { _ = w.Close() }()

	ref, fatal := writeProvenanceSidecar(context.Background(), w, sidecarDst, provenanceConfig{Mode: provenanceModeSidecar, Suffix: provenanceSuffix, OnWriteError: provenanceErrorWarn, PlacementMode: provenancePlaceMirror, SidecarRootRaw: fileURI(sidecarDir) + "/", SidecarRoot: sidecarRoot}, destSpec, reflowTask{SourceURI: "s3://source/key", SourceKey: "source/file.xml"}, "tenant/a/file.xml", "data/tenant/a/file.xml", fileURI(filepath.Join(destDir, "data", "tenant", "a", "file.xml")), nil, "{tenant}/{file}", "landed", "job-123", nil)

	require.False(t, fatal)
	require.Equal(t, "tenant/a/file.xml.gnb.json", ref.Key)
	require.Equal(t, fileURI(filepath.Join(sidecarDir, "tenant", "a", "file.xml.gnb.json")), ref.URI)
	require.FileExists(t, filepath.Join(sidecarDir, "tenant", "a", "file.xml.gnb.json"))
	require.NoFileExists(t, filepath.Join(destDir, "data", "tenant", "a", "file.xml.gnb.json"))
}

func TestWriteProvenanceSidecarWarnsOnFailure(t *testing.T) {
	var stdout bytes.Buffer
	w := output.NewJSONLWriter(&stdout, "job-123", "file")
	defer func() { _ = w.Close() }()

	destSpec, err := parseReflowDest("s3://bucket/dest/")
	require.NoError(t, err)
	ref, fatal := writeProvenanceSidecar(context.Background(), w, failingPutter{err: errors.New("boom")}, provenanceConfig{Mode: provenanceModeSidecar, Suffix: provenanceSuffix, OnWriteError: provenanceErrorWarn, PlacementMode: provenancePlaceSibling}, destSpec, reflowTask{SourceURI: "s3://source/key", SourceKey: "key"}, "dest/key", "dest/key", "s3://bucket/dest/key", nil, "{key}", "landed", "job-123", nil)

	require.False(t, fatal)
	require.Equal(t, &provenanceRef{Written: false, Key: "dest/key.gnb.json", URI: "s3://bucket/dest/key.gnb.json"}, ref)
	require.Contains(t, stdout.String(), reflowWarningRecord)
	require.Contains(t, stdout.String(), "PROVENANCE_WRITE_FAILED")
}

func TestWriteProvenanceSidecarFailsOnFailure(t *testing.T) {
	var stdout bytes.Buffer
	w := output.NewJSONLWriter(&stdout, "job-123", "file")
	defer func() { _ = w.Close() }()

	destSpec, err := parseReflowDest("s3://bucket/dest/")
	require.NoError(t, err)
	ref, fatal := writeProvenanceSidecar(context.Background(), w, failingPutter{err: errors.New("boom")}, provenanceConfig{Mode: provenanceModeSidecar, Suffix: provenanceSuffix, OnWriteError: provenanceErrorFail, PlacementMode: provenancePlaceSibling}, destSpec, reflowTask{SourceURI: "s3://source/key", SourceKey: "key"}, "dest/key", "dest/key", "s3://bucket/dest/key", nil, "{key}", "landed", "job-123", nil)

	require.True(t, fatal)
	require.Equal(t, &provenanceRef{Written: false, Key: "dest/key.gnb.json", URI: "s3://bucket/dest/key.gnb.json"}, ref)
	require.Contains(t, stdout.String(), output.TypeError)
	require.Contains(t, stdout.String(), "provenance sidecar write failed")
}

type failingPutter struct {
	err error
}

type chunkedObjectProvider struct {
	body      []byte
	chunkSize int
}

func (p chunkedObjectProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}

func (p chunkedObjectProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Size: int64(len(p.body))}}, nil
}

func (p chunkedObjectProvider) GetObject(context.Context, string) (io.ReadCloser, int64, error) {
	return io.NopCloser(&chunkedReader{data: p.body, chunkSize: p.chunkSize}), int64(len(p.body)), nil
}

func (p chunkedObjectProvider) Close() error {
	return nil
}

type chunkedReader struct {
	data      []byte
	offset    int
	chunkSize int
}

func (r *chunkedReader) Read(buf []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := len(buf)
	if r.chunkSize > 0 && r.chunkSize < n {
		n = r.chunkSize
	}
	if remaining := len(r.data) - r.offset; remaining < n {
		n = remaining
	}
	copy(buf[:n], r.data[r.offset:r.offset+n])
	r.offset += n
	return n, nil
}

type markFailingReflowState struct {
	reflowStateStore
	err error
}

func (s markFailingReflowState) MarkDestKeyObserved(context.Context, string) error {
	return s.err
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
	stdout, _, err := runTransferReflowWithProvidersAndErr(t, src, dst, input, extraArgs...)
	return stdout, err
}

func runTransferReflowWithProvidersAndErr(t *testing.T, src *reflowMemoryProvider, dst *reflowMemoryProvider, input string, extraArgs ...string) (string, string, error) {
	t.Helper()
	withTransferReflowTestState(t)

	newReflowS3Provider = func(context.Context, s3.Config) (provider.Provider, error) {
		return src, nil
	}
	newReflowFileProvider = func(providerfile.Config) (provider.Provider, error) {
		return dst, nil
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader(input + "\n"))
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
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
	return stdout.String(), stderr.String(), err
}

func runTransferReflowWithProviderFactory(t *testing.T, src *reflowMemoryProvider, dst *reflowMemoryProvider, input string, args ...string) (string, string, error) {
	t.Helper()
	withTransferReflowTestState(t)

	newReflowS3Provider = func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
		if cfg.Bucket == "source-bucket" {
			return src, nil
		}
		return dst, nil
	}
	newReflowFileProvider = func(providerfile.Config) (provider.Provider, error) {
		return dst, nil
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader(input + "\n"))
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return stdout.String(), stderr.String(), err
}

func runTransferReflowWithMemorySourceAndRealFileDest(t *testing.T, src *reflowMemoryProvider, destDir string, input string, extraArgs ...string) (string, string, error) {
	t.Helper()
	withTransferReflowTestState(t)

	newReflowS3Provider = func(context.Context, s3.Config) (provider.Provider, error) {
		return src, nil
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader(input + "\n"))
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	args := []string{
		"--stdin",
		"--dest", fileURI(destDir),
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--parallel", "1",
	}
	args = append(args, extraArgs...)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return stdout.String(), stderr.String(), err
}

func reflowInputLine(key string, etag string, size int64, routingClass string, quarantinePrefix string) string {
	return reflowInputLineWithDestRel(key, key, etag, size, routingClass, quarantinePrefix)
}

func reflowInputLineWithDestRel(key string, destRelKey string, etag string, size int64, routingClass string, quarantinePrefix string) string {
	data := map[string]any{
		"source_uri":           "s3://source-bucket/" + key,
		"source_key":           key,
		"source_etag":          etag,
		"source_size_bytes":    size,
		"dest_rel_key":         destRelKey,
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

type raceSource struct {
	key  string
	body string
	etag string
}

func makeRaceInput(src *reflowMemoryProvider, destRel string, sources []raceSource) string {
	lines := make([]string, 0, len(sources))
	for _, source := range sources {
		src.putFixture(source.key, source.body, source.etag, time.Time{})
		lines = append(lines, reflowInputLineWithDestRel(source.key, destRel, source.etag, int64(len(source.body)), "", ""))
	}
	return strings.Join(lines, "\n")
}

func reflowInputLineNoDestRel(key string, etag string, size int64) string {
	data := map[string]any{
		"source_uri":           "s3://source-bucket/" + key,
		"source_key":           key,
		"source_etag":          etag,
		"source_size_bytes":    size,
		"source_last_modified": "2026-01-15T20:53:44Z",
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
	line, err := json.Marshal(map[string]any{"type": "gonimbus.reflow.input.v1", "data": data})
	if err != nil {
		panic(err)
	}
	return string(line)
}

func reflowInputLineWithoutLastModified(key string, etag string, size int64) string {
	data := map[string]any{
		"source_uri":        "s3://source-bucket/" + key,
		"source_key":        key,
		"source_etag":       etag,
		"source_size_bytes": size,
		"dest_rel_key":      key,
		"vars": map[string]string{
			"key": key,
		},
	}
	line, err := json.Marshal(map[string]any{"type": "gonimbus.reflow.input.v1", "data": data})
	if err != nil {
		panic(err)
	}
	return string(line)
}

func reflowIndexObjectInputLine(key string, etag string, size int64, lastModified string) string {
	data := map[string]any{
		"base_uri":      "s3://source-bucket/",
		"key":           key,
		"etag":          etag,
		"size_bytes":    size,
		"last_modified": lastModified,
	}
	line, err := json.Marshal(map[string]any{"type": "gonimbus.index.object.v1", "data": data})
	if err != nil {
		panic(err)
	}
	return string(line)
}

type testRecordEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type testReflowData struct {
	SourceURI    string         `json:"source_uri"`
	SourceRoot   string         `json:"source_root"`
	SourceKey    string         `json:"source_key"`
	SourceETag   string         `json:"source_etag"`
	SourceSize   int64          `json:"source_size_bytes"`
	DestURI      string         `json:"dest_uri"`
	DestKey      string         `json:"dest_key"`
	Bytes        int64          `json:"bytes"`
	Status       string         `json:"status"`
	Reason       string         `json:"reason"`
	RoutingClass string         `json:"routing_class"`
	Collision    *collisionInfo `json:"collision"`
	Provenance   *provenanceRef `json:"provenance"`
}

type testErrorData struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Key       string         `json:"key"`
	Details   map[string]any `json:"details"`
	Collision *collisionInfo `json:"collision"`
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

func requireReflowData(t *testing.T, stdout string, status string) testReflowData {
	t.Helper()
	record := requireRecord(t, stdout, reflowRecordType, status)
	var data testReflowData
	require.NoError(t, json.Unmarshal(record.Data, &data))
	return data
}

func requireReflowRecords(t *testing.T, stdout string) []testReflowData {
	t.Helper()
	var out []testReflowData
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record testRecordEnvelope
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if record.Type != reflowRecordType {
			continue
		}
		var data testReflowData
		require.NoError(t, json.Unmarshal(record.Data, &data))
		out = append(out, data)
	}
	require.NotEmpty(t, out)
	return out
}

func requireReflowStatusReasonCount(t *testing.T, records []testReflowData, status string, reason string, want int) {
	t.Helper()
	got := 0
	for _, rec := range records {
		if rec.Status == status && rec.Reason == reason {
			got++
		}
	}
	require.Equal(t, want, got, "status=%s reason=%s records=%+v", status, reason, records)
}

func requireNoRecordType(t *testing.T, stdout string, recordType string) {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record testRecordEnvelope
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		require.NotEqual(t, recordType, record.Type)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path) // #nosec G304 -- tests read paths created under t.TempDir().
	require.NoError(t, err)
	return data
}

func readCheckpointErrorMessage(t *testing.T, path string) string {
	t.Helper()
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: path})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	var msg string
	require.NoError(t, db.QueryRowContext(context.Background(), `SELECT error_message FROM reflow_items WHERE status = 'failed' LIMIT 1`).Scan(&msg))
	return msg
}

type testCheckpointItem struct {
	Status       string
	Reason       string
	ErrorMessage string
}

func readCheckpointItem(t *testing.T, path string) testCheckpointItem {
	t.Helper()
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: path})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	var item testCheckpointItem
	require.NoError(t, db.QueryRowContext(context.Background(), `SELECT status, COALESCE(reason, ''), COALESCE(error_message, '') FROM reflow_items LIMIT 1`).Scan(&item.Status, &item.Reason, &item.ErrorMessage))
	return item
}

func requireErrorData(t *testing.T, stdout string) testErrorData {
	t.Helper()
	record := requireRecord(t, stdout, output.TypeError, "")
	var data testErrorData
	require.NoError(t, json.Unmarshal(record.Data, &data))
	return data
}

func requireCollisionEqual(t *testing.T, data testReflowData, kind string, decisionPath string, etag string, size int64) {
	t.Helper()
	require.NotNil(t, data.Collision)
	require.Equal(t, kind, data.Collision.Kind)
	require.Equal(t, decisionPath, data.Collision.DecisionPath)
	require.Equal(t, etag, data.Collision.DestETagObserved)
	require.NotNil(t, data.Collision.DestSizeObserved)
	require.Equal(t, size, *data.Collision.DestSizeObserved)
}

func requireSourceNewerCollisionEqual(t *testing.T, data testReflowData, reason string, srcLastModified string, destLastModified string) {
	t.Helper()
	require.NotNil(t, data.Collision)
	require.Equal(t, reason, data.Collision.DecisionReason)
	require.NotNil(t, data.Collision.SrcLastModified)
	require.Equal(t, srcLastModified, data.Collision.SrcLastModified.Format(time.RFC3339))
	require.NotNil(t, data.Collision.DestLastModifiedObserved)
	require.Equal(t, destLastModified, data.Collision.DestLastModifiedObserved.Format(time.RFC3339))
}

func requireNoLegacyCollisionKeys(t *testing.T, data json.RawMessage) {
	t.Helper()
	raw := string(data)
	for _, key := range []string{
		"collision_" + "kind",
		"collision_" + "etag",
		"collision_" + "size_bytes",
	} {
		require.NotContains(t, raw, `"`+key+`"`)
	}
}

func readSidecar(t *testing.T, p *reflowMemoryProvider, key string) map[string]any {
	t.Helper()
	raw := p.mustObject(key)
	var out map[string]any
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

type reflowMemoryProvider struct {
	mu                  sync.Mutex
	objects             map[string][]byte
	meta                map[string]provider.ObjectMeta
	headCalls           []string
	putCalls            []string
	conditionalPutCalls []string
	conditionalPreconds []provider.PutPrecondition
	ifAbsentErr         error
	ignoreIfAbsent      bool
	mutateBeforeIfMatch bool
	failSidecars        bool
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

func (p *reflowMemoryProvider) putFixtureWithMeta(key string, body string, etag string, lastModified time.Time, meta provider.ObjectMeta) {
	p.mu.Lock()
	defer p.mu.Unlock()
	meta.ObjectSummary = provider.ObjectSummary{Key: key, Size: int64(len(body)), ETag: etag, LastModified: lastModified}
	p.objects[key] = []byte(body)
	p.meta[key] = meta
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

func (p *reflowMemoryProvider) conditionalPutCallsSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.conditionalPutCalls...)
}

func (p *reflowMemoryProvider) conditionalPutPreconditionsSnapshot() []provider.PutPrecondition {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]provider.PutPrecondition(nil), p.conditionalPreconds...)
}

func (p *reflowMemoryProvider) metaSnapshot(key string) provider.ObjectMeta {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.meta[key]
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

func (p *reflowMemoryProvider) PutObjectWithOptions(ctx context.Context, key string, body io.Reader, contentLength int64, opts provider.PutOptions) error {
	if err := p.PutObject(ctx, key, body, contentLength); err != nil {
		return err
	}
	p.applyPutOptions(key, opts)
	return nil
}

func (p *reflowMemoryProvider) PutObjectConditional(_ context.Context, key string, body io.Reader, contentLength int64, precond provider.PutPrecondition) (provider.PutResult, error) {
	if err := precond.Validate(); err != nil {
		return provider.PutResult{}, err
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return provider.PutResult{}, err
	}
	if contentLength >= 0 && int64(len(data)) != contentLength {
		return provider.PutResult{}, fmt.Errorf("content length mismatch")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conditionalPutCalls = append(p.conditionalPutCalls, key)
	p.conditionalPreconds = append(p.conditionalPreconds, precond)
	if precond.IfAbsent {
		if _, ok := p.objects[key]; ok && !p.ignoreIfAbsent {
			err := p.ifAbsentErr
			if err == nil {
				err = provider.ErrAlreadyExists
			}
			return provider.PutResult{}, &provider.ProviderError{Op: "PutObjectConditional", Provider: provider.ProviderFile, Key: key, Err: err}
		}
		etag := "dest-" + key
		p.objects[key] = data
		p.meta[key] = provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key, Size: int64(len(data)), ETag: etag}}
		return provider.PutResult{ETag: etag}, nil
	}
	if precond.IfMatchETag != nil {
		if p.mutateBeforeIfMatch {
			p.objects[key] = []byte("concurrent mutation")
			p.meta[key] = provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{
				Key:          key,
				Size:         int64(len("concurrent mutation")),
				ETag:         "mutated-" + key,
				LastModified: time.Date(2026, 1, 16, 20, 53, 44, 0, time.UTC),
			}}
			p.mutateBeforeIfMatch = false
		}
		meta, ok := p.meta[key]
		if !ok || meta.ETag != *precond.IfMatchETag {
			return provider.PutResult{}, &provider.ProviderError{Op: "PutObjectConditional", Provider: provider.ProviderFile, Key: key, Err: provider.ErrPreconditionFailed}
		}
		etag := "dest-" + key
		p.objects[key] = data
		p.meta[key] = provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key, Size: int64(len(data)), ETag: etag, LastModified: time.Now().UTC()}}
		return provider.PutResult{ETag: etag}, nil
	}
	return provider.PutResult{}, errors.New("unsupported test precondition")
}

func (p *reflowMemoryProvider) PutObjectConditionalWithOptions(ctx context.Context, key string, body io.Reader, contentLength int64, precond provider.PutPrecondition, opts provider.PutOptions) (provider.PutResult, error) {
	result, err := p.PutObjectConditional(ctx, key, body, contentLength, precond)
	if err != nil {
		return provider.PutResult{}, err
	}
	p.applyPutOptions(key, opts)
	return result, nil
}

func (p *reflowMemoryProvider) applyPutOptions(key string, opts provider.PutOptions) {
	p.mu.Lock()
	defer p.mu.Unlock()
	meta := p.meta[key]
	meta.Metadata = cloneMetadataMap(opts.UserMetadata)
	meta.ContentType = opts.ContentType
	meta.StorageClass = opts.StorageClass
	p.meta[key] = meta
}

func (p *reflowMemoryProvider) Close() error {
	return nil
}

type reflowBareProvider struct {
	p *reflowMemoryProvider
}

func (p *reflowBareProvider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	return p.p.List(ctx, opts)
}

func (p *reflowBareProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	return p.p.Head(ctx, key)
}

func (p *reflowBareProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	return p.p.GetObject(ctx, key)
}

func (p *reflowBareProvider) PutObject(ctx context.Context, key string, body io.Reader, contentLength int64) error {
	return p.p.PutObject(ctx, key, body, contentLength)
}

func (p *reflowBareProvider) PutObjectConditional(ctx context.Context, key string, body io.Reader, contentLength int64, precond provider.PutPrecondition) (provider.PutResult, error) {
	return p.p.PutObjectConditional(ctx, key, body, contentLength, precond)
}

func (p *reflowBareProvider) Close() error {
	return nil
}

type reflowNoConditionalProvider struct {
	p *reflowMemoryProvider
}

func (p *reflowNoConditionalProvider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	return p.p.List(ctx, opts)
}

func (p *reflowNoConditionalProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	return p.p.Head(ctx, key)
}

func (p *reflowNoConditionalProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	return p.p.GetObject(ctx, key)
}

func (p *reflowNoConditionalProvider) PutObject(ctx context.Context, key string, body io.Reader, contentLength int64) error {
	return p.p.PutObject(ctx, key, body, contentLength)
}

func (p *reflowNoConditionalProvider) Close() error {
	return nil
}
