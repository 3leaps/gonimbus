package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/provider"
	providers3 "github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/fulmenhq/gofulmen/schema"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

type bridgeCommandReader struct{}

func (bridgeCommandReader) OpenExact(context.Context, string) (io.ReadCloser, int64, error) {
	return nil, 0, indexreader.ErrHubExactRead
}

type bridgeCommandPublisher struct {
	bridgeCommandReader
}

func (bridgeCommandPublisher) CreateExact(context.Context, string, io.Reader, int64) (bool, error) {
	return false, errors.New("not used")
}

type bridgeErrorCloser struct{}

func (bridgeErrorCloser) Close() error { return errors.New("injected close failure") }

func TestIndexHubBridgeDurableEmitsOneTerminalSchemaValidReceipt(t *testing.T) {
	restoreBridgeCommandGlobals(t)
	t.Setenv("GONIMBUS_READONLY", "false")
	viper.Set("readonly", false)

	identity := filepath.Join(t.TempDir(), "identity.json")
	require.NoError(t, os.WriteFile(identity, []byte("{}\n"), 0o600))
	sourceReader := bridgeCommandReader{}
	targetPublisher := bridgeCommandPublisher{}
	configuredBridgeHubReadHandleResolver = func(_ context.Context, name string) (bridgeHubReadHandleResolution, error) {
		require.Equal(t, "source-account", name)
		return bridgeHubReadHandleResolution{
			reader: sourceReader,
			rangeID: hubAuthorityRange{
				provider: string(provider.ProviderS3), endpoint: "default",
				container: "source-bucket", prefix: "custody",
			},
		}, nil
	}
	configuredHubPublishHandleResolver = func(_ context.Context, name string) (hubPublishHandleResolution, error) {
		require.Equal(t, "target-account", name)
		return hubPublishHandleResolution{
			publisher: targetPublisher,
			closer:    bridgeErrorCloser{},
			rangeID: hubAuthorityRange{
				provider: string(provider.ProviderS3), endpoint: "default",
				container: "target-bucket", prefix: "bridge",
			},
		}, nil
	}
	runLegacyDurableBridge = func(
		_ context.Context,
		source indexreader.HubExactObjectReader,
		target indexreader.HubConditionalCreatePublisher,
		authority indexreader.CanonicalIdentityAuthority,
		evidence []byte,
		opts indexreader.BridgeOptions,
	) (indexreader.BridgeResult, error) {
		require.Equal(t, sourceReader, source)
		require.Equal(t, targetPublisher, target)
		require.Equal(t, []byte("{}\n"), authority.CanonicalJSONLF)
		require.Empty(t, authority.DeclarationJSON)
		require.Empty(t, evidence)
		require.Equal(t, "idx_"+strings.Repeat("a", 64), opts.IndexSetID)
		require.Equal(t, "run_1", opts.RunID)
		require.NotEmpty(t, opts.ExportedBy)
		return validBridgeCommandResult(), nil
	}
	times := []time.Time{
		time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 10, 12, 3, 0, 0, time.UTC),
	}
	custodyBridgeClock = func() time.Time {
		next := times[0]
		times = times[1:]
		return next
	}

	cmd := newIndexHubBridgeCommandForTest()
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"--source-hub-read-handle", "source-account",
		"--target-hub-publish-handle", "target-account",
		"--index-set", "idx_" + strings.Repeat("a", 64),
		"--run-id", "run_1",
		"--identity-file", identity,
		"--output-format", custodyBridgeOutputFormat,
	})
	require.NoError(t, cmd.Execute())
	require.Equal(t, 1, bytes.Count(stdout.Bytes(), []byte{'\n'}))

	var receipt custodyBridgeReceipt
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &receipt))
	require.Equal(t, custodyBridgeReceiptType, receipt.Type)
	require.Equal(t, "success", receipt.Outcome)
	require.Regexp(t, custodyBridgeHandleRefRE, receipt.SourceHandleRef)
	require.Regexp(t, custodyBridgeHandleRefRE, receipt.TargetHandleRef)
	require.NotEqual(t, receipt.SourceHandleRef, receipt.TargetHandleRef)
	require.NoError(t, validateCustodyBridgeReceipt(receipt))
	require.NotContains(t, stdout.String(), "source-account")
	require.NotContains(t, stdout.String(), "target-account")
	require.NotContains(t, stdout.String(), identity)
	validateBridgeReceiptAgainstSchema(t, stdout.Bytes())
}

func TestIndexHubBridgeDurableBindsDeclarationAndEvidence(t *testing.T) {
	restoreBridgeCommandGlobals(t)
	t.Setenv("GONIMBUS_READONLY", "false")
	viper.Set("readonly", false)
	root := t.TempDir()
	declaration := filepath.Join(root, "declaration.json")
	evidence := filepath.Join(root, "complete.json")
	require.NoError(t, os.WriteFile(declaration, []byte(`{"type":"declaration"}`), 0o600))
	require.NoError(t, os.WriteFile(evidence, []byte(`{"type":"evidence"}`), 0o600))
	configuredBridgeHubReadHandleResolver = func(context.Context, string) (bridgeHubReadHandleResolution, error) {
		return bridgeHubReadHandleResolution{
			reader: bridgeCommandReader{},
			rangeID: hubAuthorityRange{
				provider: string(provider.ProviderS3), endpoint: "default",
				container: "source-bucket", prefix: "archive",
			},
		}, nil
	}
	configuredHubPublishHandleResolver = func(context.Context, string) (hubPublishHandleResolution, error) {
		return hubPublishHandleResolution{
			publisher: bridgeCommandPublisher{},
			rangeID: hubAuthorityRange{
				provider: string(provider.ProviderS3), endpoint: "default",
				container: "target-bucket", prefix: "custody",
			},
		}, nil
	}
	runLegacyDurableBridge = func(
		_ context.Context,
		_ indexreader.HubExactObjectReader,
		_ indexreader.HubConditionalCreatePublisher,
		authority indexreader.CanonicalIdentityAuthority,
		evidenceBytes []byte,
		_ indexreader.BridgeOptions,
	) (indexreader.BridgeResult, error) {
		require.Empty(t, authority.CanonicalJSONLF)
		require.Equal(t, []byte(`{"type":"declaration"}`), authority.DeclarationJSON)
		require.Equal(t, []byte(`{"type":"evidence"}`), evidenceBytes)
		return validBridgeCommandResult(), nil
	}
	custodyBridgeClock = func() time.Time {
		return time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	}

	cmd := newIndexHubBridgeCommandForTest()
	cmd.SetOut(io.Discard)
	cmd.SetArgs([]string{
		"--source-hub-read-handle", "source",
		"--target-hub-publish-handle", "target",
		"--index-set", "idx_" + strings.Repeat("a", 64),
		"--run-id", "run_1",
		"--identity-declaration", declaration,
		"--snapshot-completion-evidence", evidence,
		"--output-format", custodyBridgeOutputFormat,
	})
	require.NoError(t, cmd.Execute())
}

func TestIndexHubBridgeDurableRefusesOverlappingAuthorityBeforeLibrary(t *testing.T) {
	restoreBridgeCommandGlobals(t)
	t.Setenv("GONIMBUS_READONLY", "false")
	viper.Set("readonly", false)
	declaration := filepath.Join(t.TempDir(), "declaration.json")
	require.NoError(t, os.WriteFile(declaration, []byte("{}"), 0o600))

	configuredBridgeHubReadHandleResolver = func(context.Context, string) (bridgeHubReadHandleResolution, error) {
		return bridgeHubReadHandleResolution{
			reader: bridgeCommandReader{},
			rangeID: hubAuthorityRange{
				provider: string(provider.ProviderS3), endpoint: "default",
				container: "shared-bucket", prefix: "hub",
			},
		}, nil
	}
	configuredHubPublishHandleResolver = func(context.Context, string) (hubPublishHandleResolution, error) {
		return hubPublishHandleResolution{
			publisher: bridgeCommandPublisher{},
			rangeID: hubAuthorityRange{
				provider: string(provider.ProviderS3), endpoint: "default",
				container: "shared-bucket", prefix: "hub/child",
			},
		}, nil
	}
	called := false
	runLegacyDurableBridge = func(
		context.Context,
		indexreader.HubExactObjectReader,
		indexreader.HubConditionalCreatePublisher,
		indexreader.CanonicalIdentityAuthority,
		[]byte,
		indexreader.BridgeOptions,
	) (indexreader.BridgeResult, error) {
		called = true
		return indexreader.BridgeResult{}, nil
	}

	cmd := newIndexHubBridgeCommandForTest()
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"--source-hub-read-handle", "source",
		"--target-hub-publish-handle", "target",
		"--index-set", "idx_" + strings.Repeat("a", 64),
		"--run-id", "run_1",
		"--identity-declaration", declaration,
		"--output-format", custodyBridgeOutputFormat,
	})
	err := cmd.Execute()
	require.ErrorContains(t, err, "not provably disjoint")
	require.False(t, called)
	require.Empty(t, stdout.String())
}

func TestIndexHubBridgeDurableRefusesMixedProviderAuthorityBeforeLibrary(t *testing.T) {
	restoreBridgeCommandGlobals(t)
	t.Setenv("GONIMBUS_READONLY", "false")
	viper.Set("readonly", false)
	declaration := filepath.Join(t.TempDir(), "declaration.json")
	require.NoError(t, os.WriteFile(declaration, []byte("{}"), 0o600))

	configuredBridgeHubReadHandleResolver = func(context.Context, string) (bridgeHubReadHandleResolution, error) {
		return bridgeHubReadHandleResolution{
			reader: bridgeCommandReader{},
			rangeID: hubAuthorityRange{
				provider: string(provider.ProviderS3), endpoint: "default",
				container: "source-bucket", prefix: "hub",
			},
		}, nil
	}
	configuredHubPublishHandleResolver = func(context.Context, string) (hubPublishHandleResolution, error) {
		return hubPublishHandleResolution{
			publisher: bridgeCommandPublisher{},
			rangeID: hubAuthorityRange{
				provider: string(provider.ProviderFile), physicalPath: t.TempDir(),
			},
		}, nil
	}
	called := false
	runLegacyDurableBridge = func(
		context.Context,
		indexreader.HubExactObjectReader,
		indexreader.HubConditionalCreatePublisher,
		indexreader.CanonicalIdentityAuthority,
		[]byte,
		indexreader.BridgeOptions,
	) (indexreader.BridgeResult, error) {
		called = true
		return indexreader.BridgeResult{}, nil
	}

	cmd := newIndexHubBridgeCommandForTest()
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"--source-hub-read-handle", "source",
		"--target-hub-publish-handle", "target",
		"--index-set", "idx_" + strings.Repeat("a", 64),
		"--run-id", "run_1",
		"--identity-declaration", declaration,
		"--output-format", custodyBridgeOutputFormat,
	})
	err := cmd.Execute()
	require.ErrorContains(t, err, "not provably disjoint")
	require.False(t, called)
	require.Empty(t, stdout.String())
}

func TestIndexHubBridgeDurableIdentityXORPrecedesHandleResolution(t *testing.T) {
	restoreBridgeCommandGlobals(t)
	t.Setenv("GONIMBUS_READONLY", "false")
	viper.Set("readonly", false)
	resolved := false
	configuredBridgeHubReadHandleResolver = func(context.Context, string) (bridgeHubReadHandleResolution, error) {
		resolved = true
		return bridgeHubReadHandleResolution{}, nil
	}

	for _, identityArgs := range [][]string{
		nil,
		{"--identity-file", "one", "--identity-declaration", "two"},
	} {
		cmd := newIndexHubBridgeCommandForTest()
		cmd.SetArgs(append([]string{
			"--source-hub-read-handle", "source",
			"--target-hub-publish-handle", "target",
			"--index-set", "idx_" + strings.Repeat("a", 64),
			"--run-id", "run_1",
			"--output-format", custodyBridgeOutputFormat,
		}, identityArgs...))
		err := cmd.Execute()
		require.ErrorContains(t, err, "exactly one identity input")
	}
	require.False(t, resolved)
}

func TestSanitizedCustodyBridgeErrorNeverIncludesProviderDetail(t *testing.T) {
	err := sanitizedCustodyBridgeError(errors.New("GET secret-bucket/key using profile private"))
	require.EqualError(t, err, "custody bridge failed")

	err = sanitizedCustodyBridgeError(&indexreader.BridgeError{Code: indexreader.BridgeErrorSourceRead})
	require.EqualError(t, err, "custody bridge failed: bridge_source_read_failed")

	err = sanitizedCustodyBridgeError(context.Canceled)
	require.EqualError(t, err, "custody bridge interrupted")
}

func TestReadBoundedNoFollowLocalFileRefusesSymlinkAndOversize(t *testing.T) {
	root := t.TempDir()
	regular := filepath.Join(root, "identity.json")
	require.NoError(t, os.WriteFile(regular, []byte("{}\n"), 0o600))
	body, err := readBoundedNoFollowLocalFile(regular, 4)
	require.NoError(t, err)
	require.Equal(t, []byte("{}\n"), body)

	leafAlias := filepath.Join(root, "identity-alias.json")
	require.NoError(t, os.Symlink(regular, leafAlias))
	_, err = readBoundedNoFollowLocalFile(leafAlias, 4)
	require.Error(t, err)

	ancestorAlias := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(root, ancestorAlias))
	body, err = readBoundedNoFollowLocalFile(filepath.Join(ancestorAlias, "identity.json"), 4)
	require.NoError(t, err)
	require.Equal(t, []byte("{}\n"), body)

	_, err = readBoundedNoFollowLocalFile(regular, 2)
	require.Error(t, err)
}

func TestProveHubAuthorityRangesDisjoint(t *testing.T) {
	base := hubAuthorityRange{
		provider: string(provider.ProviderS3), endpoint: "default",
		container: "bucket-one", prefix: "root",
	}
	tests := []struct {
		name      string
		other     hubAuthorityRange
		disjoint  bool
		wantError bool
	}{
		{name: "equal", other: base},
		{name: "descendant", other: hubAuthorityRange{
			provider: base.provider, endpoint: base.endpoint,
			container: base.container, prefix: "root/child",
		}},
		{name: "sibling", other: hubAuthorityRange{
			provider: base.provider, endpoint: base.endpoint,
			container: base.container, prefix: "root-two",
		}, disjoint: true},
		{name: "different bucket", other: hubAuthorityRange{
			provider: base.provider, endpoint: base.endpoint,
			container: "bucket-two", prefix: "root",
		}, disjoint: true},
		{name: "default explicit alias unprovable", other: hubAuthorityRange{
			provider: base.provider, endpoint: "https://s3.example.test",
			container: "bucket-two", prefix: "root",
		}, wantError: true},
		{name: "provider mismatch unprovable", other: hubAuthorityRange{
			provider: string(provider.ProviderGCS), endpoint: "default",
			container: "bucket-two", prefix: "root",
		}, wantError: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := proveHubAuthorityRangesDisjoint(base, tc.other)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.disjoint, got)
		})
	}
}

func TestCanonicalHubAuthorityRangeRejectsAmbiguousAliases(t *testing.T) {
	tests := []hubDestSpec{
		{Provider: string(provider.ProviderS3), Bucket: "alias-123.s3-accesspoint.us-east-1.amazonaws.com", Prefix: "hub/"},
		{Provider: string(provider.ProviderS3), Bucket: "valid-bucket", Prefix: "hub//nested/"},
		{Provider: string(provider.ProviderS3), Bucket: "valid-bucket", Prefix: "hub/", Endpoint: "https://user@example.test"},
		{Provider: string(provider.ProviderGCS), Bucket: "valid-bucket", Prefix: "hub/", Endpoint: "https://storage.example.test"},
	}
	for _, hub := range tests {
		_, err := canonicalHubAuthorityRange(&hub)
		require.Error(t, err)
	}

	hub := hubDestSpec{
		Provider: string(provider.ProviderS3), Bucket: "valid-bucket", Prefix: "hub/",
		Endpoint: "https://S3.EXAMPLE.TEST.:443/",
	}
	rangeID, err := canonicalHubAuthorityRange(&hub)
	require.NoError(t, err)
	require.Equal(t, "https://s3.example.test", rangeID.endpoint)
	require.Equal(t, "hub/", hub.Prefix)
}

func TestProveFileHubAuthorityRangesDisjoint(t *testing.T) {
	parent := t.TempDir()
	first := filepath.Join(parent, "first")
	second := filepath.Join(parent, "second")
	child := filepath.Join(first, "child")
	require.NoError(t, os.MkdirAll(child, 0o700))
	require.NoError(t, os.Mkdir(second, 0o700))
	firstRange := mustFileHubRange(t, first)

	disjoint, err := proveHubAuthorityRangesDisjoint(firstRange, mustFileHubRange(t, second))
	require.NoError(t, err)
	require.True(t, disjoint)

	disjoint, err = proveHubAuthorityRangesDisjoint(firstRange, mustFileHubRange(t, child))
	require.NoError(t, err)
	require.False(t, disjoint)
}

func TestHubConditionalProviderAdapterUsesAtomicIfAbsent(t *testing.T) {
	putter := &bridgeProviderFixture{}
	adapter := &hubConditionalProviderAdapter{
		bridgeHubExactProviderAdapter: bridgeHubExactProviderAdapter{
			getter: putter, prefix: "target/",
		},
		putter: putter,
	}
	created, err := adapter.CreateExact(context.Background(), "object.json", strings.NewReader("{}"), 2)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, "target/object.json", putter.putKey)
	require.True(t, putter.precondition.IfAbsent)

	putter.putErr = &provider.ProviderError{
		Op: "PutObjectConditional", Provider: provider.ProviderS3,
		Err: provider.ErrAlreadyExists,
	}
	created, err = adapter.CreateExact(context.Background(), "object.json", strings.NewReader("{}"), 2)
	require.NoError(t, err)
	require.False(t, created)
}

func TestResolveConfiguredHubPublishHandleRequiresExactReadAndDeclaredIfAbsent(t *testing.T) {
	p := &bridgeProviderFixture{}
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(_ context.Context, cfg providers3.Config) (provider.Provider, error) {
			require.Contains(t, []string{"source-bucket", "target-bucket"}, cfg.Bucket)
			return p, nil
		},
	})
	t.Cleanup(restore)
	cfg := viper.New()
	cfg.Set("hub_read_handles.source.uri", "s3://source-bucket/archive/")
	cfg.Set("hub_read_handles.source.profile", "source-profile")
	cfg.Set("hub_publish_handles.target.uri", "s3://target-bucket/custody/")
	cfg.Set("hub_publish_handles.target.profile", "target-profile")

	source, err := resolveConfiguredBridgeHubReadHandleFrom(context.Background(), cfg, "source")
	require.NoError(t, err)
	require.NotNil(t, source.reader)
	require.Equal(t, string(provider.ProviderS3), source.rangeID.provider)
	require.Equal(t, "default", source.rangeID.endpoint)
	require.Equal(t, "source-bucket", source.rangeID.container)
	require.Equal(t, "archive", source.rangeID.prefix)
	require.NoError(t, source.closer.Close())

	resolved, err := resolveConfiguredHubPublishHandleFrom(context.Background(), cfg, "target")
	require.NoError(t, err)
	require.NotNil(t, resolved.publisher)
	require.Equal(t, string(provider.ProviderS3), resolved.rangeID.provider)
	require.Equal(t, "default", resolved.rangeID.endpoint)
	require.Equal(t, "target-bucket", resolved.rangeID.container)
	require.Equal(t, "custody", resolved.rangeID.prefix)
	require.NoError(t, resolved.closer.Close())
}

func TestResolveConfiguredHubPublishHandleRefusesUnprovenIfAbsent(t *testing.T) {
	p := &bridgeProviderWithoutReporter{}
	restore := providerdispatch.UseFactoriesForTest(providerdispatch.Factories{
		S3: func(context.Context, providers3.Config) (provider.Provider, error) {
			return p, nil
		},
	})
	t.Cleanup(restore)
	cfg := viper.New()
	cfg.Set("hub_publish_handles.target.uri", "s3://target-bucket/custody/")

	_, err := resolveConfiguredHubPublishHandleFrom(context.Background(), cfg, "target")
	require.ErrorContains(t, err, "conditional create unproven")
}

type bridgeProviderFixture struct {
	putKey       string
	precondition provider.PutPrecondition
	putErr       error
}

type bridgeProviderWithoutReporter struct {
	putKey       string
	precondition provider.PutPrecondition
}

func (p *bridgeProviderWithoutReporter) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return nil, nil
}

func (p *bridgeProviderWithoutReporter) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (p *bridgeProviderWithoutReporter) Close() error { return nil }

func (p *bridgeProviderWithoutReporter) GetObject(context.Context, string) (io.ReadCloser, int64, error) {
	return nil, 0, provider.ErrNotFound
}

func (p *bridgeProviderWithoutReporter) PutObjectConditional(
	_ context.Context,
	key string,
	_ io.Reader,
	_ int64,
	precondition provider.PutPrecondition,
) (provider.PutResult, error) {
	p.putKey = key
	p.precondition = precondition
	return provider.PutResult{}, nil
}

func (p *bridgeProviderFixture) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return nil, nil
}

func (p *bridgeProviderFixture) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (p *bridgeProviderFixture) Close() error { return nil }

func (p *bridgeProviderFixture) GetObject(context.Context, string) (io.ReadCloser, int64, error) {
	return nil, 0, provider.ErrNotFound
}

func (p *bridgeProviderFixture) PutObjectConditional(
	_ context.Context,
	key string,
	_ io.Reader,
	_ int64,
	precondition provider.PutPrecondition,
) (provider.PutResult, error) {
	p.putKey = key
	p.precondition = precondition
	return provider.PutResult{}, p.putErr
}

func (p *bridgeProviderFixture) ConditionalWriteCapabilities() provider.ConditionalWriteCapabilities {
	return provider.ConditionalWriteCapabilities{IfAbsent: true}
}

func validBridgeCommandResult() indexreader.BridgeResult {
	digest := strings.Repeat("b", 64)
	return indexreader.BridgeResult{
		IndexSetID:                "idx_" + strings.Repeat("a", 64),
		RunID:                     "run_1",
		LegacyMarkerSHA256:        digest,
		IdentitySHA256:            digest,
		IdentitySchema:            indexreader.BridgeIdentitySchema,
		IdentityProfile:           indexreader.BridgeIdentityProfile,
		ManifestSHA256:            digest,
		DeclaredRows:              4,
		DeclaredSegments:          2,
		SegmentsVerified:          2,
		BytesRead:                 100,
		BytesConditionallyCreated: 90,
		RunStart: indexreader.BridgeRunStart{
			Basis: indexreader.BridgeRunStartLegacyAsserted, StartedAt: "2026-09-10T11:00:00Z",
		},
		SnapshotTime: indexreader.BridgeSnapshotTime{
			Basis: indexreader.BridgeSnapshotTimeLegacyUnavailable,
		},
		ConversionIdentitySHA256: digest,
		CompleteSHA256:           digest,
		Disposition:              indexreader.BridgeDispositionCreated,
	}
}

func mustFileHubRange(t *testing.T, path string) hubAuthorityRange {
	t.Helper()
	physical, err := filepath.EvalSymlinks(path)
	require.NoError(t, err)
	rangeID, err := canonicalHubAuthorityRange(&hubDestSpec{
		Provider: string(provider.ProviderFile),
		BaseDir:  physical,
	})
	require.NoError(t, err)
	return rangeID
}

func newIndexHubBridgeCommandForTest() *cobra.Command {
	cmd := &cobra.Command{Use: "bridge-durable", Args: cobra.NoArgs, RunE: runIndexHubBridgeDurable}
	cmd.Flags().String("source-hub-read-handle", "", "")
	cmd.Flags().String("target-hub-publish-handle", "", "")
	cmd.Flags().String("index-set", "", "")
	cmd.Flags().String("run-id", "", "")
	cmd.Flags().String("identity-file", "", "")
	cmd.Flags().String("identity-declaration", "", "")
	cmd.Flags().String("snapshot-completion-evidence", "", "")
	cmd.Flags().String("output-format", "", "")
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

func restoreBridgeCommandGlobals(t *testing.T) {
	t.Helper()
	oldReadResolver := configuredBridgeHubReadHandleResolver
	oldPublishResolver := configuredHubPublishHandleResolver
	oldRun := runLegacyDurableBridge
	oldClock := custodyBridgeClock
	oldRandom := custodyBridgeRandom
	t.Cleanup(func() {
		configuredBridgeHubReadHandleResolver = oldReadResolver
		configuredHubPublishHandleResolver = oldPublishResolver
		runLegacyDurableBridge = oldRun
		custodyBridgeClock = oldClock
		custodyBridgeRandom = oldRandom
	})
}

func validateBridgeReceiptAgainstSchema(t *testing.T, data []byte) {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	rawSchema, err := os.ReadFile(filepath.Join(
		root, "schemas", "gonimbus", "v1.0.0", "index-custody-bridge-receipt.v1.schema.json",
	))
	require.NoError(t, err)
	validator, err := schema.NewValidator(rawSchema)
	require.NoError(t, err)
	diagnostics, err := validator.ValidateJSON(data)
	require.NoError(t, err)
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == schema.SeverityError {
			t.Fatalf("receipt failed schema validation: %s: %s", diagnostic.Pointer, diagnostic.Message)
		}
	}
}
