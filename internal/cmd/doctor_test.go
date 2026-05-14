package cmd

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/provider"
	providers3 "github.com/3leaps/gonimbus/pkg/provider/s3"
)

func TestMaskAccessKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "standard 20 char key",
			input: "AKIAIOSFODNN7EXAMPLE",
			want:  "****MPLE",
		},
		{
			name:  "short key 4 chars",
			input: "ABCD",
			want:  "****",
		},
		{
			name:  "short key 3 chars",
			input: "ABC",
			want:  "****",
		},
		{
			name:  "empty key",
			input: "",
			want:  "****",
		},
		{
			name:  "5 char key shows last 4",
			input: "ABCDE",
			want:  "****BCDE",
		},
		{
			name:  "8 char key",
			input: "12345678",
			want:  "****5678",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maskAccessKey(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPrintAWSCredentialsHelp(t *testing.T) {
	// Initialize CLI logger to avoid nil pointer
	observability.InitCLILogger("test", false)
	var stdout bytes.Buffer
	cmd := &cobra.Command{Use: "test"}
	cmd.SetOut(&stdout)
	out, err := newDiagnosticPrinter(cmd, diagnosticLogFormatPlain)
	assert.NoError(t, err)

	// This test verifies the function doesn't panic
	// It logs help text for configuring AWS credentials
	t.Run("does not panic without profile", func(t *testing.T) {
		assert.NotPanics(t, func() {
			printAWSCredentialsHelp(out, "")
		})
	})

	t.Run("does not panic with profile", func(t *testing.T) {
		assert.NotPanics(t, func() {
			printAWSCredentialsHelp(out, "my-profile")
		})
	})
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{
			name:     "hours and minutes",
			duration: 5*time.Hour + 30*time.Minute,
			want:     "5h 30m",
		},
		{
			name:     "just minutes",
			duration: 45 * time.Minute,
			want:     "45m",
		},
		{
			name:     "zero",
			duration: 0,
			want:     "0m",
		},
		{
			name:     "negative (expired)",
			duration: -1 * time.Hour,
			want:     "expired",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDuration(tt.duration)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDoctorFlagsRegistered(t *testing.T) {
	require.NotNil(t, doctorCmd.Flags().Lookup("endpoint"))
	require.NotNil(t, doctorCmd.Flags().Lookup("region"))
	require.NotNil(t, doctorCmd.Flags().Lookup("probe-uri"))
}

func TestDoctorS3OptionsRequireProviderS3(t *testing.T) {
	tests := []struct {
		name      string
		configure func()
		want      string
	}{
		{
			name: "--profile",
			configure: func() {
				doctorProfile = "demo"
			},
			want: "--profile requires --provider s3",
		},
		{
			name: "--endpoint",
			configure: func() {
				doctorEndpoint = "https://s3.example.com"
			},
			want: "--endpoint requires --provider s3",
		},
		{
			name: "--region",
			configure: func() {
				doctorRegion = "us-east-2"
			},
			want: "--region requires --provider s3",
		},
		{
			name: "--probe-uri",
			configure: func() {
				doctorProbeURI = "s3://bucket"
			},
			want: "--probe-uri requires --provider s3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetDoctorFlags(t)
			tt.configure()

			_, err := doctorS3OptionsFromFlags()

			require.Error(t, err)
			require.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestDoctorS3OptionsAcceptEndpointRegionWithEnvUnset(t *testing.T) {
	resetDoctorFlags(t)
	doctorProvider = "s3"
	doctorEndpoint = "https://s3.us-east-2.wasabisys.com"
	doctorRegion = "us-east-2"

	opts, err := doctorS3OptionsFromFlags()

	require.NoError(t, err)
	require.Equal(t, "https://s3.us-east-2.wasabisys.com", opts.Endpoint)
	require.Equal(t, "us-east-2", opts.Region)
	require.Equal(t, "https://s3.us-east-2.wasabisys.com", opts.effectiveEndpoint())
}

func TestDoctorS3OptionsCLIEndpointRegionOverrideEnv(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL_S3", "https://env-specific.example.com")
	t.Setenv("AWS_ENDPOINT_URL", "https://env-global.example.com")
	t.Setenv("AWS_REGION", "us-west-1")

	opts := &doctorS3Options{
		Endpoint: "https://cli.example.com",
		Region:   "us-east-2",
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), opts.awsConfigOptions()...)
	require.NoError(t, err)

	require.Equal(t, "us-east-2", cfg.Region)
	require.Equal(t, "https://cli.example.com", opts.effectiveEndpoint())
	providerCfg := opts.providerConfig("bucket", cfg.Region)
	require.Equal(t, "bucket", providerCfg.Bucket)
	require.Equal(t, "us-east-2", providerCfg.Region)
	require.Equal(t, "https://cli.example.com", providerCfg.Endpoint)
	require.True(t, providerCfg.ForcePathStyle)
}

func TestDoctorS3OptionsUseEndpointEnvWhenFlagUnset(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL_S3", "https://env-specific.example.com")
	t.Setenv("AWS_ENDPOINT_URL", "https://env-global.example.com")
	opts := &doctorS3Options{}

	require.Equal(t, "https://env-specific.example.com", opts.effectiveEndpoint())

	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	require.Equal(t, "https://env-global.example.com", opts.effectiveEndpoint())
}

func TestParseDoctorProbeURI(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantOp  string
		wantKey string
		wantErr string
	}{
		{name: "bucket", raw: "s3://bucket", wantOp: doctorProbeOpListObjects, wantKey: ""},
		{name: "bucket slash", raw: "s3://bucket/", wantOp: doctorProbeOpListObjects, wantKey: ""},
		{name: "prefix", raw: "s3://bucket/some/prefix/", wantOp: doctorProbeOpListObjects, wantKey: "some/prefix/"},
		{name: "key", raw: "s3://bucket/some/key.xml", wantOp: doctorProbeOpHeadObject, wantKey: "some/key.xml"},
		{name: "non s3", raw: "gs://bucket/key", wantErr: "unsupported provider"},
		{name: "glob", raw: "s3://bucket/prefix/**/*.xml", wantErr: "does not accept glob patterns"},
		{name: "question glob", raw: "s3://bucket/foo?bar", wantErr: "does not accept glob patterns"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDoctorProbeURI(tt.raw)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantOp, got.Op)
			require.Equal(t, "bucket", got.URI.Bucket)
			require.Equal(t, tt.wantKey, got.URI.Key)
		})
	}
}

func TestRunDoctorS3ProbeDispatchesListForBucketAndPrefix(t *testing.T) {
	tests := []struct {
		name       string
		probeURI   string
		wantPrefix string
	}{
		{name: "bucket", probeURI: "s3://bucket", wantPrefix: ""},
		{name: "prefix", probeURI: "s3://bucket/some/prefix/", wantPrefix: "some/prefix/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe, err := parseDoctorProbeURI(tt.probeURI)
			require.NoError(t, err)
			fake := &fakeDoctorProbeProvider{}
			var gotCfg providers3.Config
			withDoctorProbeProvider(t, fake, &gotCfg)
			cmd, stdout, stderr := testCommandBuffers()
			out, err := newDiagnosticPrinter(cmd, diagnosticLogFormatPlain)
			require.NoError(t, err)

			ok := runDoctorS3Probe(context.Background(), out, 1, 1, &doctorS3Options{
				Profile:  "demo",
				Endpoint: "https://cli.example.com",
				Probe:    probe,
			}, "us-east-2")

			require.True(t, ok)
			require.Empty(t, stderr.String())
			require.Contains(t, stdout.String(), "op=list_objects_v2")
			require.Equal(t, "bucket", gotCfg.Bucket)
			require.Equal(t, "us-east-2", gotCfg.Region)
			require.Equal(t, "https://cli.example.com", gotCfg.Endpoint)
			require.Equal(t, "demo", gotCfg.Profile)
			require.True(t, gotCfg.ForcePathStyle)
			require.Len(t, fake.listCalls, 1)
			require.Equal(t, tt.wantPrefix, fake.listCalls[0].Prefix)
			require.Equal(t, 1, fake.listCalls[0].MaxKeys)
			require.Empty(t, fake.headCalls)
		})
	}
}

func TestRunDoctorS3ProbeDispatchesHeadForExactKey(t *testing.T) {
	probe, err := parseDoctorProbeURI("s3://bucket/some/key.xml")
	require.NoError(t, err)
	fake := &fakeDoctorProbeProvider{}
	withDoctorProbeProvider(t, fake, nil)
	cmd, stdout, stderr := testCommandBuffers()
	out, err := newDiagnosticPrinter(cmd, diagnosticLogFormatPlain)
	require.NoError(t, err)

	ok := runDoctorS3Probe(context.Background(), out, 1, 1, &doctorS3Options{Probe: probe}, "us-east-1")

	require.True(t, ok)
	require.Empty(t, stderr.String())
	require.Contains(t, stdout.String(), "op=head_object")
	require.Equal(t, []string{"some/key.xml"}, fake.headCalls)
	require.Empty(t, fake.listCalls)
}

func TestClassifyDoctorProbeError(t *testing.T) {
	tests := []struct {
		name string
		op   string
		err  error
		want string
	}{
		{
			name: "credentials",
			op:   doctorProbeOpListObjects,
			err:  providerError(provider.ErrInvalidCredentials),
			want: doctorProbeFailureCredentialsInvalid,
		},
		{
			name: "endpoint unavailable",
			op:   doctorProbeOpListObjects,
			err:  providerError(provider.ErrProviderUnavailable),
			want: doctorProbeFailureEndpointUnreachable,
		},
		{
			name: "network",
			op:   doctorProbeOpListObjects,
			err:  &net.DNSError{Err: "no such host"},
			want: doctorProbeFailureEndpointUnreachable,
		},
		{
			name: "bucket missing",
			op:   doctorProbeOpListObjects,
			err:  providerError(provider.ErrBucketNotFound),
			want: doctorProbeFailureBucketNotFound,
		},
		{
			name: "access denied",
			op:   doctorProbeOpListObjects,
			err:  providerError(provider.ErrAccessDenied),
			want: doctorProbeFailureAccessDenied,
		},
		{
			name: "key missing on head",
			op:   doctorProbeOpHeadObject,
			err:  providerError(provider.ErrNotFound),
			want: doctorProbeFailureKeyNotFound,
		},
		{
			name: "empty prefix list still not key missing",
			op:   doctorProbeOpListObjects,
			err:  providerError(provider.ErrNotFound),
			want: doctorProbeFailureEndpointUnreachable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classifyDoctorProbeError(tt.op, tt.err))
		})
	}
}

func TestRunDoctorS3ProbeReportsFailureClassWithoutRawPlainError(t *testing.T) {
	probe, err := parseDoctorProbeURI("s3://bucket/missing.xml")
	require.NoError(t, err)
	fake := &fakeDoctorProbeProvider{headErr: providerError(provider.ErrNotFound)}
	withDoctorProbeProvider(t, fake, nil)
	cmd, stdout, stderr := testCommandBuffers()
	out, err := newDiagnosticPrinter(cmd, diagnosticLogFormatPlain)
	require.NoError(t, err)

	ok := runDoctorS3Probe(context.Background(), out, 1, 1, &doctorS3Options{Probe: probe}, "us-east-1")

	require.False(t, ok)
	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "failure_class=key-not-found")
	require.NotContains(t, stderr.String(), "object not found")
}

func TestRunDoctorS3ProbeReportsFailureClasses(t *testing.T) {
	tests := []struct {
		name         string
		probeURI     string
		listErr      error
		headErr      error
		failureClass string
	}{
		{
			name:         "credentials invalid",
			probeURI:     "s3://bucket",
			listErr:      providerError(provider.ErrInvalidCredentials),
			failureClass: doctorProbeFailureCredentialsInvalid,
		},
		{
			name:         "endpoint unreachable",
			probeURI:     "s3://bucket",
			listErr:      providerError(provider.ErrProviderUnavailable),
			failureClass: doctorProbeFailureEndpointUnreachable,
		},
		{
			name:         "bucket missing",
			probeURI:     "s3://bucket",
			listErr:      providerError(provider.ErrBucketNotFound),
			failureClass: doctorProbeFailureBucketNotFound,
		},
		{
			name:         "access denied",
			probeURI:     "s3://bucket/prefix/",
			listErr:      providerError(provider.ErrAccessDenied),
			failureClass: doctorProbeFailureAccessDenied,
		},
		{
			name:         "key missing",
			probeURI:     "s3://bucket/missing.xml",
			headErr:      providerError(provider.ErrNotFound),
			failureClass: doctorProbeFailureKeyNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe, err := parseDoctorProbeURI(tt.probeURI)
			require.NoError(t, err)
			fake := &fakeDoctorProbeProvider{listErr: tt.listErr, headErr: tt.headErr}
			withDoctorProbeProvider(t, fake, nil)
			cmd, stdout, stderr := testCommandBuffers()
			out, err := newDiagnosticPrinter(cmd, diagnosticLogFormatPlain)
			require.NoError(t, err)

			ok := runDoctorS3Probe(context.Background(), out, 1, 1, &doctorS3Options{Probe: probe}, "us-east-1")

			require.False(t, ok)
			require.Empty(t, stdout.String())
			require.Contains(t, stderr.String(), "failure_class="+tt.failureClass)
		})
	}
}

func TestDoctorS3DefaultNoProbeNoise(t *testing.T) {
	withDiagnosticTestState(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_REGION", "us-east-1")
	doctorProvider = "s3"

	cmd, stdout, stderr := testCommandBuffers()
	runDoctor(cmd, nil)

	require.Empty(t, stderr.String())
	require.Contains(t, stdout.String(), "S3 Provider Checks:")
	require.Contains(t, stdout.String(), "Checking S3 endpoint/region")
	require.NotContains(t, stdout.String(), "Probing S3 target")
}

type fakeDoctorProbeProvider struct {
	listErr   error
	headErr   error
	listCalls []provider.ListOptions
	headCalls []string
	closed    bool
}

func (p *fakeDoctorProbeProvider) List(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	p.listCalls = append(p.listCalls, opts)
	if p.listErr != nil {
		return nil, p.listErr
	}
	return &provider.ListResult{}, nil
}

func (p *fakeDoctorProbeProvider) Head(_ context.Context, key string) (*provider.ObjectMeta, error) {
	p.headCalls = append(p.headCalls, key)
	if p.headErr != nil {
		return nil, p.headErr
	}
	return &provider.ObjectMeta{}, nil
}

func (p *fakeDoctorProbeProvider) Close() error {
	p.closed = true
	return nil
}

func withDoctorProbeProvider(t *testing.T, fake *fakeDoctorProbeProvider, cfgOut *providers3.Config) {
	t.Helper()
	old := newDoctorS3ProbeProvider
	newDoctorS3ProbeProvider = func(_ context.Context, cfg providers3.Config) (doctorS3ProbeProvider, error) {
		if cfgOut != nil {
			*cfgOut = cfg
		}
		return fake, nil
	}
	t.Cleanup(func() {
		newDoctorS3ProbeProvider = old
	})
}

func providerError(err error) error {
	return &provider.ProviderError{
		Op:       "test",
		Provider: provider.ProviderS3,
		Bucket:   "bucket",
		Err:      err,
	}
}

func resetDoctorFlags(t *testing.T) {
	t.Helper()
	oldProvider := doctorProvider
	oldProfile := doctorProfile
	oldEndpoint := doctorEndpoint
	oldRegion := doctorRegion
	oldProbeURI := doctorProbeURI
	doctorProvider = ""
	doctorProfile = ""
	doctorEndpoint = ""
	doctorRegion = ""
	doctorProbeURI = ""
	t.Cleanup(func() {
		doctorProvider = oldProvider
		doctorProfile = oldProfile
		doctorEndpoint = oldEndpoint
		doctorRegion = oldRegion
		doctorProbeURI = oldProbeURI
	})
}
