package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	errwrap "github.com/3leaps/gonimbus/internal/errors"
	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/provider"
	providers3 "github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/uri"
	"github.com/fulmenhq/gofulmen/crucible"
)

var (
	doctorProvider  string
	doctorProfile   string
	doctorLogFormat string
	doctorEndpoint  string
	doctorRegion    string
	doctorProbeURI  string

	newDoctorS3ProbeProvider = func(ctx context.Context, cfg providers3.Config) (doctorS3ProbeProvider, error) {
		return providers3.New(ctx, cfg)
	}
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run diagnostic checks",
	Long: `Run diagnostic checks on the system and suggest fixes for common issues.

Examples:
  gonimbus doctor                             # Full environment check
  gonimbus doctor --provider s3               # S3-specific checks
  gonimbus doctor --provider s3 --profile dev # Check specific AWS profile
  gonimbus doctor --provider s3 --endpoint https://s3.example.com --region us-east-1 --probe-uri s3://bucket/prefix/`,
	Run: runDoctor,
}

func init() {
	rootCmd.AddCommand(doctorCmd)
	doctorCmd.Flags().StringVar(&doctorProvider, "provider", "", "Run provider-specific checks (s3)")
	doctorCmd.Flags().StringVar(&doctorProfile, "profile", "", "AWS profile to check (requires --provider s3)")
	doctorCmd.Flags().StringVar(&doctorEndpoint, "endpoint", "", "Custom S3 endpoint (requires --provider s3)")
	doctorCmd.Flags().StringVar(&doctorRegion, "region", "", "AWS region (requires --provider s3)")
	doctorCmd.Flags().StringVar(&doctorProbeURI, "probe-uri", "", "Opt-in read-only S3 probe target (s3://bucket[/prefix-or-key], requires --provider s3)")
	doctorCmd.Flags().StringVar(&doctorLogFormat, "log-format", diagnosticLogFormatPlain, "diagnostic output format (plain or structured)")
}

func runDoctor(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	out, err := newDiagnosticPrinter(cmd, doctorLogFormat)
	if err != nil {
		exitDiagnosticWithCode(cmd, nil, doctorLogFormat, foundry.ExitInvalidArgument, "Invalid flags", err)
		return
	}

	s3Opts, err := doctorS3OptionsFromFlags()
	if err != nil {
		exitDiagnosticWithCode(
			cmd,
			observability.CLILogger,
			doctorLogFormat,
			foundry.ExitInvalidArgument,
			"Invalid flags",
			errwrap.NewInvalidInputError(err.Error()),
		)
		return
	}

	identity := GetAppIdentity()
	bannerName := "doctor"
	if identity != nil && identity.BinaryName != "" {
		bannerName = identity.BinaryName + " doctor"
	}
	out.Info("=== " + bannerName + " ===")
	out.Info("")
	out.Info("Running diagnostic checks...")
	out.Info("")

	allChecks := true
	checkNum := 1
	totalChecks := 5

	// Add S3 checks if provider specified
	if doctorProvider == "s3" {
		totalChecks = 9 // credentials, source, endpoint/region, expiry
		if s3Opts.Probe != nil {
			totalChecks++
		}
	}

	// Check 1: Go version
	goVersion := runtime.Version()
	if goVersion >= "go1.23" {
		out.Info(fmt.Sprintf("[%d/%d] Checking Go version... ✅ %s", checkNum, totalChecks, goVersion),
			zap.String("go_version", goVersion))
	} else {
		out.Warn(fmt.Sprintf("[%d/%d] Checking Go version... ⚠️  %s (recommended: go1.23+)", checkNum, totalChecks, goVersion),
			zap.String("go_version", goVersion))
		allChecks = false
	}
	checkNum++

	// Check 2: Crucible access
	version := crucible.GetVersion()
	if version.Crucible != "" {
		out.Info(fmt.Sprintf("[%d/%d] Checking Crucible access... ✅ v%s", checkNum, totalChecks, version.Crucible),
			zap.String("crucible_version", version.Crucible))
	} else {
		out.Error(fmt.Sprintf("[%d/%d] Checking Crucible access... ❌ Cannot access Crucible", checkNum, totalChecks))
		exitDiagnosticWithCode(cmd, observability.CLILogger, doctorLogFormat, foundry.ExitExternalServiceUnavailable, "Cannot access Crucible",
			errwrap.NewExternalServiceError("Crucible service unavailable"))
		allChecks = false
	}
	checkNum++

	// Check 3: Gofulmen access
	if version.Gofulmen != "" {
		out.Info(fmt.Sprintf("[%d/%d] Checking Gofulmen access... ✅ v%s", checkNum, totalChecks, version.Gofulmen),
			zap.String("gofulmen_version", version.Gofulmen))
	} else {
		out.Error(fmt.Sprintf("[%d/%d] Checking Gofulmen access... ❌ Cannot access Gofulmen", checkNum, totalChecks))
		allChecks = false
	}
	checkNum++

	// Check 4: Config directory
	configDir, err := os.UserConfigDir()
	if err != nil {
		out.Error(fmt.Sprintf("[%d/%d] Checking config directory... ❌ Cannot find config directory", checkNum, totalChecks),
			zap.Error(err))
		exitDiagnosticWithCode(cmd, observability.CLILogger, doctorLogFormat, foundry.ExitFileNotFound, "Cannot find config directory",
			errwrap.WrapInternal(ctx, err, "Cannot find config directory"))
		allChecks = false
	} else {
		out.Info(fmt.Sprintf("[%d/%d] Checking config directory... ✅ %s", checkNum, totalChecks, configDir),
			zap.String("config_dir", configDir))
	}
	checkNum++

	// Check 5: Environment
	out.Info(fmt.Sprintf("[%d/%d] Checking environment... ✅ %s/%s", checkNum, totalChecks, runtime.GOOS, runtime.GOARCH),
		zap.String("os", runtime.GOOS),
		zap.String("arch", runtime.GOARCH))
	checkNum++

	// S3-specific checks
	if doctorProvider == "s3" {
		allChecks = runS3Checks(ctx, out, checkNum, totalChecks, allChecks, s3Opts)
	}

	out.Info("")
	if allChecks {
		out.Info(fmt.Sprintf("✅ All checks passed! Your %s installation is healthy.", bannerName))
	} else {
		out.Warn("⚠️  Some checks failed. Review the output above for details.")
	}
	out.Info("")
	out.Info("=== End Diagnostics ===")
}

type doctorS3Options struct {
	Profile  string
	Endpoint string
	Region   string
	Probe    *doctorProbeTarget
}

type doctorProbeTarget struct {
	URI *uri.ObjectURI
	Op  string
}

type doctorS3ProbeProvider interface {
	List(context.Context, provider.ListOptions) (*provider.ListResult, error)
	Head(context.Context, string) (*provider.ObjectMeta, error)
	Close() error
}

const (
	doctorProbeOpListObjects = "list_objects_v2"
	doctorProbeOpHeadObject  = "head_object"

	doctorProbeFailureCredentialsInvalid  = "credentials-invalid"
	doctorProbeFailureEndpointUnreachable = "endpoint-unreachable"
	doctorProbeFailureBucketNotFound      = "bucket-not-found"
	doctorProbeFailureAccessDenied        = "access-denied"
	doctorProbeFailureKeyNotFound         = "key-not-found"
)

func doctorS3OptionsFromFlags() (*doctorS3Options, error) {
	if doctorProfile != "" && doctorProvider != "s3" {
		return nil, fmt.Errorf("--profile requires --provider s3")
	}
	if doctorEndpoint != "" && doctorProvider != "s3" {
		return nil, fmt.Errorf("--endpoint requires --provider s3")
	}
	if doctorRegion != "" && doctorProvider != "s3" {
		return nil, fmt.Errorf("--region requires --provider s3")
	}
	if doctorProbeURI != "" && doctorProvider != "s3" {
		return nil, fmt.Errorf("--probe-uri requires --provider s3")
	}

	opts := &doctorS3Options{
		Profile:  strings.TrimSpace(doctorProfile),
		Endpoint: strings.TrimSpace(doctorEndpoint),
		Region:   strings.TrimSpace(doctorRegion),
	}
	if strings.TrimSpace(doctorProbeURI) != "" {
		probe, err := parseDoctorProbeURI(doctorProbeURI)
		if err != nil {
			return nil, err
		}
		opts.Probe = probe
	}
	return opts, nil
}

func parseDoctorProbeURI(raw string) (*doctorProbeTarget, error) {
	parsed, err := uri.ParseURI(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("invalid --probe-uri: %w", err)
	}
	if parsed.Provider != string(provider.ProviderS3) {
		return nil, fmt.Errorf("--probe-uri must use s3://")
	}
	if parsed.IsPattern() {
		return nil, fmt.Errorf("--probe-uri does not accept glob patterns; provide a bucket, prefix, or exact key")
	}

	op := doctorProbeOpHeadObject
	if parsed.IsPrefix() {
		op = doctorProbeOpListObjects
	}
	return &doctorProbeTarget{URI: parsed, Op: op}, nil
}

func (o *doctorS3Options) awsConfigOptions() []func(*config.LoadOptions) error {
	var opts []func(*config.LoadOptions) error
	if o == nil {
		return opts
	}
	if o.Region != "" {
		opts = append(opts, config.WithRegion(o.Region))
	}
	if o.Profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(o.Profile))
	}
	return opts
}

func (o *doctorS3Options) effectiveEndpoint() string {
	if o != nil && o.Endpoint != "" {
		return o.Endpoint
	}
	if endpoint := strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL_S3")); endpoint != "" {
		return endpoint
	}
	return strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL"))
}

func (o *doctorS3Options) providerConfig(bucket string, effectiveRegion string) providers3.Config {
	endpoint := o.effectiveEndpoint()
	cfg := providers3.Config{
		Bucket:         bucket,
		Region:         effectiveRegion,
		Endpoint:       endpoint,
		Profile:        o.Profile,
		ForcePathStyle: endpoint != "",
		MaxKeys:        1,
	}
	return cfg
}

// runS3Checks runs S3-specific diagnostic checks.
func runS3Checks(ctx context.Context, out *diagnosticPrinter, checkNum, totalChecks int, allChecks bool, opts *doctorS3Options) bool {
	out.Info("")
	if opts.Profile != "" {
		out.Info(fmt.Sprintf("S3 Provider Checks (profile: %s):", opts.Profile))
	} else {
		out.Info("S3 Provider Checks:")
	}

	configOpts := opts.awsConfigOptions()

	// Disable IMDS if we have profile or env credentials to avoid slow timeout
	if opts.Profile != "" || os.Getenv("AWS_ACCESS_KEY_ID") != "" || os.Getenv("AWS_PROFILE") != "" {
		configOpts = append(configOpts, config.WithEC2IMDSClientEnableState(imds.ClientDisabled))
	}

	// Check 6: AWS credentials
	cfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		out.Error(fmt.Sprintf("[%d/%d] Checking AWS credentials... ❌ Cannot load AWS config", checkNum, totalChecks),
			zap.Error(err))
		printAWSCredentialsHelp(out, opts.Profile)
		return false
	}

	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		out.Error(fmt.Sprintf("[%d/%d] Checking AWS credentials... ❌ Cannot retrieve credentials", checkNum, totalChecks),
			zap.Error(err))
		printAWSCredentialsHelp(out, opts.Profile)
		return false
	}

	// Mask the access key for display
	maskedKey := maskAccessKey(creds.AccessKeyID)
	out.Info(fmt.Sprintf("[%d/%d] Checking AWS credentials... ✅ Found credentials", checkNum, totalChecks),
		zap.String("access_key", maskedKey),
		zap.String("source", creds.Source))
	checkNum++

	// Check 7: Credential source info
	source := creds.Source
	if source == "" {
		source = "unknown"
	}
	out.Info(fmt.Sprintf("[%d/%d] Checking credential source... ✅ %s", checkNum, totalChecks, source),
		zap.String("credential_source", source))
	checkNum++

	endpoint := opts.effectiveEndpoint()
	region := cfg.Region
	out.Info(fmt.Sprintf("[%d/%d] Checking S3 endpoint/region... ✅ endpoint=%s region=%s", checkNum, totalChecks, displayDoctorValue(endpoint, "AWS default"), displayDoctorValue(region, "(not set)")),
		zap.String("endpoint", endpoint),
		zap.String("region", region))
	checkNum++

	// Check 8: Credential expiry (for SSO and assumed role credentials)
	if creds.CanExpire {
		remaining := time.Until(creds.Expires)
		expiresAt := creds.Expires.Format(time.RFC3339)
		if remaining < time.Hour {
			out.Warn(fmt.Sprintf("[%d/%d] Checking credential expiry... ⚠️  Expires in %s", checkNum, totalChecks, formatDuration(remaining)),
				zap.String("expires_at", expiresAt),
				zap.Duration("remaining", remaining))
			out.Info("  Consider re-authenticating soon:")
			if opts.Profile != "" {
				out.Info(fmt.Sprintf("    aws sso login --profile %s", opts.Profile))
			} else {
				out.Info("    aws sso login --profile <your-profile>")
			}
		} else {
			out.Info(fmt.Sprintf("[%d/%d] Checking credential expiry... ✅ Valid for %s", checkNum, totalChecks, formatDuration(remaining)),
				zap.String("expires_at", expiresAt),
				zap.Duration("remaining", remaining))
		}
	} else {
		out.Info(fmt.Sprintf("[%d/%d] Checking credential expiry... ✅ Non-expiring credentials", checkNum, totalChecks))
	}
	checkNum++

	if opts.Probe != nil {
		if !runDoctorS3Probe(ctx, out, checkNum, totalChecks, opts, cfg.Region) {
			allChecks = false
		}
	}

	return allChecks
}

func runDoctorS3Probe(ctx context.Context, out *diagnosticPrinter, checkNum, totalChecks int, opts *doctorS3Options, effectiveRegion string) bool {
	probe := opts.Probe
	providerCfg := opts.providerConfig(probe.URI.Bucket, effectiveRegion)
	prov, err := newDoctorS3ProbeProvider(ctx, providerCfg)
	if err != nil {
		writeDoctorProbeFailure(out, checkNum, totalChecks, probe, classifyDoctorProbeError(probe.Op, err), err)
		return false
	}
	defer func() { _ = prov.Close() }()

	switch probe.Op {
	case doctorProbeOpListObjects:
		_, err = prov.List(ctx, provider.ListOptions{Prefix: probe.URI.Key, MaxKeys: 1})
	case doctorProbeOpHeadObject:
		_, err = prov.Head(ctx, probe.URI.Key)
	default:
		err = fmt.Errorf("unsupported probe op %q", probe.Op)
	}
	if err != nil {
		writeDoctorProbeFailure(out, checkNum, totalChecks, probe, classifyDoctorProbeError(probe.Op, err), err)
		return false
	}

	out.Info(fmt.Sprintf("[%d/%d] Probing S3 target... ✅ op=%s target=%s", checkNum, totalChecks, probe.Op, probe.URI.String()),
		zap.String("probe_op", probe.Op),
		zap.String("probe_uri", probe.URI.String()))
	return true
}

func writeDoctorProbeFailure(out *diagnosticPrinter, checkNum, totalChecks int, probe *doctorProbeTarget, failureClass string, err error) {
	out.Error(fmt.Sprintf("[%d/%d] Probing S3 target... ❌ op=%s target=%s failure_class=%s", checkNum, totalChecks, probe.Op, probe.URI.String(), failureClass),
		zap.String("probe_op", probe.Op),
		zap.String("probe_uri", probe.URI.String()),
		zap.String("failure_class", failureClass),
		zap.String("error_detail", err.Error()))
}

func classifyDoctorProbeError(op string, err error) string {
	switch {
	case provider.IsInvalidCredentials(err):
		return doctorProbeFailureCredentialsInvalid
	case provider.IsBucketNotFound(err):
		return doctorProbeFailureBucketNotFound
	case provider.IsAccessDenied(err):
		return doctorProbeFailureAccessDenied
	case op == doctorProbeOpHeadObject && provider.IsNotFound(err):
		return doctorProbeFailureKeyNotFound
	case provider.IsProviderUnavailable(err), isNetworkEndpointError(err):
		return doctorProbeFailureEndpointUnreachable
	default:
		return doctorProbeFailureEndpointUnreachable
	}
}

func isNetworkEndpointError(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "no such host") ||
		strings.Contains(errMsg, "tls") ||
		strings.Contains(errMsg, "timeout")
}

func displayDoctorValue(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

// formatDuration formats a duration in a human-readable way.
func formatDuration(d time.Duration) string {
	if d < 0 {
		return "expired"
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%dm", minutes)
}

// maskAccessKey masks all but the last 4 characters of an access key.
func maskAccessKey(key string) string {
	if len(key) <= 4 {
		return "****"
	}
	return "****" + key[len(key)-4:]
}

// printAWSCredentialsHelp prints help for configuring AWS credentials.
func printAWSCredentialsHelp(out *diagnosticPrinter, profile string) {
	out.Info("")
	out.Info("To configure AWS credentials:")
	out.Info("")
	if profile != "" {
		out.Info(fmt.Sprintf("  For profile '%s':", profile))
		out.Info(fmt.Sprintf("    aws sso login --profile %s", profile))
		out.Info("")
	}
	out.Info("  Options:")
	out.Info("    1. For SSO profiles: aws sso login --profile <name>")
	out.Info("    2. For access keys: aws configure --profile <name>")
	out.Info("    3. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars")
	out.Info("    4. Use IAM role when running on AWS infrastructure")
	out.Info("")
	out.Info("For S3-compatible storage (MinIO, Wasabi, etc.), also set:")
	out.Info("  - AWS_ENDPOINT_URL_S3 / AWS_ENDPOINT_URL, or use --endpoint")
	out.Info("  - AWS_REGION / AWS_DEFAULT_REGION, or use --region")
	out.Info("  - Use --probe-uri s3://bucket[/prefix-or-key] for an opt-in read-only probe")
	out.Info("")
}
