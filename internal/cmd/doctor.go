package cmd

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	errwrap "github.com/3leaps/gonimbus/internal/errors"
	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/fulmenhq/gofulmen/crucible"
)

var (
	doctorProvider string
	doctorProfile  string
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run diagnostic checks",
	Long: `Run diagnostic checks on the system and suggest fixes for common issues.

Examples:
  gonimbus doctor                             # Full environment check
  gonimbus doctor --provider s3               # S3-specific checks
  gonimbus doctor --provider s3 --profile dev # Check specific AWS profile`,
	Run: runDoctor,
}

func init() {
	rootCmd.AddCommand(doctorCmd)
	doctorCmd.Flags().StringVar(&doctorProvider, "provider", "", "Run provider-specific checks (s3)")
	doctorCmd.Flags().StringVar(&doctorProfile, "profile", "", "AWS profile to check (requires --provider s3)")
}

func runDoctor(cmd *cobra.Command, args []string) {
	identity := GetAppIdentity()
	bannerName := "doctor"
	if identity != nil && identity.BinaryName != "" {
		bannerName = identity.BinaryName + " doctor"
	}
	observability.CLILogger.Info("=== " + bannerName + " ===")
	observability.CLILogger.Info("")
	observability.CLILogger.Info("Running diagnostic checks...")
	observability.CLILogger.Info("")

	allChecks := true
	checkNum := 1
	totalChecks := 5

	// Add S3 checks if provider specified
	if doctorProvider == "s3" {
		totalChecks = 8 // credentials, source, expiry
	}

	if doctorProfile != "" && doctorProvider != "s3" {
		observability.CLILogger.Error("--profile requires --provider s3")
		ExitWithCode(
			observability.CLILogger,
			foundry.ExitInvalidArgument,
			"Invalid flags",
			errwrap.NewInvalidInputError("--profile requires --provider s3"),
		)
		return
	}

	// Check 1: Go version
	goVersion := runtime.Version()
	if goVersion >= "go1.23" {
		observability.CLILogger.Info(fmt.Sprintf("[%d/%d] Checking Go version... ✅ %s", checkNum, totalChecks, goVersion),
			zap.String("go_version", goVersion))
	} else {
		observability.CLILogger.Warn(fmt.Sprintf("[%d/%d] Checking Go version... ⚠️  %s (recommended: go1.23+)", checkNum, totalChecks, goVersion),
			zap.String("go_version", goVersion))
		allChecks = false
	}
	checkNum++

	// Check 2: Crucible access
	version := crucible.GetVersion()
	if version.Crucible != "" {
		observability.CLILogger.Info(fmt.Sprintf("[%d/%d] Checking Crucible access... ✅ v%s", checkNum, totalChecks, version.Crucible),
			zap.String("crucible_version", version.Crucible))
	} else {
		observability.CLILogger.Error(fmt.Sprintf("[%d/%d] Checking Crucible access... ❌ Cannot access Crucible", checkNum, totalChecks))
		ExitWithCode(observability.CLILogger, foundry.ExitExternalServiceUnavailable, "Cannot access Crucible",
			errwrap.NewExternalServiceError("Crucible service unavailable"))
		allChecks = false
	}
	checkNum++

	// Check 3: Gofulmen access
	if version.Gofulmen != "" {
		observability.CLILogger.Info(fmt.Sprintf("[%d/%d] Checking Gofulmen access... ✅ v%s", checkNum, totalChecks, version.Gofulmen),
			zap.String("gofulmen_version", version.Gofulmen))
	} else {
		observability.CLILogger.Error(fmt.Sprintf("[%d/%d] Checking Gofulmen access... ❌ Cannot access Gofulmen", checkNum, totalChecks))
		allChecks = false
	}
	checkNum++

	// Check 4: Config directory
	configDir, err := os.UserConfigDir()
	if err != nil {
		observability.CLILogger.Error(fmt.Sprintf("[%d/%d] Checking config directory... ❌ Cannot find config directory", checkNum, totalChecks),
			zap.Error(err))
		ExitWithCode(observability.CLILogger, foundry.ExitFileNotFound, "Cannot find config directory",
			errwrap.WrapInternal(cmd.Context(), err, "Cannot find config directory"))
		allChecks = false
	} else {
		observability.CLILogger.Info(fmt.Sprintf("[%d/%d] Checking config directory... ✅ %s", checkNum, totalChecks, configDir),
			zap.String("config_dir", configDir))
	}
	checkNum++

	// Check 5: Environment
	observability.CLILogger.Info(fmt.Sprintf("[%d/%d] Checking environment... ✅ %s/%s", checkNum, totalChecks, runtime.GOOS, runtime.GOARCH),
		zap.String("os", runtime.GOOS),
		zap.String("arch", runtime.GOARCH))
	checkNum++

	// S3-specific checks
	if doctorProvider == "s3" {
		allChecks = runS3Checks(cmd.Context(), checkNum, totalChecks, allChecks)
	}

	observability.CLILogger.Info("")
	if allChecks {
		observability.CLILogger.Info(fmt.Sprintf("✅ All checks passed! Your %s installation is healthy.", bannerName))
	} else {
		observability.CLILogger.Warn("⚠️  Some checks failed. Review the output above for details.")
	}
	observability.CLILogger.Info("")
	observability.CLILogger.Info("=== End Diagnostics ===")
}

// runS3Checks runs S3-specific diagnostic checks.
func runS3Checks(ctx context.Context, checkNum, totalChecks int, allChecks bool) bool {
	observability.CLILogger.Info("")
	if doctorProfile != "" {
		observability.CLILogger.Info(fmt.Sprintf("S3 Provider Checks (profile: %s):", doctorProfile))
	} else {
		observability.CLILogger.Info("S3 Provider Checks:")
	}

	// Build config options
	var opts []func(*config.LoadOptions) error

	// Use specific profile if requested
	if doctorProfile != "" {
		opts = append(opts, config.WithSharedConfigProfile(doctorProfile))
	}

	// Disable IMDS if we have profile or env credentials to avoid slow timeout
	if doctorProfile != "" || os.Getenv("AWS_ACCESS_KEY_ID") != "" || os.Getenv("AWS_PROFILE") != "" {
		opts = append(opts, config.WithEC2IMDSClientEnableState(imds.ClientDisabled))
	}

	// Check 6: AWS credentials
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		observability.CLILogger.Error(fmt.Sprintf("[%d/%d] Checking AWS credentials... ❌ Cannot load AWS config", checkNum, totalChecks),
			zap.Error(err))
		printAWSCredentialsHelp(doctorProfile)
		return false
	}

	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		observability.CLILogger.Error(fmt.Sprintf("[%d/%d] Checking AWS credentials... ❌ Cannot retrieve credentials", checkNum, totalChecks),
			zap.Error(err))
		printAWSCredentialsHelp(doctorProfile)
		return false
	}

	// Mask the access key for display
	maskedKey := maskAccessKey(creds.AccessKeyID)
	observability.CLILogger.Info(fmt.Sprintf("[%d/%d] Checking AWS credentials... ✅ Found credentials", checkNum, totalChecks),
		zap.String("access_key", maskedKey),
		zap.String("source", creds.Source))
	checkNum++

	// Check 7: Credential source info
	source := creds.Source
	if source == "" {
		source = "unknown"
	}
	observability.CLILogger.Info(fmt.Sprintf("[%d/%d] Checking credential source... ✅ %s", checkNum, totalChecks, source),
		zap.String("credential_source", source))
	checkNum++

	// Check 8: Credential expiry (for SSO and assumed role credentials)
	if creds.CanExpire {
		remaining := time.Until(creds.Expires)
		expiresAt := creds.Expires.Format(time.RFC3339)
		if remaining < time.Hour {
			observability.CLILogger.Warn(fmt.Sprintf("[%d/%d] Checking credential expiry... ⚠️  Expires in %s", checkNum, totalChecks, formatDuration(remaining)),
				zap.String("expires_at", expiresAt),
				zap.Duration("remaining", remaining))
			observability.CLILogger.Info("  Consider re-authenticating soon:")
			if doctorProfile != "" {
				observability.CLILogger.Info(fmt.Sprintf("    aws sso login --profile %s", doctorProfile))
			} else {
				observability.CLILogger.Info("    aws sso login --profile <your-profile>")
			}
		} else {
			observability.CLILogger.Info(fmt.Sprintf("[%d/%d] Checking credential expiry... ✅ Valid for %s", checkNum, totalChecks, formatDuration(remaining)),
				zap.String("expires_at", expiresAt),
				zap.Duration("remaining", remaining))
		}
	} else {
		observability.CLILogger.Info(fmt.Sprintf("[%d/%d] Checking credential expiry... ✅ Non-expiring credentials", checkNum, totalChecks))
	}

	return allChecks
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
func printAWSCredentialsHelp(profile string) {
	observability.CLILogger.Info("")
	observability.CLILogger.Info("To configure AWS credentials:")
	observability.CLILogger.Info("")
	if profile != "" {
		observability.CLILogger.Info(fmt.Sprintf("  For profile '%s':", profile))
		observability.CLILogger.Info(fmt.Sprintf("    aws sso login --profile %s", profile))
		observability.CLILogger.Info("")
	}
	observability.CLILogger.Info("  Options:")
	observability.CLILogger.Info("    1. For SSO profiles: aws sso login --profile <name>")
	observability.CLILogger.Info("    2. For access keys: aws configure --profile <name>")
	observability.CLILogger.Info("    3. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars")
	observability.CLILogger.Info("    4. Use IAM role when running on AWS infrastructure")
	observability.CLILogger.Info("")
	observability.CLILogger.Info("For S3-compatible storage (MinIO, Wasabi, etc.), also set:")
	observability.CLILogger.Info("  - AWS_ENDPOINT_URL or use --endpoint flag")
	observability.CLILogger.Info("")
}
