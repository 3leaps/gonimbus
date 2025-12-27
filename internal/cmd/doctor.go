package cmd

import (
	"context"
	"fmt"
	"os"
	"runtime"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	errwrap "github.com/3leaps/gonimbus/internal/errors"
	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/fulmenhq/gofulmen/crucible"
)

var (
	doctorProvider string
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run diagnostic checks",
	Long: `Run diagnostic checks on the system and suggest fixes for common issues.

Examples:
  gonimbus doctor              # Full environment check
  gonimbus doctor --provider s3  # S3-specific checks`,
	Run: runDoctor,
}

func init() {
	rootCmd.AddCommand(doctorCmd)
	doctorCmd.Flags().StringVar(&doctorProvider, "provider", "", "Run provider-specific checks (s3)")
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
		totalChecks = 7
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
	observability.CLILogger.Info("S3 Provider Checks:")

	// Check 6: AWS credentials
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		observability.CLILogger.Error(fmt.Sprintf("[%d/%d] Checking AWS credentials... ❌ Cannot load AWS config", checkNum, totalChecks),
			zap.Error(err))
		printAWSCredentialsHelp()
		return false
	}

	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		observability.CLILogger.Error(fmt.Sprintf("[%d/%d] Checking AWS credentials... ❌ Cannot retrieve credentials", checkNum, totalChecks),
			zap.Error(err))
		printAWSCredentialsHelp()
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

	return allChecks
}

// maskAccessKey masks all but the last 4 characters of an access key.
func maskAccessKey(key string) string {
	if len(key) <= 4 {
		return "****"
	}
	return "****" + key[len(key)-4:]
}

// printAWSCredentialsHelp prints help for configuring AWS credentials.
func printAWSCredentialsHelp() {
	observability.CLILogger.Info("")
	observability.CLILogger.Info("To configure AWS credentials:")
	observability.CLILogger.Info("  1. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables, or")
	observability.CLILogger.Info("  2. Run 'aws configure' to set up a profile, or")
	observability.CLILogger.Info("  3. Use IAM role when running on AWS infrastructure")
	observability.CLILogger.Info("")
	observability.CLILogger.Info("For S3-compatible storage (MinIO, Wasabi, etc.), also set:")
	observability.CLILogger.Info("  - AWS_ENDPOINT_URL or use --endpoint flag")
	observability.CLILogger.Info("")
}
