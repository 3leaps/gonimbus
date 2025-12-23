package cmd

import (
	"fmt"
	"os"
	"runtime"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	errwrap "github.com/3leaps/gonimbus/internal/errors"
	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/fulmenhq/gofulmen/crucible"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run diagnostic checks",
	Long:  "Run diagnostic checks on the system and suggest fixes for common issues.",
	Run: func(cmd *cobra.Command, args []string) {
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

		// Check 1: Go version
		goVersion := runtime.Version()
		if goVersion >= "go1.23" {
			observability.CLILogger.Info("[1/5] Checking Go version... ✅ "+goVersion, zap.String("go_version", goVersion))
		} else {
			observability.CLILogger.Warn("[1/5] Checking Go version... ⚠️  "+goVersion+" (recommended: go1.23+)", zap.String("go_version", goVersion))
			allChecks = false
		}

		// Check 2: Crucible access
		version := crucible.GetVersion()
		if version.Crucible != "" {
			observability.CLILogger.Info("[2/5] Checking Crucible access... ✅ v"+version.Crucible, zap.String("crucible_version", version.Crucible))
		} else {
			observability.CLILogger.Error("[2/5] Checking Crucible access... ❌ Cannot access Crucible")
			ExitWithCode(observability.CLILogger, foundry.ExitExternalServiceUnavailable, "Cannot access Crucible", errwrap.NewExternalServiceError("Crucible service unavailable"))
			allChecks = false
		}

		// Check 3: Gofulmen access
		if version.Gofulmen != "" {
			observability.CLILogger.Info("[3/5] Checking Gofulmen access... ✅ v"+version.Gofulmen, zap.String("gofulmen_version", version.Gofulmen))
		} else {
			observability.CLILogger.Error("[3/5] Checking Gofulmen access... ❌ Cannot access Gofulmen")
			allChecks = false
		}

		// Check 4: Config directory
		configDir, err := os.UserConfigDir()
		if err != nil {
			observability.CLILogger.Error("[4/5] Checking config directory... ❌ Cannot find config directory", zap.Error(err))
			ExitWithCode(observability.CLILogger, foundry.ExitFileNotFound, "Cannot find config directory", errwrap.WrapInternal(cmd.Context(), err, "Cannot find config directory"))
			allChecks = false
		} else {
			observability.CLILogger.Info("[4/5] Checking config directory... ✅ "+configDir, zap.String("config_dir", configDir))
		}

		// Check 5: Environment
		observability.CLILogger.Info("[5/5] Checking environment... ✅ "+runtime.GOOS+"/"+runtime.GOARCH,
			zap.String("os", runtime.GOOS),
			zap.String("arch", runtime.GOARCH))

		observability.CLILogger.Info("")
		if allChecks {
			observability.CLILogger.Info(fmt.Sprintf("✅ All checks passed! Your %s installation is healthy.", bannerName))
		} else {
			observability.CLILogger.Warn("⚠️  Some checks failed. Review the output above for details.")
		}
		observability.CLILogger.Info("")
		observability.CLILogger.Info("=== End Diagnostics ===")
	},
}

func init() {
	rootCmd.AddCommand(doctorCmd)
}
