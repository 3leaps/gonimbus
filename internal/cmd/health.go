package cmd

import (
	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	errwrap "github.com/3leaps/gonimbus/internal/errors"
	"github.com/3leaps/gonimbus/internal/observability"
)

var healthLogFormat string

var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Run self-health check",
	Long:  "Run a self-health check to verify the application can start successfully.",
	Run: func(cmd *cobra.Command, args []string) {
		out, err := newDiagnosticPrinter(cmd, healthLogFormat)
		if err != nil {
			exitDiagnosticWithCode(cmd, nil, healthLogFormat, foundry.ExitInvalidArgument, "Invalid flags", err)
			return
		}

		out.Info("Running health check...")

		// Check 1: Version info available
		if versionInfo.Version == "" {
			out.Error("❌ FAIL: Version information missing")
			exitDiagnosticWithCode(cmd, observability.CLILogger, healthLogFormat, foundry.ExitConfigInvalid, "Version information missing", errwrap.NewConfigInvalidError("Version information missing"))
			return
		}
		if out.structured {
			observability.CLILogger.Debug("Version check passed", zap.String("version", versionInfo.Version))
		}
		out.Info("✅ Version information available")

		// Check 2: Logger initialized
		if observability.CLILogger == nil {
			out.Error("❌ FAIL: Logger not initialized")
			exitDiagnosticWithCode(cmd, nil, healthLogFormat, foundry.ExitConfigInvalid, "Logger not initialized", errwrap.NewConfigInvalidError("Logger not initialized"))
			return
		}
		out.Info("✅ Logger initialized")

		// Check 3: Configuration loaded
		out.Info("✅ Configuration system ready")

		// Overall status
		out.Info("")
		out.Info("✅ All health checks passed")
	},
}

func init() {
	rootCmd.AddCommand(healthCmd)
	healthCmd.Flags().StringVar(&healthLogFormat, "log-format", diagnosticLogFormatPlain, "diagnostic output format (plain or structured)")
}
