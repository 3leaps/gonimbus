package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/fulmenhq/gofulmen/crucible"
)

var envInfoCmd = &cobra.Command{
	Use:   "envinfo",
	Short: "Display environment information",
	Long:  "Display comprehensive environment, configuration, and version information.",
	Run: func(cmd *cobra.Command, args []string) {
		version := crucible.GetVersion()

		observability.CLILogger.Info("=== Gonimbus Environment Information ===")
		observability.CLILogger.Info("")

		// Application Info
		identity := GetAppIdentity()
		observability.CLILogger.Info("Application:")
		observability.CLILogger.Info("  Name:       " + identity.BinaryName)
		observability.CLILogger.Info("  Version:    " + versionInfo.Version)
		observability.CLILogger.Info("  Commit:     " + versionInfo.Commit)
		observability.CLILogger.Info("  Built:      " + versionInfo.BuildDate)
		observability.CLILogger.Info("")

		// SSOT Info
		observability.CLILogger.Info("SSOT:")
		observability.CLILogger.Info("  Gofulmen:   "+version.Gofulmen, zap.String("gofulmen_version", version.Gofulmen))
		observability.CLILogger.Info("  Crucible:   "+version.Crucible, zap.String("crucible_version", version.Crucible))
		observability.CLILogger.Info("")

		// Runtime Info
		observability.CLILogger.Info("Runtime:")
		observability.CLILogger.Info("  Go Version: "+runtime.Version(), zap.String("go_version", runtime.Version()))
		observability.CLILogger.Info("  GOOS:       "+runtime.GOOS, zap.String("goos", runtime.GOOS))
		observability.CLILogger.Info("  GOARCH:     "+runtime.GOARCH, zap.String("goarch", runtime.GOARCH))
		observability.CLILogger.Info(fmt.Sprintf("  NumCPU:     %d", runtime.NumCPU()), zap.Int("num_cpu", runtime.NumCPU()))
		observability.CLILogger.Info("")

		// Configuration
		observability.CLILogger.Info("Configuration:")
		observability.CLILogger.Info("  Server Host:    "+viper.GetString("server.host"), zap.String("host", viper.GetString("server.host")))
		observability.CLILogger.Info(fmt.Sprintf("  Server Port:    %d", viper.GetInt("server.port")), zap.Int("port", viper.GetInt("server.port")))
		observability.CLILogger.Info("  Log Level:      "+viper.GetString("logging.level"), zap.String("log_level", viper.GetString("logging.level")))
		observability.CLILogger.Info("  Log Profile:    "+viper.GetString("logging.profile"), zap.String("log_profile", viper.GetString("logging.profile")))
		observability.CLILogger.Info(fmt.Sprintf("  Metrics Port:   %d", viper.GetInt("metrics.port")), zap.Int("metrics_port", viper.GetInt("metrics.port")))
		configFile := viper.ConfigFileUsed()
		if configFile == "" {
			observability.CLILogger.Info("  Config File:    (using defaults and environment variables)")
		} else {
			observability.CLILogger.Info("  Config File:    "+configFile, zap.String("config_file", configFile))
		}
		observability.CLILogger.Info("")

		observability.CLILogger.Info("=== End Environment Information ===")
	},
}

func init() {
	rootCmd.AddCommand(envInfoCmd)
}
