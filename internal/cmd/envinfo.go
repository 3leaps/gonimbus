package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/fulmenhq/gofulmen/crucible"
	"github.com/fulmenhq/gofulmen/foundry"
)

var envInfoLogFormat string

var envInfoCmd = &cobra.Command{
	Use:   "envinfo",
	Short: "Display environment information",
	Long:  "Display comprehensive environment, configuration, and version information.",
	Run: func(cmd *cobra.Command, args []string) {
		out, err := newDiagnosticPrinter(cmd, envInfoLogFormat)
		if err != nil {
			exitDiagnosticWithCode(cmd, nil, envInfoLogFormat, foundry.ExitInvalidArgument, "Invalid flags", err)
			return
		}

		version := crucible.GetVersion()

		out.Info("=== Gonimbus Environment Information ===")
		out.Info("")

		// Application Info
		identity := GetAppIdentity()
		out.Info("Application:")
		out.Info("  Name:       " + identity.BinaryName)
		out.Info("  Version:    " + versionInfo.Version)
		out.Info("  Commit:     " + versionInfo.Commit)
		out.Info("  Built:      " + versionInfo.BuildDate)
		out.Info("")

		// SSOT Info
		out.Info("SSOT:")
		out.Info("  Gofulmen:   "+version.Gofulmen, zap.String("gofulmen_version", version.Gofulmen))
		out.Info("  Crucible:   "+version.Crucible, zap.String("crucible_version", version.Crucible))
		out.Info("")

		// Runtime Info
		out.Info("Runtime:")
		out.Info("  Go Version: "+runtime.Version(), zap.String("go_version", runtime.Version()))
		out.Info("  GOOS:       "+runtime.GOOS, zap.String("goos", runtime.GOOS))
		out.Info("  GOARCH:     "+runtime.GOARCH, zap.String("goarch", runtime.GOARCH))
		out.Info(fmt.Sprintf("  NumCPU:     %d", runtime.NumCPU()), zap.Int("num_cpu", runtime.NumCPU()))
		out.Info("")

		// Configuration
		out.Info("Configuration:")
		out.Info("  Server Host:    "+viper.GetString("server.host"), zap.String("host", viper.GetString("server.host")))
		out.Info(fmt.Sprintf("  Server Port:    %d", viper.GetInt("server.port")), zap.Int("port", viper.GetInt("server.port")))
		out.Info("  Log Level:      "+viper.GetString("logging.level"), zap.String("log_level", viper.GetString("logging.level")))
		out.Info("  Log Profile:    "+viper.GetString("logging.profile"), zap.String("log_profile", viper.GetString("logging.profile")))
		out.Info(fmt.Sprintf("  Metrics Port:   %d", viper.GetInt("metrics.port")), zap.Int("metrics_port", viper.GetInt("metrics.port")))
		configFile := viper.ConfigFileUsed()
		if configFile == "" {
			out.Info("  Config File:    (using defaults and environment variables)")
		} else {
			out.Info("  Config File:    "+configFile, zap.String("config_file", configFile))
		}
		out.Info("")

		out.Info("=== End Environment Information ===")
	},
}

func init() {
	rootCmd.AddCommand(envInfoCmd)
	envInfoCmd.Flags().StringVar(&envInfoLogFormat, "log-format", diagnosticLogFormatPlain, "diagnostic output format (plain or structured)")
}
