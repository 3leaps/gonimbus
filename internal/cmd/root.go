package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/fulmenhq/gofulmen/appidentity"
	"github.com/fulmenhq/gofulmen/foundry"

	"github.com/3leaps/gonimbus/internal/appid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
)

var (
	cfgFile  string
	verbose  bool
	readOnly bool

	// App identity loaded from .fulmen/app.yaml
	appIdentity *appidentity.Identity

	// Version info set by main package
	versionInfo struct {
		Version   string
		Commit    string
		BuildDate string
	}
)

// SetVersionInfo is called by main package to set version information
func SetVersionInfo(version, commit, buildDate string) {
	versionInfo.Version = version
	versionInfo.Commit = commit
	versionInfo.BuildDate = buildDate
}

// GetAppIdentity returns the loaded app identity (only valid after initConfig)
func GetAppIdentity() *appidentity.Identity {
	return appIdentity
}

// IsReadOnly reports whether the CLI is running with provider-side mutations
// disabled. It can be enabled via `--readonly` or by setting the environment
// variable `GONIMBUS_READONLY=1`.
func IsReadOnly() bool {
	if readOnly {
		return true
	}
	if viper.GetBool("readonly") {
		return true
	}
	if value, ok := os.LookupEnv("GONIMBUS_READONLY"); ok {
		if value == "" {
			return true
		}
		b, err := strconv.ParseBool(value)
		if err == nil {
			return b
		}
		// If set but unparsable, treat as enabled.
		return true
	}
	return false
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	// NOTE: initConfig() overwrites these from app identity.
	Use:   filepath.Base(os.Args[0]),
	Short: "A Fulmen workhorse application template",
	Long: `A production-ready Fulmen workhorse service template.

Use the subcommands to perform specific operations.`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	// Global flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (optional; defaults to app identity config path)")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output (sets log level to debug)")
	rootCmd.PersistentFlags().BoolVar(&readOnly, "readonly", false, "Disable provider-side mutations (blocks transfers and write-probe preflight)")

	// Bind flags to viper
	_ = viper.BindPFlag("verbose", rootCmd.PersistentFlags().Lookup("verbose"))
	_ = viper.BindPFlag("readonly", rootCmd.PersistentFlags().Lookup("readonly"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	// Load app identity from .fulmen/app.yaml
	ctx := context.Background()
	identity, err := appid.Get(ctx)
	if err != nil {
		ExitWithCodeStderr(foundry.ExitFileNotFound, "Failed to load app identity from .fulmen/app.yaml", err)
	}
	appIdentity = identity

	// Update CLI help surfaces from app identity (CDRL-friendly)
	if identity != nil {
		if identity.BinaryName != "" {
			rootCmd.Use = identity.BinaryName
		}
		if identity.Description != "" {
			rootCmd.Short = identity.Description
			rootCmd.Long = fmt.Sprintf("%s - %s\n\nUse the subcommands to perform specific operations.", identity.BinaryName, identity.Description)
		}
		if f := rootCmd.PersistentFlags().Lookup("config"); f != nil && identity.ConfigName != "" {
			f.Usage = fmt.Sprintf("config file (default is $XDG_CONFIG_HOME/%s/config.yaml)", identity.ConfigName)
		}
	}

	// Initialize CLI logger early so we can use it in config loading
	observability.InitCLILogger(appIdentity.BinaryName, verbose)

	if cfgFile != "" {
		// Use config file from flag
		viper.SetConfigFile(cfgFile)
	} else {
		// Find XDG config directory
		configDir, err := os.UserConfigDir()
		if err != nil {
			if verbose {
				observability.CLILogger.Warn("Could not find user config directory", zap.Error(err))
			}
			// Fall back to home directory
			home, err := os.UserHomeDir()
			if err != nil {
				ExitWithCode(observability.CLILogger, foundry.ExitFileNotFound, "Could not find home directory", err)
			}
			viper.AddConfigPath(home)
			viper.SetConfigName("." + appIdentity.ConfigName)
		} else {
			// Use XDG config directory with app identity
			appConfigDir := configDir + "/" + appIdentity.ConfigName
			viper.AddConfigPath(appConfigDir)
			viper.SetConfigName("config")

			// Also check legacy location (older templates hardcoded a name)
			oldConfigDir := configDir + "/" + appIdentity.BinaryName
			viper.AddConfigPath(oldConfigDir)
		}

		// Also search in current directory
		viper.AddConfigPath("./config")
		viper.SetConfigType("yaml")
	}

	// Read in environment variables with prefix from app identity.
	// App identity prefixes end with an underscore (e.g., "GONIMBUS_"), while
	// Viper adds its own underscore separator. Trim the trailing underscore so
	// env vars remain idiomatic: "GONIMBUS_READONLY", not "GONIMBUS__READONLY".
	viper.SetEnvPrefix(strings.TrimSuffix(appIdentity.EnvPrefix, "_"))
	viper.AutomaticEnv()

	// If a config file is found, read it in
	if err := viper.ReadInConfig(); err == nil {
		if verbose {
			observability.CLILogger.Debug("Using config file", zap.String("path", viper.ConfigFileUsed()))
		}
	} else {
		// It's OK if config file doesn't exist, we have defaults
		if verbose {
			if _, ok := err.(viper.ConfigFileNotFoundError); ok {
				observability.CLILogger.Debug("No config file found, using defaults and environment variables")
			} else {
				observability.CLILogger.Warn("Error reading config file", zap.Error(err))
			}
		}
	}

	// Set defaults
	setDefaults()
}

// setDefaults sets default configuration values
func setDefaults() {
	// Server defaults
	viper.SetDefault("server.host", "localhost")
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("server.read_timeout", "30s")
	viper.SetDefault("server.write_timeout", "30s")
	viper.SetDefault("server.idle_timeout", "120s")
	viper.SetDefault("server.shutdown_timeout", "10s")

	// Logging defaults
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.profile", "structured")

	// Metrics defaults
	viper.SetDefault("metrics.enabled", true)
	viper.SetDefault("metrics.port", 9090)

	// Health check defaults
	viper.SetDefault("health.enabled", true)

	// Worker defaults
	viper.SetDefault("workers", 4)

	// Safety defaults
	viper.SetDefault("readonly", false)

	// Debug defaults
	viper.SetDefault("debug.enabled", false)
	viper.SetDefault("debug.pprof_enabled", false)
}
