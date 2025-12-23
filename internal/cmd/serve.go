package cmd

import (
	"context"
	"net/http"
	"time"

	"github.com/fulmenhq/gofulmen/signals"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	errwrap "github.com/3leaps/gonimbus/internal/errors"
	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/internal/server"
	"github.com/3leaps/gonimbus/internal/server/handlers"
)

var (
	serverPort int
	serverHost string
)

// signalHealthChecker implements HealthChecker for signal system
type signalHealthChecker struct{}

func (s signalHealthChecker) CheckHealth(ctx context.Context) error {
	// Check if signal system is responsive
	// This is a basic check - in production you might want more sophisticated checks
	return nil // Signal handlers are registered and ready
}

// telemetryHealthChecker ensures telemetry system and exporter are available
type telemetryHealthChecker struct{}

func (telemetryHealthChecker) CheckHealth(ctx context.Context) error {
	if observability.TelemetrySystem == nil || observability.PrometheusExporter == nil {
		return errwrap.NewInternalError("telemetry system not initialized")
	}
	return nil
}

// identityHealthChecker validates app identity metadata
type identityHealthChecker struct {
	binaryName string
	envPrefix  string
	configName string
}

func (i identityHealthChecker) CheckHealth(ctx context.Context) error {
	switch {
	case i.binaryName == "":
		return errwrap.NewConfigInvalidError("app identity missing binary name")
	case i.envPrefix == "":
		return errwrap.NewConfigInvalidError("app identity missing env prefix")
	case i.configName == "":
		return errwrap.NewConfigInvalidError("app identity missing config name")
	}
	return nil
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the HTTP server",
	Long: `Start the HTTP server with graceful shutdown support.

Signal Handling:
  • Ctrl+C (SIGINT) or SIGTERM: Graceful shutdown
  • Ctrl+C twice within 2s: Force quit
  • SIGHUP: Config reload (placeholder - restart recommended)

The server will cleanly shut down the HTTP server and flush logs on shutdown.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get app identity for telemetry namespace
		identity := GetAppIdentity()
		namespace := identity.TelemetryNamespace()

		// Initialize server logger with namespace
		logLevel := viper.GetString("logging.level")
		observability.InitServerLogger(identity.BinaryName, logLevel, namespace)

		metricsPort := viper.GetInt("metrics.port")
		if metricsPort == 0 {
			metricsPort = 9090
		}

		// Initialize metrics with namespace
		if err := observability.InitMetrics(identity.BinaryName, metricsPort, namespace); err != nil {
			observability.ServerLogger.Error("Failed to initialize metrics",
				zap.Error(err))
			return errwrap.WrapInternal(cmd.Context(), err, "metrics initialization failed")
		}

		observability.ServerLogger.Info("Initializing server",
			zap.String("service", identity.BinaryName),
			zap.String("namespace", namespace),
			zap.String("version", versionInfo.Version),
			zap.String("host", serverHost),
			zap.Int("port", serverPort),
			zap.Int("metrics_port", metricsPort))

		// Initialize health manager
		handlers.InitHealthManager(versionInfo.Version)
		hm := handlers.GetHealthManager()
		hm.RegisterChecker("signal_handlers", signalHealthChecker{})
		hm.RegisterChecker("telemetry", telemetryHealthChecker{})
		hm.RegisterChecker("app_identity", identityHealthChecker{
			binaryName: identity.BinaryName,
			envPrefix:  identity.EnvPrefix,
			configName: identity.ConfigName,
		})

		// Create server
		srv := server.New(serverHost, serverPort)

		// Set app identity for handlers
		handlers.SetAppIdentity(identity)

		// Get shutdown timeout from config
		shutdownTimeout := viper.GetDuration("server.shutdown_timeout")
		if shutdownTimeout == 0 {
			shutdownTimeout = 10 * time.Second
		}

		// Register graceful shutdown handlers (LIFO order - last registered, first executed)
		// Handler 1: Flush logger (executed last)
		signals.OnShutdown(func(ctx context.Context) error {
			observability.ServerLogger.Info("Flushing logger...")
			if err := observability.ServerLogger.Sync(); err != nil {
				// Sync errors are often benign (stdout/stderr already closed)
				observability.ServerLogger.Warn("Logger sync returned error (may be benign)",
					zap.Error(err))
			}
			return nil
		})

		// Handler 2: Shutdown HTTP server (executed first)
		signals.OnShutdown(func(ctx context.Context) error {
			observability.ServerLogger.Info("Shutting down HTTP server...")
			shutdownCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
			defer cancel()

			if err := srv.Shutdown(shutdownCtx); err != nil {
				return errwrap.WrapInternal(ctx, err, "server shutdown failed")
			}

			observability.ServerLogger.Info("HTTP server stopped gracefully")
			return nil
		})

		// Register config reload handler (SIGHUP)
		signals.OnReload(func(ctx context.Context) error {
			observability.ServerLogger.Info("Received SIGHUP: attempting config reload")

			// Attempt to reload configuration
			if err := viper.ReadInConfig(); err != nil {
				if _, ok := err.(viper.ConfigFileNotFoundError); ok {
					observability.ServerLogger.Info("No config file found - using defaults and environment variables")
					return nil
				}
				observability.ServerLogger.Error("Failed to reload config file",
					zap.String("file", viper.ConfigFileUsed()),
					zap.Error(err))
				return errwrap.WrapConfigInvalid(ctx, err, "config reload failed")
			}

			observability.ServerLogger.Info("Configuration reloaded successfully",
				zap.String("file", viper.ConfigFileUsed()))

			// TODO: Add hooks for components that need to react to config changes
			// - Update log levels if changed
			// - Update metrics configuration if changed
			// - Notify other components of config changes

			return nil
		})

		// Enable double-tap force quit (Ctrl+C within 2 seconds)
		if err := signals.EnableDoubleTap(signals.DoubleTapConfig{
			Window:  2 * time.Second,
			Message: "Press Ctrl+C again within 2 seconds to force quit",
		}); err != nil {
			observability.ServerLogger.Warn("Failed to enable double-tap force quit",
				zap.Error(err))
		}

		// Start server in background goroutine
		errChan := make(chan error, 1)
		go func() {
			observability.ServerLogger.Info("Starting HTTP server...",
				zap.String("host", serverHost),
				zap.Int("port", serverPort))
			if err := srv.Start(); err != nil && err != http.ErrServerClosed {
				errChan <- err
			}
		}()

		// Start signal listener in background
		go func() {
			if err := signals.Listen(cmd.Context()); err != nil {
				observability.ServerLogger.Error("Signal handler error", zap.Error(err))
				errChan <- err
			}
		}()

		// Wait for error or shutdown completion
		if err := <-errChan; err != nil {
			return errwrap.WrapInternal(cmd.Context(), err, "server error")
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)

	serveCmd.Flags().StringVar(&serverHost, "host", "localhost", "server host")
	serveCmd.Flags().IntVarP(&serverPort, "port", "p", 8080, "server port")

	_ = viper.BindPFlag("server.host", serveCmd.Flags().Lookup("host"))
	_ = viper.BindPFlag("server.port", serveCmd.Flags().Lookup("port"))
}
