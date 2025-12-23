package observability

import (
	"fmt"
	"os"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/fulmenhq/gofulmen/logging"
)

var (
	// CLILogger is used for CLI commands (SIMPLE profile)
	CLILogger *logging.Logger

	// ServerLogger is used for HTTP server (STRUCTURED profile)
	ServerLogger *logging.Logger
)

// InitCLILogger initializes the CLI logger with SIMPLE profile
func InitCLILogger(serviceName string, verbose bool) {
	// Use the simplified NewCLI helper for CLI logging
	logger, err := logging.NewCLI(serviceName)
	if err != nil {
		exitWithCodeStderr(foundry.ExitConfigInvalid, "Failed to initialize CLI logger", err)
	}

	// Set level to DEBUG if verbose
	if verbose {
		logger.SetLevel(logging.DEBUG)
	}

	CLILogger = logger
}

// InitServerLogger initializes the server logger with STRUCTURED profile
// Optional namespace parameter for telemetry integration
func InitServerLogger(serviceName string, logLevel string, namespace ...string) {
	level := parseLogLevel(logLevel)

	// Build static fields with optional namespace
	staticFields := make(map[string]any)
	if len(namespace) > 0 && namespace[0] != "" {
		staticFields["namespace"] = namespace[0]
	}

	config := &logging.LoggerConfig{
		Profile:      logging.ProfileStructured,
		DefaultLevel: level,
		Service:      serviceName,
		Environment:  "production",
		StaticFields: staticFields,
		Middleware: []logging.MiddlewareConfig{
			{
				Name:    "correlation",
				Enabled: true,
				Order:   100,
				Config:  make(map[string]any),
			},
		},
		Sinks: []logging.SinkConfig{
			{
				Type:   "console",
				Format: "json",
				Console: &logging.ConsoleSinkConfig{
					Stream:   "stderr",
					Colorize: false,
				},
			},
		},
		EnableCaller:     true,
		EnableStacktrace: true,
	}

	logger, err := logging.New(config)
	if err != nil {
		exitWithCodeStderr(foundry.ExitConfigInvalid, "Failed to initialize server logger", err)
	}

	ServerLogger = logger
}

// parseLogLevel converts string log level to logging severity string
func parseLogLevel(levelStr string) string {
	switch levelStr {
	case "trace":
		return "TRACE"
	case "debug":
		return "DEBUG"
	case "info":
		return "INFO"
	case "warn", "warning":
		return "WARN"
	case "error":
		return "ERROR"
	default:
		return "INFO"
	}
}

// exitWithCodeStderr exits with a semantic exit code, writing to stderr.
// This is a local helper for logger initialization failures before CLI logger is available.
func exitWithCodeStderr(exitCode foundry.ExitCode, msg string, err error) {
	info, ok := foundry.GetExitCodeInfo(exitCode)
	if !ok {
		// Fallback if we can't get exit code info
		if err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: %s: %v (exit code: %d)\n", msg, err, exitCode)
		} else {
			fmt.Fprintf(os.Stderr, "FATAL: %s (exit code: %d)\n", msg, exitCode)
		}
		os.Exit(int(exitCode))
	}

	// Write to stderr with exit code metadata
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %s: %v\n", msg, err)
	} else {
		fmt.Fprintf(os.Stderr, "FATAL: %s\n", msg)
	}
	fmt.Fprintf(os.Stderr, "Exit Code: %d (%s) - %s\n", info.Code, info.Name, info.Description)

	os.Exit(info.Code)
}
