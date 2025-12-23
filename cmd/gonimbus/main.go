package main

import (
	"github.com/fulmenhq/gofulmen/foundry"

	"github.com/3leaps/gonimbus/internal/cmd"
	"github.com/3leaps/gonimbus/internal/server/handlers"
)

// Version information set via ldflags during build
// Example: go build -ldflags="-X main.version=1.0.0 -X main.commit=abc123 -X main.buildDate=2025-10-28"
var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

func main() {
	// Set version info for commands to access
	cmd.SetVersionInfo(version, commit, buildDate)

	// Set version info for HTTP handlers
	handlers.SetVersionInfo(version, commit, buildDate)

	// Execute root command
	if err := cmd.Execute(); err != nil {
		// Command execution failed - delegate to exit helper
		// Individual commands may have already logged specific errors
		cmd.ExitWithCodeStderr(foundry.ExitFailure, "Command execution failed", err)
	}
}
