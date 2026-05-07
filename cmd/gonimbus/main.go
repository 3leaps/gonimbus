package main

import (
	"github.com/fulmenhq/gofulmen/foundry"

	"github.com/3leaps/gonimbus/internal/buildinfo"
	"github.com/3leaps/gonimbus/internal/cmd"
	"github.com/3leaps/gonimbus/internal/server/handlers"
)

// Build-time identification.
//
// `make build` injects values into these symbols via -ldflags. When the binary
// is produced by a path that does not run the Makefile (e.g. `go install
// github.com/3leaps/gonimbus/cmd/gonimbus@v0.2.0` or `go install ./cmd/...`),
// these stay at their defaults and buildinfo.Resolve falls back to
// runtime/debug.ReadBuildInfo and the embedded VERSION file. See
// internal/buildinfo and gonimbus#6.
var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

func main() {
	v, c, b := buildinfo.Resolve(version, commit, buildDate)

	// Set version info for commands to access
	cmd.SetVersionInfo(v, c, b)

	// Set version info for HTTP handlers
	handlers.SetVersionInfo(v, c, b)

	// Execute root command
	if err := cmd.Execute(); err != nil {
		// Command execution failed - delegate to exit helper
		// Individual commands may have already logged specific errors
		cmd.ExitWithCodeStderr(foundry.ExitFailure, "Command execution failed", err)
	}
}
