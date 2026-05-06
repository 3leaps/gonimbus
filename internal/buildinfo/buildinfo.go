// Package buildinfo resolves build-time version information for the gonimbus
// binary. It is intentionally tiny and dependency-free so that the resolution
// logic remains predictable across build paths.
//
// Resolution priority (highest first):
//
//  1. Linker ldflags overrides — passed in via main as `version`, `commit`,
//     `buildDate` symbols. `make build` sets these from the VERSION file,
//     git rev-parse, and current UTC. This is the historical mechanism and
//     remains authoritative when present.
//  2. runtime/debug.ReadBuildInfo — populated automatically by the Go
//     toolchain. Covers `go install module@vX.Y.Z`, where `Main.Version`
//     and the `vcs.revision`/`vcs.time` build settings encode the module
//     version and commit metadata.
//  3. Embedded VERSION file — kept in sync with the repo-root VERSION via
//     the Makefile's sync-app-version target. Covers `go install ./cmd/...`
//     from a working tree (where Main.Version reports "(devel)") and any
//     other path where neither ldflags nor BuildInfo carries a real version.
//  4. The string "dev" / "unknown" fallbacks — only reached when none of the
//     above produces a value, which should be rare.
//
// See docs/architecture.md and gonimbus#6 for context.
package buildinfo

import (
	_ "embed"
	"runtime/debug"
	"strings"
)

//go:embed VERSION
var embeddedVersion string

// Resolve returns the effective version, commit, and build date for the
// running binary. ldVersion/ldCommit/ldBuildDate are the ldflags-injected
// values from the main package; pass them through unchanged.
func Resolve(ldVersion, ldCommit, ldBuildDate string) (version, commit, buildDate string) {
	// Tier 3: embedded VERSION as the baseline default.
	version = strings.TrimSpace(embeddedVersion)
	if version == "" {
		version = "dev"
	}
	commit = "unknown"
	buildDate = "unknown"

	// Tier 2: runtime/debug.ReadBuildInfo overlays version/commit/buildDate
	// when the toolchain has captured them. This is what makes
	// `go install ...@vX.Y.Z` report the correct version.
	if info, ok := debug.ReadBuildInfo(); ok {
		if v := normalizeModuleVersion(info.Main.Version); v != "" {
			version = v
		}
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				if s.Value != "" {
					commit = shortCommit(s.Value)
				}
			case "vcs.time":
				if s.Value != "" {
					buildDate = s.Value
				}
			}
		}
	}

	// Tier 1: ldflags-injected values take final precedence when explicitly
	// set. We treat the placeholder defaults ("dev"/"unknown") as
	// "unspecified" so we don't clobber better information from BuildInfo or
	// the embed when the binary was built without ldflags.
	if ldVersion != "" && ldVersion != "dev" {
		version = ldVersion
	}
	if ldCommit != "" && ldCommit != "unknown" {
		commit = ldCommit
	}
	if ldBuildDate != "" && ldBuildDate != "unknown" {
		buildDate = ldBuildDate
	}
	return version, commit, buildDate
}

// normalizeModuleVersion strips the leading "v" from a Go module version so
// it matches the format in the VERSION file (e.g., "v0.2.0" → "0.2.0").
// Returns the empty string for placeholder/unset values.
func normalizeModuleVersion(v string) string {
	if v == "" || v == "(devel)" {
		return ""
	}
	return strings.TrimPrefix(v, "v")
}

// shortCommit truncates a full git SHA to the conventional 7-character form,
// matching what `git rev-parse --short=7` produces in the Makefile.
func shortCommit(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}
