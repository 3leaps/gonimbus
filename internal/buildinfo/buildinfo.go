// Package buildinfo resolves build-time version information for the gonimbus
// binary. It is intentionally tiny and dependency-free so that the resolution
// logic remains predictable across build paths.
//
// Resolution priority for the version string (highest first):
//
//  1. Linker ldflags overrides — passed in via main as the `version` symbol.
//     `make build` sets this from the VERSION file. Authoritative when set
//     to anything other than the placeholder "dev".
//  2. Embedded VERSION file — read at build time via `//go:embed VERSION`,
//     kept in sync with the repo-root VERSION via `make sync-app-version`.
//     This is the *repo's* statement of what version it is, which is what
//     we want to report regardless of how the binary was produced. Notably:
//     `go install ./cmd/...` from a working tree, `go build` from a clean
//     checkout (which would otherwise return a Go pseudo-version derived
//     from the latest tag), and any other build path without ldflags.
//  3. runtime/debug.ReadBuildInfo Main.Version — only used as a final
//     fallback if the embed is somehow empty. Generally not reached because
//     the embed is bundled with the package.
//  4. The literal "dev" — only if every source above is empty.
//
// For commit and buildDate (which are NOT in the VERSION file), the order
// is ldflags → BuildInfo (vcs.revision, vcs.time) → "unknown". BuildInfo
// is the right fallback there because `go install module@vX.Y.Z` populates
// these settings from the module source even without ldflags.
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
	// Version: embedded VERSION is the baseline (the repo's authoritative
	// statement of its own version). ldflags override it when explicitly
	// set; BuildInfo only fills in if the embed is somehow empty.
	version = strings.TrimSpace(embeddedVersion)
	commit = "unknown"
	buildDate = "unknown"

	// BuildInfo overlay for commit/buildDate (and version-as-last-resort).
	// `go install module@vX.Y.Z` populates vcs.revision/vcs.time from the
	// module source even without ldflags, which is what we want.
	if info, ok := debug.ReadBuildInfo(); ok {
		if version == "" {
			if v := normalizeModuleVersion(info.Main.Version); v != "" {
				version = v
			}
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

	if version == "" {
		version = "dev"
	}

	// ldflags-injected values take final precedence when explicitly set.
	// We treat the placeholder defaults ("dev"/"unknown") as "unspecified"
	// so we don't clobber better information from the embed or BuildInfo
	// when the binary was built without ldflags.
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
