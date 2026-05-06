package buildinfo

import (
	"os"
	"strings"
	"testing"
)

// TestResolve_LdflagsWin verifies that ldflags-injected values take
// precedence over BuildInfo and the embedded VERSION when explicitly set.
// This is the `make build` path.
func TestResolve_LdflagsWin(t *testing.T) {
	v, c, b := Resolve("9.9.9-explicit", "deadbee", "2026-01-02T03:04:05Z")
	if v != "9.9.9-explicit" {
		t.Errorf("version: got %q, want %q (ldflags should override embed)", v, "9.9.9-explicit")
	}
	if c != "deadbee" {
		t.Errorf("commit: got %q, want %q", c, "deadbee")
	}
	if b != "2026-01-02T03:04:05Z" {
		t.Errorf("buildDate: got %q, want %q", b, "2026-01-02T03:04:05Z")
	}
}

// TestResolve_PlaceholdersFallback verifies that when ldflags carry only
// the placeholder defaults ("dev"/"unknown"), Resolve falls back to the
// embedded VERSION file rather than reporting "dev".
//
// This is the regression test for gonimbus#6: a binary produced without
// ldflags (e.g. `go install`) must still report a real version.
func TestResolve_PlaceholdersFallback(t *testing.T) {
	v, _, _ := Resolve("dev", "unknown", "unknown")
	if v == "dev" || v == "" {
		t.Fatalf("version: got %q; expected the embedded VERSION value (gonimbus#6 regression)", v)
	}
	embedded := strings.TrimSpace(embeddedVersion)
	// Under `go test` the binary is built with -buildvcs information, so
	// BuildInfo.Main.Version may report "(devel)" (which we discard) or
	// the module version when running against a tagged build. Either way,
	// Resolve must produce *something* sensible — never the literal "dev".
	if v != embedded {
		// BuildInfo overlay may have produced a different but real version;
		// accept anything non-placeholder as long as it isn't "dev".
		t.Logf("version resolved to %q (embedded VERSION is %q); both acceptable", v, embedded)
	}
}

// TestResolve_EmbeddedVersionMatchesRepo verifies that the embedded
// VERSION file is in sync with the repo-root VERSION. The Makefile's
// sync-app-version target maintains this; if it ever drifts, this test
// fails loudly so we catch it before release.
func TestResolve_EmbeddedVersionMatchesRepo(t *testing.T) {
	// Walk up from the package directory to find the repo root VERSION.
	// Tests run from the package directory, so VERSION is at ../../VERSION.
	repoVersionBytes, err := os.ReadFile("../../VERSION")
	if err != nil {
		t.Skipf("could not read repo-root VERSION (test running outside source tree?): %v", err)
	}
	repoVersion := strings.TrimSpace(string(repoVersionBytes))
	embedded := strings.TrimSpace(embeddedVersion)
	if repoVersion != embedded {
		t.Fatalf("embedded VERSION %q does not match repo-root VERSION %q; run `make sync-app-version`", embedded, repoVersion)
	}
}

// TestNormalizeModuleVersion exercises the v-prefix stripping logic.
func TestNormalizeModuleVersion(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"v0.2.0", "0.2.0"},
		{"v1.2.3-rc.4", "1.2.3-rc.4"},
		{"0.2.0", "0.2.0"},
		{"(devel)", ""},
		{"", ""},
	}
	for _, tc := range tests {
		if got := normalizeModuleVersion(tc.in); got != tc.want {
			t.Errorf("normalizeModuleVersion(%q) = %q; want %q", tc.in, got, tc.want)
		}
	}
}

// TestShortCommit verifies the SHA truncation matches `git rev-parse --short=7`.
func TestShortCommit(t *testing.T) {
	full := "abcdef1234567890abcdef1234567890abcdef12"
	if got := shortCommit(full); got != "abcdef1" {
		t.Errorf("shortCommit: got %q, want %q", got, "abcdef1")
	}
	if got := shortCommit("abc"); got != "abc" {
		t.Errorf("shortCommit short input: got %q, want %q", got, "abc")
	}
}
