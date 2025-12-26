// Package match provides pattern matching for cloud object keys using
// doublestar semantics with prefix derivation for efficient listing.
package match

import (
	"strings"
)

// Glob metacharacters that can be escaped with backslash in patterns.
const globEscapable = `*?[]{}\`

// NormalizeKey returns a cloud object key unchanged.
//
// Cloud storage keys (S3, GCS) are opaque strings where any character
// is valid, including /, //, leading /, *, ?, etc. We preserve key
// identity exactly as returned by the provider.
//
// This function exists for documentation and potential future options.
// Currently it returns the key unchanged.
func NormalizeKey(key string) string {
	return key
}

// NormalizePattern converts a user-provided glob pattern to canonical form.
//
// Normalization rules:
//   - Unescaped backslashes converted to forward slashes (Windows compat)
//   - Escaped backslashes and glob metacharacters preserved (\*, \?, \[, etc.)
//   - Leading slash, trailing slash, and // sequences preserved
//
// This allows Windows users to write patterns like "data\2024\**\*.parquet"
// while preserving escape semantics for literal matching.
//
// Examples:
//
//	"data/2024/**"        → "data/2024/**"       (unchanged)
//	"data\2024\**"        → "data/2024/**"       (backslash → slash)
//	"data/file\*.txt"     → "data/file\*.txt"    (escape preserved)
//	"data\\backup\\*"     → "data/backup/*"      (unescaped \ → /)
//	"/data/2024/**"       → "/data/2024/**"      (leading slash preserved)
//	"data//2024/**"       → "data//2024/**"      (// preserved)
func NormalizePattern(pattern string) string {
	if pattern == "" {
		return ""
	}

	var result strings.Builder
	result.Grow(len(pattern))

	runes := []rune(pattern)
	for i := 0; i < len(runes); i++ {
		r := runes[i]

		if r == '\\' && i+1 < len(runes) {
			next := runes[i+1]
			// Check if this is an escape sequence for a glob metacharacter
			if strings.ContainsRune(globEscapable, next) {
				// Preserve the escape sequence
				result.WriteRune('\\')
				result.WriteRune(next)
				i++ // Skip the next character
				continue
			}
			// Unescaped backslash - convert to forward slash
			result.WriteRune('/')
			continue
		}

		if r == '\\' {
			// Trailing backslash - convert to forward slash
			result.WriteRune('/')
			continue
		}

		result.WriteRune(r)
	}

	return result.String()
}

// Normalize is deprecated. Use NormalizeKey for keys or NormalizePattern for patterns.
//
// This function exists for backward compatibility and calls NormalizePattern.
// New code should use the specific functions.
func Normalize(s string) string {
	return NormalizePattern(s)
}

// IsHidden returns true if any path segment starts with a dot.
//
// Hidden segments follow Unix convention where files/directories
// starting with '.' are considered hidden.
//
// The key is matched as-is without normalization, using '/' as separator.
//
// Examples:
//
//	"path/to/file.txt"      → false
//	".hidden/file.txt"      → true
//	"path/.hidden/file.txt" → true
//	"path/to/.gitignore"    → true
//	"path/to/file.txt."     → false (dot at end is not hidden)
func IsHidden(key string) bool {
	if key == "" {
		return false
	}

	// Check each segment using / as separator
	// Keys from cloud storage use / natively
	segments := strings.Split(key, "/")
	for _, seg := range segments {
		if seg != "" && strings.HasPrefix(seg, ".") {
			return true
		}
	}

	return false
}

// SplitKeySegments splits a key into path segments using / as separator.
// Empty segments are preserved (for keys with // sequences).
func SplitKeySegments(key string) []string {
	if key == "" {
		return nil
	}
	return strings.Split(key, "/")
}

// JoinKeySegments joins path segments into a key using / as separator.
func JoinKeySegments(segments []string) string {
	if len(segments) == 0 {
		return ""
	}
	return strings.Join(segments, "/")
}

// HasTrailingSlash returns true if the key ends with a slash.
// This typically indicates a prefix/directory rather than a file.
func HasTrailingSlash(key string) bool {
	return len(key) > 0 && key[len(key)-1] == '/'
}

// EnsureTrailingSlash adds a trailing slash if not present.
// Returns empty string unchanged.
func EnsureTrailingSlash(key string) string {
	if key == "" {
		return ""
	}
	if !HasTrailingSlash(key) {
		return key + "/"
	}
	return key
}
