package match

import (
	"sort"
	"strings"
)

// DerivePrefix extracts the longest static prefix from a glob pattern.
//
// The prefix is the portion of the pattern before any unescaped glob metacharacter.
// Escaped metacharacters (\*, \?, \[, \{) are treated as literals and included
// in the prefix. This prefix can be used to efficiently filter cloud storage listings.
//
// Examples:
//
//	"data/2024/**/*.parquet" → "data/2024/"
//	"*.json"                 → ""
//	"logs/app-{a,b}/*.log"   → "logs/"
//	"exact/path/file.txt"    → "exact/path/file.txt"
//	"data/[0-9]*/*.csv"      → "data/"
//	"prefix/"                → "prefix/"
//	"data/file\*.txt"        → "data/file*.txt" (escaped * is literal)
//	"data/\[backup\]/*.log"  → "data/[backup]/" (escaped brackets are literal)
func DerivePrefix(pattern string) string {
	if pattern == "" {
		return ""
	}

	// Normalize the pattern for Windows compat
	pattern = NormalizePattern(pattern)

	// Find first unescaped metacharacter, building the unescaped prefix
	metaIdx := findFirstUnescapedMeta(pattern)

	if metaIdx == -1 {
		// No unescaped metacharacters - pattern is an exact match
		// Unescape for the S3 prefix (remove backslashes before metacharacters)
		return unescapePrefix(pattern)
	}

	if metaIdx == 0 {
		// Starts with unescaped metacharacter - no prefix
		return ""
	}

	// Extract prefix up to the unescaped metacharacter
	prefix := pattern[:metaIdx]

	// Truncate to last complete path segment
	// e.g., "data/2024-" becomes "data/" not "data/2024-"
	lastSlash := strings.LastIndex(prefix, "/")
	if lastSlash >= 0 {
		return unescapePrefix(prefix[:lastSlash+1])
	}

	// No slash before metacharacter - no usable prefix
	return ""
}

// findFirstUnescapedMeta returns the index of the first unescaped glob metacharacter
// (* ? [ {) in the pattern, or -1 if none found.
//
// This scan is necessary because simple string search (IndexAny) cannot distinguish
// between literal metacharacters (escaped with \) and glob metacharacters. Without
// this, patterns like "data/file\*.txt" would incorrectly terminate at \* even though
// the user wants to match a literal asterisk in the filename.
func findFirstUnescapedMeta(pattern string) int {
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]

		// Check for escape sequence: backslash followed by another character
		if c == '\\' && i+1 < len(pattern) {
			next := pattern[i+1]
			// If next char is a glob metacharacter or backslash, it's escaped.
			// Skip both the backslash and the escaped char so we don't treat
			// the metacharacter as a glob terminator.
			if next == '*' || next == '?' || next == '[' || next == '{' || next == '\\' {
				i++ // Skip the escaped character
				continue
			}
			// Backslash before non-meta char (e.g., \n) - not an escape sequence
			// in glob context, just continue scanning
			continue
		}

		// Found an unescaped metacharacter - this is where the glob begins
		if c == '*' || c == '?' || c == '[' || c == '{' {
			return i
		}
	}
	return -1
}

// unescapePrefix removes escape backslashes from glob metacharacters in a prefix.
// This converts the pattern prefix to the actual S3 key prefix.
//
// This step is necessary because S3 keys don't use escape sequences - they're
// opaque strings. When a user writes "data/file\*.txt" to match a literal asterisk,
// we need to send "data/file*.txt" as the S3 prefix, not "data/file\*.txt".
//
// Example: pattern "data/file\*.txt" → S3 prefix "data/file*.txt"
func unescapePrefix(prefix string) string {
	if !strings.ContainsRune(prefix, '\\') {
		return prefix // Fast path: no escapes, nothing to transform
	}

	var result strings.Builder
	result.Grow(len(prefix))

	for i := 0; i < len(prefix); i++ {
		c := prefix[i]

		if c == '\\' && i+1 < len(prefix) {
			next := prefix[i+1]
			// Remove the escape backslash, keep the literal character.
			// This transforms glob escape syntax into the actual S3 key characters.
			// Include all characters that can be escaped in glob patterns:
			// - Opening metacharacters: * ? [ {
			// - Closing brackets/braces: ] }
			// - Escape character itself: \
			if next == '*' || next == '?' || next == '[' || next == ']' ||
				next == '{' || next == '}' || next == '\\' {
				result.WriteByte(next)
				i++ // Skip past the escaped character (we just wrote it)
				continue
			}
		}

		result.WriteByte(c)
	}

	return result.String()
}

// DerivePrefixes extracts prefixes from multiple patterns and deduplicates them.
//
// The returned prefixes are:
//   - Derived from each include pattern
//   - Deduplicated (parent prefixes subsume children)
//   - Sorted for deterministic ordering
//
// Examples:
//
//	["data/2024/**", "data/2025/**"] → ["data/2024/", "data/2025/"]
//	["data/**", "data/2024/**"]      → ["data/"]  (parent subsumes child)
//	["**/*.json"]                    → [""]       (empty = full listing)
func DerivePrefixes(patterns []string) []string {
	if len(patterns) == 0 {
		return nil
	}

	// Derive prefix for each pattern
	prefixes := make([]string, 0, len(patterns))
	for _, p := range patterns {
		prefix := DerivePrefix(p)
		prefixes = append(prefixes, prefix)
	}

	// Deduplicate: remove prefixes subsumed by shorter ones
	return deduplicatePrefixes(prefixes)
}

// deduplicatePrefixes removes prefixes that are subsumed by shorter prefixes.
//
// A prefix P1 subsumes P2 if P2 starts with P1.
// For example, "data/" subsumes "data/2024/".
//
// Special case: empty string "" subsumes all prefixes (means full listing).
func deduplicatePrefixes(prefixes []string) []string {
	if len(prefixes) == 0 {
		return nil
	}

	// Check for empty prefix first (subsumes everything)
	for _, p := range prefixes {
		if p == "" {
			return []string{""}
		}
	}

	// Sort by length (shortest first) for subsumption check
	sorted := make([]string, len(prefixes))
	copy(sorted, prefixes)
	sort.Slice(sorted, func(i, j int) bool {
		return len(sorted[i]) < len(sorted[j])
	})

	// Keep only non-subsumed prefixes
	result := make([]string, 0, len(sorted))
	for _, candidate := range sorted {
		subsumed := false
		for _, existing := range result {
			if strings.HasPrefix(candidate, existing) {
				subsumed = true
				break
			}
		}
		if !subsumed {
			result = append(result, candidate)
		}
	}

	// Sort alphabetically for deterministic output
	sort.Strings(result)

	return result
}

// HasEmptyPrefix returns true if any derived prefix is empty.
// An empty prefix means a full bucket listing is required.
// This is a scale concern and callers may want to warn users.
func HasEmptyPrefix(patterns []string) bool {
	for _, p := range patterns {
		if DerivePrefix(p) == "" {
			return true
		}
	}
	return false
}

// IsGlobPattern returns true if the pattern contains unescaped glob metacharacters.
//
// This is escape-aware: literal metacharacters escaped with backslash (\*, \?, \[, \{)
// are not considered glob characters. This allows matching filenames that contain
// literal asterisks, question marks, or brackets.
//
// Examples:
//
//	"data/**/*.parquet"  → true  (unescaped glob)
//	"data/file\*.txt"    → false (escaped asterisk is literal)
//	"data/file?.csv"     → true  (unescaped question mark)
//	"path/to/file.txt"   → false (no metacharacters)
func IsGlobPattern(pattern string) bool {
	return findFirstUnescapedMeta(pattern) != -1
}
