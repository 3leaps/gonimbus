package match

import (
	"errors"

	"github.com/bmatcuk/doublestar/v4"
)

// Matcher evaluates patterns against cloud object keys.
//
// A Matcher is configured with include and exclude patterns:
//   - Include patterns: object must match at least one
//   - Exclude patterns: object must not match any
//
// The Matcher is safe for concurrent use after creation.
type Matcher struct {
	includes      []pattern
	excludes      []pattern
	prefixes      []string
	includeHidden bool
}

// pattern holds a compiled pattern with its original string and derived prefix.
type pattern struct {
	raw    string
	prefix string
}

// Config configures a Matcher.
type Config struct {
	// Includes are glob patterns that objects must match (at least one).
	// Required: at least one include pattern must be specified.
	Includes []string

	// Excludes are glob patterns that objects must not match (any).
	// Optional: if empty, no excludes are applied.
	Excludes []string

	// IncludeHidden controls whether hidden files are matched.
	// Hidden files have path segments starting with '.'.
	// Default: false (hidden files are excluded).
	IncludeHidden bool
}

// Errors returned by Matcher operations.
var (
	// ErrNoIncludes is returned when no include patterns are provided.
	ErrNoIncludes = errors.New("at least one include pattern is required")

	// ErrInvalidPattern is returned when a pattern cannot be compiled.
	ErrInvalidPattern = errors.New("invalid glob pattern")
)

// PatternError wraps pattern-related errors with context.
type PatternError struct {
	Pattern string
	Err     error
}

func (e *PatternError) Error() string {
	return "pattern " + e.Pattern + ": " + e.Err.Error()
}

func (e *PatternError) Unwrap() error {
	return e.Err
}

// New creates a new Matcher from the given configuration.
//
// Patterns are normalized to handle Windows-style backslash separators
// while preserving escape sequences for literal glob metacharacters.
//
// Returns an error if:
//   - No include patterns are provided
//   - Any pattern is invalid (cannot be compiled)
func New(cfg Config) (*Matcher, error) {
	if len(cfg.Includes) == 0 {
		return nil, ErrNoIncludes
	}

	// Compile include patterns (normalize for Windows compat)
	includes := make([]pattern, 0, len(cfg.Includes))
	for _, raw := range cfg.Includes {
		normalized := NormalizePattern(raw)
		if !doublestar.ValidatePattern(normalized) {
			return nil, &PatternError{Pattern: raw, Err: ErrInvalidPattern}
		}
		includes = append(includes, pattern{
			raw:    normalized,
			prefix: DerivePrefix(normalized),
		})
	}

	// Compile exclude patterns (normalize for Windows compat)
	excludes := make([]pattern, 0, len(cfg.Excludes))
	for _, raw := range cfg.Excludes {
		normalized := NormalizePattern(raw)
		if !doublestar.ValidatePattern(normalized) {
			return nil, &PatternError{Pattern: raw, Err: ErrInvalidPattern}
		}
		excludes = append(excludes, pattern{
			raw:    normalized,
			prefix: DerivePrefix(normalized),
		})
	}

	// Derive deduplicated prefixes from normalized includes
	normalizedIncludes := make([]string, len(includes))
	for i, p := range includes {
		normalizedIncludes[i] = p.raw
	}
	prefixes := DerivePrefixes(normalizedIncludes)

	return &Matcher{
		includes:      includes,
		excludes:      excludes,
		prefixes:      prefixes,
		includeHidden: cfg.IncludeHidden,
	}, nil
}

// Match returns true if the key matches the include/exclude patterns.
//
// A key matches if:
//  1. It matches at least one include pattern
//  2. It does not match any exclude pattern
//  3. It is not hidden (unless IncludeHidden is true)
//
// Keys are matched as-is (not normalized) since cloud storage keys
// are opaque strings where any character is valid.
func (m *Matcher) Match(key string) bool {
	// Keys are used as-is - cloud storage keys are opaque
	// (NormalizeKey currently returns unchanged, but documents the intent)

	// Check hidden first (fast path)
	if !m.includeHidden && IsHidden(key) {
		return false
	}

	// Must match at least one include pattern
	matched := false
	for _, inc := range m.includes {
		if matchPattern(inc.raw, key) {
			matched = true
			break
		}
	}

	if !matched {
		return false
	}

	// Must not match any exclude pattern
	for _, exc := range m.excludes {
		if matchPattern(exc.raw, key) {
			return false
		}
	}

	return true
}

// Prefixes returns the deduplicated list prefixes for efficient listing.
//
// These prefixes can be used to filter cloud storage list operations,
// reducing the number of objects that need to be retrieved and evaluated.
//
// An empty string in the result means at least one pattern requires
// a full bucket listing (no prefix filter possible).
func (m *Matcher) Prefixes() []string {
	return m.prefixes
}

// IncludePatterns returns the raw include patterns.
func (m *Matcher) IncludePatterns() []string {
	patterns := make([]string, len(m.includes))
	for i, p := range m.includes {
		patterns[i] = p.raw
	}
	return patterns
}

// ExcludePatterns returns the raw exclude patterns.
func (m *Matcher) ExcludePatterns() []string {
	patterns := make([]string, len(m.excludes))
	for i, p := range m.excludes {
		patterns[i] = p.raw
	}
	return patterns
}

// HasEmptyPrefix returns true if any prefix is empty (requires full listing).
func (m *Matcher) HasEmptyPrefix() bool {
	for _, p := range m.prefixes {
		if p == "" {
			return true
		}
	}
	return false
}

// matchPattern matches a key against a doublestar pattern.
func matchPattern(pattern, key string) bool {
	matched, err := doublestar.Match(pattern, key)
	if err != nil {
		// Pattern was validated at construction time, so this shouldn't happen
		return false
	}
	return matched
}
