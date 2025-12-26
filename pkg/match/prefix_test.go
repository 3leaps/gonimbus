package match

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDerivePrefix(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		expected string
	}{
		// Basic cases
		{"empty pattern", "", ""},
		{"exact match", "exact/path/file.txt", "exact/path/file.txt"},
		{"simple wildcard", "*.json", ""},
		{"wildcard at end", "data/*.json", "data/"},
		{"double star", "data/**", "data/"},
		{"double star with suffix", "data/**/*.parquet", "data/"},

		// Complex patterns
		{"brace expansion", "logs/app-{a,b}/*.log", "logs/"},
		{"character class", "data/[0-9]*/*.csv", "data/"},
		{"question mark", "data/file?.txt", "data/"},
		{"nested wildcards", "a/b/c/**/*.json", "a/b/c/"},

		// Edge cases
		{"leading wildcard", "**/file.txt", ""},
		{"wildcard in middle", "data/*/file.txt", "data/"},
		{"partial segment wildcard", "data/2024-*/*.csv", "data/"},
		{"only slash", "/", "/"},
		{"trailing slash preserved", "data/2024/", "data/2024/"},

		// Pattern normalization (Windows compat)
		// In "data\2024\**\*.csv": \2 → /2 (not escapable), but \* is escape.
		// Normalized: "data/2024\**\*.csv" where \* is literal asterisk.
		// The pattern has \* (escaped) followed by * (glob), so first unescaped
		// meta is at position 11. Prefix truncates to last / before that = "data/".
		{"backslashes with escapes", "data\\2024\\**\\*.csv", "data/"},
		// Windows path with \** also has \* (escape) + * (glob), same behavior.
		// To avoid this, Windows users should use forward slashes in glob patterns.
		{"windows path with glob", "data\\2024\\subdir\\**", "data/2024/"},
		// Windows users who want full prefix should use forward slashes for globs
		{"windows path forward glob", "data\\2024\\subdir/**", "data/2024/subdir/"},
		// Leading slash is preserved (pattern identity)
		{"leading slash preserved", "/data/2024/**", "/data/2024/"},

		// Escaped metacharacters (literal matching)
		// \* means literal asterisk, not glob - should be included in prefix
		{"escaped asterisk exact", "data/file\\*.txt", "data/file*.txt"},
		{"escaped asterisk in dir", "data/file\\*/logs/*.log", "data/file*/logs/"},
		{"escaped question mark", "data/file\\?.txt", "data/file?.txt"},
		{"escaped bracket", "data/\\[backup\\]/file.txt", "data/[backup]/file.txt"},
		{"escaped brace", "data/\\{v1\\}/file.txt", "data/{v1}/file.txt"},
		{"escaped backslash", "data/path\\\\/file.txt", "data/path\\/file.txt"},
		{"mixed escaped and glob", "data/\\[2024\\]/**/*.csv", "data/[2024]/"},
		// Edge case: escaped at segment boundary
		{"escaped asterisk before slash", "data/file\\*/*.log", "data/file*/"},

		// Real-world examples
		{"s3 parquet", "data/year=2024/**/*.parquet", "data/year=2024/"},
		{"spark temp exclude", "**/_temporary/**", ""},
		{"gitignore", "**/.git/**", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DerivePrefix(tt.pattern)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDerivePrefixes(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		expected []string
	}{
		{"nil", nil, nil},
		{"empty", []string{}, nil},
		{"single pattern", []string{"data/2024/**"}, []string{"data/2024/"}},

		// Deduplication
		{"parent subsumes child", []string{"data/**", "data/2024/**"}, []string{"data/"}},
		{"child not subsumed", []string{"data/2024/**", "data/2025/**"}, []string{"data/2024/", "data/2025/"}},
		{"multiple parents", []string{"a/**", "b/**", "a/x/**"}, []string{"a/", "b/"}},

		// Empty prefix handling
		{"empty prefix from wildcard", []string{"**/*.json"}, []string{""}},
		{"empty subsumes all", []string{"data/2024/**", "**/*.json"}, []string{""}},

		// Sorting
		{"sorted output", []string{"z/**", "a/**", "m/**"}, []string{"a/", "m/", "z/"}},

		// Real-world
		{
			"typical crawl job",
			[]string{"data/2024/**/*.parquet", "data/2024/**/*.csv"},
			[]string{"data/2024/"},
		},
		{
			"multi-year",
			[]string{"data/2023/**", "data/2024/**", "data/2025/**"},
			[]string{"data/2023/", "data/2024/", "data/2025/"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DerivePrefixes(tt.patterns)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDeduplicatePrefixes(t *testing.T) {
	tests := []struct {
		name     string
		prefixes []string
		expected []string
	}{
		{"nil", nil, nil},
		{"empty", []string{}, nil},
		{"single", []string{"data/"}, []string{"data/"}},
		{"no overlap", []string{"a/", "b/"}, []string{"a/", "b/"}},
		{"parent subsumes", []string{"data/", "data/2024/"}, []string{"data/"}},
		{"child before parent", []string{"data/2024/", "data/"}, []string{"data/"}},
		{"empty subsumes all", []string{"data/", ""}, []string{""}},
		{"multiple empty", []string{"", "", "data/"}, []string{""}},
		{"complex chain", []string{"a/b/c/", "a/b/", "a/"}, []string{"a/"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deduplicatePrefixes(tt.prefixes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHasEmptyPrefix(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		expected bool
	}{
		{"nil", nil, false},
		{"no empty", []string{"data/2024/**"}, false},
		{"has empty", []string{"**/*.json"}, true},
		{"mixed", []string{"data/**", "**/*.json"}, true},
		// Escaped patterns should NOT have empty prefix
		{"escaped asterisk not empty", []string{"data/file\\*.txt"}, false},
		{"escaped leading not empty", []string{"\\*\\*/data/*.log"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HasEmptyPrefix(tt.patterns)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Benchmark for DerivePrefix
func BenchmarkDerivePrefix(b *testing.B) {
	pattern := "data/year=2024/month=*/day=*/**/*.parquet"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DerivePrefix(pattern)
	}
}

func BenchmarkDerivePrefixes(b *testing.B) {
	patterns := []string{
		"data/2024/**/*.parquet",
		"data/2024/**/*.csv",
		"data/2025/**/*.parquet",
		"logs/**/*.log",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DerivePrefixes(patterns)
	}
}
