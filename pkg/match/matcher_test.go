package match

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name        string
		cfg         Config
		wantErr     error
		wantErrType interface{}
	}{
		{
			name:    "valid single include",
			cfg:     Config{Includes: []string{"data/**"}},
			wantErr: nil,
		},
		{
			name:    "valid with excludes",
			cfg:     Config{Includes: []string{"data/**"}, Excludes: []string{"**/_temporary/**"}},
			wantErr: nil,
		},
		{
			name:    "no includes",
			cfg:     Config{},
			wantErr: ErrNoIncludes,
		},
		{
			name:    "empty includes slice",
			cfg:     Config{Includes: []string{}},
			wantErr: ErrNoIncludes,
		},
		{
			name:        "invalid include pattern",
			cfg:         Config{Includes: []string{"[invalid"}},
			wantErrType: &PatternError{},
		},
		{
			name:        "invalid exclude pattern",
			cfg:         Config{Includes: []string{"**"}, Excludes: []string{"[invalid"}},
			wantErrType: &PatternError{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := New(tt.cfg)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tt.wantErr))
				assert.Nil(t, m)
			} else if tt.wantErrType != nil {
				require.Error(t, err)
				assert.IsType(t, tt.wantErrType, err)
				assert.Nil(t, m)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, m)
			}
		})
	}
}

func TestMatcher_Match(t *testing.T) {
	tests := []struct {
		name     string
		includes []string
		excludes []string
		hidden   bool
		key      string
		expected bool
	}{
		// Basic matching
		{"simple match", []string{"**/*.txt"}, nil, false, "file.txt", true},
		{"simple no match", []string{"**/*.txt"}, nil, false, "file.json", false},
		{"nested match", []string{"data/**/*.parquet"}, nil, false, "data/2024/01/file.parquet", true},
		{"nested no match", []string{"data/**/*.parquet"}, nil, false, "logs/file.parquet", false},

		// Exclude patterns
		{"excluded", []string{"**/*"}, []string{"**/*.log"}, false, "file.log", false},
		{"not excluded", []string{"**/*"}, []string{"**/*.log"}, false, "file.txt", true},
		{"temp excluded", []string{"data/**"}, []string{"**/_temporary/**"}, false, "data/_temporary/file.txt", false},
		{"temp not excluded", []string{"data/**"}, []string{"**/_temporary/**"}, false, "data/real/file.txt", true},

		// Hidden file handling
		{"hidden excluded by default", []string{"**/*"}, nil, false, ".hidden", false},
		{"hidden dir excluded by default", []string{"**/*"}, nil, false, ".git/config", false},
		{"hidden included when enabled", []string{"**/*"}, nil, true, ".hidden", true},
		{"hidden dir included when enabled", []string{"**/*"}, nil, true, ".git/config", true},
		{"hidden in path excluded", []string{"**/*"}, nil, false, "path/.hidden/file.txt", false},

		// Multiple includes (OR)
		{"multi include first", []string{"*.txt", "*.json"}, nil, false, "file.txt", true},
		{"multi include second", []string{"*.txt", "*.json"}, nil, false, "file.json", true},
		{"multi include none", []string{"*.txt", "*.json"}, nil, false, "file.csv", false},

		// Keys are opaque - no normalization applied
		// Backslash in key is treated as literal character (S3 allows this)
		{"backslash in key literal", []string{"data/**"}, nil, false, "data\\file.txt", false},
		// Pattern with leading slash matches key with leading slash
		{"leading slash in pattern and key", []string{"/data/**"}, nil, false, "/data/file.txt", true},
		// Pattern without leading slash does not match key with leading slash
		{"leading slash mismatch", []string{"data/**"}, nil, false, "/data/file.txt", false},
		// Pattern without leading slash matches key without leading slash
		{"no leading slash", []string{"data/**"}, nil, false, "data/file.txt", true},

		// Edge cases
		{"empty key", []string{"**"}, nil, false, "", true},
		{"exact match", []string{"exact/file.txt"}, nil, false, "exact/file.txt", true},
		{"exact no match", []string{"exact/file.txt"}, nil, false, "exact/other.txt", false},

		// Real-world patterns
		{"parquet files", []string{"data/**/*.parquet"}, []string{"**/_temporary/**", "**/.spark-*/**"}, false, "data/2024/01/data.parquet", true},
		{"spark temp", []string{"data/**/*.parquet"}, []string{"**/_temporary/**"}, false, "data/_temporary/part-00000.parquet", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := New(Config{
				Includes:      tt.includes,
				Excludes:      tt.excludes,
				IncludeHidden: tt.hidden,
			})
			require.NoError(t, err)

			result := m.Match(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatcher_Prefixes(t *testing.T) {
	tests := []struct {
		name     string
		includes []string
		expected []string
	}{
		{"single pattern", []string{"data/2024/**"}, []string{"data/2024/"}},
		{"multiple patterns", []string{"data/2024/**", "data/2025/**"}, []string{"data/2024/", "data/2025/"}},
		{"parent subsumes", []string{"data/**", "data/2024/**"}, []string{"data/"}},
		{"wildcard at start", []string{"**/*.json"}, []string{""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := New(Config{Includes: tt.includes})
			require.NoError(t, err)

			result := m.Prefixes()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatcher_HasEmptyPrefix(t *testing.T) {
	tests := []struct {
		name     string
		includes []string
		expected bool
	}{
		{"no empty", []string{"data/2024/**"}, false},
		{"has empty", []string{"**/*.json"}, true},
		{"mixed", []string{"data/**", "**/*.json"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := New(Config{Includes: tt.includes})
			require.NoError(t, err)

			result := m.HasEmptyPrefix()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatcher_IncludePatterns(t *testing.T) {
	m, err := New(Config{Includes: []string{"data/**", "logs/**"}})
	require.NoError(t, err)

	patterns := m.IncludePatterns()
	assert.Equal(t, []string{"data/**", "logs/**"}, patterns)
}

func TestMatcher_ExcludePatterns(t *testing.T) {
	m, err := New(Config{
		Includes: []string{"**"},
		Excludes: []string{"**/_temporary/**", "**/.git/**"},
	})
	require.NoError(t, err)

	patterns := m.ExcludePatterns()
	assert.Equal(t, []string{"**/_temporary/**", "**/.git/**"}, patterns)
}

func TestPatternError(t *testing.T) {
	err := &PatternError{Pattern: "[invalid", Err: ErrInvalidPattern}

	assert.Equal(t, "pattern [invalid: invalid glob pattern", err.Error())
	assert.True(t, errors.Is(err, ErrInvalidPattern))
	assert.Equal(t, ErrInvalidPattern, err.Unwrap())
}

// Benchmark Match - this is the hot path
func BenchmarkMatcher_Match(b *testing.B) {
	m, _ := New(Config{
		Includes: []string{"data/**/*.parquet", "data/**/*.csv"},
		Excludes: []string{"**/_temporary/**", "**/.spark-*/**"},
	})

	key := "data/year=2024/month=01/day=15/part-00000.parquet"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Match(key)
	}
}

func BenchmarkMatcher_Match_NoMatch(b *testing.B) {
	m, _ := New(Config{
		Includes: []string{"data/**/*.parquet"},
	})

	key := "logs/2024/01/15/app.log"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Match(key)
	}
}

func BenchmarkMatcher_Match_Hidden(b *testing.B) {
	m, _ := New(Config{
		Includes: []string{"**/*"},
	})

	key := "data/.hidden/file.txt"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Match(key)
	}
}

func BenchmarkMatcher_Match_Excluded(b *testing.B) {
	m, _ := New(Config{
		Includes: []string{"data/**"},
		Excludes: []string{"**/_temporary/**"},
	})

	key := "data/_temporary/part-00000.parquet"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Match(key)
	}
}
