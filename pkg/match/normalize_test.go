package match

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeKey(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty string", "", ""},
		{"simple path", "path/to/file.txt", "path/to/file.txt"},
		{"leading slash preserved", "/path/to/file.txt", "/path/to/file.txt"},
		{"double slashes preserved", "path//to//file.txt", "path//to//file.txt"},
		{"backslashes preserved", "path\\to\\file.txt", "path\\to\\file.txt"},
		{"trailing slash preserved", "path/to/dir/", "path/to/dir/"},
		{"special chars preserved", "path/file*.txt", "path/file*.txt"},
		{"s3 key with spaces", "path/file name.txt", "path/file name.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeKey(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNormalizePattern(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Basic cases
		{"empty string", "", ""},
		{"simple path", "path/to/file.txt", "path/to/file.txt"},
		{"glob pattern", "data/**/*.parquet", "data/**/*.parquet"},

		// Backslash to forward slash conversion (Windows compat)
		{"backslashes converted", "path\\to\\file.txt", "path/to/file.txt"},
		{"mixed slashes", "path\\to/file.txt", "path/to/file.txt"},
		{"trailing backslash", "path\\to\\dir\\", "path/to/dir/"},

		// Escape sequences preserved
		{"escaped asterisk", "data/file\\*.txt", "data/file\\*.txt"},
		{"escaped question", "data/file\\?.txt", "data/file\\?.txt"},
		{"escaped bracket", "data/file\\[0-9\\].txt", "data/file\\[0-9\\].txt"},
		{"escaped brace", "data/file\\{a,b\\}.txt", "data/file\\{a,b\\}.txt"},
		{"escaped backslash", "data/file\\\\.txt", "data/file\\\\.txt"},

		// Mixed escapes and path separators
		{"windows path with escape", "data\\2024\\file\\*.txt", "data/2024/file\\*.txt"},
		{"escape at end", "data\\file\\*", "data/file\\*"},

		// Leading slash and // preserved (pattern identity)
		{"leading slash preserved", "/path/to/file.txt", "/path/to/file.txt"},
		{"double slashes preserved", "path//to//file.txt", "path//to//file.txt"},
		{"leading double slash preserved", "//path/to/file.txt", "//path/to/file.txt"},

		// Edge cases
		{"single backslash", "\\", "/"},
		{"double backslash", "\\\\", "\\\\"}, // \\ is escaped backslash
		{"only slashes", "///", "///"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizePattern(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNormalize_Deprecated(t *testing.T) {
	// Verify deprecated Normalize() calls NormalizePattern()
	input := "data\\2024\\file\\*.txt"
	expected := NormalizePattern(input)
	result := Normalize(input)
	assert.Equal(t, expected, result)
}

func TestIsHidden(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected bool
	}{
		{"empty string", "", false},
		{"regular file", "path/to/file.txt", false},
		{"hidden file", "path/to/.hidden", true},
		{"hidden directory", ".hidden/file.txt", true},
		{"hidden in middle", "path/.hidden/file.txt", true},
		{"dot at end", "path/to/file.txt.", false},
		{"double dot", "path/../file.txt", true},
		{"gitignore", "path/to/.gitignore", true},
		{"dot only segment", "path/./file.txt", true},
		{"aws hidden", ".aws/credentials", true},
		{"s3 temp", "_temporary/file.txt", false}, // underscore is not hidden

		// Keys with backslashes are NOT normalized - treated as opaque
		// The backslash is just another character in the key
		{"backslash in key not hidden", "path\\.hidden\\file.txt", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsHidden(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSplitKeySegments(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected []string
	}{
		{"empty string", "", nil},
		{"single segment", "file.txt", []string{"file.txt"}},
		{"multiple segments", "path/to/file.txt", []string{"path", "to", "file.txt"}},
		// Keys are not normalized - leading slash creates empty first segment
		{"with leading slash", "/path/to/file.txt", []string{"", "path", "to", "file.txt"}},
		// Keys are not normalized - trailing slash creates empty last segment
		{"with trailing slash", "path/to/dir/", []string{"path", "to", "dir", ""}},
		// Keys are not normalized - // creates empty segments
		{"double slashes", "path//to//file.txt", []string{"path", "", "to", "", "file.txt"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SplitKeySegments(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestJoinKeySegments(t *testing.T) {
	tests := []struct {
		name     string
		segments []string
		expected string
	}{
		{"nil", nil, ""},
		{"empty slice", []string{}, ""},
		{"single segment", []string{"file.txt"}, "file.txt"},
		{"multiple segments", []string{"path", "to", "file.txt"}, "path/to/file.txt"},
		// Preserves empty segments
		{"with empty first", []string{"", "path", "to"}, "/path/to"},
		{"with empty last", []string{"path", "to", ""}, "path/to/"},
		{"with empty middle", []string{"path", "", "to"}, "path//to"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := JoinKeySegments(tt.segments)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHasTrailingSlash(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected bool
	}{
		{"empty string", "", false},
		{"no trailing slash", "path/to/file.txt", false},
		{"with trailing slash", "path/to/dir/", true},
		{"only slash", "/", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HasTrailingSlash(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEnsureTrailingSlash(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{"empty string", "", ""},
		{"no trailing slash", "path/to/dir", "path/to/dir/"},
		{"with trailing slash", "path/to/dir/", "path/to/dir/"},
		{"single segment", "dir", "dir/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EnsureTrailingSlash(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Benchmark for NormalizePattern since it's called frequently
func BenchmarkNormalizePattern(b *testing.B) {
	pattern := "data\\2024\\**\\*.parquet"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NormalizePattern(pattern)
	}
}

func BenchmarkNormalizePattern_NoChange(b *testing.B) {
	pattern := "data/2024/**/*.parquet"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NormalizePattern(pattern)
	}
}

// Benchmark for IsHidden since it's called per object
func BenchmarkIsHidden(b *testing.B) {
	key := "path/to/some/deeply/nested/file.txt"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IsHidden(key)
	}
}

func BenchmarkIsHidden_Hidden(b *testing.B) {
	key := "path/to/.hidden/deeply/nested/file.txt"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IsHidden(key)
	}
}
