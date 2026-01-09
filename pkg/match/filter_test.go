package match

import (
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		// Raw bytes
		{name: "raw bytes", input: "1024", want: 1024},
		{name: "zero bytes", input: "0", want: 0},
		{name: "large bytes", input: "104857600", want: 104857600},

		// Base-10 (SI) units
		{name: "KB lowercase", input: "1kb", want: 1000},
		{name: "KB uppercase", input: "1KB", want: 1000},
		{name: "MB", input: "100MB", want: 100 * 1000 * 1000},
		{name: "GB", input: "1GB", want: 1000 * 1000 * 1000},
		{name: "TB", input: "2TB", want: 2 * 1000 * 1000 * 1000 * 1000},

		// Base-2 (IEC) units
		{name: "KiB", input: "1KiB", want: 1024},
		{name: "MiB", input: "100MiB", want: 100 * 1024 * 1024},
		{name: "GiB", input: "1GiB", want: 1024 * 1024 * 1024},
		{name: "TiB", input: "1TiB", want: 1024 * 1024 * 1024 * 1024},

		// Shorthand units
		{name: "K shorthand", input: "1K", want: 1000},
		{name: "M shorthand", input: "1M", want: 1000 * 1000},
		{name: "G shorthand", input: "1G", want: 1000 * 1000 * 1000},

		// Decimal values
		{name: "decimal KB", input: "1.5KB", want: 1500},
		{name: "decimal MiB", input: "2.5MiB", want: int64(2.5 * 1024 * 1024)},

		// With spaces
		{name: "space before unit", input: "100 MB", want: 100 * 1000 * 1000},
		{name: "leading space", input: " 100MB", want: 100 * 1000 * 1000},
		{name: "trailing space", input: "100MB ", want: 100 * 1000 * 1000},

		// B suffix
		{name: "explicit bytes", input: "1024B", want: 1024},

		// Error cases
		{name: "empty string", input: "", wantErr: true},
		{name: "negative", input: "-100", wantErr: true},
		{name: "negative with unit", input: "-1KB", wantErr: true},
		{name: "overflow raw bytes", input: "9223372036854775808", wantErr: true},
		{name: "overflow with unit", input: "1000000000000000000000TB", wantErr: true},
		{name: "invalid unit", input: "100XB", wantErr: true},
		{name: "no number", input: "KB", wantErr: true},
		{name: "garbage", input: "abc", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSize(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFormatSize(t *testing.T) {
	tests := []struct {
		bytes int64
		want  string
	}{
		{0, "0B"},
		{100, "100B"},
		{1023, "1023B"},
		{1024, "1.0KiB"},
		{1536, "1.5KiB"},
		{1024 * 1024, "1.0MiB"},
		{1024 * 1024 * 1024, "1.0GiB"},
		{1024 * 1024 * 1024 * 1024, "1.0TiB"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := FormatSize(tt.bytes)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseDate(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Time
		wantErr bool
	}{
		{
			name:  "date only",
			input: "2024-01-15",
			want:  time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "datetime UTC",
			input: "2024-01-15T10:30:00Z",
			want:  time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:  "datetime with offset",
			input: "2024-01-15T10:30:00+05:00",
			want:  time.Date(2024, 1, 15, 5, 30, 0, 0, time.UTC), // normalized to UTC
		},
		{
			name:  "datetime with nanoseconds",
			input: "2024-01-15T10:30:00.123456789Z",
			want:  time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC),
		},
		{
			name:  "with leading space",
			input: " 2024-01-15",
			want:  time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid format",
			input:   "01-15-2024",
			wantErr: true,
		},
		{
			name:    "garbage",
			input:   "not a date",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDate(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.True(t, tt.want.Equal(got), "want %v, got %v", tt.want, got)
		})
	}
}

func TestSizeFilter(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *SizeFilterConfig
		obj     provider.ObjectSummary
		want    bool
		wantErr bool
	}{
		{
			name: "min only - pass",
			cfg:  &SizeFilterConfig{Min: "1KB"},
			obj:  provider.ObjectSummary{Size: 2000},
			want: true,
		},
		{
			name: "min only - fail",
			cfg:  &SizeFilterConfig{Min: "1KB"},
			obj:  provider.ObjectSummary{Size: 500},
			want: false,
		},
		{
			name: "max only - pass",
			cfg:  &SizeFilterConfig{Max: "100KB"},
			obj:  provider.ObjectSummary{Size: 50000},
			want: true,
		},
		{
			name: "max only - fail",
			cfg:  &SizeFilterConfig{Max: "100KB"},
			obj:  provider.ObjectSummary{Size: 200000},
			want: false,
		},
		{
			name: "range - pass",
			cfg:  &SizeFilterConfig{Min: "1KB", Max: "100KB"},
			obj:  provider.ObjectSummary{Size: 50000},
			want: true,
		},
		{
			name: "range - below min",
			cfg:  &SizeFilterConfig{Min: "1KB", Max: "100KB"},
			obj:  provider.ObjectSummary{Size: 500},
			want: false,
		},
		{
			name: "range - above max",
			cfg:  &SizeFilterConfig{Min: "1KB", Max: "100KB"},
			obj:  provider.ObjectSummary{Size: 200000},
			want: false,
		},
		{
			name: "exact min boundary",
			cfg:  &SizeFilterConfig{Min: "1000"},
			obj:  provider.ObjectSummary{Size: 1000},
			want: true,
		},
		{
			name: "exact max boundary",
			cfg:  &SizeFilterConfig{Max: "1000"},
			obj:  provider.ObjectSummary{Size: 1000},
			want: true,
		},
		{
			name: "zero byte filter - skip empty",
			cfg:  &SizeFilterConfig{Min: "1"},
			obj:  provider.ObjectSummary{Size: 0},
			want: false,
		},
		{
			name:    "min > max error",
			cfg:     &SizeFilterConfig{Min: "100KB", Max: "1KB"},
			wantErr: true,
		},
		{
			name:    "invalid min",
			cfg:     &SizeFilterConfig{Min: "invalid"},
			wantErr: true,
		},
		{
			name:    "invalid max",
			cfg:     &SizeFilterConfig{Max: "xyz"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewSizeFilter(tt.cfg)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, f)
			assert.Equal(t, tt.want, f.Match(&tt.obj))
			assert.False(t, f.RequiresEnrichment())
		})
	}
}

func TestSizeFilter_Nil(t *testing.T) {
	f, err := NewSizeFilter(nil)
	require.NoError(t, err)
	assert.Nil(t, f)
}

func TestDateFilter(t *testing.T) {
	baseTime := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		cfg     *DateFilterConfig
		modTime time.Time
		want    bool
		wantErr bool
	}{
		{
			name:    "after only - pass",
			cfg:     &DateFilterConfig{After: "2024-01-01"},
			modTime: baseTime,
			want:    true,
		},
		{
			name:    "after only - fail",
			cfg:     &DateFilterConfig{After: "2024-12-01"},
			modTime: baseTime,
			want:    false,
		},
		{
			name:    "before only - pass",
			cfg:     &DateFilterConfig{Before: "2024-12-01"},
			modTime: baseTime,
			want:    true,
		},
		{
			name:    "before only - fail",
			cfg:     &DateFilterConfig{Before: "2024-01-01"},
			modTime: baseTime,
			want:    false,
		},
		{
			name:    "range - pass",
			cfg:     &DateFilterConfig{After: "2024-01-01", Before: "2024-12-31"},
			modTime: baseTime,
			want:    true,
		},
		{
			name:    "range - before range",
			cfg:     &DateFilterConfig{After: "2024-07-01", Before: "2024-12-31"},
			modTime: baseTime,
			want:    false,
		},
		{
			name:    "range - after range",
			cfg:     &DateFilterConfig{After: "2024-01-01", Before: "2024-03-01"},
			modTime: baseTime,
			want:    false,
		},
		{
			name:    "exact boundary - after is exclusive",
			cfg:     &DateFilterConfig{After: "2024-06-15"},
			modTime: time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
			want:    false, // after is exclusive
		},
		{
			name:    "exact boundary - before is exclusive",
			cfg:     &DateFilterConfig{Before: "2024-06-15"},
			modTime: time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
			want:    false, // before is exclusive
		},
		{
			name:    "after >= before error",
			cfg:     &DateFilterConfig{After: "2024-12-01", Before: "2024-01-01"},
			wantErr: true,
		},
		{
			name:    "invalid after",
			cfg:     &DateFilterConfig{After: "not-a-date"},
			wantErr: true,
		},
		{
			name:    "invalid before",
			cfg:     &DateFilterConfig{Before: "garbage"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewDateFilter(tt.cfg)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, f)

			obj := &provider.ObjectSummary{LastModified: tt.modTime}
			assert.Equal(t, tt.want, f.Match(obj))
			assert.False(t, f.RequiresEnrichment())
		})
	}
}

func TestDateFilter_Nil(t *testing.T) {
	f, err := NewDateFilter(nil)
	require.NoError(t, err)
	assert.Nil(t, f)
}

func TestRegexFilter(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		key     string
		want    bool
		wantErr bool
	}{
		{
			name:    "simple match",
			pattern: "^data/.*\\.json$",
			key:     "data/file.json",
			want:    true,
		},
		{
			name:    "simple no match",
			pattern: "^data/.*\\.json$",
			key:     "data/file.csv",
			want:    false,
		},
		{
			name:    "transaction pattern",
			pattern: `TXN-\d{8}-.*\.json`,
			key:     "stores/001/TXN-20240115-batch001.json",
			want:    true,
		},
		{
			name:    "partial match",
			pattern: "foo",
			key:     "path/foobar/file.txt",
			want:    true,
		},
		{
			name:    "anchored no match",
			pattern: "^foo$",
			key:     "foobar",
			want:    false,
		},
		{
			name:    "case sensitive",
			pattern: "DATA",
			key:     "data/file.json",
			want:    false,
		},
		{
			name:    "case insensitive flag",
			pattern: "(?i)DATA",
			key:     "data/file.json",
			want:    true,
		},
		{
			name:    "invalid regex",
			pattern: "[invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewRegexFilter(tt.pattern)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, f)

			obj := &provider.ObjectSummary{Key: tt.key}
			assert.Equal(t, tt.want, f.Match(obj))
			assert.False(t, f.RequiresEnrichment())
		})
	}
}

func TestRegexFilter_Empty(t *testing.T) {
	f, err := NewRegexFilter("")
	require.NoError(t, err)
	assert.Nil(t, f)
}

func TestCompositeFilter(t *testing.T) {
	// Object that passes all filters
	goodObj := &provider.ObjectSummary{
		Key:          "data/TXN-20240615-batch.json",
		Size:         50000,
		LastModified: time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC),
	}

	// Object that fails size
	smallObj := &provider.ObjectSummary{
		Key:          "data/TXN-20240615-batch.json",
		Size:         100,
		LastModified: time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC),
	}

	// Object that fails date
	oldObj := &provider.ObjectSummary{
		Key:          "data/TXN-20240115-batch.json",
		Size:         50000,
		LastModified: time.Date(2023, 1, 15, 12, 0, 0, 0, time.UTC),
	}

	// Object that fails regex
	wrongNameObj := &provider.ObjectSummary{
		Key:          "data/other-file.json",
		Size:         50000,
		LastModified: time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC),
	}

	sizeFilter, _ := NewSizeFilter(&SizeFilterConfig{Min: "1KB"})
	dateFilter, _ := NewDateFilter(&DateFilterConfig{After: "2024-01-01"})
	regexFilter, _ := NewRegexFilter(`TXN-\d{8}`)

	composite := NewCompositeFilter(sizeFilter, dateFilter, regexFilter)
	require.NotNil(t, composite)

	t.Run("all filters pass", func(t *testing.T) {
		assert.True(t, composite.Match(goodObj))
	})

	t.Run("size filter fails", func(t *testing.T) {
		assert.False(t, composite.Match(smallObj))
	})

	t.Run("date filter fails", func(t *testing.T) {
		assert.False(t, composite.Match(oldObj))
	})

	t.Run("regex filter fails", func(t *testing.T) {
		assert.False(t, composite.Match(wrongNameObj))
	})

	t.Run("requires enrichment", func(t *testing.T) {
		assert.False(t, composite.RequiresEnrichment())
	})

	t.Run("string representation", func(t *testing.T) {
		s := composite.String()
		assert.Contains(t, s, "size")
		assert.Contains(t, s, "modified")
		assert.Contains(t, s, "key_regex")
	})
}

func TestCompositeFilter_NilFilters(t *testing.T) {
	composite := NewCompositeFilter(nil, nil, nil)
	assert.Nil(t, composite)
}

func TestCompositeFilter_SingleFilter(t *testing.T) {
	sizeFilter, _ := NewSizeFilter(&SizeFilterConfig{Min: "1KB"})
	composite := NewCompositeFilter(sizeFilter)
	require.NotNil(t, composite)

	obj := &provider.ObjectSummary{Size: 2000}
	assert.True(t, composite.Match(obj))

	smallObj := &provider.ObjectSummary{Size: 100}
	assert.False(t, composite.Match(smallObj))
}

func TestNewFilterFromConfig(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		f, err := NewFilterFromConfig(nil)
		require.NoError(t, err)
		assert.Nil(t, f)
	})

	t.Run("empty config", func(t *testing.T) {
		f, err := NewFilterFromConfig(&FilterConfig{})
		require.NoError(t, err)
		assert.Nil(t, f)
	})

	t.Run("size only", func(t *testing.T) {
		f, err := NewFilterFromConfig(&FilterConfig{
			Size: &SizeFilterConfig{Min: "1KB"},
		})
		require.NoError(t, err)
		require.NotNil(t, f)
		assert.Len(t, f.Filters(), 1)
	})

	t.Run("all filters", func(t *testing.T) {
		f, err := NewFilterFromConfig(&FilterConfig{
			Size:     &SizeFilterConfig{Min: "1KB", Max: "100MB"},
			Modified: &DateFilterConfig{After: "2024-01-01"},
			KeyRegex: `\.json$`,
		})
		require.NoError(t, err)
		require.NotNil(t, f)
		assert.Len(t, f.Filters(), 3)
	})

	t.Run("invalid size", func(t *testing.T) {
		_, err := NewFilterFromConfig(&FilterConfig{
			Size: &SizeFilterConfig{Min: "invalid"},
		})
		assert.Error(t, err)
	})

	t.Run("invalid date", func(t *testing.T) {
		_, err := NewFilterFromConfig(&FilterConfig{
			Modified: &DateFilterConfig{After: "not-a-date"},
		})
		assert.Error(t, err)
	})

	t.Run("invalid regex", func(t *testing.T) {
		_, err := NewFilterFromConfig(&FilterConfig{
			KeyRegex: "[invalid",
		})
		assert.Error(t, err)
	})
}
