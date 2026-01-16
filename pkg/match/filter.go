package match

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// Filter evaluates whether an object passes filter criteria.
//
// Filters operate on ObjectSummary data available from List operations
// (key, size, lastModified). ContentType filtering requires enrichment
// via HEAD calls and is handled separately.
type Filter interface {
	// Match returns true if the object passes the filter.
	Match(obj *provider.ObjectSummary) bool

	// RequiresEnrichment returns true if filter needs HEAD metadata.
	// Size, date, and regex filters do not require enrichment.
	// ContentType filters do require enrichment.
	RequiresEnrichment() bool

	// String returns a human-readable description of the filter.
	String() string
}

// FilterConfig holds filter criteria from manifest or CLI flags.
type FilterConfig struct {
	// Size specifies min/max size constraints.
	Size *SizeFilterConfig `json:"size,omitempty" yaml:"size,omitempty"`

	// Modified specifies date range constraints.
	Modified *DateFilterConfig `json:"modified,omitempty" yaml:"modified,omitempty"`

	// ContentType specifies allowed MIME types (requires enrichment).
	ContentType []string `json:"content_type,omitempty" yaml:"content_type,omitempty"`

	// KeyRegex is a regex pattern applied to object keys after glob matching.
	KeyRegex string `json:"key_regex,omitempty" yaml:"key_regex,omitempty"`
}

// SizeFilterConfig specifies size constraints.
type SizeFilterConfig struct {
	// Min is the minimum size (inclusive). Supports human-readable: "1KB", "100MiB".
	Min string `json:"min,omitempty" yaml:"min,omitempty"`

	// Max is the maximum size (inclusive). Supports human-readable: "1GB", "100MiB".
	Max string `json:"max,omitempty" yaml:"max,omitempty"`
}

// DateFilterConfig specifies date range constraints.
type DateFilterConfig struct {
	// After filters to objects modified at or after this time (inclusive).
	// Supports ISO 8601: "2024-01-15" or "2024-01-15T10:30:00Z".
	After string `json:"after,omitempty" yaml:"after,omitempty"`

	// Before filters to objects modified before this time (exclusive end).
	// Supports ISO 8601: "2024-01-15" or "2024-01-15T10:30:00Z".
	Before string `json:"before,omitempty" yaml:"before,omitempty"`
}

// Filter errors.
var (
	ErrInvalidSize       = errors.New("invalid size value")
	ErrInvalidDate       = errors.New("invalid date value")
	ErrInvalidRegex      = errors.New("invalid regex pattern")
	ErrUnsupportedFilter = errors.New("unsupported filter")
)

// SizeFilter filters objects by size range.
type SizeFilter struct {
	min int64 // -1 means no minimum
	max int64 // -1 means no maximum
}

// NewSizeFilter creates a size filter from config.
// Returns nil if no size constraints are specified.
func NewSizeFilter(cfg *SizeFilterConfig) (*SizeFilter, error) {
	if cfg == nil {
		return nil, nil
	}

	f := &SizeFilter{min: -1, max: -1}

	if cfg.Min != "" {
		size, err := ParseSize(cfg.Min)
		if err != nil {
			return nil, fmt.Errorf("min size: %w", err)
		}
		f.min = size
	}

	if cfg.Max != "" {
		size, err := ParseSize(cfg.Max)
		if err != nil {
			return nil, fmt.Errorf("max size: %w", err)
		}
		f.max = size
	}

	// Validate min <= max if both specified
	if f.min >= 0 && f.max >= 0 && f.min > f.max {
		return nil, fmt.Errorf("%w: min (%d) > max (%d)", ErrInvalidSize, f.min, f.max)
	}

	return f, nil
}

// Match returns true if object size is within the configured range.
func (f *SizeFilter) Match(obj *provider.ObjectSummary) bool {
	if f.min >= 0 && obj.Size < f.min {
		return false
	}
	if f.max >= 0 && obj.Size > f.max {
		return false
	}
	return true
}

// RequiresEnrichment returns false - size is available from List.
func (f *SizeFilter) RequiresEnrichment() bool {
	return false
}

// String returns a human-readable description.
func (f *SizeFilter) String() string {
	switch {
	case f.min >= 0 && f.max >= 0:
		return fmt.Sprintf("size: %s - %s", FormatSize(f.min), FormatSize(f.max))
	case f.min >= 0:
		return fmt.Sprintf("size: >= %s", FormatSize(f.min))
	case f.max >= 0:
		return fmt.Sprintf("size: <= %s", FormatSize(f.max))
	default:
		return "size: any"
	}
}

// DateFilter filters objects by modification date range.
type DateFilter struct {
	after  time.Time // zero means no after constraint
	before time.Time // zero means no before constraint
}

// NewDateFilter creates a date filter from config.
// Returns nil if no date constraints are specified.
func NewDateFilter(cfg *DateFilterConfig) (*DateFilter, error) {
	if cfg == nil {
		return nil, nil
	}

	f := &DateFilter{}

	if cfg.After != "" {
		t, err := ParseDate(cfg.After)
		if err != nil {
			return nil, fmt.Errorf("after date: %w", err)
		}
		f.after = t
	}

	if cfg.Before != "" {
		t, err := ParseDate(cfg.Before)
		if err != nil {
			return nil, fmt.Errorf("before date: %w", err)
		}
		f.before = t
	}

	// Validate after < before if both specified
	if !f.after.IsZero() && !f.before.IsZero() && !f.after.Before(f.before) {
		return nil, fmt.Errorf("%w: after (%s) >= before (%s)", ErrInvalidDate, f.after, f.before)
	}

	return f, nil
}

// Match returns true if object modification time is within range.
func (f *DateFilter) Match(obj *provider.ObjectSummary) bool {
	if !f.after.IsZero() && obj.LastModified.Before(f.after) {
		return false
	}
	if !f.before.IsZero() && !obj.LastModified.Before(f.before) {
		return false
	}
	return true
}

// RequiresEnrichment returns false - lastModified is available from List.
func (f *DateFilter) RequiresEnrichment() bool {
	return false
}

// String returns a human-readable description.
func (f *DateFilter) String() string {
	switch {
	case !f.after.IsZero() && !f.before.IsZero():
		return fmt.Sprintf("modified: %s to %s", f.after.Format("2006-01-02"), f.before.Format("2006-01-02"))
	case !f.after.IsZero():
		return fmt.Sprintf("modified: on/after %s", f.after.Format("2006-01-02"))
	case !f.before.IsZero():
		return fmt.Sprintf("modified: before %s", f.before.Format("2006-01-02"))
	default:
		return "modified: any"
	}
}

// RegexFilter filters objects by key pattern.
type RegexFilter struct {
	pattern *regexp.Regexp
	raw     string
}

// NewRegexFilter creates a regex filter from pattern string.
// Returns nil if pattern is empty.
func NewRegexFilter(pattern string) (*RegexFilter, error) {
	if pattern == "" {
		return nil, nil
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidRegex, err)
	}

	return &RegexFilter{pattern: re, raw: pattern}, nil
}

// Match returns true if object key matches the regex.
func (f *RegexFilter) Match(obj *provider.ObjectSummary) bool {
	return f.pattern.MatchString(obj.Key)
}

// RequiresEnrichment returns false - key is available from List.
func (f *RegexFilter) RequiresEnrichment() bool {
	return false
}

// String returns a human-readable description.
func (f *RegexFilter) String() string {
	return fmt.Sprintf("key_regex: %s", f.raw)
}

// CompositeFilter combines multiple filters with AND semantics.
// All filters must pass for the object to match.
type CompositeFilter struct {
	filters []Filter
}

// NewCompositeFilter creates a composite filter from the given filters.
// Nil filters are ignored. Returns nil if no non-nil filters provided.
func NewCompositeFilter(filters ...Filter) *CompositeFilter {
	var nonNil []Filter
	for _, f := range filters {
		if f != nil {
			nonNil = append(nonNil, f)
		}
	}
	if len(nonNil) == 0 {
		return nil
	}
	return &CompositeFilter{filters: nonNil}
}

// NewFilterFromConfig creates a CompositeFilter from FilterConfig.
// Returns nil if no filters are configured.
func NewFilterFromConfig(cfg *FilterConfig) (*CompositeFilter, error) {
	if cfg == nil {
		return nil, nil
	}

	var filters []Filter

	// Size filter
	sizeFilter, err := NewSizeFilter(cfg.Size)
	if err != nil {
		return nil, err
	}
	if sizeFilter != nil {
		filters = append(filters, sizeFilter)
	}

	// Date filter
	dateFilter, err := NewDateFilter(cfg.Modified)
	if err != nil {
		return nil, err
	}
	if dateFilter != nil {
		filters = append(filters, dateFilter)
	}

	// Regex filter
	regexFilter, err := NewRegexFilter(cfg.KeyRegex)
	if err != nil {
		return nil, err
	}
	if regexFilter != nil {
		filters = append(filters, regexFilter)
	}

	// ContentType filtering requires enrichment (HEAD calls), which the crawler
	// pipeline does not currently implement.
	if len(cfg.ContentType) > 0 {
		return nil, fmt.Errorf("%w: content_type requires enrichment", ErrUnsupportedFilter)
	}

	if len(filters) == 0 {
		return nil, nil
	}

	return &CompositeFilter{filters: filters}, nil
}

// Match returns true if all filters pass.
func (f *CompositeFilter) Match(obj *provider.ObjectSummary) bool {
	for _, filter := range f.filters {
		if !filter.Match(obj) {
			return false
		}
	}
	return true
}

// RequiresEnrichment returns true if any filter requires enrichment.
func (f *CompositeFilter) RequiresEnrichment() bool {
	for _, filter := range f.filters {
		if filter.RequiresEnrichment() {
			return true
		}
	}
	return false
}

// String returns a human-readable description.
func (f *CompositeFilter) String() string {
	if len(f.filters) == 0 {
		return "no filters"
	}
	parts := make([]string, len(f.filters))
	for i, filter := range f.filters {
		parts[i] = filter.String()
	}
	return strings.Join(parts, ", ")
}

// Filters returns the underlying filters.
func (f *CompositeFilter) Filters() []Filter {
	return f.filters
}

// Size unit multipliers.
const (
	Byte int64 = 1

	// Base-10 (SI) units
	KB int64 = 1000
	MB int64 = 1000 * KB
	GB int64 = 1000 * MB
	TB int64 = 1000 * GB

	// Base-2 (IEC) units
	KiB int64 = 1024
	MiB int64 = 1024 * KiB
	GiB int64 = 1024 * MiB
	TiB int64 = 1024 * GiB
)

// ParseSize parses a human-readable size string.
//
// Supported formats:
//   - Raw bytes: "1024", "104857600"
//   - Base-10 (SI): "1KB", "100MB", "1GB" (1KB = 1000 bytes)
//   - Base-2 (IEC): "1KiB", "100MiB", "1GiB" (1KiB = 1024 bytes)
//   - Case insensitive: "1kb", "1KB", "1Kb" all work
//
// Note: KB/MB/GB use base-10 (SI standard), KiB/MiB/GiB use base-2 (IEC).
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, ErrInvalidSize
	}

	// Find where the numeric part ends
	numEnd := 0
	for i, c := range s {
		if c >= '0' && c <= '9' || c == '.' {
			numEnd = i + 1
		} else {
			break
		}
	}

	if numEnd == 0 {
		return 0, fmt.Errorf("%w: %q", ErrInvalidSize, s)
	}

	numStr := s[:numEnd]
	unitStr := strings.TrimSpace(s[numEnd:])

	// Parse unit
	var multiplier int64
	switch strings.ToUpper(unitStr) {
	case "", "B":
		multiplier = Byte
	case "K", "KB":
		multiplier = KB
	case "M", "MB":
		multiplier = MB
	case "G", "GB":
		multiplier = GB
	case "T", "TB":
		multiplier = TB
	case "KI", "KIB":
		multiplier = KiB
	case "MI", "MIB":
		multiplier = MiB
	case "GI", "GIB":
		multiplier = GiB
	case "TI", "TIB":
		multiplier = TiB
	default:
		return 0, fmt.Errorf("%w: unknown unit %q", ErrInvalidSize, unitStr)
	}

	// Parse numeric part
	if strings.Contains(numStr, ".") {
		num, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: %q", ErrInvalidSize, s)
		}
		if num < 0 {
			return 0, fmt.Errorf("%w: negative size", ErrInvalidSize)
		}
		if math.IsNaN(num) || math.IsInf(num, 0) {
			return 0, fmt.Errorf("%w: invalid number", ErrInvalidSize)
		}

		bytes := num * float64(multiplier)
		maxInt64Float := float64(int64(^uint64(0) >> 1))
		if bytes > maxInt64Float {
			return 0, fmt.Errorf("%w: size overflows int64", ErrInvalidSize)
		}

		return int64(bytes), nil
	}

	n, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q", ErrInvalidSize, s)
	}

	mult := uint64(multiplier)
	maxInt64 := ^uint64(0) >> 1
	if mult == 0 || n > maxInt64/mult {
		return 0, fmt.Errorf("%w: size overflows int64", ErrInvalidSize)
	}

	return int64(n * mult), nil
}

// FormatSize formats bytes as human-readable string using base-2 units.
func FormatSize(bytes int64) string {
	switch {
	case bytes >= TiB:
		return fmt.Sprintf("%.1fTiB", float64(bytes)/float64(TiB))
	case bytes >= GiB:
		return fmt.Sprintf("%.1fGiB", float64(bytes)/float64(GiB))
	case bytes >= MiB:
		return fmt.Sprintf("%.1fMiB", float64(bytes)/float64(MiB))
	case bytes >= KiB:
		return fmt.Sprintf("%.1fKiB", float64(bytes)/float64(KiB))
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}

// ParseDate parses an ISO 8601 date or datetime string.
//
// Supported formats:
//   - Date only: "2024-01-15" (interpreted as start of day UTC)
//   - Datetime: "2024-01-15T10:30:00Z"
//   - Datetime with offset: "2024-01-15T10:30:00+05:00"
//
// All times are normalized to UTC for comparison.
func ParseDate(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, ErrInvalidDate
	}

	// Try RFC3339 first (full datetime)
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}

	// Try date-only format
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t.UTC(), nil
	}

	// Try RFC3339Nano
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}

	return time.Time{}, fmt.Errorf("%w: %q", ErrInvalidDate, s)
}
