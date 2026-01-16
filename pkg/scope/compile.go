package scope

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bmatcuk/doublestar/v4"

	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/provider"
)

const defaultDelimiter = "/"
const defaultDateFormat = "2006-01-02"

type Plan struct {
	Prefixes []string
}

// Compile compiles a build.scope configuration into an explicit prefix plan.
func Compile(ctx context.Context, cfg *manifest.IndexScopeConfig, basePrefix string, lister provider.PrefixLister) (*Plan, error) {
	if cfg == nil {
		return nil, nil
	}

	switch cfg.Type {
	case "prefix_list":
		prefixes, err := compilePrefixList(cfg, basePrefix)
		if err != nil {
			return nil, err
		}
		return &Plan{Prefixes: prefixes}, nil
	case "union":
		prefixes, err := compileUnion(ctx, cfg, basePrefix, lister)
		if err != nil {
			return nil, err
		}
		return &Plan{Prefixes: prefixes}, nil
	case "date_partitions":
		prefixes, err := compileDatePartitions(ctx, cfg, basePrefix, lister)
		if err != nil {
			return nil, err
		}
		return &Plan{Prefixes: prefixes}, nil
	case "":
		return nil, errors.New("scope.type is required")
	default:
		return nil, fmt.Errorf("unsupported scope.type %q", cfg.Type)
	}
}

func RequiresPrefixLister(cfg *manifest.IndexScopeConfig) bool {
	if cfg == nil {
		return false
	}
	if cfg.Type == "date_partitions" {
		return true
	}
	if cfg.Type == "union" {
		for i := range cfg.Scopes {
			if RequiresPrefixLister(&cfg.Scopes[i]) {
				return true
			}
		}
	}
	return false
}

func compilePrefixList(cfg *manifest.IndexScopeConfig, basePrefix string) ([]string, error) {
	if len(cfg.Prefixes) == 0 {
		return nil, errors.New("scope.prefixes must not be empty")
	}

	delimiter := normalizeDelimiter(cfg.Delimiter)
	root := joinPrefix(basePrefix, cfg.BasePrefix, delimiter)

	prefixes := make([]string, 0, len(cfg.Prefixes))
	for _, raw := range cfg.Prefixes {
		prefix := strings.TrimSpace(raw)
		if prefix == "" {
			continue
		}
		prefix = strings.TrimPrefix(prefix, "/")
		prefixes = append(prefixes, joinPrefix(root, prefix, delimiter))
	}

	return normalizePrefixes(prefixes), nil
}

func compileUnion(ctx context.Context, cfg *manifest.IndexScopeConfig, basePrefix string, lister provider.PrefixLister) ([]string, error) {
	if len(cfg.Scopes) == 0 {
		return nil, errors.New("scope.scopes must not be empty")
	}

	var prefixes []string
	for i := range cfg.Scopes {
		plan, err := Compile(ctx, &cfg.Scopes[i], basePrefix, lister)
		if err != nil {
			return nil, err
		}
		if plan != nil {
			prefixes = append(prefixes, plan.Prefixes...)
		}
	}

	return normalizePrefixes(prefixes), nil
}

func compileDatePartitions(ctx context.Context, cfg *manifest.IndexScopeConfig, basePrefix string, lister provider.PrefixLister) ([]string, error) {
	if cfg.Date == nil {
		return nil, errors.New("scope.date is required")
	}
	if cfg.Date.Range == nil {
		return nil, errors.New("scope.date.range is required")
	}
	if lister == nil {
		return nil, errors.New("scope.date_partitions requires provider prefix listing")
	}

	delimiter := normalizeDelimiter(cfg.Delimiter)
	root := joinPrefix(basePrefix, cfg.BasePrefix, delimiter)
	root = ensureTrailingDelimiter(root, delimiter)

	start, end, err := parseDateRange(cfg.Date.Range)
	if err != nil {
		return nil, err
	}
	format := strings.TrimSpace(cfg.Date.Format)
	if format == "" {
		format = defaultDateFormat
	}

	segmentIndex := cfg.Date.SegmentIndex
	if segmentIndex < 0 {
		return nil, fmt.Errorf("scope.date.segment_index must be >= 0")
	}

	segments, err := buildDiscoverSegments(cfg.Discover)
	if err != nil {
		return nil, err
	}

	prefixes := []string{root}
	for index := 0; index < segmentIndex; index++ {
		segment, ok := segments[index]
		if !ok {
			return nil, fmt.Errorf("scope.discover.segments must define index %d", index)
		}

		next, err := discoverSegmentPrefixes(ctx, lister, prefixes, delimiter, segment)
		if err != nil {
			return nil, err
		}
		prefixes = next
	}

	if len(prefixes) == 0 {
		return nil, nil
	}

	var out []string
	for _, prefix := range prefixes {
		for d := start; d.Before(end); d = d.AddDate(0, 0, 1) {
			datePrefix := ensureTrailingDelimiter(prefix+formatDate(d, format), delimiter)
			out = append(out, datePrefix)
		}
	}

	return normalizePrefixes(out), nil
}

func parseDateRange(cfg *manifest.IndexScopeDateRange) (time.Time, time.Time, error) {
	if cfg == nil {
		return time.Time{}, time.Time{}, errors.New("scope.date.range is required")
	}
	after := strings.TrimSpace(cfg.After)
	before := strings.TrimSpace(cfg.Before)
	if after == "" || before == "" {
		return time.Time{}, time.Time{}, errors.New("scope.date.range.after and scope.date.range.before are required")
	}

	start, err := match.ParseDate(after)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("scope.date.range.after: %w", err)
	}
	end, err := match.ParseDate(before)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("scope.date.range.before: %w", err)
	}

	start = start.UTC()
	end = end.UTC()
	if !start.Before(end) {
		return time.Time{}, time.Time{}, fmt.Errorf("scope.date.range.after must be before scope.date.range.before")
	}

	return start, end, nil
}

func buildDiscoverSegments(cfg *manifest.IndexScopeDiscoverConfig) (map[int]manifest.IndexScopeDiscoverSegment, error) {
	segments := make(map[int]manifest.IndexScopeDiscoverSegment)
	if cfg == nil {
		return segments, nil
	}
	for _, segment := range cfg.Segments {
		if _, exists := segments[segment.Index]; exists {
			return nil, fmt.Errorf("scope.discover.segments index %d specified more than once", segment.Index)
		}
		segments[segment.Index] = segment
	}
	return segments, nil
}

func discoverSegmentPrefixes(ctx context.Context, lister provider.PrefixLister, prefixes []string, delimiter string, segment manifest.IndexScopeDiscoverSegment) ([]string, error) {
	var out []string
	for _, prefix := range prefixes {
		children, err := listAllPrefixes(ctx, lister, prefix, delimiter)
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			value := strings.TrimPrefix(child, prefix)
			value = strings.TrimSuffix(value, delimiter)
			if value == "" {
				continue
			}
			allowed, err := segmentAllowed(value, segment)
			if err != nil {
				return nil, err
			}
			if !allowed {
				continue
			}
			out = append(out, ensureTrailingDelimiter(child, delimiter))
		}
	}

	return normalizePrefixes(out), nil
}

func segmentAllowed(value string, segment manifest.IndexScopeDiscoverSegment) (bool, error) {
	if len(segment.Allow) > 0 && !containsString(segment.Allow, value) {
		return false, nil
	}
	if len(segment.GlobAllow) > 0 {
		matchFound, err := matchAnyGlob(segment.GlobAllow, value)
		if err != nil {
			return false, err
		}
		if !matchFound {
			return false, nil
		}
	}
	if containsString(segment.Deny, value) {
		return false, nil
	}
	if len(segment.GlobDeny) > 0 {
		matched, err := matchAnyGlob(segment.GlobDeny, value)
		if err != nil {
			return false, err
		}
		if matched {
			return false, nil
		}
	}

	return true, nil
}

func matchAnyGlob(patterns []string, value string) (bool, error) {
	for _, raw := range patterns {
		pattern := match.NormalizePattern(strings.TrimSpace(raw))
		if pattern == "" {
			continue
		}
		if !doublestar.ValidatePattern(pattern) {
			return false, fmt.Errorf("invalid glob pattern %q", raw)
		}
		matched, err := doublestar.Match(pattern, value)
		if err != nil {
			return false, fmt.Errorf("match glob %q: %w", raw, err)
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

func listAllPrefixes(ctx context.Context, lister provider.PrefixLister, prefix string, delimiter string) ([]string, error) {
	var token string
	var out []string
	for {
		res, err := lister.ListCommonPrefixes(ctx, provider.ListCommonPrefixesOptions{Prefix: prefix, Delimiter: delimiter, ContinuationToken: token})
		if err != nil {
			return nil, err
		}
		out = append(out, res.Prefixes...)
		if !res.IsTruncated || res.ContinuationToken == "" {
			return out, nil
		}
		token = res.ContinuationToken
	}
}

func normalizePrefixes(prefixes []string) []string {
	if len(prefixes) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(prefixes))
	out := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		trimmed := strings.TrimSpace(prefix)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	if len(out) == 0 {
		return nil
	}

	sort.Strings(out)
	return out
}

func joinPrefix(basePrefix string, extra string, delimiter string) string {
	if delimiter == "" {
		delimiter = defaultDelimiter
	}
	base := strings.TrimSpace(basePrefix)
	if base != "" && !strings.HasSuffix(base, delimiter) {
		base += delimiter
	}
	segment := strings.TrimSpace(extra)
	segment = strings.TrimPrefix(segment, "/")
	if segment == "" {
		return base
	}
	return base + segment
}

func ensureTrailingDelimiter(prefix string, delimiter string) string {
	if prefix == "" {
		return prefix
	}
	if delimiter == "" {
		delimiter = defaultDelimiter
	}
	if strings.HasSuffix(prefix, delimiter) {
		return prefix
	}
	return prefix + delimiter
}

func normalizeDelimiter(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultDelimiter
	}
	return value
}

func formatDate(value time.Time, format string) string {
	if format == "" {
		format = defaultDateFormat
	}
	return value.UTC().Format(format)
}

func containsString(list []string, value string) bool {
	for _, item := range list {
		if strings.TrimSpace(item) == value {
			return true
		}
	}
	return false
}
