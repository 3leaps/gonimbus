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

const (
	// MaxSegmentsTemplateDepth is the maximum number of path segments in a
	// scope.type="segments" template (ADR-0009).
	MaxSegmentsTemplateDepth = 16
	// MaxDiscoveredPrefixesPerLevel is the maximum number of discovered
	// prefixes permitted at any single segment level in scope.type="segments".
	MaxDiscoveredPrefixesPerLevel = 10000
	// MaxCompiledScopePrefixes is the maximum total compiled prefixes permitted
	// from scope.type="segments" (matches durable-index/v0 MaxPlanScopes).
	MaxCompiledScopePrefixes = 65536
)

// ErrDiscoveryBudgetExceeded is returned when a scope.type="segments" template
// exceeds template depth or discovered-prefix resource bounds.
var ErrDiscoveryBudgetExceeded = errors.New("scope discovery budget exceeded")

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
	case "segments":
		prefixes, err := compileSegments(ctx, cfg, basePrefix, lister)
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
	if cfg.Type == "date_partitions" || cfg.Type == "segments" {
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

func compileSegments(ctx context.Context, cfg *manifest.IndexScopeConfig, basePrefix string, lister provider.PrefixLister) ([]string, error) {
	template, err := CanonicalizeSegmentsTemplate(cfg)
	if err != nil {
		return nil, err
	}
	if lister == nil {
		return nil, errors.New("scope.segments requires provider prefix listing")
	}

	delimiter := normalizeDelimiter(cfg.Delimiter)
	root := joinPrefix(basePrefix, cfg.BasePrefix, delimiter)
	root = ensureTrailingDelimiter(root, delimiter)

	prefixes := []string{root}
	for _, segment := range template {
		if len(prefixes) == 0 {
			return nil, nil
		}
		if isLiteralSegmentRule(segment) {
			next := make([]string, 0, len(prefixes)*len(segment.Allow))
			for _, prefix := range prefixes {
				for _, lit := range segment.Allow {
					next = append(next, ensureTrailingDelimiter(prefix+lit, delimiter))
					if len(next) > MaxCompiledScopePrefixes {
						return nil, fmt.Errorf("%w: compiled prefix count exceeds maximum %d", ErrDiscoveryBudgetExceeded, MaxCompiledScopePrefixes)
					}
				}
			}
			prefixes = normalizePrefixes(next)
			if len(prefixes) > MaxDiscoveredPrefixesPerLevel {
				return nil, fmt.Errorf("%w: level %d prefix count %d exceeds maximum %d", ErrDiscoveryBudgetExceeded, segment.Index, len(prefixes), MaxDiscoveredPrefixesPerLevel)
			}
			continue
		}

		next, err := discoverSegmentPrefixesBounded(ctx, lister, prefixes, delimiter, segment, MaxDiscoveredPrefixesPerLevel)
		if err != nil {
			return nil, err
		}
		prefixes = next
	}

	if len(prefixes) > MaxCompiledScopePrefixes {
		return nil, fmt.Errorf("%w: compiled prefix count %d exceeds maximum %d", ErrDiscoveryBudgetExceeded, len(prefixes), MaxCompiledScopePrefixes)
	}
	return normalizePrefixes(prefixes), nil
}

// CanonicalizeSegmentsTemplate validates a scope.type="segments" configuration
// and returns its ordered, canonical segment rules (indices 0..N-1).
func CanonicalizeSegmentsTemplate(cfg *manifest.IndexScopeConfig) ([]manifest.IndexScopeDiscoverSegment, error) {
	if cfg == nil {
		return nil, errors.New("scope config is nil")
	}
	if cfg.Date != nil {
		return nil, errors.New("scope.segments does not support date configuration (use date_partitions)")
	}
	if len(cfg.Prefixes) > 0 {
		return nil, errors.New("scope.segments does not support prefixes (use prefix_list)")
	}
	if len(cfg.Scopes) > 0 {
		return nil, errors.New("scope.segments does not support nested scopes (use union)")
	}
	if strings.TrimSpace(cfg.DepthMode) != "" {
		return nil, fmt.Errorf("scope.segments unsupported depth_mode %q", cfg.DepthMode)
	}

	delimiter := normalizeDelimiter(cfg.Delimiter)
	hasDiscover := cfg.Discover != nil && len(cfg.Discover.Segments) > 0
	hasConstraints := len(cfg.Constraints) > 0
	if hasDiscover && hasConstraints {
		return nil, errors.New("scope.segments cannot specify both discover.segments and constraints")
	}
	if !hasDiscover && !hasConstraints {
		return nil, errors.New("scope.segments requires discover.segments or constraints")
	}

	var rawSegments []manifest.IndexScopeDiscoverSegment
	if hasConstraints {
		rawSegments = make([]manifest.IndexScopeDiscoverSegment, 0, len(cfg.Constraints))
		for _, c := range cfg.Constraints {
			pat := strings.TrimSpace(c.Glob)
			if pat == "" {
				return nil, fmt.Errorf("scope.constraints[%d].glob must not be empty", c.Index)
			}
			if err := validateSingleSegmentPattern(pat, delimiter, true); err != nil {
				return nil, fmt.Errorf("scope.constraints[%d].glob: %w", c.Index, err)
			}
			if isWildcardSegmentPattern(pat) {
				rawSegments = append(rawSegments, manifest.IndexScopeDiscoverSegment{
					Index:     c.Index,
					GlobAllow: []string{pat},
				})
			} else {
				rawSegments = append(rawSegments, manifest.IndexScopeDiscoverSegment{
					Index: c.Index,
					Allow: []string{pat},
				})
			}
		}
	} else {
		rawSegments = append(rawSegments, cfg.Discover.Segments...)
	}

	if len(rawSegments) > MaxSegmentsTemplateDepth {
		return nil, fmt.Errorf("%w: scope.segments depth %d exceeds maximum %d", ErrDiscoveryBudgetExceeded, len(rawSegments), MaxSegmentsTemplateDepth)
	}

	byIndex := make(map[int]manifest.IndexScopeDiscoverSegment, len(rawSegments))
	for _, seg := range rawSegments {
		if seg.Index < 0 {
			return nil, fmt.Errorf("scope.segments index %d must be >= 0", seg.Index)
		}
		if seg.Index >= MaxSegmentsTemplateDepth {
			return nil, fmt.Errorf("%w: scope.segments index %d exceeds maximum depth %d", ErrDiscoveryBudgetExceeded, seg.Index, MaxSegmentsTemplateDepth)
		}
		if _, exists := byIndex[seg.Index]; exists {
			return nil, fmt.Errorf("scope.segments index %d specified more than once", seg.Index)
		}
		norm, err := normalizeAndValidateSegmentRule(seg, delimiter)
		if err != nil {
			return nil, err
		}
		byIndex[seg.Index] = norm
	}

	out := make([]manifest.IndexScopeDiscoverSegment, len(rawSegments))
	hasDiscoveredLevel := false
	for i := 0; i < len(rawSegments); i++ {
		seg, ok := byIndex[i]
		if !ok {
			return nil, fmt.Errorf("scope.segments must define contiguous indices starting at 0 (missing index %d)", i)
		}
		if !isLiteralSegmentRule(seg) {
			hasDiscoveredLevel = true
		}
		out[i] = seg
	}
	if !hasDiscoveredLevel {
		return nil, errors.New("scope.segments requires at least one discovered segment level (use prefix_list for static prefixes)")
	}
	return out, nil
}

func isLiteralSegmentRule(seg manifest.IndexScopeDiscoverSegment) bool {
	return len(seg.Allow) > 0 && len(seg.Deny) == 0 && len(seg.GlobAllow) == 0 && len(seg.GlobDeny) == 0
}

func isWildcardSegmentPattern(pattern string) bool {
	return strings.ContainsAny(pattern, "*?[]{}\\")
}

func normalizeAndValidateSegmentRule(seg manifest.IndexScopeDiscoverSegment, delimiter string) (manifest.IndexScopeDiscoverSegment, error) {
	allow := normalizeLiteralList(seg.Allow)
	deny := normalizeLiteralList(seg.Deny)
	globAllow := normalizeGlobList(seg.GlobAllow)
	globDeny := normalizeGlobList(seg.GlobDeny)

	for _, lit := range allow {
		if err := validateSingleSegmentPattern(lit, delimiter, false); err != nil {
			return manifest.IndexScopeDiscoverSegment{}, fmt.Errorf("scope.segments[%d].allow %q: %w", seg.Index, lit, err)
		}
	}
	for _, lit := range deny {
		if err := validateSingleSegmentPattern(lit, delimiter, false); err != nil {
			return manifest.IndexScopeDiscoverSegment{}, fmt.Errorf("scope.segments[%d].deny %q: %w", seg.Index, lit, err)
		}
	}
	for _, pat := range globAllow {
		if err := validateSingleSegmentPattern(pat, delimiter, true); err != nil {
			return manifest.IndexScopeDiscoverSegment{}, fmt.Errorf("scope.segments[%d].glob_allow %q: %w", seg.Index, pat, err)
		}
	}
	for _, pat := range globDeny {
		if err := validateSingleSegmentPattern(pat, delimiter, true); err != nil {
			return manifest.IndexScopeDiscoverSegment{}, fmt.Errorf("scope.segments[%d].glob_deny %q: %w", seg.Index, pat, err)
		}
	}

	if len(allow) == 0 && len(deny) == 0 && len(globAllow) == 0 && len(globDeny) == 0 {
		return manifest.IndexScopeDiscoverSegment{}, fmt.Errorf("scope.segments[%d] must specify allow, deny, glob_allow, or glob_deny", seg.Index)
	}

	return manifest.IndexScopeDiscoverSegment{
		Index:     seg.Index,
		Allow:     allow,
		Deny:      deny,
		GlobAllow: globAllow,
		GlobDeny:  globDeny,
	}, nil
}

func validateSingleSegmentPattern(raw string, delimiter string, allowGlob bool) error {
	if raw == "" {
		return errors.New("segment pattern must not be empty")
	}
	if strings.Contains(raw, "\\") {
		return errors.New("backslash is not permitted in segment patterns")
	}
	if strings.Contains(raw, "/") || (delimiter != "" && strings.Contains(raw, delimiter)) {
		return errors.New("segment pattern must not contain path delimiter")
	}
	if strings.Contains(raw, "**") {
		return errors.New("recursive wildcard ** is not permitted inside a segment template")
	}
	if !allowGlob && isWildcardSegmentPattern(raw) {
		return errors.New("literal segment value must not contain glob metacharacters (use glob_allow/glob_deny)")
	}
	if allowGlob && !doublestar.ValidatePattern(raw) {
		return fmt.Errorf("invalid glob pattern %q", raw)
	}
	return nil
}

func discoverSegmentPrefixesBounded(
	ctx context.Context,
	lister provider.PrefixLister,
	prefixes []string,
	delimiter string,
	segment manifest.IndexScopeDiscoverSegment,
	maxPerLevel int,
) ([]string, error) {
	var out []string
	var rawObserved int
	for _, prefix := range prefixes {
		children, err := listAllPrefixesBounded(ctx, lister, prefix, delimiter, maxPerLevel-rawObserved)
		if err != nil {
			return nil, err
		}
		rawObserved += len(children)
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
			if maxPerLevel > 0 && len(out) > maxPerLevel {
				return nil, fmt.Errorf("%w: level %d discovered prefix count exceeds maximum %d", ErrDiscoveryBudgetExceeded, segment.Index, maxPerLevel)
			}
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
	return listAllPrefixesBounded(ctx, lister, prefix, delimiter, 0)
}

func listAllPrefixesBounded(ctx context.Context, lister provider.PrefixLister, prefix string, delimiter string, maxPrefixes int) ([]string, error) {
	var token string
	var out []string
	seen := map[string]struct{}{}
	for {
		res, err := lister.ListCommonPrefixes(ctx, provider.ListCommonPrefixesOptions{Prefix: prefix, Delimiter: delimiter, ContinuationToken: token})
		if err != nil {
			return nil, err
		}
		out = append(out, res.Prefixes...)
		if maxPrefixes > 0 && len(out) > maxPrefixes {
			return nil, fmt.Errorf("list prefixes under %q: %w: discovered prefixes exceed level budget (%d)", prefix, ErrDiscoveryBudgetExceeded, maxPrefixes)
		}
		if !res.IsTruncated {
			return out, nil
		}
		// A truncated page must advance to a fresh token; anything else would
		// end discovery early and drop partitions from the plan unannounced.
		if res.ContinuationToken == "" {
			return nil, fmt.Errorf("list prefixes under %q: %w: truncated page has no continuation token", prefix, provider.ErrIncompleteListing)
		}
		if _, repeated := seen[res.ContinuationToken]; repeated {
			return nil, fmt.Errorf("list prefixes under %q: %w: continuation token repeated", prefix, provider.ErrIncompleteListing)
		}
		seen[res.ContinuationToken] = struct{}{}
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
