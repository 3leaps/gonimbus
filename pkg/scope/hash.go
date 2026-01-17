package scope

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/match"
)

type scopeHashPayload struct {
	Type       string                `json:"type"`
	BasePrefix string                `json:"base_prefix,omitempty"`
	Delimiter  string                `json:"delimiter,omitempty"`
	Prefixes   []string              `json:"prefixes,omitempty"`
	Scopes     []scopeHashPayload    `json:"scopes,omitempty"`
	Discover   *scopeDiscoverPayload `json:"discover,omitempty"`
	Date       *scopeDatePayload     `json:"date,omitempty"`
}

type scopeDiscoverPayload struct {
	Segments []scopeDiscoverSegmentPayload `json:"segments,omitempty"`
}

type scopeDiscoverSegmentPayload struct {
	Index     int      `json:"index"`
	Allow     []string `json:"allow,omitempty"`
	Deny      []string `json:"deny,omitempty"`
	GlobAllow []string `json:"glob_allow,omitempty"`
	GlobDeny  []string `json:"glob_deny,omitempty"`
}

type scopeDatePayload struct {
	SegmentIndex int            `json:"segment_index"`
	Format       string         `json:"format,omitempty"`
	Range        scopeDateRange `json:"range"`
	Glob         string         `json:"glob,omitempty"`
}

type scopeDateRange struct {
	After  time.Time `json:"after"`
	Before time.Time `json:"before"`
}

// HashConfig computes a canonical scope hash for identity purposes.
func HashConfig(cfg *manifest.IndexScopeConfig) (string, error) {
	if cfg == nil {
		return "", nil
	}

	payload, err := buildScopeHashPayload(cfg)
	if err != nil {
		return "", err
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal scope hash payload: %w", err)
	}

	sha := sha256.Sum256(b)
	return hex.EncodeToString(sha[:]), nil
}

func buildScopeHashPayload(cfg *manifest.IndexScopeConfig) (scopeHashPayload, error) {
	if cfg == nil {
		return scopeHashPayload{}, errors.New("scope config is nil")
	}

	scopeType := strings.TrimSpace(cfg.Type)
	if scopeType == "" {
		return scopeHashPayload{}, errors.New("scope.type is required")
	}

	payload := scopeHashPayload{Type: scopeType}
	switch scopeType {
	case "prefix_list":
		prefixes := normalizeLiteralList(cfg.Prefixes)
		if len(prefixes) == 0 {
			return scopeHashPayload{}, errors.New("scope.prefixes must not be empty")
		}
		payload.BasePrefix = normalizePrefixField(cfg.BasePrefix)
		payload.Delimiter = normalizeDelimiter(cfg.Delimiter)
		payload.Prefixes = prefixes
	case "union":
		if len(cfg.Scopes) == 0 {
			return scopeHashPayload{}, errors.New("scope.scopes must not be empty")
		}
		payload.Scopes = make([]scopeHashPayload, 0, len(cfg.Scopes))
		for i := range cfg.Scopes {
			child, err := buildScopeHashPayload(&cfg.Scopes[i])
			if err != nil {
				return scopeHashPayload{}, err
			}
			payload.Scopes = append(payload.Scopes, child)
		}
		sortScopePayloads(payload.Scopes)
	case "date_partitions":
		payload.BasePrefix = normalizePrefixField(cfg.BasePrefix)
		payload.Delimiter = normalizeDelimiter(cfg.Delimiter)
		discover, err := buildDiscoverPayload(cfg.Discover)
		if err != nil {
			return scopeHashPayload{}, err
		}
		payload.Discover = discover

		datePayload, err := buildDatePayload(cfg.Date)
		if err != nil {
			return scopeHashPayload{}, err
		}
		payload.Date = datePayload
	default:
		return scopeHashPayload{}, fmt.Errorf("unsupported scope.type %q", scopeType)
	}

	return payload, nil
}

func buildDiscoverPayload(cfg *manifest.IndexScopeDiscoverConfig) (*scopeDiscoverPayload, error) {
	if cfg == nil || len(cfg.Segments) == 0 {
		return nil, nil
	}

	segments := make([]scopeDiscoverSegmentPayload, 0, len(cfg.Segments))
	for _, segment := range cfg.Segments {
		segments = append(segments, scopeDiscoverSegmentPayload{
			Index:     segment.Index,
			Allow:     normalizeLiteralList(segment.Allow),
			Deny:      normalizeLiteralList(segment.Deny),
			GlobAllow: normalizeGlobList(segment.GlobAllow),
			GlobDeny:  normalizeGlobList(segment.GlobDeny),
		})
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].Index < segments[j].Index
	})

	return &scopeDiscoverPayload{Segments: segments}, nil
}

func buildDatePayload(cfg *manifest.IndexScopeDateConfig) (*scopeDatePayload, error) {
	if cfg == nil {
		return nil, errors.New("scope.date is required")
	}
	if cfg.Range == nil {
		return nil, errors.New("scope.date.range is required")
	}
	if cfg.SegmentIndex < 0 {
		return nil, errors.New("scope.date.segment_index must be >= 0")
	}

	start, end, err := parseScopeDateRange(cfg.Range)
	if err != nil {
		return nil, err
	}

	format := strings.TrimSpace(cfg.Format)
	if format == "" {
		format = defaultDateFormat
	}

	payload := &scopeDatePayload{
		SegmentIndex: cfg.SegmentIndex,
		Format:       format,
		Range: scopeDateRange{
			After:  start,
			Before: end,
		},
		Glob: strings.TrimSpace(cfg.Glob),
	}

	return payload, nil
}

func parseScopeDateRange(cfg *manifest.IndexScopeDateRange) (time.Time, time.Time, error) {
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
		return time.Time{}, time.Time{}, errors.New("scope.date.range.after must be before scope.date.range.before")
	}

	return start, end, nil
}

func normalizeLiteralList(values []string) []string {
	return normalizeStringList(values, true)
}

func normalizeGlobList(values []string) []string {
	return normalizeStringList(values, false)
}

func normalizeStringList(values []string, trimLeadingSlash bool) []string {
	if len(values) == 0 {
		return nil
	}

	unique := make(map[string]struct{})
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimLeadingSlash {
			trimmed = strings.TrimPrefix(trimmed, "/")
		}
		if trimmed == "" {
			continue
		}
		unique[trimmed] = struct{}{}
	}
	if len(unique) == 0 {
		return nil
	}

	out := make([]string, 0, len(unique))
	for value := range unique {
		out = append(out, value)
	}
	// Sort for deterministic output
	sort.Strings(out)
	return out
}

func sortScopePayloads(values []scopeHashPayload) {
	sort.Slice(values, func(i, j int) bool {
		left, _ := json.Marshal(values[i])
		right, _ := json.Marshal(values[j])
		return string(left) < string(right)
	})
}

func normalizePrefixField(value string) string {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.TrimPrefix(trimmed, "/")
	return trimmed
}
