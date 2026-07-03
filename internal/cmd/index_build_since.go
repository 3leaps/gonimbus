package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/match"
)

const indexBuildSinceFutureSkew = 5 * time.Minute

type indexBuildSincePlan struct {
	Enabled                     bool
	Mode                        string
	Watermark                   time.Time
	Filter                      *match.CompositeFilter
	RuntimeScope                *manifest.IndexScopeConfig
	EnumerationReductionApplied bool
	EnumerationReductionPartial bool
	Reason                      string
	Warnings                    []string
	AutoFallback                bool
}

func resolveIndexBuildSince(ctx context.Context, db *sql.DB, indexSetID string, m *manifest.IndexManifest, raw string, now time.Time) (*indexBuildSincePlan, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return &indexBuildSincePlan{}, nil
	}
	if !strings.EqualFold(raw, "auto") {
		return planIndexBuildSince(ctx, m, raw, nil, now)
	}

	run, err := latestSuccessfulIndexRun(ctx, db, indexSetID)
	if err != nil {
		return planIndexBuildSince(ctx, m, raw, nil, now)
	}
	watermark := run.StartedAt
	return planIndexBuildSince(ctx, m, raw, &watermark, now)
}

func latestSuccessfulIndexRun(ctx context.Context, db *sql.DB, indexSetID string) (*indexstore.IndexRun, error) {
	runs, err := indexstore.ListIndexRuns(ctx, db, indexSetID)
	if err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		return nil, fmt.Errorf("no index runs found for index set %s", indexSetID)
	}
	if runs[0].Status != indexstore.RunStatusSuccess {
		return nil, fmt.Errorf("latest index run %s is %s, not success", runs[0].RunID, runs[0].Status)
	}
	return &runs[0], nil
}

func planIndexBuildSince(ctx context.Context, m *manifest.IndexManifest, raw string, autoWatermark *time.Time, now time.Time) (*indexBuildSincePlan, error) {
	_ = ctx
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return &indexBuildSincePlan{}, nil
	}

	plan := &indexBuildSincePlan{Enabled: true}
	var watermark time.Time
	if strings.EqualFold(raw, "auto") {
		plan.Mode = "auto"
		if autoWatermark == nil || autoWatermark.IsZero() {
			plan.AutoFallback = true
			plan.Reason = "auto watermark unavailable"
			plan.Warnings = append(plan.Warnings, "--since auto could not find a prior successful run for this IndexSet; using full enumeration")
			return plan, nil
		}
		watermark = autoWatermark.UTC()
		if watermark.After(now.Add(indexBuildSinceFutureSkew)) {
			plan.AutoFallback = true
			plan.Reason = "auto watermark is in the future"
			plan.Warnings = append(plan.Warnings, "--since auto resolved to an implausibly future watermark; using full enumeration")
			return plan, nil
		}
	} else {
		parsed, err := match.ParseDate(raw)
		if err != nil {
			return nil, fmt.Errorf("--since: %w", err)
		}
		plan.Mode = "explicit"
		watermark = parsed.UTC()
	}

	plan.Watermark = watermark
	filter, err := match.NewFilterFromConfig(&match.FilterConfig{
		Modified: &match.DateFilterConfig{After: watermark.Format(time.RFC3339Nano)},
	})
	if err != nil {
		return nil, fmt.Errorf("--since filter: %w", err)
	}
	plan.Filter = filter

	scopeCfg := manifestScope(m)
	narrowed, applied, partial, reason, warnings, err := narrowScopeForSince(scopeCfg, watermark)
	if err != nil {
		return nil, err
	}
	plan.RuntimeScope = narrowed
	plan.EnumerationReductionApplied = applied
	plan.EnumerationReductionPartial = partial
	plan.Reason = reason
	plan.Warnings = append(plan.Warnings, warnings...)
	if !applied {
		plan.Warnings = append(plan.Warnings, "--since could not narrow provider listing scope; using full enumeration with last-modified filtering")
	}
	return plan, nil
}

func manifestScope(m *manifest.IndexManifest) *manifest.IndexScopeConfig {
	if m == nil || m.Build == nil {
		return nil
	}
	return m.Build.Scope
}

func narrowScopeForSince(cfg *manifest.IndexScopeConfig, watermark time.Time) (*manifest.IndexScopeConfig, bool, bool, string, []string, error) {
	if cfg == nil {
		return nil, false, false, "manifest has no build.scope date_partitions plan", nil, nil
	}
	switch cfg.Type {
	case "date_partitions":
		return narrowDatePartitionScopeForSince(cfg, watermark)
	case "union":
		out := *cfg
		out.Scopes = make([]manifest.IndexScopeConfig, len(cfg.Scopes))
		appliedAny := false
		partial := false
		unnarrowedAny := false
		var warnings []string
		for i := range cfg.Scopes {
			child, applied, childPartial, _, childWarnings, err := narrowScopeForSince(&cfg.Scopes[i], watermark)
			if err != nil {
				return nil, false, false, "", nil, err
			}
			if child != nil {
				out.Scopes[i] = *child
			} else {
				out.Scopes[i] = cfg.Scopes[i]
			}
			appliedAny = appliedAny || applied
			partial = partial || childPartial
			if !applied {
				unnarrowedAny = true
			}
			warnings = append(warnings, childWarnings...)
		}
		if appliedAny {
			if unnarrowedAny {
				partial = true
				warnings = append(warnings, "--since partially narrowed union provider listing scope; non-date child scopes will use full enumeration with last-modified filtering")
			}
			if partial {
				return &out, true, true, "union scope partially narrowed from --since watermark", warnings, nil
			}
			return &out, true, false, "date_partitions child scope narrowed from --since watermark", warnings, nil
		}
		return nil, false, false, "union has no date_partitions child scope to narrow", nil, nil
	default:
		return nil, false, false, fmt.Sprintf("scope.type %q cannot be narrowed by timestamp", cfg.Type), nil, nil
	}
}

func narrowDatePartitionScopeForSince(cfg *manifest.IndexScopeConfig, watermark time.Time) (*manifest.IndexScopeConfig, bool, bool, string, []string, error) {
	if cfg.Date == nil || cfg.Date.Range == nil {
		return nil, false, false, "date_partitions scope has no date range to narrow", nil, nil
	}

	start, err := match.ParseDate(cfg.Date.Range.After)
	if err != nil {
		return nil, false, false, "", nil, fmt.Errorf("scope.date.range.after: %w", err)
	}
	end, err := match.ParseDate(cfg.Date.Range.Before)
	if err != nil {
		return nil, false, false, "", nil, fmt.Errorf("scope.date.range.before: %w", err)
	}
	sinceDay := time.Date(watermark.UTC().Year(), watermark.UTC().Month(), watermark.UTC().Day(), 0, 0, 0, 0, time.UTC)
	if sinceDay.Before(start.UTC()) {
		sinceDay = start.UTC()
	}
	if !sinceDay.Before(end.UTC()) {
		return nil, false, false, "watermark is outside the configured date_partitions range", nil, nil
	}

	out := cloneScopeConfig(cfg)
	out.Date.Range.After = sinceDay.Format("2006-01-02")
	return out, true, false, "date_partitions range narrowed from --since watermark", nil, nil
}

func cloneScopeConfig(cfg *manifest.IndexScopeConfig) *manifest.IndexScopeConfig {
	if cfg == nil {
		return nil
	}
	out := *cfg
	if cfg.Prefixes != nil {
		out.Prefixes = append([]string(nil), cfg.Prefixes...)
	}
	if cfg.Scopes != nil {
		out.Scopes = make([]manifest.IndexScopeConfig, len(cfg.Scopes))
		for i := range cfg.Scopes {
			child := cloneScopeConfig(&cfg.Scopes[i])
			if child != nil {
				out.Scopes[i] = *child
			}
		}
	}
	if cfg.Discover != nil {
		discover := *cfg.Discover
		discover.Segments = append([]manifest.IndexScopeDiscoverSegment(nil), cfg.Discover.Segments...)
		out.Discover = &discover
	}
	if cfg.Date != nil {
		date := *cfg.Date
		if cfg.Date.Range != nil {
			dateRange := *cfg.Date.Range
			date.Range = &dateRange
		}
		out.Date = &date
	}
	if cfg.Constraints != nil {
		out.Constraints = append([]manifest.IndexScopeSegmentConstraint(nil), cfg.Constraints...)
	}
	return &out
}

func manifestForSincePlan(m *manifest.IndexManifest, plan *indexBuildSincePlan) *manifest.IndexManifest {
	if m == nil || plan == nil || plan.RuntimeScope == nil {
		return m
	}
	out := *m
	build := manifest.IndexBuildConfig{}
	if m.Build != nil {
		build = *m.Build
	}
	build.Scope = cloneScopeConfig(plan.RuntimeScope)
	out.Build = &build
	return &out
}

func combineIndexBuildFilters(base, since *match.CompositeFilter) *match.CompositeFilter {
	if base == nil {
		return since
	}
	if since == nil {
		return base
	}
	return match.NewCompositeFilter(base, since)
}

func bindIndexBuildSince(cfg *indexBuildCheckpointConfig, plan *indexBuildSincePlan) {
	if cfg == nil || plan == nil || !plan.Enabled {
		return
	}
	cfg.SinceMode = plan.Mode
	if !plan.Watermark.IsZero() {
		cfg.SinceWatermark = plan.Watermark.Format(time.RFC3339Nano)
	}
	cfg.SinceAutoFallback = plan.AutoFallback
	cfg.SinceRuntimeScope = cloneScopeConfig(plan.RuntimeScope)
	cfg.SinceEnumerationReductionPartial = plan.EnumerationReductionPartial
}

func indexBuildSincePlanFromCheckpoint(cfg *indexBuildCheckpointConfig) (*indexBuildSincePlan, error) {
	if cfg == nil || strings.TrimSpace(cfg.SinceMode) == "" {
		return &indexBuildSincePlan{}, nil
	}
	plan := &indexBuildSincePlan{
		Enabled:      true,
		Mode:         cfg.SinceMode,
		AutoFallback: cfg.SinceAutoFallback,
		RuntimeScope: cloneScopeConfig(cfg.SinceRuntimeScope),
	}
	plan.EnumerationReductionApplied = plan.RuntimeScope != nil
	plan.EnumerationReductionPartial = cfg.SinceEnumerationReductionPartial || scopeHasMixedUnion(plan.RuntimeScope)
	if strings.TrimSpace(cfg.SinceWatermark) == "" {
		return plan, nil
	}
	watermark, err := match.ParseDate(cfg.SinceWatermark)
	if err != nil {
		return nil, fmt.Errorf("checkpoint since watermark: %w", err)
	}
	plan.Watermark = watermark.UTC()
	filter, err := match.NewFilterFromConfig(&match.FilterConfig{
		Modified: &match.DateFilterConfig{After: plan.Watermark.Format(time.RFC3339Nano)},
	})
	if err != nil {
		return nil, fmt.Errorf("checkpoint since filter: %w", err)
	}
	plan.Filter = filter
	return plan, nil
}

func indexBuildSinceEvents(runID string, plan *indexBuildSincePlan, at time.Time) []indexstore.RunEvent {
	if plan == nil || !plan.Enabled {
		return nil
	}
	detail, err := json.Marshal(map[string]any{
		"mode":                          plan.Mode,
		"watermark":                     optionalSinceWatermark(plan),
		"enumeration_reduction_applied": plan.EnumerationReductionApplied,
		"enumeration_reduction":         enumerationReductionStatus(plan),
		"enumeration_reduction_partial": plan.EnumerationReductionPartial,
		"reason":                        plan.Reason,
		"warnings":                      plan.Warnings,
		"auto_fallback":                 plan.AutoFallback,
	})
	if err != nil {
		return nil
	}
	detailString := string(detail)
	category := string(indexstore.EventCategoryInfo)
	if len(plan.Warnings) > 0 || plan.AutoFallback {
		category = string(indexstore.EventCategoryWarning)
	}
	return []indexstore.RunEvent{{
		EventID:       fmt.Sprintf("evt_%d", at.UnixNano()),
		RunID:         runID,
		OccurredAt:    at,
		EventType:     "since_plan",
		EventCategory: category,
		Detail:        &detailString,
	}}
}

func optionalSinceWatermark(plan *indexBuildSincePlan) string {
	if plan == nil || plan.Watermark.IsZero() {
		return ""
	}
	return plan.Watermark.Format(time.RFC3339Nano)
}

func writeIndexBuildSinceStart(w io.Writer, plan *indexBuildSincePlan) {
	if plan == nil || !plan.Enabled {
		return
	}
	_, _ = fmt.Fprintf(w, "Since plan\n")
	_, _ = fmt.Fprintf(w, "  mode: %s\n", plan.Mode)
	if !plan.Watermark.IsZero() {
		_, _ = fmt.Fprintf(w, "  watermark: %s\n", plan.Watermark.Format(time.RFC3339Nano))
	}
	_, _ = fmt.Fprintf(w, "  enumeration_reduction: %s\n", enumerationReductionStatus(plan))
	if plan.Reason != "" {
		_, _ = fmt.Fprintf(w, "  reason: %s\n", plan.Reason)
	}
	for _, warning := range plan.Warnings {
		_, _ = fmt.Fprintf(w, "  warning: %s\n", warning)
	}
	_, _ = fmt.Fprintf(w, "  note: soft-delete disabled for --since run\n")
}

func writeIndexBuildDeltaReport(w io.Writer, result *indexBuildResult, plan *indexBuildSincePlan) {
	if plan == nil || !plan.Enabled || result == nil || len(result.DeltaByPrefix) == 0 {
		return
	}
	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, "Since delta report:")
	for _, row := range sortedIndexBuildDeltaRows(result.DeltaByPrefix) {
		_, _ = fmt.Fprintf(w, "  prefix: %s added=%d changed=%d unchanged=%d\n", valueOrDefault(row.Prefix, "(root)"), row.Added, row.Changed, row.Unchanged)
	}
}

type indexBuildDeltaRow struct {
	Prefix    string
	Added     int64
	Changed   int64
	Unchanged int64
}

func sortedIndexBuildDeltaRows(in map[string]indexBuildDeltaCounts) []indexBuildDeltaRow {
	rows := make([]indexBuildDeltaRow, 0, len(in))
	for prefix, counts := range in {
		rows = append(rows, indexBuildDeltaRow{
			Prefix:    prefix,
			Added:     counts.Added,
			Changed:   counts.Changed,
			Unchanged: counts.Unchanged,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Prefix < rows[j].Prefix
	})
	return rows
}

func enumerationReductionStatus(plan *indexBuildSincePlan) string {
	if plan == nil || !plan.EnumerationReductionApplied {
		return "no"
	}
	if plan.EnumerationReductionPartial {
		return "partial"
	}
	return "yes"
}

func scopeHasMixedUnion(cfg *manifest.IndexScopeConfig) bool {
	if cfg == nil {
		return false
	}
	if cfg.Type == "union" {
		hasDate := false
		hasOther := false
		for i := range cfg.Scopes {
			child := &cfg.Scopes[i]
			if child.Type == "date_partitions" {
				hasDate = true
			} else {
				hasOther = true
			}
			if scopeHasMixedUnion(child) {
				return true
			}
		}
		return hasDate && hasOther
	}
	return false
}
