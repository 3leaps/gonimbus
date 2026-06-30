package reflow

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/3leaps/gonimbus/pkg/transfer"
)

// run dispatches a Source to its execution path. Deferral to the command layer
// (ErrNotImplemented) is decided here and in runRecordStream from the source form
// and config alone, before any stream bytes are read — so a caller can fall back
// to the CLI path with the same Source. Once the engine commits to reading a
// stream it never returns ErrNotImplemented; per-record problems surface as
// CLI-equivalent INVALID_INPUT events.
func (r *Runner) run(ctx context.Context, src Source) (Summary, error) {
	switch s := src.(type) {
	case RecordStreamSource:
		return r.runRecordStream(ctx, s)
	default:
		// ObjectSource/PrefixSource/FileTreeSource execution lands in later slices.
		return Summary{}, ErrNotImplemented
	}
}

// runRecordStream executes a preselected reflow-input stream. This slice
// implements the dry-run plane — plan destination mappings, emit planned records,
// no provider mutation, no checkpoint. The copy plane (non-dry-run) defers to the
// command layer before any bytes are read.
//
// The supported stream is processed record-by-record without materializing the
// whole stream, so multi-million-object streams stay bounded in memory. A record
// the engine cannot plan (wrong type, non-s3 source, prefix URI, malformed, or
// missing both dest_rel_key and rewrite templates) is reported as a per-record
// INVALID_INPUT event — matching the command path — rather than aborting the run.
func (r *Runner) runRecordStream(ctx context.Context, src RecordStreamSource) (Summary, error) {
	if !r.cfg.DryRun {
		return Summary{}, ErrNotImplemented
	}
	if src.Records == nil {
		return Summary{}, errors.New("reflow: RecordStreamSource.Records is required")
	}
	layout, err := ParseDestLayout(r.cfg.Destination.BaseURI)
	if err != nil {
		return Summary{}, err
	}
	rewrite, err := r.compileRewrite()
	if err != nil {
		return Summary{}, err
	}

	capability := dryRunIfAbsentCapability(r.cfg.Destination.ProviderID, r.cfg.Collision.Mode)
	concurrency := NewConcurrencyLimiter(r.cfg.Concurrency).Snapshot()

	// Event order is irrelevant: an EventSink consumer is event-based, and the
	// parity harness normalizes by sorting.
	if err := r.emitRun(ctx, RunRecord{
		DestURI:          layout.BaseURI,
		DryRun:           true,
		Parallel:         r.cfg.Concurrency.RequestedCeiling,
		ConcurrencyStats: concurrency,
	}); err != nil {
		return Summary{}, err
	}
	if w := fallbackWarning(r.cfg.Destination.ProviderID, r.cfg.Collision.Mode, capability); w != nil {
		if err := r.emitWarning(ctx, *w); err != nil {
			return Summary{}, err
		}
	}

	stats := newRunStats()
	scanner := bufio.NewScanner(src.Records)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return Summary{}, ctx.Err()
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if err := r.planAndEmit(ctx, layout, rewrite, stats, line); err != nil {
			return Summary{}, err
		}
	}
	if err := scanner.Err(); err != nil {
		return Summary{}, err
	}

	summary := stats.summary(layout.BaseURI, r.cfg.Collision.Mode, true, capability, concurrency)
	if err := r.emitSummary(ctx, summary); err != nil {
		return Summary{}, err
	}
	if summary.InvalidInputs > 0 {
		// Mirror the command path, which writes the summary and then exits non-zero
		// on invalid inputs. The Summary is returned alongside the error.
		return Summary{SummaryRecord: summary}, &InvalidInputsError{Count: summary.InvalidInputs}
	}
	return Summary{SummaryRecord: summary}, nil
}

// InvalidInputsError reports that a run completed and emitted its terminal summary
// but encountered one or more invalid input records (surfaced as INVALID_INPUT
// events). The Summary is still returned with it. It mirrors the command path,
// which exits non-zero when invalid inputs occur, so a library caller does not
// observe success for a stream the CLI reports as failed.
type InvalidInputsError struct{ Count int64 }

func (e *InvalidInputsError) Error() string {
	return fmt.Sprintf("reflow: completed with %d invalid input(s)", e.Count)
}

// planAndEmit plans one reflow-input line and emits its planned record, or emits
// a CLI-equivalent INVALID_INPUT event when the record cannot be parsed or mapped.
// It returns an error only when the EventSink itself fails.
func (r *Runner) planAndEmit(ctx context.Context, layout DestLayout, rewrite *transfer.ReflowRewrite, stats *runStats, line string) error {
	in, err := parseReflowInputLine(line)
	if err != nil {
		stats.recordInvalidInput()
		return r.emitError(ctx, ErrorEvent{Code: ErrCodeInvalidInput, Message: "invalid reflow input", Details: map[string]any{"error": err.Error()}})
	}
	destRel, err := planDestRel(in, rewrite)
	if err != nil {
		stats.recordInvalidInput()
		return r.emitError(ctx, ErrorEvent{Code: ErrCodeInvalidInput, Key: in.SourceKey, Message: "destination mapping unavailable", Details: map[string]any{"error": err.Error(), "source_uri": in.SourceURI}})
	}
	destKey := layout.DestKey(destRel)
	destURI := layout.DestURI(destKey)
	rec := in.record(destURI, destKey, "planned")
	stats.record(rec)
	return r.emitRecord(ctx, rec)
}

// planDestRel resolves the destination-relative key for an input, mirroring the
// command path: quarantine routing wins, then an explicit dest_rel_key, then the
// rewrite templates.
func planDestRel(in reflowInput, rewrite *transfer.ReflowRewrite) (string, error) {
	if in.RoutingClass == "quarantine" {
		return QuarantineDestRel(in.QuarantinePrefix, in.SourceKey), nil
	}
	if in.DestRelKey != "" {
		return in.DestRelKey, nil
	}
	if rewrite != nil {
		mapped, _, err := rewrite.ApplyWithVars(in.SourceKey, in.Vars)
		if err != nil {
			return "", err
		}
		return mapped, nil
	}
	return "", fmt.Errorf("record lacks dest_rel_key and no rewrite templates were supplied")
}

// compileRewrite compiles the configured rewrite templates, or returns nil when
// none are set. A malformed template is a configuration error, surfaced before
// any stream bytes are read.
func (r *Runner) compileRewrite() (*transfer.ReflowRewrite, error) {
	from := strings.TrimSpace(r.cfg.Rewrite.From)
	to := strings.TrimSpace(r.cfg.Rewrite.To)
	if from == "" && to == "" {
		return nil, nil
	}
	rewrite, err := transfer.CompileReflowRewrite(r.cfg.Rewrite.From, r.cfg.Rewrite.To)
	if err != nil {
		return nil, fmt.Errorf("reflow: invalid rewrite templates: %w", err)
	}
	return rewrite, nil
}

// Event-emission helpers apply the engine's redaction boundary before delivery:
// every Details map is per-field sanitized, and Record source URIs are sanitized
// at construction. A nil EventSink disables delivery.

func (r *Runner) emitRun(ctx context.Context, rec RunRecord) error {
	if r.cfg.Events == nil {
		return nil
	}
	return r.cfg.Events.OnRun(ctx, rec)
}

func (r *Runner) emitRecord(ctx context.Context, rec Record) error {
	if r.cfg.Events == nil {
		return nil
	}
	rec.Details = sanitizeDetails(rec.Details)
	return r.cfg.Events.OnRecord(ctx, rec)
}

func (r *Runner) emitWarning(ctx context.Context, w Warning) error {
	if r.cfg.Events == nil {
		return nil
	}
	w.Details = sanitizeDetails(w.Details)
	return r.cfg.Events.OnWarning(ctx, w)
}

func (r *Runner) emitError(ctx context.Context, e ErrorEvent) error {
	if r.cfg.Events == nil {
		return nil
	}
	e.Details = sanitizeDetails(e.Details)
	return r.cfg.Events.OnError(ctx, e)
}

func (r *Runner) emitSummary(ctx context.Context, rec SummaryRecord) error {
	if r.cfg.Events == nil {
		return nil
	}
	return r.cfg.Events.OnSummary(ctx, rec)
}
