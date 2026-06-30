package reflow

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/3leaps/gonimbus/pkg/provider"
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

// runRecordStream executes a preselected reflow-input stream.
//
// The supported stream is processed record-by-record without materializing the
// whole stream, so multi-million-object streams stay bounded in memory. A record
// the engine cannot plan (wrong type, non-s3 source, prefix URI, malformed, or
// missing both dest_rel_key and rewrite templates) is reported as a per-record
// INVALID_INPUT event — matching the command path — rather than aborting the run.
func (r *Runner) runRecordStream(ctx context.Context, src RecordStreamSource) (Summary, error) {
	if src.Records == nil {
		return Summary{}, errors.New("reflow: RecordStreamSource.Records is required")
	}
	if !r.cfg.DryRun && r.cfg.ReadOnly {
		return Summary{}, errors.New("reflow: Config.ReadOnly requires DryRun for RecordStreamSource copy execution")
	}
	if !r.cfg.DryRun && !recordStreamCopyCollisionModeSupported(r.cfg.Collision.Mode) {
		return Summary{}, ErrNotImplemented
	}
	if !r.cfg.DryRun && src.Resolve == nil {
		return Summary{}, errors.New("reflow: RecordStreamSource.Resolve is required for copy execution")
	}
	layout, err := ParseDestLayout(r.cfg.Destination.BaseURI)
	if err != nil {
		return Summary{}, err
	}
	rewrite, err := r.compileRewrite()
	if err != nil {
		return Summary{}, err
	}

	var capability IfAbsentCapability
	if r.cfg.DryRun {
		capability = dryRunIfAbsentCapability(r.cfg.Destination.ProviderID, r.cfg.Collision.Mode)
	} else {
		capability = liveIfAbsentCapability(ctx, r.cfg.Destination.Provider, layout, r.cfg.Collision.Mode, r.cfg.ReadOnly)
	}
	limiter := NewConcurrencyLimiter(r.cfg.Concurrency)
	runConcurrency := limiter.Snapshot()

	// Event order is irrelevant: an EventSink consumer is event-based, and the
	// parity harness normalizes by sorting.
	if err := r.emitRun(ctx, RunRecord{
		DestURI:          layout.BaseURI,
		DryRun:           r.cfg.DryRun,
		Parallel:         r.cfg.Concurrency.RequestedCeiling,
		ConcurrencyStats: runConcurrency,
	}); err != nil {
		return Summary{}, err
	}
	if w := fallbackWarning(r.cfg.Destination.ProviderID, r.cfg.Collision.Mode, capability); w != nil {
		if err := r.emitWarning(ctx, *w); err != nil {
			return Summary{}, err
		}
	}

	stats := newRunStats()
	sourceIdentity := ""
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
		if err := r.executeInputLine(ctx, src, layout, rewrite, stats, capability, limiter, &sourceIdentity, line); err != nil {
			return Summary{}, err
		}
	}
	if err := scanner.Err(); err != nil {
		return Summary{}, err
	}

	summary := stats.summary(layout.BaseURI, r.cfg.Collision.Mode, r.cfg.DryRun, capability, limiter.Snapshot())
	if err := r.emitSummary(ctx, summary); err != nil {
		return Summary{}, err
	}
	if summary.InvalidInputs > 0 {
		// Mirror the command path, which writes the summary and then exits non-zero
		// on invalid inputs. The Summary is returned alongside the error.
		return Summary{SummaryRecord: summary}, &InvalidInputsError{Count: summary.InvalidInputs}
	}
	if summary.Errors > 0 {
		// Mirror the command path, which writes the summary and then exits non-zero
		// when object-level errors occurred. The Summary is returned alongside the error.
		return Summary{SummaryRecord: summary}, &ObjectErrorsError{Count: summary.Errors}
	}
	return Summary{SummaryRecord: summary}, nil
}

func recordStreamCopyCollisionModeSupported(mode string) bool {
	switch mode {
	case CollisionSkipIfDuplicate, CollisionFail:
		return true
	default:
		return false
	}
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

// ObjectErrorsError reports that a run completed and emitted its terminal summary
// but encountered one or more object-level errors. The Summary is still returned
// with it. It mirrors the command path, which exits non-zero when per-object
// errors occur, so a library caller does not observe success for a run the CLI
// reports as failed.
type ObjectErrorsError struct{ Count int64 }

func (e *ObjectErrorsError) Error() string {
	return fmt.Sprintf("reflow: completed with %d object error(s)", e.Count)
}

func (r *Runner) executeInputLine(ctx context.Context, src RecordStreamSource, layout DestLayout, rewrite *transfer.ReflowRewrite, stats *runStats, capability IfAbsentCapability, limiter *ConcurrencyLimiter, sourceIdentity *string, line string) error {
	in, err := parseReflowInputLine(line)
	if err != nil {
		stats.recordInvalidInput()
		return r.emitError(ctx, ErrorEvent{Code: ErrCodeInvalidInput, Message: FormatErrorMessage("invalid reflow input", err), Details: map[string]any{"error": err.Error()}})
	}
	if err := validateSourceIdentity(sourceIdentity, in); err != nil {
		stats.recordInvalidInput()
		return r.emitError(ctx, ErrorEvent{Code: ErrCodeInvalidInput, Key: in.SourceKey, Message: FormatErrorMessage("invalid input", err), Details: map[string]any{"error": err.Error(), "source_uri": in.SourceURI}})
	}
	destRel, err := planDestRel(in, rewrite)
	if err != nil {
		stats.recordInvalidInput()
		return r.emitError(ctx, ErrorEvent{Code: ErrCodeInvalidInput, Key: in.SourceKey, Message: FormatErrorMessage("destination mapping unavailable", err), Details: map[string]any{"error": err.Error(), "source_uri": in.SourceURI}})
	}
	destKey := layout.DestKey(destRel)
	destURI := layout.DestURI(destKey)
	if r.cfg.DryRun {
		rec := in.record(destURI, destKey, "planned")
		stats.record(rec)
		return r.emitRecord(ctx, rec)
	}
	return r.copyAndEmit(ctx, src, layout, stats, capability, limiter, in, destKey, destURI)
}

func validateSourceIdentity(current *string, in reflowInput) error {
	if current == nil {
		return nil
	}
	identity := in.sourceIdentity()
	if identity == "" {
		return nil
	}
	if *current == "" {
		*current = identity
		return nil
	}
	if *current != identity {
		return fmt.Errorf("multiple source roots are not supported: got %q expected %q", identity, *current)
	}
	return nil
}

func (r *Runner) copyAndEmit(ctx context.Context, src RecordStreamSource, layout DestLayout, stats *runStats, capability IfAbsentCapability, limiter *ConcurrencyLimiter, in reflowInput, destKey, destURI string) error {
	sourceURI := sanitizeSourceURI(in.SourceURI)
	sourceProvider, err := src.Resolve(ctx, in.SourceURI)
	if err != nil {
		return r.recordObjectError(ctx, stats, in, destURI, destKey, "failed to connect to provider", err, map[string]any{"source_uri": sourceURI, "dest_uri": destURI}, nil)
	}

	if r.cfg.Checkpoint != nil {
		done, status, err := r.cfg.Checkpoint.ItemDone(ctx, sourceURI, destURI)
		if err != nil {
			return r.recordObjectError(ctx, stats, in, destURI, destKey, "checkpoint read failed", err, map[string]any{"source_uri": sourceURI, "dest_uri": destURI}, nil)
		}
		if done {
			rec := in.record(destURI, destKey, "skipped")
			rec.Reason = "resume." + status
			stats.record(rec)
			if err := r.checkpointItem(ctx, in, destURI, destKey, "skipped", rec.Reason, 0, "", ""); err != nil {
				return err
			}
			return r.emitRecord(ctx, rec)
		}
	}

	inProgress := in.record(destURI, destKey, "in_progress")
	stats.record(inProgress)
	if err := r.emitRecord(ctx, inProgress); err != nil {
		return err
	}

	sourceETag := in.SourceETag
	sourceSize := in.SourceSize
	var sourceMeta *provider.ObjectMeta
	if r.cfg.Metadata.NeedsSourceHead() || sourceETag == "" || sourceSize == 0 {
		meta, err := limitedHead(ctx, limiter, sourceProvider, in.SourceKey)
		if err != nil {
			return r.recordObjectError(ctx, stats, in, destURI, destKey, "source metadata read failed", err, map[string]any{"source_uri": sourceURI, "dest_uri": destURI}, nil)
		}
		sourceMeta = meta
		sourceETag = meta.ETag
		sourceSize = meta.Size
	}

	putOptions, err := r.cfg.Metadata.PutOptions(sourceMeta)
	if err == nil && layout.ProviderID == string(provider.ProviderS3) {
		err = ValidateMetadataBudget(putOptions.UserMetadata)
	}
	if err != nil {
		details := map[string]any{"source_uri": sourceURI, "dest_uri": destURI}
		var budgetErr *MetadataBudgetError
		if errors.As(err, &budgetErr) {
			for k, v := range budgetErr.Details() {
				details[k] = v
			}
		}
		var derivErr *MetadataDerivationError
		if errors.As(err, &derivErr) {
			for k, v := range derivErr.Details() {
				details[k] = v
			}
		}
		return r.recordObjectError(ctx, stats, in.withSourceMeta(sourceETag, sourceSize), destURI, destKey, "destination metadata options failed", err, details, nil)
	}

	bytes, putResult, collision, status, reason, err := r.copyWithCollision(ctx, sourceProvider, layout, stats, capability, limiter, in.withSourceMeta(sourceETag, sourceSize), destKey, putOptions)
	if err != nil {
		details := map[string]any{"source_uri": sourceURI, "dest_uri": destURI}
		msg := "copy failed"
		if collision != nil {
			msg = "collision"
		}
		return r.recordObjectError(ctx, stats, in.withSourceMeta(sourceETag, sourceSize), destURI, destKey, msg, err, details, collision)
	}

	if r.cfg.Checkpoint != nil {
		if err := r.cfg.Checkpoint.MarkDestKeyObserved(ctx, destKey); err != nil {
			return err
		}
		if err := r.cfg.Checkpoint.NoteDestKeySource(ctx, destKey, sourceURI, sourceETag, sourceSize); err != nil {
			return err
		}
		if err := r.checkpointItem(ctx, in.withSourceMeta(sourceETag, sourceSize), destURI, destKey, status, reason, bytes, "", ""); err != nil {
			return err
		}
	}
	_ = putResult
	rec := in.withSourceMeta(sourceETag, sourceSize).record(destURI, destKey, status)
	rec.Reason = reason
	rec.Bytes = bytes
	rec = recordWithCollision(rec, collision)
	stats.record(rec)
	return r.emitRecord(ctx, rec)
}

func (r *Runner) copyWithCollision(ctx context.Context, src provider.Provider, layout DestLayout, stats *runStats, capability IfAbsentCapability, limiter *ConcurrencyLimiter, in reflowInput, destKey string, opts provider.PutOptions) (int64, provider.PutResult, *CollisionInfo, string, string, error) {
	dst := r.cfg.Destination.Provider
	if r.cfg.Collision.Mode == CollisionOverwrite {
		bytes, err := limitedCopy(ctx, limiter, src, dst, in.SourceKey, destKey, in.SourceSize, opts)
		return bytes, provider.PutResult{}, nil, "complete", "", err
	}
	if r.cfg.Checkpoint != nil {
		observed, err := r.cfg.Checkpoint.DestKeyObserved(ctx, destKey)
		if err != nil {
			return 0, provider.PutResult{}, nil, "", "", err
		}
		if observed {
			dstMeta, headErr := limitedHead(ctx, limiter, dst, destKey)
			if headErr != nil {
				return 0, provider.PutResult{}, nil, "", "", headErr
			}
			return r.handleExistingDestination(ctx, src, layout, in, destKey, dstMeta, decisionIfAbsentHead)
		}
	}

	if capability.FallbackActive {
		stats.recordFallbackObject()
		dstMeta, headErr := limitedHead(ctx, limiter, dst, destKey)
		switch {
		case headErr == nil:
			return r.handleExistingDestination(ctx, src, layout, in, destKey, dstMeta, decisionHeadFallback)
		case provider.IsNotFound(headErr):
			bytes, err := limitedCopy(ctx, limiter, src, dst, in.SourceKey, destKey, in.SourceSize, opts)
			return bytes, provider.PutResult{}, nil, "complete", "", err
		default:
			return 0, provider.PutResult{}, nil, "", "", headErr
		}
	}

	bytes, result, err := limitedCopyConditional(ctx, limiter, src, dst, in.SourceKey, destKey, in.SourceSize, provider.PutPrecondition{IfAbsent: true}, opts)
	if err == nil {
		return bytes, result, nil, "complete", "", nil
	}
	if !isConditionalExists(err) {
		return 0, provider.PutResult{}, nil, "", "", err
	}
	dstMeta, headErr := limitedHead(ctx, limiter, dst, destKey)
	if headErr != nil {
		return 0, provider.PutResult{}, nil, "", "", headErr
	}
	return r.handleExistingDestination(ctx, src, layout, in, destKey, dstMeta, decisionIfAbsentHead)
}

func (r *Runner) handleExistingDestination(ctx context.Context, src provider.Provider, layout DestLayout, in reflowInput, destKey string, dstMeta *provider.ObjectMeta, decisionPath string) (int64, provider.PutResult, *CollisionInfo, string, string, error) {
	duplicate, err := isDuplicateCollisionForReflow(ctx, src, r.cfg.Destination.Provider, in.SourceKey, destKey, in.SourceProvider, layout.ProviderID, in.SourceETag, in.SourceSize, dstMeta)
	if err != nil {
		return 0, provider.PutResult{}, nil, "", "", err
	}
	if duplicate {
		collision := newCollisionInfo(collisionDuplicate, dstMeta, decisionPath)
		if err := r.noteCollision(ctx, destKey, "duplicate", in, dstMeta); err != nil {
			return 0, provider.PutResult{}, nil, "", "", err
		}
		if r.cfg.Collision.Mode == CollisionSkipIfDuplicate || r.cfg.Collision.Mode == CollisionQuarantine || r.cfg.Collision.Mode == CollisionOverwriteIfSourceNewer {
			return 0, provider.PutResult{}, collision, "skipped", "collision.duplicate", nil
		}
		return 0, provider.PutResult{}, collision, "", "", fmt.Errorf("destination key exists with identical content: %s", destKey)
	}

	collision := newCollisionInfo(collisionConflict, dstMeta, decisionPath)
	if err := r.noteCollision(ctx, destKey, "conflict", in, dstMeta); err != nil {
		return 0, provider.PutResult{}, nil, "", "", err
	}
	if r.cfg.Collision.Mode == CollisionFail {
		return 0, provider.PutResult{}, collision, "", "", fmt.Errorf("destination key exists with different content: %s", destKey)
	}
	return 0, provider.PutResult{}, collision, "", "", fmt.Errorf("destination key exists with different content: %s", destKey)
}

func (r *Runner) recordObjectError(ctx context.Context, stats *runStats, in reflowInput, destURI, destKey, msg string, err error, details map[string]any, collision *CollisionInfo) error {
	stats.recordError()
	code := reflowErrCode(err)
	if details == nil {
		details = map[string]any{}
	}
	if _, ok := details["mode"]; !ok {
		details["mode"] = "transfer_reflow"
	}
	if _, ok := details["reason"]; !ok {
		details["reason"] = reflowReasonForErrCode(code)
	}
	if err := r.emitError(ctx, ErrorEvent{Code: code, Key: in.SourceKey, Message: FormatErrorMessage(msg, err), Details: details, Collision: collision}); err != nil {
		return err
	}
	if err := r.checkpointItem(ctx, in, destURI, destKey, "failed", reflowReasonForErrCode(code), 0, code, SanitizeOperationCauseMessage(err)); err != nil {
		return err
	}
	rec := in.record(destURI, destKey, "failed")
	rec.Reason = failedRecordReason(code, collision)
	rec = recordWithCollision(rec, collision)
	stats.record(rec)
	return r.emitRecord(ctx, rec)
}

func limitedHead(ctx context.Context, limiter *ConcurrencyLimiter, p provider.Provider, key string) (*provider.ObjectMeta, error) {
	release, err := limiter.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	meta, err := p.Head(ctx, key)
	limiter.ObserveProviderResult(err)
	return meta, err
}

func limitedCopy(ctx context.Context, limiter *ConcurrencyLimiter, src provider.Provider, dst provider.Provider, srcKey, dstKey string, sourceSize int64, opts provider.PutOptions) (int64, error) {
	release, err := limiter.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	bytes, err := transfer.CopyObjectWithOptions(ctx, src, dst, srcKey, dstKey, sourceSize, transfer.DefaultRetryBufferMaxMemoryBytes, opts)
	limiter.ObserveProviderResult(err)
	return bytes, err
}

func limitedCopyConditional(ctx context.Context, limiter *ConcurrencyLimiter, src provider.Provider, dst provider.Provider, srcKey, dstKey string, sourceSize int64, precond provider.PutPrecondition, opts provider.PutOptions) (int64, provider.PutResult, error) {
	release, err := limiter.Acquire(ctx)
	if err != nil {
		return 0, provider.PutResult{}, err
	}
	defer release()
	bytes, result, err := transfer.CopyObjectConditionalWithOptions(ctx, src, dst, srcKey, dstKey, sourceSize, transfer.DefaultRetryBufferMaxMemoryBytes, precond, opts)
	limiter.ObserveProviderResult(err)
	return bytes, result, err
}

func failedRecordReason(code string, collision *CollisionInfo) string {
	if collision == nil {
		return reflowReasonForErrCode(code)
	}
	if collision.Kind == collisionDuplicate {
		return "collision.exists.duplicate"
	}
	return "collision.exists.conflict"
}

func (r *Runner) checkpointItem(ctx context.Context, in reflowInput, destURI, destKey, status, reason string, bytes int64, errorCode, errorMessage string) error {
	if r.cfg.Checkpoint == nil {
		return nil
	}
	return r.cfg.Checkpoint.UpsertItem(ctx, CheckpointItem{
		SourceURI:    sanitizeSourceURI(in.SourceURI),
		DestURI:      sanitizeSourceURI(destURI),
		SourceKey:    in.SourceKey,
		DestKey:      destKey,
		SourceETag:   in.SourceETag,
		SourceSize:   in.SourceSize,
		Status:       status,
		Reason:       reason,
		Bytes:        bytes,
		ErrorCode:    errorCode,
		ErrorMessage: errorMessage,
	})
}

func (r *Runner) noteCollision(ctx context.Context, destKey, kind string, in reflowInput, dstMeta *provider.ObjectMeta) error {
	if r.cfg.Checkpoint == nil || dstMeta == nil {
		return nil
	}
	return r.cfg.Checkpoint.NoteCollision(ctx, CheckpointCollision{
		DestKey:    destKey,
		Kind:       kind,
		SourceURI:  sanitizeSourceURI(in.SourceURI),
		SourceETag: in.SourceETag,
		SourceSize: in.SourceSize,
		DestETag:   dstMeta.ETag,
		DestSize:   dstMeta.Size,
	})
}

func (in reflowInput) withSourceMeta(etag string, size int64) reflowInput {
	in.SourceETag = etag
	in.SourceSize = size
	return in
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
