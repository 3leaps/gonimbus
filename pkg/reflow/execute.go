package reflow

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/3leaps/gonimbus/internal/reflowprobe"
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
	// The library owns the collision write-precondition capability check (ADR-0006
	// authority), before any stream read, event emission, IfAbsent probe, or
	// destination mutation — a direct embedder must be refused the same way the
	// command adapter refuses it, not left to fail only on the first conflict.
	if err := r.validateCollisionCapability(); err != nil {
		return Summary{}, err
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
	limiter.ResetOccupancyWindow()
	runConcurrency := limiter.Snapshot()

	// Event order is irrelevant: an EventSink consumer is event-based, and the
	// parity harness normalizes by sorting.
	if err := r.emitRun(ctx, RunRecord{
		DestURI:          layout.BaseURI,
		DryRun:           r.cfg.DryRun,
		Parallel:         r.cfg.Concurrency.RequestedCeiling,
		ExecutionPath:    ExecutionPathEngine,
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
	arbiter := newDestKeyArbiter()

	// Worker pool honoring the resolved concurrency ceiling. The reader stage
	// (this goroutine) parses, validates, and plans records serially so
	// INVALID_INPUT events keep input order; plannable live copies fan out to
	// EffectiveCeiling workers. The AIMD limiter remains the per-operation
	// concurrency authority — workers still acquire per provider op. Contract:
	// per-object transitions stay ordered, exactly one terminal record is
	// emitted per accepted input, the summary follows worker join, and global
	// input order is not promised.
	poolCtx, cancelPool := context.WithCancel(ctx)
	defer cancelPool()
	workers := r.cfg.Concurrency.EffectiveCeiling
	if workers < 1 {
		workers = 1
	}
	var (
		fatalMu  sync.Mutex
		fatalErr error
	)
	recordFatal := func(err error) {
		fatalMu.Lock()
		if fatalErr == nil {
			fatalErr = err
		}
		fatalMu.Unlock()
		cancelPool()
	}

	tasks := make(chan plannedRecord, workers*2)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				if poolCtx.Err() != nil {
					continue // drain remaining tasks after cancellation
				}
				if err := r.copyAndEmit(poolCtx, task.src, layout, stats, capability, limiter, arbiter, task.in, task.destKey, task.destURI); err != nil {
					recordFatal(err)
				}
			}
		}()
	}

	sourceIdentity := ""
	scanner := bufio.NewScanner(src.Records)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var readerErr error
	for scanner.Scan() {
		if poolCtx.Err() != nil {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		task, ok, err := r.planInputLine(ctx, layout, rewrite, stats, &sourceIdentity, line)
		if err != nil {
			readerErr = err
			break
		}
		if !ok {
			continue
		}
		if r.cfg.DryRun {
			rec := task.in.record(task.destURI, task.destKey, "planned")
			stats.record(rec)
			if err := r.emitRecord(ctx, rec); err != nil {
				readerErr = err
				break
			}
			continue
		}
		// Source resolution stays on the serial reader stage: SourceResolver
		// implementations are never invoked concurrently, so the adapter's lazy
		// provider cache needs no synchronization and library resolvers carry no
		// hidden concurrency requirement. Identity validation (planInputLine)
		// precedes resolution, so a divergent source root never resolves.
		sourceProvider, resolveErr := src.Resolve(ctx, task.in.SourceURI)
		if resolveErr != nil {
			if err := r.recordObjectError(ctx, stats, task.in, task.destURI, task.destKey, "failed to connect to provider", resolveErr, map[string]any{"source_uri": sanitizeSourceURI(task.in.SourceURI), "dest_uri": task.destURI}, nil); err != nil {
				readerErr = err
				break
			}
			continue
		}
		task.src = sourceProvider
		select {
		case tasks <- task:
		case <-poolCtx.Done():
		}
	}
	scanErr := scanner.Err()
	close(tasks)
	wg.Wait()

	fatalMu.Lock()
	firstErr := fatalErr
	fatalMu.Unlock()
	if firstErr == nil {
		firstErr = readerErr
	}
	if firstErr != nil {
		return Summary{}, firstErr
	}
	if err := ctx.Err(); err != nil {
		return Summary{}, err
	}
	if scanErr != nil {
		return Summary{}, scanErr
	}

	summary := stats.summary(layout.BaseURI, r.cfg.Collision.Mode, r.cfg.DryRun, capability, limiter.Snapshot())
	summary.ExecutionPath = ExecutionPathEngine
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
	case CollisionSkipIfDuplicate, CollisionFail, CollisionOverwrite, CollisionOverwriteIfSourceNewer:
		return true
	default:
		return false
	}
}

// MissingConditionalCapabilityError reports that a destination provider cannot
// honor a conditional-write predicate a collision mode requires. It carries the
// provider-dispatch capability label so the command adapter can surface a
// structured missing_capability preflight detail identical to its other
// capability errors, and both the library and the command adapter derive it from
// the same RequireSourceNewerCapability authority.
type MissingConditionalCapabilityError struct {
	// Mode is the collision mode that required the missing predicate.
	Mode string
	// MissingCapability is the provider-dispatch capability label, e.g.
	// "ConditionalPutter.IfMatchETag" or "ConditionalMultipartCompleter.IfMatchETag".
	MissingCapability string
	// reason is the human-readable defect phrased for the destination provider.
	reason string
}

func (e *MissingConditionalCapabilityError) Error() string {
	return fmt.Sprintf("destination provider %s, required by --on-collision=%s", e.reason, e.Mode)
}

// RequireSourceNewerCapability validates, from the destination provider's own
// capability declaration, that it can honor the conditional-write predicates
// overwrite-if-source-newer needs before any read or destination mutation:
//   - an If-Match single-PUT compare-and-swap, always; and
//   - conditional multipart completion when the provider routes large objects
//     through multipart (implements MultipartUploader), so a >threshold overwrite
//     cannot upload parts and then discover the predicate is unsupported only at
//     completion time.
//
// The provider's ConditionalCapabilityReporter declaration is the authority: a
// provider that does not declare its conditional-write capabilities cannot prove
// If-Match support and is refused fail-closed. The mere presence of
// ConditionalPutter is never accepted as proof — an IfAbsent-only implementation
// exposes ConditionalPutter yet cannot honor If-Match. A remote endpoint reached
// through a declaring adapter remains a documented trust boundary. Both the
// library (validateCollisionCapability) and the command adapter
// (ensureCollisionCapability) call this so the two surfaces refuse identically.
func RequireSourceNewerCapability(dst provider.Provider) error {
	reporter, ok := dst.(provider.ConditionalCapabilityReporter)
	if !ok {
		return &MissingConditionalCapabilityError{
			Mode:              CollisionOverwriteIfSourceNewer,
			MissingCapability: "ConditionalPutter.IfMatchETag",
			reason:            "does not declare conditional-write capabilities (If-Match)",
		}
	}
	caps := reporter.ConditionalWriteCapabilities()
	if !caps.IfMatchETag {
		return &MissingConditionalCapabilityError{
			Mode:              CollisionOverwriteIfSourceNewer,
			MissingCapability: "ConditionalPutter.IfMatchETag",
			reason:            "does not honor the If-Match write precondition",
		}
	}
	if _, isMultipart := dst.(provider.MultipartUploader); isMultipart && !caps.ConditionalMultipartCompletion {
		return &MissingConditionalCapabilityError{
			Mode:              CollisionOverwriteIfSourceNewer,
			MissingCapability: "ConditionalMultipartCompleter.IfMatchETag",
			reason:            "supports multipart uploads but cannot complete them conditionally (If-Match) for large objects",
		}
	}
	return nil
}

// validateCollisionCapability enforces the destination write-precondition a
// collision mode requires, in the library (the ADR-0006 authority) rather than
// only in the command adapter. overwrite-if-source-newer resolves conflicts with
// an If-Match conditional PUT, so a destination that cannot prove it honors
// If-Match (and conditional multipart completion for large objects) must be
// refused up front via RequireSourceNewerCapability — otherwise a direct embedder
// mutates fresh keys unconditionally and fails only when a later conflict happens
// to need the predicate. This mirrors the command's ensureCollisionCapability so
// both surfaces refuse identically from one authority.
func (r *Runner) validateCollisionCapability() error {
	if r.cfg.Collision.Mode != CollisionOverwriteIfSourceNewer {
		return nil
	}
	if err := RequireSourceNewerCapability(r.cfg.Destination.Provider); err != nil {
		return fmt.Errorf("reflow: %w", err)
	}
	return nil
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

// plannedRecord is a reader-stage-planned input ready for worker execution. The
// source provider handle is resolved in the serial reader stage (SourceResolver
// implementations are never invoked concurrently) and carried to the worker.
type plannedRecord struct {
	in      reflowInput
	destKey string
	destURI string
	src     provider.Provider
}

// planInputLine parses, validates, and plans one input line in the reader
// stage. ok is false when the line was consumed as an INVALID_INPUT event; a
// non-nil error is an infrastructure (sink) failure that aborts the run.
func (r *Runner) planInputLine(ctx context.Context, layout DestLayout, rewrite *transfer.ReflowRewrite, stats *runStats, sourceIdentity *string, line string) (plannedRecord, bool, error) {
	in, err := parseReflowInputLine(line)
	if err != nil {
		stats.recordInvalidInput()
		return plannedRecord{}, false, r.emitError(ctx, ErrorEvent{Code: ErrCodeInvalidInput, Message: FormatErrorMessage("invalid reflow input", err), Details: map[string]any{"error": err.Error()}})
	}
	if err := validateSourceIdentity(sourceIdentity, in); err != nil {
		stats.recordInvalidInput()
		return plannedRecord{}, false, r.emitError(ctx, ErrorEvent{Code: ErrCodeInvalidInput, Key: in.SourceKey, Message: FormatErrorMessage("invalid input", err), Details: map[string]any{"error": err.Error(), "source_uri": in.SourceURI}})
	}
	destRel, err := planDestRel(in, rewrite)
	if err != nil {
		stats.recordInvalidInput()
		return plannedRecord{}, false, r.emitError(ctx, ErrorEvent{Code: ErrCodeInvalidInput, Key: in.SourceKey, Message: FormatErrorMessage("destination mapping unavailable", err), Details: map[string]any{"error": err.Error(), "source_uri": in.SourceURI}})
	}
	destKey := layout.DestKey(destRel)
	return plannedRecord{in: in, destKey: destKey, destURI: layout.DestURI(destKey)}, true, nil
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

func (r *Runner) copyAndEmit(ctx context.Context, sourceProvider provider.Provider, layout DestLayout, stats *runStats, capability IfAbsentCapability, limiter *ConcurrencyLimiter, arbiter *destKeyArbiter, in reflowInput, destKey, destURI string) error {
	sourceURI := sanitizeSourceURI(in.SourceURI)

	if r.cfg.Checkpoint != nil {
		done, status, err := r.cfg.Checkpoint.ItemDone(ctx, sourceURI, destURI)
		if err != nil {
			return r.recordObjectError(ctx, stats, in, destURI, destKey, "checkpoint read failed", err, map[string]any{"source_uri": sourceURI, "dest_uri": destURI}, nil)
		}
		if done {
			rec := in.record(destURI, destKey, "skipped")
			rec.Reason = "resume." + status
			stats.record(rec)
			// The item's terminal state is already durable from the prior run;
			// failing to refresh it as a resume-skip warns rather than failing
			// an object whose destination state is settled.
			if err := r.checkpointItem(ctx, in, destURI, destKey, "skipped", rec.Reason, 0, "", ""); err != nil {
				if werr := r.emitCheckpointWriteWarning(ctx, warningCodeCheckpointWrite, destKey, destURI, err); werr != nil {
					return werr
				}
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
	sourceSizeKnown := in.SourceSizeKnown
	sourceLastMod := in.SourceLastMod
	var sourceMeta *provider.ObjectMeta
	// overwrite-if-source-newer needs a source LastModified to compare against the
	// destination; head the source to recover it when the input record omitted it,
	// mirroring the CLI pool's needsSourceHeadForCollision.
	needsSourceHeadForCollision := r.cfg.Collision.Mode == CollisionOverwriteIfSourceNewer && sourceLastMod.IsZero()
	// A head required by metadata policy or source-newer collision handling is
	// mandatory; one probed only to recover an absent etag/size is optional and
	// tolerates failure (the copy proceeds with the input's values, an unknown
	// size reserving the conservative memory cap). Mirrors the CLI pool.
	mandatorySourceHead := r.cfg.Metadata.NeedsSourceHead() || needsSourceHeadForCollision
	if mandatorySourceHead || sourceETag == "" || sourceSize == 0 {
		meta, err := limitedHead(ctx, limiter, sourceProvider, in.SourceKey)
		switch {
		case err == nil:
			sourceMeta = meta
			sourceETag = meta.ETag
			sourceSize = meta.Size
			sourceSizeKnown = true // a successful HEAD measures the size
			if !meta.LastModified.IsZero() {
				sourceLastMod = meta.LastModified
			}
		case mandatorySourceHead:
			return r.recordObjectError(ctx, stats, in, destURI, destKey, "source metadata read failed", err, map[string]any{"source_uri": sourceURI, "dest_uri": destURI}, nil)
		}
	}
	if r.cfg.Collision.Mode == CollisionOverwriteIfSourceNewer && sourceLastMod.IsZero() {
		// The comparison is undecidable without a source timestamp; refuse rather
		// than overwrite blindly. Unlike the CLI pool (which emits only an error
		// event), the engine emits one terminal record per accepted input.
		return r.recordObjectError(ctx, stats, in.withSourceMeta(sourceETag, sourceSize), destURI, destKey, "source metadata unavailable",
			&collisionResolveError{code: ErrCodeInvalidInput, reason: "collision.missing_source_last_modified", msg: fmt.Sprintf("source LastModified is required for --on-collision=%s", CollisionOverwriteIfSourceNewer)},
			map[string]any{"source_uri": sourceURI, "dest_uri": destURI}, nil)
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

	copyInput := in.withSourceMeta(sourceETag, sourceSize)
	copyInput.SourceLastMod = sourceLastMod
	copyInput.SourceSizeKnown = sourceSizeKnown
	bytes, putResult, collision, status, reason, err := r.copyWithCollision(ctx, sourceProvider, layout, stats, capability, limiter, arbiter, copyInput, destKey, putOptions)
	if err != nil {
		details := map[string]any{"source_uri": sourceURI, "dest_uri": destURI}
		msg := "copy failed"
		if collision != nil {
			msg = "collision"
		}
		return r.recordObjectError(ctx, stats, in.withSourceMeta(sourceETag, sourceSize), destURI, destKey, msg, err, details, collision)
	}

	if r.cfg.Checkpoint != nil {
		// The durable destination-observed mark is written inside the arbiter
		// gate (copyWithCollision); writing it again here would double a
		// serialized state-store operation per successful object.
		//
		// NoteDestKeySource is auxiliary audit state: failure warns (typed) and
		// continues. The terminal UpsertItem is the resume authority: failure is
		// strict — the object is reported failed, never acknowledged with its
		// success status on a store that could not record it.
		if err := r.cfg.Checkpoint.NoteDestKeySource(ctx, destKey, sourceURI, sourceETag, sourceSize); err != nil {
			if werr := r.emitCheckpointWriteWarning(ctx, warningCodeArbitrationStateWrite, destKey, destURI, err); werr != nil {
				return werr
			}
		}
		if err := r.checkpointItem(ctx, in.withSourceMeta(sourceETag, sourceSize), destURI, destKey, status, reason, bytes, "", ""); err != nil {
			stats.recordError()
			if e := r.emitError(ctx, ErrorEvent{Code: reflowErrCode(err), Key: in.SourceKey, Message: FormatErrorMessage("checkpoint write failed", err), Details: map[string]any{"source_uri": sourceURI, "dest_uri": destURI, "mode": "transfer_reflow", "reason": "checkpoint.write_failed"}, Collision: collision}); e != nil {
				return e
			}
			rec := in.withSourceMeta(sourceETag, sourceSize).record(destURI, destKey, "failed")
			rec.Reason = "checkpoint.write_failed"
			rec.Bytes = bytes
			rec = recordWithCollision(rec, collision)
			stats.record(rec)
			return r.emitRecord(ctx, rec)
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

func (r *Runner) copyWithCollision(ctx context.Context, src provider.Provider, layout DestLayout, stats *runStats, capability IfAbsentCapability, limiter *ConcurrencyLimiter, arbiter *destKeyArbiter, in reflowInput, destKey string, opts provider.PutOptions) (int64, provider.PutResult, *CollisionInfo, string, string, error) {
	dst := r.cfg.Destination.Provider
	if r.cfg.Collision.Mode == CollisionOverwrite {
		return r.copyUnconditionalOverwrite(ctx, src, layout, limiter, in, destKey, opts)
	}

	// Per-dest-key gate: concurrent workers targeting the same destination key
	// serialize through the observed-check/copy critical section so a conditional
	// PUT never races another in-process worker for the same key. For read-only
	// resolution modes (skip/fail) the head + duplicate compare runs after an
	// early release. overwrite-if-source-newer, however, WRITES inside
	// handleExistingDestination (an If-Match conditional overwrite), so its
	// critical section must span head + decision + conditional PUT: hold the gate
	// for the whole resolution so a less-new same-run contender cannot win the
	// If-Match first and durably strand the newer source as concurrent_mutation.
	gate, release := arbiter.acquire(destKey)
	if r.cfg.Collision.Mode == CollisionOverwriteIfSourceNewer {
		// Defer captures the real release (fired once at return); neutralizing the
		// variable turns the paths' inline release() calls into no-ops so the gate
		// stays held across the entire source-newer resolution. An external
		// mutation between head and PUT still yields a concurrent_mutation skip.
		defer release()
		release = func() {}
	}
	known := gate.observed
	if !known && r.cfg.Checkpoint != nil {
		observed, err := r.cfg.Checkpoint.DestKeyObserved(ctx, destKey)
		if err != nil {
			release()
			return 0, provider.PutResult{}, nil, "", "", err
		}
		if observed {
			gate.observed = true
			known = true
		}
	}
	if known {
		release()
		dstMeta, headErr := limitedHead(ctx, limiter, dst, destKey)
		if headErr != nil {
			return 0, provider.PutResult{}, nil, "", "", headErr
		}
		return r.handleExistingDestination(ctx, src, layout, limiter, in, destKey, dstMeta, decisionIfAbsentHead, opts)
	}

	// markObserved records the key as observed on the in-process gate AND in the
	// durable checkpoint store while the gate is still held. The gate entry is
	// deleted once idle, so only the durable mark prevents a later same-key
	// worker from re-attempting a conditional PUT (mirroring the CLI pool, whose
	// durable per-run destination observations live in the checkpoint DB).
	//
	// The mark is auxiliary arbitration state, not the terminal resume
	// authority: on store failure it warns (typed, same code as the CLI pool)
	// and continues. Correctness holds — the in-process gate memo still covers
	// concurrent same-key workers, and a later worker at worst re-drives a
	// conditional PUT the provider (or head fallback) refuses. A returned error
	// is an event-sink infrastructure failure only.
	markObserved := func() error {
		gate.observed = true
		if r.cfg.Checkpoint == nil {
			return nil
		}
		if err := r.cfg.Checkpoint.MarkDestKeyObserved(ctx, destKey); err != nil {
			return r.emitCheckpointWriteWarning(ctx, warningCodeArbitrationStateWrite, destKey, "", err)
		}
		return nil
	}

	if capability.FallbackActive {
		stats.recordFallbackObject()
		dstMeta, headErr := limitedHead(ctx, limiter, dst, destKey)
		switch {
		case headErr == nil:
			markErr := markObserved()
			release()
			if markErr != nil {
				return 0, provider.PutResult{}, nil, "", "", markErr
			}
			return r.handleExistingDestination(ctx, src, layout, limiter, in, destKey, dstMeta, decisionHeadFallback, opts)
		case provider.IsNotFound(headErr):
			bytes, err := limitedCopy(ctx, limiter, src, dst, in.SourceKey, destKey, in.SourceSize, opts)
			if err == nil {
				err = markObserved()
			}
			release()
			return bytes, provider.PutResult{}, nil, "complete", "", err
		default:
			release()
			return 0, provider.PutResult{}, nil, "", "", headErr
		}
	}

	bytes, result, err := limitedCopyConditional(ctx, limiter, src, dst, in.SourceKey, destKey, in.SourceSize, provider.PutPrecondition{IfAbsent: true}, opts)
	if err == nil {
		markErr := markObserved()
		release()
		if markErr != nil {
			return 0, provider.PutResult{}, nil, "", "", markErr
		}
		return bytes, result, nil, "complete", "", nil
	}
	if !isConditionalExists(err) {
		release()
		return 0, provider.PutResult{}, nil, "", "", err
	}
	markErr := markObserved()
	release()
	if markErr != nil {
		return 0, provider.PutResult{}, nil, "", "", markErr
	}
	dstMeta, headErr := limitedHead(ctx, limiter, dst, destKey)
	if headErr != nil {
		return 0, provider.PutResult{}, nil, "", "", headErr
	}
	return r.handleExistingDestination(ctx, src, layout, limiter, in, destKey, dstMeta, decisionIfAbsentHead, opts)
}

// copyUnconditionalOverwrite lands the source over the destination without a
// precondition. Mirroring the CLI pool, it first heads the destination so an
// existing object is reported as a collision (duplicate or conflict) on the
// "unconditional_overwrite" decision path, then copies last-write-wins. A dest
// head returning NotFound simply lands with no collision; any other head error
// is fatal. No per-key arbiter is needed — overwrite is inherently last-writer.
func (r *Runner) copyUnconditionalOverwrite(ctx context.Context, src provider.Provider, layout DestLayout, limiter *ConcurrencyLimiter, in reflowInput, destKey string, opts provider.PutOptions) (int64, provider.PutResult, *CollisionInfo, string, string, error) {
	dst := r.cfg.Destination.Provider
	var collision *CollisionInfo
	dstMeta, headErr := limitedHead(ctx, limiter, dst, destKey)
	switch {
	case headErr == nil:
		kind := collisionConflict
		if isDuplicateCollision(in.SourceProvider, layout.ProviderID, in.SourceETag, in.SourceSize, dstMeta) {
			kind = collisionDuplicate
		}
		collision = newCollisionInfo(kind, dstMeta, decisionOverwrite)
		if err := r.noteCollisionBestEffort(ctx, destKey, "overwrite", in, dstMeta); err != nil {
			return 0, provider.PutResult{}, nil, "", "", err
		}
	case provider.IsNotFound(headErr):
		// Destination absent: land it, no collision.
	default:
		return 0, provider.PutResult{}, nil, "", "", headErr
	}
	bytes, err := limitedCopy(ctx, limiter, src, dst, in.SourceKey, destKey, in.SourceSize, opts)
	return bytes, provider.PutResult{}, collision, "complete", "", err
}

func (r *Runner) handleExistingDestination(ctx context.Context, src provider.Provider, layout DestLayout, limiter *ConcurrencyLimiter, in reflowInput, destKey string, dstMeta *provider.ObjectMeta, decisionPath string, opts provider.PutOptions) (int64, provider.PutResult, *CollisionInfo, string, string, error) {
	duplicate, err := reflowprobe.Run(ctx, limiter, func(ctx context.Context) (bool, error) {
		return isDuplicateCollisionForReflow(ctx, src, r.cfg.Destination.Provider, in.SourceKey, destKey, in.SourceProvider, layout.ProviderID, in.SourceETag, in.SourceSize, dstMeta)
	})
	if err != nil {
		return 0, provider.PutResult{}, nil, "", "", err
	}
	if duplicate {
		collision := newCollisionInfo(collisionDuplicate, dstMeta, decisionPath)
		if err := r.noteCollisionBestEffort(ctx, destKey, "duplicate", in, dstMeta); err != nil {
			return 0, provider.PutResult{}, nil, "", "", err
		}
		if r.cfg.Collision.Mode == CollisionSkipIfDuplicate || r.cfg.Collision.Mode == CollisionQuarantine || r.cfg.Collision.Mode == CollisionOverwriteIfSourceNewer {
			return 0, provider.PutResult{}, collision, "skipped", "collision.duplicate", nil
		}
		return 0, provider.PutResult{}, collision, "", "", fmt.Errorf("destination key exists with identical content: %s", destKey)
	}

	// A genuine content conflict. overwrite-if-source-newer resolves it by
	// comparing timestamps and conditionally overwriting; every other conflict
	// terminal mode fails closed.
	if r.cfg.Collision.Mode == CollisionOverwriteIfSourceNewer {
		return r.resolveSourceNewerConflict(ctx, src, limiter, in, destKey, dstMeta, decisionPath, opts)
	}

	collision := newCollisionInfo(collisionConflict, dstMeta, decisionPath)
	if err := r.noteCollisionBestEffort(ctx, destKey, "conflict", in, dstMeta); err != nil {
		return 0, provider.PutResult{}, nil, "", "", err
	}
	return 0, provider.PutResult{}, collision, "", "", fmt.Errorf("destination key exists with different content: %s", destKey)
}

// resolveSourceNewerConflict resolves an overwrite-if-source-newer content
// conflict against an existing destination, mirroring the CLI pool: the source
// wins (and is conditionally overwritten with If-Match on the observed dest
// ETag) only when it is strictly newer, or equally-timed but a different size;
// otherwise the destination is preserved. A dest mutated between the head and
// the conditional PUT yields a concurrent-mutation skip. All three terminals
// carry byte-identical source-newer collision metadata for dual-path parity.
func (r *Runner) resolveSourceNewerConflict(ctx context.Context, src provider.Provider, limiter *ConcurrencyLimiter, in reflowInput, destKey string, dstMeta *provider.ObjectMeta, decisionPath string, opts provider.PutOptions) (int64, provider.PutResult, *CollisionInfo, string, string, error) {
	sourceNewerDecisionPath := decisionHeadCompare
	if decisionPath == decisionHeadFallback {
		sourceNewerDecisionPath = decisionHeadFallback
	}
	if dstMeta.LastModified.IsZero() {
		// The destination key travels only in the record's structured key/dest_uri
		// fields, never interpolated into this free-text message/cause.
		return 0, provider.PutResult{}, nil, "", "", &collisionResolveError{
			code:   ErrCodeInvalidInput,
			reason: "collision.missing_dest_last_modified",
			msg:    fmt.Sprintf("destination LastModified is required for --on-collision=%s", CollisionOverwriteIfSourceNewer),
		}
	}

	decisionReason := reasonSrcOlder
	shouldOverwrite := false
	switch {
	case in.SourceLastMod.After(dstMeta.LastModified):
		decisionReason = reasonSrcNewer
		shouldOverwrite = true
	case in.SourceLastMod.Equal(dstMeta.LastModified):
		// Equal timestamps: only a KNOWN size difference breaks the tie. An unknown
		// source size (numeric-zero sentinel from an absent field plus a tolerated
		// optional HEAD) must never be read as "different" and authorize a
		// destructive overwrite — refuse fail-closed instead.
		if !in.SourceSizeKnown {
			return 0, provider.PutResult{}, nil, "", "", &collisionResolveError{
				code:   ErrCodeInvalidInput,
				reason: "collision.source_size_unavailable",
				msg:    fmt.Sprintf("source size is required to resolve an equal-timestamp conflict for --on-collision=%s", CollisionOverwriteIfSourceNewer),
			}
		}
		if in.SourceSize != dstMeta.Size {
			decisionReason = reasonEqualSizeDiffers
			shouldOverwrite = true
		}
	}

	if !shouldOverwrite {
		collision := newSourceNewerCollisionInfo(collisionSrcOlder, dstMeta, in.SourceLastMod, sourceNewerDecisionPath, decisionReason)
		if err := r.noteCollisionBestEffort(ctx, destKey, "conflict", in, dstMeta); err != nil {
			return 0, provider.PutResult{}, nil, "", "", err
		}
		return 0, provider.PutResult{}, collision, "skipped", "collision.skipped_src_older", nil
	}

	collision := newSourceNewerCollisionInfo(collisionOverwritten, dstMeta, in.SourceLastMod, sourceNewerDecisionPath, decisionReason)
	etag := dstMeta.ETag
	bytes, result, err := limitedCopyConditional(ctx, limiter, src, r.cfg.Destination.Provider, in.SourceKey, destKey, in.SourceSize, provider.PutPrecondition{IfMatchETag: &etag}, opts)
	if err != nil {
		if isConditionalExists(err) {
			concurrent := newSourceNewerCollisionInfo(collisionConcurrentMut, dstMeta, in.SourceLastMod, sourceNewerDecisionPath, reasonConcurrentMut)
			if nerr := r.noteCollisionBestEffort(ctx, destKey, "conflict", in, dstMeta); nerr != nil {
				return 0, provider.PutResult{}, nil, "", "", nerr
			}
			return 0, provider.PutResult{}, concurrent, "skipped", "collision.skipped_concurrent_mutation", nil
		}
		return 0, provider.PutResult{}, collision, "", "", err
	}
	// The overwrite has landed: recording the overwrite collision is auxiliary
	// audit state, so a store failure warns (typed) rather than un-landing a
	// completed object (mirrors NoteDestKeySource and the CLI pool).
	if err := r.noteCollisionBestEffort(ctx, destKey, "overwrite", in, dstMeta); err != nil {
		return 0, provider.PutResult{}, nil, "", "", err
	}
	return bytes, result, collision, "complete", "", nil
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
	// Recording the failure in the checkpoint store is best-effort: the object
	// is already being reported failed, so a store write failure here warns
	// (typed) rather than escalating — an unrecorded failed item is simply
	// re-driven on resume.
	if cperr := r.checkpointItem(ctx, in, destURI, destKey, "failed", reflowReasonForErrCode(code), 0, code, SanitizeOperationCauseMessage(err)); cperr != nil {
		if werr := r.emitCheckpointWriteWarning(ctx, warningCodeCheckpointWrite, destKey, destURI, cperr); werr != nil {
			return werr
		}
	}
	rec := in.record(destURI, destKey, "failed")
	rec.Reason = failedRecordReason(err, code, collision)
	rec = recordWithCollision(rec, collision)
	stats.record(rec)
	return r.emitRecord(ctx, rec)
}

// Checkpoint-write warning codes. warningCodeArbitrationStateWrite matches the
// CLI pool's code for the same condition (auxiliary arbitration/audit state
// write failed; run continues); warningCodeCheckpointWrite covers best-effort
// item writes whose failure does not change the object's reported outcome.
const (
	warningCodeArbitrationStateWrite = "REFLOW_ARBITRATION_STATE_WRITE_FAILED"
	warningCodeCheckpointWrite       = "REFLOW_CHECKPOINT_WRITE_FAILED"
)

func (r *Runner) emitCheckpointWriteWarning(ctx context.Context, code, destKey, destURI string, cause error) error {
	details := map[string]any{}
	if destURI != "" {
		details["dest_uri"] = destURI
	}
	return r.emitWarning(ctx, Warning{
		Code:    code,
		Message: "checkpoint state write failed: " + SanitizeOperationCauseMessage(cause),
		Key:     destKey,
		Details: details,
	})
}

func limitedHead(ctx context.Context, limiter *ConcurrencyLimiter, p provider.Provider, key string) (*provider.ObjectMeta, error) {
	return reflowprobe.Run(ctx, limiter, func(ctx context.Context) (*provider.ObjectMeta, error) {
		return p.Head(ctx, key)
	})
}

func limitedCopy(ctx context.Context, limiter *ConcurrencyLimiter, src provider.Provider, dst provider.Provider, srcKey, dstKey string, sourceSize int64, opts provider.PutOptions) (int64, error) {
	releaseMem, err := limiter.ReserveCopyMemory(ctx, sourceSize)
	if err != nil {
		return 0, err
	}
	defer releaseMem()
	release, err := limiter.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	bytes, err := transfer.CopyObjectWithOptions(ctx, src, dst, srcKey, dstKey, sourceSize, limiter.RetryBufferCap(), opts)
	limiter.ObserveProviderResult(err)
	return bytes, err
}

func limitedCopyConditional(ctx context.Context, limiter *ConcurrencyLimiter, src provider.Provider, dst provider.Provider, srcKey, dstKey string, sourceSize int64, precond provider.PutPrecondition, opts provider.PutOptions) (int64, provider.PutResult, error) {
	releaseMem, err := limiter.ReserveCopyMemory(ctx, sourceSize)
	if err != nil {
		return 0, provider.PutResult{}, err
	}
	defer releaseMem()
	release, err := limiter.Acquire(ctx)
	if err != nil {
		return 0, provider.PutResult{}, err
	}
	defer release()
	bytes, result, err := transfer.CopyObjectConditionalWithOptions(ctx, src, dst, srcKey, dstKey, sourceSize, limiter.RetryBufferCap(), precond, opts)
	limiter.ObserveProviderResult(err)
	return bytes, result, err
}

func failedRecordReason(err error, code string, collision *CollisionInfo) string {
	var collisionErr *collisionResolveError
	if errors.As(err, &collisionErr) && collisionErr.reason != "" {
		return collisionErr.reason
	}
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

// noteCollisionBestEffort records collision audit state as auxiliary state,
// matching the CLI pool's checkpointWriteFailed contract: a store-write failure
// emits the sanitized checkpoint-state warning and is swallowed so it never
// alters the collision decision, the copy, or the terminal outcome. Only an
// event-sink failure — a genuine infrastructure fault — propagates and aborts
// the run. The terminal UpsertItem remains the strict resume authority.
func (r *Runner) noteCollisionBestEffort(ctx context.Context, destKey, kind string, in reflowInput, dstMeta *provider.ObjectMeta) error {
	if err := r.noteCollision(ctx, destKey, kind, in, dstMeta); err != nil {
		return r.emitCheckpointWriteWarning(ctx, warningCodeArbitrationStateWrite, destKey, "", err)
	}
	return nil
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
// at construction. A nil EventSink disables delivery. Delivery is serialized
// (emitMu) so sink implementations never observe concurrent calls even though
// engine workers execute objects concurrently.

func (r *Runner) emitRun(ctx context.Context, rec RunRecord) error {
	if r.cfg.Events == nil {
		return nil
	}
	r.emitMu.Lock()
	defer r.emitMu.Unlock()
	return r.cfg.Events.OnRun(ctx, rec)
}

func (r *Runner) emitRecord(ctx context.Context, rec Record) error {
	if r.cfg.Events == nil {
		return nil
	}
	rec.Details = sanitizeDetails(rec.Details)
	r.emitMu.Lock()
	defer r.emitMu.Unlock()
	return r.cfg.Events.OnRecord(ctx, rec)
}

func (r *Runner) emitWarning(ctx context.Context, w Warning) error {
	if r.cfg.Events == nil {
		return nil
	}
	w.Details = sanitizeDetails(w.Details)
	r.emitMu.Lock()
	defer r.emitMu.Unlock()
	return r.cfg.Events.OnWarning(ctx, w)
}

func (r *Runner) emitError(ctx context.Context, e ErrorEvent) error {
	if r.cfg.Events == nil {
		return nil
	}
	e.Details = sanitizeDetails(e.Details)
	r.emitMu.Lock()
	defer r.emitMu.Unlock()
	return r.cfg.Events.OnError(ctx, e)
}

func (r *Runner) emitSummary(ctx context.Context, rec SummaryRecord) error {
	if r.cfg.Events == nil {
		return nil
	}
	r.emitMu.Lock()
	defer r.emitMu.Unlock()
	return r.cfg.Events.OnSummary(ctx, rec)
}
