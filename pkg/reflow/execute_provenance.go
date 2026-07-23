package reflow

import (
	"context"
	"io"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// provenanceMode labels the engine surface in emitted sidecar warn/error details.
// It matches the command pool's historical value ("transfer_reflow", underscore)
// so a details field is identical across paths.
const provenanceMode = "transfer_reflow"

// engineProvenanceEmitter delivers the shared sidecar writer's warn/fail outcomes
// through the engine's EventSink (sanitized), so the engine keeps its native
// event shape while sharing content and policy with the command pool. Sink
// delivery errors propagate: a warn/error that cannot be delivered must not
// let the item reach a success terminal.
type engineProvenanceEmitter struct {
	r   *Runner
	key string
}

func (e engineProvenanceEmitter) EmitProvenanceWarning(ctx context.Context, w Warning) error {
	return e.r.emitWarning(ctx, w)
}

func (e engineProvenanceEmitter) EmitProvenanceError(ctx context.Context, key, message string, cause error, details map[string]any) error {
	return e.r.emitError(ctx, ErrorEvent{
		Code:    ErrCodeInternal,
		Key:     e.key,
		Message: FormatErrorMessage(message, cause),
		Details: details,
	})
}

// provenanceActionForTerminal maps a resolved terminal (status/reason) to the
// pool sidecar action and reports eligibility. Only the shipped-eligible
// terminals get a sidecar: a landed object (complete, including an overwrite or a
// source-newer re-land) and a duplicate skip. Every other terminal — source-older
// / concurrent-mutation skips, resume, dry-run, and error — is ineligible, so the
// engine never expands the written audit surface beyond the published contract.
func provenanceActionForTerminal(status, reason string) (string, bool) {
	switch {
	case status == "complete":
		return "landed", true
	case status == "skipped" && reason == "collision.duplicate":
		return "skipped.duplicate", true
	default:
		return "", false
	}
}

// engineProvenancePut builds the AIMD-limited write transport for a sidecar PUT,
// returning nil when the resolved sidecar destination cannot accept a PUT (so the
// shared writer reports it through the on-write-error policy). The PUT traverses
// the engine limiter (Acquire + ObserveProviderResult) so a dynamically reduced
// ceiling and throttle/recovery are honored, not just the worker count.
func engineProvenancePut(limiter *ConcurrencyLimiter, sidecarDst provider.Provider) ProvenancePutFunc {
	putter, ok := sidecarDst.(provider.ObjectPutter)
	if !ok {
		return nil
	}
	return func(ctx context.Context, key string, body io.Reader, size int64) error {
		return limitedProvenancePut(ctx, limiter, putter, key, body, size)
	}
}

func limitedProvenancePut(ctx context.Context, limiter *ConcurrencyLimiter, putter provider.ObjectPutter, key string, body io.Reader, size int64) error {
	release, err := limiter.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	err = putter.PutObject(ctx, key, body, size)
	limiter.ObserveProviderResult(err)
	return err
}

// writeItemProvenanceSidecar writes the provenance sidecar for an eligible
// terminal, resolving the sidecar key/URI/authority from the validated plan and
// delegating content + policy to the shared authority. It reconstructs the
// destination ETag/size the way the command pool does (a fresh land from the PUT
// result and copied byte count; a duplicate skip from the observed collision
// metadata) so the sidecar destination block is byte-identical across paths.
// Returns (nil, false, nil) for an ineligible terminal.
func (r *Runner) writeItemProvenanceSidecar(ctx context.Context, in reflowInput, layout DestLayout, limiter *ConcurrencyLimiter, destRel, destKey, destURI, status, reason string, collision *CollisionInfo, putResult provider.PutResult, bytes int64) (*ProvenanceRef, bool, error) {
	action, eligible := provenanceActionForTerminal(status, reason)
	if !eligible {
		return nil, false, nil
	}
	plan := r.cfg.Provenance
	sidecarKey := plan.sidecarKeyFor(destRel, destKey)
	sidecarURI := plan.sidecarURIFor(layout, sidecarKey)
	sidecarDst := plan.sidecarProvider(r.cfg.Destination.Provider)

	sidecarInput := ProvenanceSidecarInput{
		SourceURI:        in.SourceURI,
		SourceETag:       in.SourceETag,
		SourceSize:       in.SourceSize,
		SourceLastMod:    in.SourceLastMod,
		DestURI:          destURI,
		RoutingClass:     in.RoutingClass,
		RewriteTemplate:  r.cfg.Rewrite.To,
		QuarantinePrefix: in.QuarantinePrefix,
		Collision:        collision,
		Vars:             in.Vars,
		Probe:            in.Probe,
		Action:           action,
	}
	switch {
	case status == "complete":
		sidecarInput.DestETag = putResult.ETag
		sidecarInput.DestSize = bytes
	case collision != nil:
		sidecarInput.DestETag = collision.DestETagObserved
		if collision.DestSizeObserved != nil {
			sidecarInput.DestSize = *collision.DestSizeObserved
		}
	}

	return WriteProvenanceSidecar(ctx, engineProvenancePut(limiter, sidecarDst), plan.RunID, plan.ToolVersion, ProvenanceNow(), provenanceMode, plan.OnWriteError, sidecarKey, sidecarURI, destURI, sidecarInput, engineProvenanceEmitter{r: r, key: in.SourceKey})
}

// finalizeProvenanceFailed reports the terminal for a fail-policy sidecar write
// failure: the main object may have landed, but the item is failed with
// provenance.write_failed and is never acknowledged complete. The
// checkpoint write is best-effort here (the item is already failed), matching the
// command pool and recordObjectError.
func (r *Runner) finalizeProvenanceFailed(ctx context.Context, stats *runStats, in reflowInput, destURI, destKey string, bytes int64, collision *CollisionInfo, ref *ProvenanceRef) error {
	stats.recordError()
	if err := r.checkpointItem(ctx, in, destURI, destKey, "failed", "provenance.write_failed", bytes, ErrCodeInternal, "provenance sidecar write failed"); err != nil {
		if werr := r.emitCheckpointWriteWarning(ctx, warningCodeCheckpointWrite, destKey, destURI, err); werr != nil {
			return werr
		}
	}
	rec := in.record(destURI, destKey, "failed")
	rec.Reason = "provenance.write_failed"
	rec.Bytes = bytes
	rec.Provenance = ref
	rec = recordWithCollision(rec, collision)
	stats.record(rec)
	return r.emitRecord(ctx, rec)
}
