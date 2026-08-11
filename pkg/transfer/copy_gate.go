package transfer

import "context"

// CopyStage names a sequential phase of CopyObject for concurrency gating and
// sterile stage attribution. Source-read fully buffers small objects before
// dest-write begins, so the phases do not need a shared concurrency token.
const (
	// CopyStageSourceRead covers GetObject plus full body materialization into
	// the retry buffer (memory or temp file) for the single-part path.
	CopyStageSourceRead = "source_read"
	// CopyStageDestWrite covers the destination Put (or multipart upload from
	// an already-materialized body).
	CopyStageDestWrite = "dest_write"
	// CopyStageCoupled covers paths that keep source and dest open together
	// (streaming multipart from a live Get body). The global token still spans
	// the whole operation for those paths.
	CopyStageCoupled = "coupled_copy"
)

// CopyGate wraps sequential copy phases so callers can acquire and release
// concurrency permits per phase. A nil gate runs each phase directly (callers
// that hold an outer token, or ungated library use).
//
// Library-first (ADR-0006): both the pkg/reflow engine and the CLI pool pass
// the same gate type so stage boundaries stay identical.
type CopyGate interface {
	// Do runs fn under the gate for the named stage. Implementations must call
	// fn exactly once on the success path and propagate its error.
	Do(ctx context.Context, stage string, fn func(context.Context) error) error
}

type nopCopyGate struct{}

func (nopCopyGate) Do(ctx context.Context, _ string, fn func(context.Context) error) error {
	return fn(ctx)
}

func resolveCopyGate(gate CopyGate) CopyGate {
	if gate == nil {
		return nopCopyGate{}
	}
	return gate
}
