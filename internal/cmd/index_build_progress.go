package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/3leaps/gonimbus/pkg/indexbuild"
	"github.com/3leaps/gonimbus/pkg/output"
)

// stderrProgressWriter is a progress-only observation sink for durable-only
// builds. It renders crawl ProgressRecord lines to stderr and no-ops every
// other record type so it never materializes SQLite or journal side effects.
//
// WriteProgress always returns nil (best-effort): fanout propagates sink errors
// and a broken diagnostic stream must not abort a durable build.
type stderrProgressWriter struct {
	w io.Writer
}

func newStderrProgressWriter(w io.Writer) *stderrProgressWriter {
	if w == nil {
		w = os.Stderr
	}
	return &stderrProgressWriter{w: w}
}

func (w *stderrProgressWriter) WriteObject(context.Context, *output.ObjectRecord) error {
	return nil
}

func (w *stderrProgressWriter) WriteError(context.Context, *output.ErrorRecord) error {
	return nil
}

func (w *stderrProgressWriter) WriteProgress(_ context.Context, prog *output.ProgressRecord) error {
	if prog == nil {
		return nil
	}
	prefix := prog.Prefix
	if prefix == "" {
		prefix = "(root)"
	}
	// Mirror indexIngestWriter: swallow write errors so progress is never a
	// build-failure vector (broken pipe / full redirected stderr).
	_, _ = fmt.Fprintf(w.w, "progress: phase=%s prefix=%s objects_found=%d objects_matched=%d bytes=%d\n",
		prog.Phase,
		prefix,
		prog.ObjectsFound,
		prog.ObjectsMatched,
		prog.BytesTotal,
	)
	return nil
}

func (w *stderrProgressWriter) WriteSummary(context.Context, *output.SummaryRecord) error {
	return nil
}

func (w *stderrProgressWriter) WritePrefix(context.Context, *output.PrefixRecord) error {
	return nil
}

func (w *stderrProgressWriter) WritePreflight(context.Context, *output.PreflightRecord) error {
	return nil
}

func (w *stderrProgressWriter) WriteTransfer(context.Context, *output.TransferRecord) error {
	return nil
}

func (w *stderrProgressWriter) WriteSkip(context.Context, *output.SkipRecord) error {
	return nil
}

func (w *stderrProgressWriter) Close() error { return nil }

var _ output.Writer = (*stderrProgressWriter)(nil)

// newStderrSegmentProgress returns an observational segment-write progress
// hook (counts/phase only — no keys/rel_keys/prefixes). Best-effort: write
// errors are swallowed. The streaming segment writer cannot know the segment
// total up front and reports Total=0 by contract; the total is rendered only
// when known, so a streaming build shows "segment=3" rather than "segment=3/0".
func newStderrSegmentProgress(w io.Writer) indexbuild.OnSegmentProgressFunc {
	if w == nil {
		w = os.Stderr
	}
	return func(progress indexbuild.SegmentProgress) {
		if progress.Total > 0 {
			_, _ = fmt.Fprintf(w, "progress: phase=segmenting segment=%d/%d rows=%d\n",
				progress.Segment, progress.Total, progress.Rows)
			return
		}
		_, _ = fmt.Fprintf(w, "progress: phase=segmenting segment=%d rows=%d\n",
			progress.Segment, progress.Rows)
	}
}
