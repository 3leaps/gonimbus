package reflow

import (
	"context"
	"errors"
)

// ErrNotImplemented is returned by Runner.Run while the data/decision plane is
// still driven by the command layer. Until the engine migration completes, the
// CLI remains the execution path.
var ErrNotImplemented = errors.New("reflow: runner execution not yet implemented (engine migration in progress)")

// Runner executes reflow runs against an injected destination/provider matrix.
// Construct one with NewRunner.
//
// Experimental: the execution surface is a skeleton in this release — Run returns
// ErrNotImplemented until the engine migration lands. The contract types
// (Config, Source, Destination, EventSink, CheckpointStore, Summary) are shaped
// to build against; see docs/api-stability.md.
type Runner struct {
	cfg Config
}

// NewRunner validates cfg and returns a Runner. It performs no I/O.
func NewRunner(cfg Config) (*Runner, error) {
	if cfg.Destination.Provider == nil {
		return nil, errors.New("reflow: Config.Destination.Provider is required")
	}
	if cfg.Destination.ProviderID == "" {
		return nil, errors.New("reflow: Config.Destination.ProviderID is required")
	}
	return &Runner{cfg: cfg}, nil
}

// Config returns a copy of the runner's configuration.
func (r *Runner) Config() Config { return r.cfg }

// Run executes the reflow for src and returns a Summary. When Config.Events is
// set, the typed event stream carries the run/source records, per-object Records,
// Warnings, Errors, and the terminal Summary — all redacted before delivery.
//
// Experimental: the engine migration is landing incrementally. Run executes the
// migrated paths (currently the dry-run plane over a RecordStreamSource) and
// returns ErrNotImplemented for forms and scenarios still driven by the command
// layer, so callers can fall back to the CLI path for those.
func (r *Runner) Run(ctx context.Context, src Source) (Summary, error) {
	if src == nil {
		return Summary{}, errors.New("reflow: source is required")
	}
	return r.run(ctx, src)
}

// Summary is the typed result of a reflow Run, mirroring the
// gonimbus.reflow.summary.v1 payload.
type Summary struct {
	SummaryRecord
}
