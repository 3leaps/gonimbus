package indexbuild

import (
	"context"
	"fmt"

	"github.com/3leaps/gonimbus/pkg/output"
)

type observationFanoutWriter struct {
	primary *journalWriter
	extra   []output.Writer
}

func newObservationFanoutWriter(primary *journalWriter, extra []output.Writer) *observationFanoutWriter {
	return &observationFanoutWriter{
		primary: primary,
		extra:   append([]output.Writer(nil), extra...),
	}
}

func (w *observationFanoutWriter) WriteObject(ctx context.Context, obj *output.ObjectRecord) error {
	if err := w.primary.WriteObject(ctx, obj); err != nil {
		return err
	}
	for i, sink := range w.extra {
		if sink == nil {
			continue
		}
		if err := sink.WriteObject(ctx, obj); err != nil {
			return fmt.Errorf("observation sink %d object: %w", i, err)
		}
	}
	return nil
}

func (w *observationFanoutWriter) WriteError(ctx context.Context, errRec *output.ErrorRecord) error {
	if err := w.primary.WriteError(ctx, errRec); err != nil {
		return err
	}
	for i, sink := range w.extra {
		if sink == nil {
			continue
		}
		if err := sink.WriteError(ctx, errRec); err != nil {
			return fmt.Errorf("observation sink %d error: %w", i, err)
		}
	}
	return nil
}

func (w *observationFanoutWriter) WriteProgress(ctx context.Context, prog *output.ProgressRecord) error {
	if err := w.primary.WriteProgress(ctx, prog); err != nil {
		return err
	}
	for i, sink := range w.extra {
		if sink == nil {
			continue
		}
		if err := sink.WriteProgress(ctx, prog); err != nil {
			return fmt.Errorf("observation sink %d progress: %w", i, err)
		}
	}
	return nil
}

func (w *observationFanoutWriter) WriteSummary(ctx context.Context, sum *output.SummaryRecord) error {
	if err := w.primary.WriteSummary(ctx, sum); err != nil {
		return err
	}
	for i, sink := range w.extra {
		if sink == nil {
			continue
		}
		if err := sink.WriteSummary(ctx, sum); err != nil {
			return fmt.Errorf("observation sink %d summary: %w", i, err)
		}
	}
	return nil
}

func (w *observationFanoutWriter) WritePrefix(ctx context.Context, prefix *output.PrefixRecord) error {
	if err := w.primary.WritePrefix(ctx, prefix); err != nil {
		return err
	}
	for i, sink := range w.extra {
		if sink == nil {
			continue
		}
		if err := sink.WritePrefix(ctx, prefix); err != nil {
			return fmt.Errorf("observation sink %d prefix: %w", i, err)
		}
	}
	return nil
}

func (w *observationFanoutWriter) WritePreflight(ctx context.Context, preflight *output.PreflightRecord) error {
	if err := w.primary.WritePreflight(ctx, preflight); err != nil {
		return err
	}
	for i, sink := range w.extra {
		if sink == nil {
			continue
		}
		if err := sink.WritePreflight(ctx, preflight); err != nil {
			return fmt.Errorf("observation sink %d preflight: %w", i, err)
		}
	}
	return nil
}

func (w *observationFanoutWriter) WriteTransfer(ctx context.Context, transfer *output.TransferRecord) error {
	if err := w.primary.WriteTransfer(ctx, transfer); err != nil {
		return err
	}
	for i, sink := range w.extra {
		if sink == nil {
			continue
		}
		if err := sink.WriteTransfer(ctx, transfer); err != nil {
			return fmt.Errorf("observation sink %d transfer: %w", i, err)
		}
	}
	return nil
}

func (w *observationFanoutWriter) WriteSkip(ctx context.Context, skip *output.SkipRecord) error {
	if err := w.primary.WriteSkip(ctx, skip); err != nil {
		return err
	}
	for i, sink := range w.extra {
		if sink == nil {
			continue
		}
		if err := sink.WriteSkip(ctx, skip); err != nil {
			return fmt.Errorf("observation sink %d skip: %w", i, err)
		}
	}
	return nil
}

func (w *observationFanoutWriter) Close() error {
	var firstErr error
	if err := w.primary.Close(); err != nil {
		firstErr = err
	}
	for i, sink := range w.extra {
		if sink == nil {
			continue
		}
		if err := sink.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("observation sink %d close: %w", i, err)
		}
	}
	return firstErr
}

var _ output.Writer = (*observationFanoutWriter)(nil)
