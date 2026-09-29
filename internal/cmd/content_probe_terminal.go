package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
)

// probeTerminalWriter owns the output boundary, including failure latching.
// Cancellation caused by this latch is internal shutdown, not caller abort.
// After any failed write no worker or finalizer may append another record.
type probeTerminalWriter struct {
	output.Writer
	jsonl      *output.JSONLWriter
	jobID      string
	mu         sync.Mutex
	failure    error
	cancel     context.CancelFunc
	summary    probe.Summary
	inputs     atomic.Int64
	enumerated atomic.Int64
	processed  atomic.Int64
	bytesRead  atomic.Int64
	invalid    atomic.Int64
	fatalCode  int
}

func newProbeTerminalWriter(w *output.JSONLWriter, cancel context.CancelFunc) *probeTerminalWriter {
	return &probeTerminalWriter{Writer: w, jsonl: w, cancel: cancel, summary: probe.Summary{ErrorsByCode: map[string]int64{}}}
}

func (w *probeTerminalWriter) WriteAny(ctx context.Context, kind string, data any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failure != nil {
		return w.failure
	}
	if err := w.jsonl.WriteAny(ctx, kind, data); err != nil {
		return w.latch(err)
	}
	switch kind {
	case "gonimbus.reflow.input.v1":
		w.summary.Emitted.ReflowInput++
	case "gonimbus.content.probe.v1":
		w.summary.Emitted.Probe++
	}
	return nil
}

func (w *probeTerminalWriter) WriteError(ctx context.Context, rec *output.ErrorRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failure != nil {
		return w.failure
	}
	if rec == nil || !probe.KnownErrorCode(rec.Code) {
		return w.latch(fmt.Errorf("invalid probe error record code"))
	}
	if err := w.jsonl.WriteError(ctx, rec); err != nil {
		return w.latch(err)
	}
	w.summary.Errors++
	w.summary.ErrorsByCode[rec.Code]++
	return nil
}

// latch runs inside the serialized output boundary before another writer can
// enter. The first error is authoritative even when cancellation follows.
func (w *probeTerminalWriter) latch(err error) error {
	w.failure = err
	w.cancel()
	return err
}

func (w *probeTerminalWriter) outputFailure() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failure
}

func (w *probeTerminalWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.jsonl.Close()
}

func (w *probeTerminalWriter) snapshot() probe.Summary {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := w.summary
	s.ErrorsByCode = make(map[string]int64, len(w.summary.ErrorsByCode))
	for k, v := range w.summary.ErrorsByCode {
		s.ErrorsByCode[k] = v
	}
	return s
}

type probeExitCause struct {
	code    int
	message string
	cause   error
}

func (e *probeExitCause) Error() string {
	return fmt.Sprintf("%s: %v (exit code %d)", e.message, e.cause, e.code)
}
func (e *probeExitCause) Unwrap() error { return e.cause }
func probeExit(code int, message string, err error) error {
	return &probeExitCause{code: code, message: message, cause: err}
}

// ProbeExitCode recognizes only content-probe's typed outcome. Other command
// errors retain their existing process dispatch, regardless of message text.
func ProbeExitCode(err error) (foundry.ExitCode, bool) {
	var cause *probeExitCause
	if errors.As(err, &cause) {
		return foundry.ExitCode(cause.code), true
	}
	return 0, false
}

// Setup diagnostics are persistent evidence. Raw parser/regexp/file errors
// can quote arbitrary configuration values, so neither Error nor Unwrap carries
// that material into stdout or process-dispatch stderr.
func probeSetupExit(code int, stage string) error {
	return probeExit(code, stage, errors.New("probe setup rejected"))
}

func (w *probeTerminalWriter) markFatal(code int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fatalCode == 0 {
		w.fatalCode = code
	}
}

func (w *probeTerminalWriter) routed(route string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if route == "quarantine" {
		w.summary.Routing.Quarantine++
	} else {
		w.summary.Routing.Normal++
	}
}

func runContentProbe(cmd *cobra.Command, args []string) error {
	started := time.Now()
	caller, stopSignals := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	// Let a broken stdout reach the typed output-failure path rather than the
	// runtime's special immediate SIGPIPE exit for standard output descriptors.
	pipeSignals := make(chan os.Signal, 1)
	signal.Notify(pipeSignals, syscall.SIGPIPE)
	defer signal.Stop(pipeSignals)
	ctx, cancel := context.WithCancel(caller)
	defer cancel()
	jobID := uuid.NewString()
	w := newProbeTerminalWriter(output.NewJSONLWriter(cmd.OutOrStdout(), jobID, commandOutputProviderForInputs(args, "s3")), cancel)
	w.jobID = jobID
	defer func() { _ = w.Close() }()
	err := runContentProbeWork(cmd, args, ctx, w)
	if failure := w.outputFailure(); failure != nil {
		return probeExit(foundry.ExitFailure, "content probe output failed", failure)
	}
	s := w.snapshot()
	s.Inputs, s.Enumerated, s.Processed = w.inputs.Load(), w.enumerated.Load(), w.processed.Load()
	s.InvalidInputs, s.BytesRead = w.invalid.Load(), w.bytesRead.Load()
	s.WallMS = time.Since(started).Milliseconds()
	s.Termination = "completed"
	switch {
	case caller.Err() != nil:
		s.Termination, s.ExitCode = "aborted", foundry.ExitSignalInt
		err = probeExit(s.ExitCode, "content probe cancelled", caller.Err())
	case err != nil:
		s.Termination, s.ExitCode = "failed", foundry.ExitFailure
		var cause *probeExitCause
		if errors.As(err, &cause) {
			s.ExitCode = cause.code
		}
		_ = emitContentProbeError(context.Background(), w, "", "content probe setup failed", err, nil)
	case w.fatalCode != 0:
		s.Termination, s.ExitCode = "failed", w.fatalCode
		err = probeExit(s.ExitCode, "content probe failed", fmt.Errorf("run-fatal probe outcome"))
	case s.InvalidInputs > 0:
		s.Termination, s.ExitCode = "failed", foundry.ExitInvalidArgument
		err = probeExit(s.ExitCode, "content probe completed with invalid inputs", fmt.Errorf("invalid_inputs=%d", s.InvalidInputs))
	case s.Errors > 0:
		s.Termination, s.ExitCode = "completed_with_errors", foundry.ExitDataInvalid
		err = probeExit(s.ExitCode, "content probe completed with errors", fmt.Errorf("errors=%d", s.Errors))
	}
	// Setup diagnostics may have updated counts or failed output. Workers have
	// drained; finalization never resumes provider work and ignores caller cancel.
	if failure := w.outputFailure(); failure != nil {
		return probeExit(foundry.ExitFailure, "content probe output failed", failure)
	}
	counts := w.snapshot()
	s.Errors, s.ErrorsByCode = counts.Errors, counts.ErrorsByCode
	if failure := w.WriteAny(context.Background(), probe.SummaryRecordType, s); failure != nil {
		return probeExit(foundry.ExitFailure, "content probe output failed", failure)
	}
	return err
}
