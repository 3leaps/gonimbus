package cmd

import (
	"fmt"
	"io"
	"strings"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/fulmenhq/gofulmen/logging"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
)

const (
	diagnosticLogFormatPlain      = "plain"
	diagnosticLogFormatStructured = "structured"
)

type diagnosticPrinter struct {
	cmd        *cobra.Command
	logger     *logging.Logger
	structured bool
}

func newDiagnosticPrinter(cmd *cobra.Command, logFormat string) (*diagnosticPrinter, error) {
	switch strings.ToLower(strings.TrimSpace(logFormat)) {
	case "", diagnosticLogFormatPlain:
		return &diagnosticPrinter{cmd: cmd}, nil
	case diagnosticLogFormatStructured:
		if observability.CLILogger == nil {
			return nil, fmt.Errorf("structured diagnostic output requires an initialized CLI logger")
		}
		return &diagnosticPrinter{cmd: cmd, logger: observability.CLILogger, structured: true}, nil
	default:
		return nil, fmt.Errorf("invalid --log-format %q (expected plain or structured)", logFormat)
	}
}

func (p *diagnosticPrinter) Info(msg string, fields ...zap.Field) {
	if p.structured {
		p.logger.Info(msg, fields...)
		return
	}
	_, _ = fmt.Fprintln(p.output(), msg)
}

func (p *diagnosticPrinter) Warn(msg string, fields ...zap.Field) {
	if p.structured {
		p.logger.Warn(msg, fields...)
		return
	}
	_, _ = fmt.Fprintln(p.output(), msg)
}

func (p *diagnosticPrinter) Error(msg string, fields ...zap.Field) {
	if p.structured {
		p.logger.Error(msg, fields...)
		return
	}
	_, _ = fmt.Fprintln(p.errorOutput(), msg)
}

func (p *diagnosticPrinter) output() io.Writer {
	if p.cmd == nil {
		return io.Discard
	}
	return p.cmd.OutOrStdout()
}

func (p *diagnosticPrinter) errorOutput() io.Writer {
	if p.cmd == nil {
		return io.Discard
	}
	return p.cmd.ErrOrStderr()
}

func exitDiagnosticWithCode(cmd *cobra.Command, logger *logging.Logger, logFormat string, exitCode foundry.ExitCode, msg string, err error) {
	if strings.EqualFold(strings.TrimSpace(logFormat), diagnosticLogFormatStructured) {
		ExitWithCode(logger, exitCode, msg, err)
		return
	}
	if cmd == nil {
		ExitWithCodeWriter(io.Discard, exitCode, msg, err)
		return
	}
	ExitWithCodeWriter(cmd.ErrOrStderr(), exitCode, msg, err)
}
