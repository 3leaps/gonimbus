package cmd

import (
	"fmt"
	"os"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/fulmenhq/gofulmen/logging"
	"go.uber.org/zap"

	"github.com/fulmenhq/gofulmen/errors"
)

// ExitWithCode exits the program with a semantic foundry exit code and logs the error.
// This helper ensures consistent error logging with exit code metadata before exiting.
//
// Parameters:
//   - logger: The logger to use for error output (can be nil for early failures)
//   - exitCode: The foundry exit code constant (e.g., foundry.ExitConfigInvalid)
//   - msg: Human-readable error message
//   - err: The underlying error (can be nil)
func ExitWithCode(logger *logging.Logger, exitCode foundry.ExitCode, msg string, err error) {
	// Get exit code metadata from foundry catalog
	info, ok := foundry.GetExitCodeInfo(exitCode)
	if !ok {
		// Fallback if we can't get exit code info (should never happen)
		fmt.Fprintf(os.Stderr, "FATAL: %s: %v (exit code: %d)\n", msg, err, exitCode)
		os.Exit(int(exitCode))
	}

	// Log error with exit code metadata
	if logger != nil {
		// Use structured logger if available
		fields := []zap.Field{
			zap.Int("exit_code", info.Code),
			zap.String("exit_name", info.Name),
			zap.String("exit_description", info.Description),
			zap.String("exit_category", info.Category),
		}

		// Add structured error fields if it's an ErrorEnvelope
		if envelope, ok := err.(*errors.ErrorEnvelope); ok {
			fields = append(fields,
				zap.String("error_code", envelope.Code),
				zap.String("error_message", envelope.Message),
				zap.String("correlation_id", envelope.CorrelationID),
				zap.String("trace_id", envelope.TraceID),
			)
			if envelope.Context != nil {
				fields = append(fields, zap.Any("error_context", envelope.Context))
			}
			if envelope.Original != nil {
				if originalErr, ok := envelope.Original.(error); ok {
					err = originalErr // Log the underlying error
				}
			}
		}

		fields = append(fields, zap.Error(err))
		logger.Error(msg, fields...)
	} else {
		// Fall back to stderr if no logger available
		if err != nil {
			if envelope, ok := err.(*errors.ErrorEnvelope); ok {
				fmt.Fprintf(os.Stderr, "FATAL: %s [%s]: %v (correlation: %s, trace: %s)\n",
					msg, envelope.Code, envelope.Message, envelope.CorrelationID, envelope.TraceID)
				if envelope.Original != nil {
					if originalErr, ok := envelope.Original.(error); ok {
						fmt.Fprintf(os.Stderr, "Underlying error: %v\n", originalErr)
					}
				}
			} else {
				fmt.Fprintf(os.Stderr, "FATAL: %s: %v\n", msg, err)
			}
		} else {
			fmt.Fprintf(os.Stderr, "FATAL: %s\n", msg)
		}
		fmt.Fprintf(os.Stderr, "Exit Code: %d (%s) - %s\n", info.Code, info.Name, info.Description)
	}

	// Exit with semantic code
	os.Exit(info.Code)
}

// ExitWithCodeStderr is a variant that writes to stderr without a logger.
// Use this for early failures before logger initialization.
//
// Parameters:
//   - exitCode: The foundry exit code constant
//   - msg: Human-readable error message
//   - err: The underlying error (can be nil)
func ExitWithCodeStderr(exitCode foundry.ExitCode, msg string, err error) {
	info, ok := foundry.GetExitCodeInfo(exitCode)
	if !ok {
		// Fallback if we can't get exit code info
		if err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: %s: %v (exit code: %d)\n", msg, err, exitCode)
		} else {
			fmt.Fprintf(os.Stderr, "FATAL: %s (exit code: %d)\n", msg, exitCode)
		}
		os.Exit(int(exitCode))
	}

	// Write to stderr with exit code metadata
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %s: %v\n", msg, err)
	} else {
		fmt.Fprintf(os.Stderr, "FATAL: %s\n", msg)
	}
	fmt.Fprintf(os.Stderr, "Exit Code: %d (%s) - %s\n", info.Code, info.Name, info.Description)

	os.Exit(info.Code)
}
