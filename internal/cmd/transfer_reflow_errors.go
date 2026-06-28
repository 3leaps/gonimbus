package cmd

import (
	"context"
	"errors"

	"github.com/fulmenhq/gofulmen/foundry"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/transfer"
)

func emitReflowConfigError(ctx context.Context, w output.Writer, msg string, err error, details map[string]any) error {
	if details == nil {
		details = map[string]any{}
	}
	details["mode"] = "transfer_reflow"
	if werr := w.WriteError(ctx, &output.ErrorRecord{Code: output.ErrCodeInvalidInput, Message: reflowpkg.FormatErrorMessage(msg, err), Details: details}); werr != nil {
		observability.CLILogger.Debug("Failed to emit reflow config error record", zap.Error(werr))
	}
	return exitError(foundry.ExitInvalidArgument, msg, err)
}

func emitReflowError(ctx context.Context, w output.Writer, key, msg string, err error, details map[string]any) error {
	return emitReflowErrorWithCode(ctx, w, reflowErrCode(err), key, msg, err, details)
}

func emitReflowInputError(ctx context.Context, w output.Writer, key, msg string, err error, details map[string]any) error {
	return emitReflowErrorWithCode(ctx, w, output.ErrCodeInvalidInput, key, msg, err, details)
}

func emitReflowErrorWithCode(ctx context.Context, w output.Writer, code string, key, msg string, err error, details map[string]any) error {
	if details == nil {
		details = map[string]any{}
	}
	var collision any
	if v, ok := details["collision"]; ok {
		collision = v
		delete(details, "collision")
	}
	details["mode"] = "transfer_reflow"
	if _, ok := details["reason"]; !ok {
		details["reason"] = reflowReasonForErrCode(code)
	}
	if werr := w.WriteError(ctx, &output.ErrorRecord{Code: code, Message: reflowpkg.FormatErrorMessage(msg, err), Key: key, Details: details, Collision: collision}); werr != nil {
		observability.CLILogger.Debug("Failed to emit reflow error record", zap.Error(werr))
	}
	return nil
}

func reflowErrCode(err error) string {
	var budgetErr *metadataBudgetError
	switch {
	case errors.As(err, &budgetErr):
		return output.ErrCodeInvalidInput
	case provider.IsNotFound(err):
		return output.ErrCodeNotFound
	case provider.IsAccessDenied(err):
		return output.ErrCodeAccessDenied
	case provider.IsThrottled(err):
		return output.ErrCodeThrottled
	case provider.IsProviderUnavailable(err):
		return output.ErrCodeProviderUnavailable
	case transfer.IsTransientNetworkError(err):
		return output.ErrCodeTransient
	default:
		return output.ErrCodeInternal
	}
}

func reflowReasonForErrCode(code string) string {
	switch code {
	case output.ErrCodeAccessDenied:
		return "access_denied"
	case output.ErrCodeNotFound:
		return "not_found"
	case output.ErrCodeTimeout:
		return "timeout"
	case output.ErrCodeThrottled:
		return "provider.throttled"
	case output.ErrCodeProviderUnavailable:
		return "provider.unavailable"
	case output.ErrCodeTransient:
		return "transient.network"
	case output.ErrCodeAlreadyExists:
		return "already_exists"
	case output.ErrCodeInvalidInput:
		return "invalid_input"
	default:
		return "internal"
	}
}
