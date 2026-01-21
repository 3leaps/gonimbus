package transfer

import (
	"context"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
)

func classifyErrCode(err error) string {
	switch {
	case provider.IsNotFound(err):
		return output.ErrCodeNotFound
	case provider.IsAccessDenied(err):
		return output.ErrCodeAccessDenied
	case provider.IsThrottled(err):
		return output.ErrCodeThrottled
	case provider.IsProviderUnavailable(err):
		return output.ErrCodeProviderUnavailable
	case err == context.Canceled || err == context.DeadlineExceeded:
		return output.ErrCodeTimeout
	case isSizeMismatch(err):
		// A stale list/index case; treat as NOT_FOUND for now to keep taxonomy small.
		return output.ErrCodeNotFound
	default:
		return output.ErrCodeInternal
	}
}

func isSizeMismatch(err error) bool {
	_, ok := err.(*SizeMismatchError)
	return ok
}
