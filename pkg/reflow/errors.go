package reflow

import (
	"errors"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/transfer"
)

const (
	ErrCodeAccessDenied        = "ACCESS_DENIED"
	ErrCodeNotFound            = "NOT_FOUND"
	ErrCodeThrottled           = "THROTTLED"
	ErrCodeProviderUnavailable = "PROVIDER_UNAVAILABLE"
	ErrCodeTransient           = "TRANSIENT"
	ErrCodeAlreadyExists       = "ALREADY_EXISTS"
	ErrCodeInternal            = "INTERNAL"
)

func reflowErrCode(err error) string {
	var budgetErr *MetadataBudgetError
	switch {
	case errors.As(err, &budgetErr):
		return ErrCodeInvalidInput
	case provider.IsNotFound(err):
		return ErrCodeNotFound
	case provider.IsAccessDenied(err):
		return ErrCodeAccessDenied
	case provider.IsThrottled(err):
		return ErrCodeThrottled
	case provider.IsProviderUnavailable(err):
		return ErrCodeProviderUnavailable
	case transfer.IsTransientNetworkError(err):
		return ErrCodeTransient
	default:
		return ErrCodeInternal
	}
}

func reflowReasonForErrCode(code string) string {
	switch code {
	case ErrCodeAccessDenied:
		return "access_denied"
	case ErrCodeNotFound:
		return "not_found"
	case ErrCodeThrottled:
		return "provider.throttled"
	case ErrCodeProviderUnavailable:
		return "provider.unavailable"
	case ErrCodeTransient:
		return "transient.network"
	case ErrCodeAlreadyExists:
		return "already_exists"
	case ErrCodeInvalidInput:
		return "invalid_input"
	default:
		return "internal"
	}
}
