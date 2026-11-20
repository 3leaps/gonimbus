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

// ErrSourceTokenMissing reports that source-read binding was required but the
// input record supplied no expected source token (source_etag or source_version_id).
var ErrSourceTokenMissing = errors.New("source revision token is required")

// ReplayUnverifiableError reports a run-level preflight refusal when required
// source-read binding cannot be enforced by the source provider.
type ReplayUnverifiableError struct {
	Provider string
	Err      error
}

func (e *ReplayUnverifiableError) Error() string {
	if e == nil {
		return provider.ErrReplayUnverifiable.Error()
	}
	if e.Provider != "" {
		return "source provider " + e.Provider + " does not support pinned revision reads: " + provider.ErrReplayUnverifiable.Error()
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return provider.ErrReplayUnverifiable.Error()
}

func (e *ReplayUnverifiableError) Unwrap() error {
	if e == nil || e.Err == nil {
		return provider.ErrReplayUnverifiable
	}
	return e.Err
}

// collisionResolveError carries a specific failed-terminal classification
// (error code + record reason) for a collision-resolution outcome the generic
// error mapping cannot derive — for example the overwrite-if-source-newer guard
// that refuses when the destination has no LastModified to compare against. It
// mirrors the corresponding CLI-pool failed terminal.
type collisionResolveError struct {
	code   string
	reason string
	msg    string
}

func (e *collisionResolveError) Error() string { return e.msg }

func reflowErrCode(err error) string {
	var budgetErr *MetadataBudgetError
	var collisionErr *collisionResolveError
	switch {
	case errors.As(err, &collisionErr):
		return collisionErr.code
	case errors.As(err, &budgetErr):
		return ErrCodeInvalidInput
	case errors.Is(err, ErrSourceTokenMissing), provider.IsSourceChanged(err), provider.IsReplayUnverifiable(err):
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
