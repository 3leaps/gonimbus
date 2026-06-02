package opcheckpoint

import (
	"errors"
	"strings"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// ErrorClass is the stable operation-level failure class emitted in structured
// runtime errors and stored with resumable checkpoints.
type ErrorClass string

const (
	ErrorClassAuthDenied               ErrorClass = "auth_denied"
	ErrorClassCredentialsRefreshFailed ErrorClass = "credentials_refresh_failed"
	ErrorClassInterrupted              ErrorClass = "interrupted"
	ErrorClassTransientRetryExhausted  ErrorClass = "transient_retry_exhausted"
	ErrorClassRuntimeFailure           ErrorClass = "runtime_failure"
)

// ClassifierInput describes the auth model and trigger context available at the
// command boundary. RefreshableCredentials must only be true for auth models
// with refresh semantics, such as SSO/OIDC/STS/OAuth.
type ClassifierInput struct {
	RefreshableCredentials  bool
	Interrupted             bool
	TransientRetryExhausted bool
}

// Classification is the operation-level interpretation of a fatal error.
type Classification struct {
	Class     ErrorClass
	Resumable bool
}

// ClassifyFatalError keeps auth-deny failures ahead of credential-refresh
// detection so revoked or invalid credentials are never presented as benignly
// resumable refresh failures.
func ClassifyFatalError(err error, in ClassifierInput) Classification {
	if isAuthDeny(err) {
		return Classification{Class: ErrorClassAuthDenied, Resumable: false}
	}
	if in.Interrupted {
		return Classification{Class: ErrorClassInterrupted, Resumable: true}
	}
	if in.RefreshableCredentials && isRefreshFailure(err) {
		return Classification{Class: ErrorClassCredentialsRefreshFailed, Resumable: true}
	}
	if in.TransientRetryExhausted {
		return Classification{Class: ErrorClassTransientRetryExhausted, Resumable: true}
	}
	return Classification{Class: ErrorClassRuntimeFailure, Resumable: false}
}

func isAuthDeny(err error) bool {
	if err == nil {
		return false
	}
	if provider.IsAccessDenied(err) || provider.IsInvalidCredentials(err) {
		return true
	}

	msg := strings.ToLower(err.Error())
	denyNeedles := []string{
		"accessdenied",
		"access denied",
		"forbidden",
		"signaturedoesnotmatch",
		"signature does not match",
		"invalidaccesskeyid",
		"invalid access key",
		"invalid static credential",
		"permission denied",
		"policy deny",
		"policy denied",
		"revoked",
	}
	for _, needle := range denyNeedles {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

func isRefreshFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	refreshNeedles := []string{
		"failed to refresh cached credentials",
		"refresh cached credentials failed",
		"refresh cached",
		"invalid_grant",
		"token expired",
		"expired token",
		"sso session has expired",
		"oauth2: token expired",
		"sts token expired",
	}
	for _, needle := range refreshNeedles {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return errors.Is(err, ErrCredentialsRefreshFailed)
}

// ErrCredentialsRefreshFailed is a local sentinel for tests and future provider
// adapters that can identify refresh failure without relying on string matches.
var ErrCredentialsRefreshFailed = errors.New("credentials refresh failed")
