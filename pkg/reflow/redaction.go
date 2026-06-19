package reflow

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// PathErrorOptions controls how local path errors cross the library boundary.
type PathErrorOptions struct {
	Verbose bool
}

// NewPathError returns either the default redacted message or the verbose
// diagnostic message when the caller explicitly opts in.
func NewPathError(defaultMessage string, verboseMessage string, opts PathErrorOptions) error {
	if opts.Verbose && verboseMessage != "" {
		return errors.New(verboseMessage)
	}
	return errors.New(defaultMessage)
}

// SanitizeOperationCauseMessage returns the redacted provider-error message
// used in reflow records, warnings, summaries, and checkpoint causes.
func SanitizeOperationCauseMessage(err error) string {
	if err == nil {
		return ""
	}
	raw := compactOperationErrorMessage(err.Error())
	if raw == "" {
		return ""
	}
	if operationCauseContainsCredentialMaterial(raw) {
		if root := operationCauseRootMessage(err); root != "" {
			return compactOperationErrorMessage(root)
		}
		return "provider error redacted"
	}
	return redactOperationCauseMessage(raw)
}

// FormatErrorMessage combines an operator-facing prefix with a sanitized error.
func FormatErrorMessage(prefix string, err error) string {
	if err == nil {
		return prefix
	}
	if prefix == "" {
		return SanitizeOperationCauseMessage(err)
	}
	return fmt.Sprintf("%s: %s", prefix, SanitizeOperationCauseMessage(err))
}

var (
	operationCauseURLPattern                 = regexp.MustCompile(`https?://[^\s"'<>]+`)
	operationCauseBearerPattern              = regexp.MustCompile(`(?i)(authorization\s*:\s*bearer\s+)[^,\s;]+`)
	operationCauseKeyValuePattern            = regexp.MustCompile(`(?i)\b(x-amz-signature|x-amz-credential|x-amz-security-token|x-goog-signature|x-goog-credential|x-goog-security-token|aws_secret_access_key|aws_session_token|access_token|refresh_token|sessiontoken|authtoken|client_secret|sig|token)\s*[:=]\s*[^,\s;&]+`)
	operationCauseServiceAccountEmailPattern = regexp.MustCompile(`(?i)\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.gserviceaccount\.com\b`)
	operationCauseSensitiveNeedles           = []string{
		"x-amz-signature=",
		"x-amz-credential=",
		"x-amz-security-token=",
		"x-goog-signature=",
		"x-goog-credential=",
		"x-goog-security-token=",
		"authorization: bearer ",
		"authtoken=",
		"aws_secret_access_key",
		"aws_session_token",
		"sharedaccesssignature",
	}
)

func operationCauseRootMessage(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return context.Canceled.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded.Error()
	case provider.IsCredentialsRefreshFailed(err), errors.Is(err, opcheckpoint.ErrCredentialsRefreshFailed):
		return "failed to refresh cached credentials"
	case provider.IsThrottled(err):
		return provider.ErrThrottled.Error()
	case provider.IsProviderUnavailable(err):
		return provider.ErrProviderUnavailable.Error()
	case provider.IsAccessDenied(err):
		return provider.ErrAccessDenied.Error()
	case provider.IsInvalidCredentials(err):
		return provider.ErrInvalidCredentials.Error()
	}
	var providerErr *provider.ProviderError
	if errors.As(err, &providerErr) && providerErr.Err != nil {
		return operationCauseRootMessage(providerErr.Err)
	}
	return ""
}

func operationCauseContainsCredentialMaterial(message string) bool {
	lower := strings.ToLower(message)
	for _, needle := range operationCauseSensitiveNeedles {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	if strings.HasPrefix(lower, "sig=") || strings.Contains(lower, "?sig=") || strings.Contains(lower, "&sig=") {
		return true
	}
	if operationCauseServiceAccountEmailPattern.MatchString(message) {
		return true
	}
	for _, match := range operationCauseURLPattern.FindAllString(message, -1) {
		if operationCauseURLContainsCredentialMaterial(match) {
			return true
		}
	}
	return false
}

func operationCauseURLContainsCredentialMaterial(raw string) bool {
	u, err := url.Parse(strings.TrimRight(raw, ".,);]'\":"))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}
	if u.User != nil {
		if u.User.Username() != "" {
			return true
		}
		if password, ok := u.User.Password(); ok && password != "" {
			return true
		}
	}
	for key := range u.Query() {
		if operationCauseSensitiveQueryKey(key) {
			return true
		}
	}
	return false
}

func redactOperationCauseMessage(message string) string {
	message = operationCauseBearerPattern.ReplaceAllString(message, "${1}<redacted>")
	message = operationCauseKeyValuePattern.ReplaceAllStringFunc(message, func(match string) string {
		for i, r := range match {
			if r == '=' || r == ':' {
				return strings.TrimSpace(match[:i]) + string(r) + "<redacted>"
			}
		}
		return "<redacted>"
	})
	message = operationCauseURLPattern.ReplaceAllStringFunc(message, redactOperationCauseURL)
	return compactOperationErrorMessage(message)
}

func redactOperationCauseURL(raw string) string {
	trailing := ""
	for raw != "" {
		last := raw[len(raw)-1]
		if !strings.ContainsRune(".,);]'\":", rune(last)) {
			break
		}
		trailing = string(last) + trailing
		raw = raw[:len(raw)-1]
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return raw + trailing
	}
	if u.User != nil {
		u.User = url.User("<redacted>")
	}
	query := u.Query()
	for key := range query {
		query.Set(key, "<redacted>")
	}
	u.RawQuery = query.Encode()
	return u.String() + trailing
}

func operationCauseSensitiveQueryKey(key string) bool {
	switch strings.ToLower(key) {
	case "x-amz-signature", "x-amz-credential", "x-amz-security-token",
		"x-goog-signature", "x-goog-credential", "x-goog-security-token",
		"sig", "token", "access_token", "refresh_token", "sessiontoken", "authtoken", "client_secret":
		return true
	default:
		return false
	}
}

func compactOperationErrorMessage(message string) string {
	message = strings.Join(strings.Fields(strings.TrimSpace(message)), " ")
	const maxOperationCauseMessageLen = 2048
	if len(message) > maxOperationCauseMessageLen {
		return message[:maxOperationCauseMessageLen] + "..."
	}
	return message
}
