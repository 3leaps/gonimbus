package indexenrich

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/3leaps/gonimbus/pkg/provider"
)

var (
	urlPattern      = regexp.MustCompile(`https?://[^\s"'<>]+`)
	bearerPattern   = regexp.MustCompile(`(?i)(authorization\s*:\s*bearer\s+)[^,\s;]+`)
	keyValuePattern = regexp.MustCompile(`(?i)\b(x-amz-signature|x-amz-credential|x-amz-security-token|x-goog-signature|x-goog-credential|x-goog-security-token|aws_secret_access_key|aws_session_token|access_token|refresh_token|sessiontoken|authtoken|client_secret|sig|token)\s*[:=]\s*[^,\s;&]+`)
)

func classifyHeadError(err error) (code string, retryable bool) {
	switch {
	case err == nil:
		return "", false
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "interrupted", false
	case provider.IsThrottled(err):
		return "throttled", true
	case provider.IsProviderUnavailable(err):
		return "provider_unavailable", true
	case provider.IsAccessDenied(err):
		return "access_denied", false
	case provider.IsNotFound(err):
		return "not_found", false
	case provider.IsInvalidCredentials(err):
		return "invalid_credentials", false
	default:
		return "provider_error", false
	}
}

// publicHeadError returns a presentation-safe error that still preserves
// machine-detectable sentinel identity via errors.Is for throttle/unavailable
// and common provider sentinels. The raw (possibly sensitive) provider string
// is never returned.
func publicHeadError(err error) error {
	if err == nil {
		return nil
	}
	msg := sanitizeProviderErrorMessage(err)
	switch {
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("%w: %s", context.Canceled, msg)
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: %s", context.DeadlineExceeded, msg)
	case provider.IsThrottled(err):
		return fmt.Errorf("%w: %s", provider.ErrThrottled, msg)
	case provider.IsProviderUnavailable(err):
		return fmt.Errorf("%w: %s", provider.ErrProviderUnavailable, msg)
	case provider.IsAccessDenied(err):
		return fmt.Errorf("%w: %s", provider.ErrAccessDenied, msg)
	case provider.IsNotFound(err):
		return fmt.Errorf("%w: %s", provider.ErrNotFound, msg)
	case provider.IsInvalidCredentials(err):
		return fmt.Errorf("%w: %s", provider.ErrInvalidCredentials, msg)
	default:
		return errors.New(msg)
	}
}

func sanitizeProviderErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	var providerErr *provider.ProviderError
	if errors.As(err, &providerErr) {
		msg := ""
		if providerErr.Err != nil {
			msg = sanitizeMessage(providerErr.Err.Error())
		}
		return fmt.Sprintf("%s %s failed: %s", providerErr.Provider, providerErr.Op, msg)
	}
	return sanitizeMessage(err.Error())
}

func sanitizeMessage(message string) string {
	message = strings.Join(strings.Fields(strings.TrimSpace(message)), " ")
	message = urlPattern.ReplaceAllStringFunc(message, sanitizeURI)
	message = bearerPattern.ReplaceAllString(message, "${1}<redacted>")
	message = keyValuePattern.ReplaceAllStringFunc(message, func(match string) string {
		for i, r := range match {
			if r == '=' || r == ':' {
				return strings.TrimSpace(match[:i]) + string(r) + "<redacted>"
			}
		}
		return "<redacted>"
	})
	const maxMessageLen = 2048
	if len(message) > maxMessageLen {
		return message[:maxMessageLen] + "..."
	}
	return message
}

func sanitizeURI(raw string) string {
	trailing := ""
	for raw != "" {
		last := raw[len(raw)-1]
		if !strings.ContainsRune(".,);]'\":", rune(last)) {
			break
		}
		trailing = string(last) + trailing
		raw = raw[:len(raw)-1]
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" {
		return "<redacted-url>" + trailing
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String() + trailing
}
