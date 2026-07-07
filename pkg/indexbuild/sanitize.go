package indexbuild

import (
	"net/url"
	"regexp"
	"strings"
)

var (
	urlPattern      = regexp.MustCompile(`https?://[^\s"'<>]+`)
	bearerPattern   = regexp.MustCompile(`(?i)(authorization\s*:\s*bearer\s+)[^,\s;]+`)
	keyValuePattern = regexp.MustCompile(`(?i)\b(x-amz-signature|x-amz-credential|x-amz-security-token|x-goog-signature|x-goog-credential|x-goog-security-token|aws_secret_access_key|aws_session_token|access_token|refresh_token|sessiontoken|authtoken|client_secret|sig|token)\s*[:=]\s*[^,\s;&]+`)
)

func sanitizeDetails(details map[string]any) map[string]any {
	if details == nil {
		return nil
	}
	out := make(map[string]any, len(details))
	for key, value := range details {
		out[key] = sanitizeDetailValue(key, value)
	}
	return out
}

func sanitizeDetailValue(key string, value any) any {
	switch v := value.(type) {
	case string:
		if sensitiveDetailKey(key) && v != "" {
			return "redacted"
		}
		return sanitizeMessage(v)
	case map[string]any:
		return sanitizeDetails(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = sanitizeDetailValue(key, item)
		}
		return out
	default:
		return value
	}
}

func sensitiveDetailKey(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	return strings.Contains(key, "authorization") ||
		strings.Contains(key, "credential") ||
		strings.Contains(key, "secret") ||
		strings.Contains(key, "signature") ||
		strings.Contains(key, "token")
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
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
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
