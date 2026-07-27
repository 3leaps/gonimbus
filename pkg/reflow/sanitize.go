package reflow

import "strings"

// sanitizeSourceURI removes credential material from a URI before it crosses the
// event boundary — and, for an object URI, is required to be LOSSLESS.
//
// The two jobs are separated by scheme, because the same string is both a
// disclosure surface and an identity:
//
//   - http/https carries presigned material in its query (X-Amz-Signature and
//     friends), so query values and userinfo are redacted there.
//   - An object URI (s3://, gs://, file://) is NOT redacted beyond userinfo. Its
//     path is an opaque object key: `?` is key or glob syntax rather than a query
//     delimiter, and a space, a `%`, or a non-ASCII byte is an ordinary key
//     character. Re-rendering such a URI through net/url would percent-encode
//     those characters and blank every `?`-suffixed value.
//
// That distinction is load-bearing rather than cosmetic. This value becomes the
// per-item checkpoint authority (ItemDone and UpsertItem) and the emitted
// record's source URI, so a lossy rewrite would give two distinct objects the
// same authority — `a/file?v=one` and `a/file?v=two` both collapsing to one
// row under the (source_uri, dest_uri) primary key, letting one object's
// terminal answer for the other on resume.
//
// Userinfo is still stripped on every scheme: it is never part of an object key
// under the gonimbus URI grammar, so removing it cannot lose identity.
func sanitizeSourceURI(sourceURI string) string {
	if sourceURI == "" {
		return ""
	}
	if hasPresignedCandidateScheme(sourceURI) {
		return redactOperationCauseURL(sourceURI)
	}
	return redactObjectURIUserinfo(sourceURI)
}

// hasPresignedCandidateScheme reports whether a URI is on a transport that can
// carry presigned credential material in its query. Only http and https can;
// every other scheme reaching here is a gonimbus object URI.
func hasPresignedCandidateScheme(raw string) bool {
	lower := strings.ToLower(raw)
	return strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")
}

// redactObjectURIUserinfo strips `user:pass@` from an object URI's authority
// without touching the key.
//
// It works on the raw string rather than net/url precisely so the key is never
// re-encoded. The authority ends at the first `/` after the scheme separator, so
// an `@` inside the key (a legal character) is outside the span considered here.
func redactObjectURIUserinfo(raw string) string {
	const sep = "://"
	schemeEnd := strings.Index(raw, sep)
	if schemeEnd < 0 {
		return raw
	}
	authorityStart := schemeEnd + len(sep)
	authorityEnd := strings.Index(raw[authorityStart:], "/")
	if authorityEnd < 0 {
		authorityEnd = len(raw)
	} else {
		authorityEnd += authorityStart
	}
	authority := raw[authorityStart:authorityEnd]
	at := strings.LastIndex(authority, "@")
	if at < 0 {
		return raw
	}
	return raw[:authorityStart] + "<redacted>@" + authority[at+1:] + raw[authorityEnd:]
}

// uriDetailKeys are Details map keys whose string values are URIs and are
// sanitized as URIs (path preserved, query/userinfo redacted).
var uriDetailKeys = map[string]bool{
	"source_uri":        true,
	"dest_uri":          true,
	"original_dest_uri": true,
	"uri":               true,
	"sidecar_uri":       true,
}

// sanitizeDetails returns a copy of a Details map with each field sanitized
// before delivery to an EventSink: URI-bearing fields have their query/userinfo
// redacted, and any other string value carrying credential material is redacted.
// Benign values are returned verbatim, so the sanitized map is byte-equivalent
// when nothing sensitive is present. Nested maps and slices are sanitized
// recursively.
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
		if uriDetailKeys[strings.ToLower(key)] {
			return sanitizeSourceURI(v)
		}
		// Redact only when credential material is detected, so benign values pass
		// through verbatim. Detection spans the needle/URL set plus the key-value
		// (token=..., x-amz-signature=...) and bearer patterns the redactor strips.
		if operationCauseContainsCredentialMaterial(v) ||
			operationCauseKeyValuePattern.MatchString(v) ||
			operationCauseBearerPattern.MatchString(v) {
			return redactOperationCauseMessage(v)
		}
		return v
	case map[string]any:
		return sanitizeDetails(v)
	case []any:
		out := make([]any, len(v))
		for i, elem := range v {
			out[i] = sanitizeDetailValue(key, elem)
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
