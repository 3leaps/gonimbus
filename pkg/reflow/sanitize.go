package reflow

import "strings"

// sanitizeSourceURI removes presigned-URL query material (signatures, tokens,
// embedded credentials) from a source URI before it crosses the event boundary.
// A plain object URI with no query is returned unchanged.
func sanitizeSourceURI(sourceURI string) string {
	if sourceURI == "" {
		return ""
	}
	return redactOperationCauseURL(sourceURI)
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
