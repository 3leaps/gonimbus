package reflow

import (
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/transfer"
)

func receiptTimestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// SafeReceiptHandle preserves an opaque write identifier exactly or omits it.
// Redacting part of an identifier would falsely identify a different write.
func SafeReceiptHandle(value string) string {
	if isUnsafeReceiptHandle(value) {
		return ""
	}
	candidate := value
	for {
		next := stripReceiptHandleWrapper(candidate)
		if next == candidate {
			break
		}
		if isUnsafeReceiptHandle(next) {
			return ""
		}
		candidate = next
	}
	return value
}

// stripReceiptHandleWrapper peels one RFC 9110 weak-tag prefix (W/ or w/) or
// enclosing DQUOTE pair so credential-shaped payloads wrapped as W/"Bearer …"
// or "Bearer …" cannot bypass handle sanitization.
func stripReceiptHandleWrapper(value string) string {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) >= 2 && (trimmed[0] == 'W' || trimmed[0] == 'w') && trimmed[1] == '/' {
		return strings.TrimSpace(trimmed[2:])
	}
	if len(trimmed) >= 2 && trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"' {
		return strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	}
	return trimmed
}

func isUnsafeReceiptHandle(value string) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) != value ||
		strings.HasPrefix(strings.ToLower(value), "bearer ") ||
		strings.Contains(value, "://") || operationCauseContainsCredentialMaterial(value) ||
		operationCauseKeyValuePattern.MatchString(value) || operationCauseBearerPattern.MatchString(value) {
		return true
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}

// SafeReceiptSHA256 accepts only the canonical full-object digest encoding.
func SafeReceiptSHA256(value string) string {
	if len(value) != 64 {
		return ""
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return ""
		}
	}
	return value
}

// SafeSourceVersionID normalizes an opaque source version/generation token,
// omitting empty values and S3's sentinel "null" version id.
func SafeSourceVersionID(value string) string {
	if value == "" || value == "null" {
		return ""
	}
	return SafeReceiptHandle(value)
}

// NormalizeETagForCompare trims surrounding whitespace and HTTP ETag quotes
// without assuming MD5 or interpreting opaque token internals.
func NormalizeETagForCompare(etag string) string {
	trimmed := strings.TrimSpace(etag)
	if len(trimmed) >= 2 && trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"' {
		return trimmed[1 : len(trimmed)-1]
	}
	return trimmed
}

// RecordWithReceipt projects only successful write facts onto a terminal.
// Source read metadata takes precedence over an existing input observation.
func RecordWithReceipt(rec Record, receipt transfer.CopyReceipt, destProvider string) Record {
	// A caller may reuse a record; never retain stale write claims on a skip
	// or failure, or an old revision when the current write has none.
	rec.DestSHA256, rec.DestETag, rec.DestVersionID = "", "", ""
	if !receipt.SourceLastModified.IsZero() {
		rec.SourceLastModified = receiptTimestamp(receipt.SourceLastModified)
	}
	if rec.Status != "complete" && rec.Status != "quarantined" {
		return rec
	}
	rec.DestSHA256 = SafeReceiptSHA256(receipt.SHA256)
	rec.DestETag = SafeReceiptHandle(receipt.ETag)
	if destProvider != string(provider.ProviderFile) && receipt.Version != "null" {
		rec.DestVersionID = SafeReceiptHandle(receipt.Version)
	}
	return rec
}

// RecordWithSourceBinding projects source-read binding evidence onto a terminal
// record when source-read binding applied. On complete/quarantined terminals,
// source_etag (and optional source_version_id) reflect the exact token returned
// by the read that supplied the bytes, and source_match records expected and
// observed tokens.
func RecordWithSourceBinding(rec Record, receipt transfer.CopyReceipt, expectedETag, expectedVersionID string) Record {
	if rec.Status != "complete" && rec.Status != "quarantined" {
		return rec
	}
	observedETag := SafeReceiptHandle(receipt.SourceETag)
	observedVersionID := SafeSourceVersionID(receipt.SourceVersion)
	if observedETag != "" {
		rec.SourceETag = observedETag
	}
	if observedVersionID != "" {
		rec.SourceVersionID = observedVersionID
	}
	match := &SourceMatchInfo{
		ExpectedETag:      SafeReceiptHandle(NormalizeETagForCompare(expectedETag)),
		ExpectedVersionID: SafeSourceVersionID(expectedVersionID),
		ObservedETag:      observedETag,
		ObservedVersionID: observedVersionID,
	}
	if match.ExpectedETag != "" || match.ExpectedVersionID != "" || match.ObservedETag != "" || match.ObservedVersionID != "" {
		rec.SourceMatch = match
	}
	return rec
}
