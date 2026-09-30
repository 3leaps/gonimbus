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
	if !utf8.ValidString(value) || strings.TrimSpace(value) != value ||
		strings.HasPrefix(strings.ToLower(value), "bearer ") ||
		strings.Contains(value, "://") || operationCauseContainsCredentialMaterial(value) ||
		operationCauseKeyValuePattern.MatchString(value) || operationCauseBearerPattern.MatchString(value) {
		return ""
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return ""
		}
	}
	return value
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
