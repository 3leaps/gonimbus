// Package output provides JSONL output for crawl results.
//
// Output is structured as typed record envelopes containing objects,
// errors, and progress updates. Each line is a self-contained JSON
// object that can be parsed independently.
package output

import (
	"encoding/json"
	"errors"
	"time"
)

// Record type constants define the envelope types for JSONL output.
// These follow the pattern: gonimbus.<type>.v<version>
const (
	// TypeObject identifies object listing records.
	TypeObject = "gonimbus.object.v1"

	// TypeError identifies error records.
	TypeError = "gonimbus.error.v1"

	// TypeProgress identifies progress update records.
	TypeProgress = "gonimbus.progress.v1"

	// TypeSummary identifies final summary records.
	TypeSummary = "gonimbus.summary.v1"

	// TypePreflight identifies preflight capability check records.
	TypePreflight = "gonimbus.preflight.v1"
)

// Record is the envelope for all JSONL output.
//
// Each line of JSONL output contains a Record with a type-specific
// payload in the Data field. The type field determines how to
// interpret the Data payload.
type Record struct {
	// Type identifies the record type (e.g., "gonimbus.object.v1").
	Type string `json:"type"`

	// TS is the timestamp when the record was created (RFC3339Nano).
	TS time.Time `json:"ts"`

	// JobID is the correlation ID for this crawl job.
	JobID string `json:"job_id"`

	// Provider identifies the storage provider (e.g., "s3", "gcs").
	Provider string `json:"provider"`

	// Data contains the type-specific payload as raw JSON.
	Data json.RawMessage `json:"data"`
}

// ObjectRecord is the data payload for object listings.
//
// This contains the metadata for a single object that matched
// the crawl patterns.
type ObjectRecord struct {
	// Key is the full object key (path) in the bucket.
	Key string `json:"key"`

	// Size is the object size in bytes.
	Size int64 `json:"size"`

	// ETag is the entity tag, typically an MD5 hash of the object.
	ETag string `json:"etag"`

	// LastModified is when the object was last modified.
	LastModified time.Time `json:"last_modified"`

	// ContentType is the MIME type of the object.
	// Only populated if metadata enrichment is enabled.
	ContentType string `json:"content_type,omitempty"`

	// Metadata contains user-defined metadata key-value pairs.
	// Only populated if metadata enrichment is enabled.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// PreflightRecord is the data payload for preflight capability checks.
//
// Preflight records are emitted early, before long-running operations.
// They provide an explicit contract for what was checked and whether the
// principal appears to have the required permissions.
type PreflightRecord struct {
	Mode          string                 `json:"mode"`
	ProbeStrategy string                 `json:"probe_strategy,omitempty"`
	ProbePrefix   string                 `json:"probe_prefix,omitempty"`
	Results       []PreflightCheckResult `json:"results"`
}

// PreflightCheckResult is a single capability check result.
type PreflightCheckResult struct {
	Capability string `json:"capability"`
	Allowed    bool   `json:"allowed"`
	Method     string `json:"method,omitempty"`
	ErrorCode  string `json:"error_code,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

// ErrorRecord is the data payload for errors.
//
// Errors are emitted as records rather than failing the entire crawl,
// allowing partial results when some operations fail.
type ErrorRecord struct {
	// Code is a machine-readable error code.
	Code string `json:"code"`

	// Message is a human-readable error description.
	Message string `json:"message"`

	// Key is the object key related to this error, if applicable.
	Key string `json:"key,omitempty"`

	// Prefix is the prefix being listed when the error occurred.
	Prefix string `json:"prefix,omitempty"`

	// Details contains additional error context.
	Details any `json:"details,omitempty"`
}

// Error codes for ErrorRecord.
const (
	// ErrCodeAccessDenied indicates permission failure.
	ErrCodeAccessDenied = "ACCESS_DENIED"

	// ErrCodeNotFound indicates the object or bucket was not found.
	ErrCodeNotFound = "NOT_FOUND"

	// ErrCodeTimeout indicates an operation timed out.
	ErrCodeTimeout = "TIMEOUT"

	// ErrCodeThrottled indicates rate limiting.
	ErrCodeThrottled = "THROTTLED"

	// ErrCodeInternal indicates an unexpected internal error.
	ErrCodeInternal = "INTERNAL"
)

// ProgressRecord is the data payload for progress updates.
//
// Progress records are emitted periodically during crawls to provide
// visibility into long-running operations.
type ProgressRecord struct {
	// Phase indicates the current crawl phase.
	Phase string `json:"phase"`

	// ObjectsFound is the total number of objects seen so far.
	ObjectsFound int64 `json:"objects_found"`

	// ObjectsMatched is the number of objects matching patterns.
	ObjectsMatched int64 `json:"objects_matched"`

	// BytesTotal is the cumulative size of matched objects in bytes.
	BytesTotal int64 `json:"bytes_total"`

	// Prefix is the current prefix being listed, if applicable.
	Prefix string `json:"prefix,omitempty"`
}

// Progress phase constants.
const (
	// PhaseStarting indicates the crawl is initializing.
	PhaseStarting = "starting"

	// PhaseListing indicates objects are being listed.
	PhaseListing = "listing"

	// PhaseEnriching indicates metadata enrichment is in progress.
	PhaseEnriching = "enriching"

	// PhaseComplete indicates the crawl has finished.
	PhaseComplete = "complete"
)

// SummaryRecord is the data payload for final summaries.
//
// A summary record is emitted at the end of a crawl with aggregate
// statistics.
type SummaryRecord struct {
	// ObjectsFound is the total number of objects seen.
	ObjectsFound int64 `json:"objects_found"`

	// ObjectsMatched is the number of objects matching patterns.
	ObjectsMatched int64 `json:"objects_matched"`

	// BytesTotal is the cumulative size of matched objects in bytes.
	BytesTotal int64 `json:"bytes_total"`

	// Duration is the total crawl duration.
	Duration time.Duration `json:"duration_ns"`

	// DurationHuman is a human-readable duration string.
	DurationHuman string `json:"duration"`

	// Errors is the count of errors encountered.
	Errors int64 `json:"errors"`

	// Prefixes lists the prefixes that were crawled.
	Prefixes []string `json:"prefixes,omitempty"`
}

// Writer errors.
var (
	// ErrWriterClosed is returned when writing to a closed writer.
	ErrWriterClosed = errors.New("writer is closed")
)

// WriteError wraps errors that occur during write operations.
type WriteError struct {
	Op  string // Operation that failed (e.g., "marshal_data", "write")
	Err error  // Underlying error
}

func (e *WriteError) Error() string {
	return "output: " + e.Op + ": " + e.Err.Error()
}

func (e *WriteError) Unwrap() error {
	return e.Err
}
