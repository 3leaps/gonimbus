package reflow

import "context"

// ErrorEventType is the JSONL type for per-object/run reflow error records.
const ErrorEventType = "gonimbus.error.v1"

// ErrorEvent is the redacted payload describing a per-object or run error.
// Message and Details are sanitized (provider-error redaction applied) by the
// engine before delivery, so a sink may log or persist them without further
// scrubbing.
type ErrorEvent struct {
	Code    string         `json:"code"`
	Key     string         `json:"key,omitempty"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// EventSink receives typed engine events. The engine applies its redaction
// boundary before the sink — every payload is already sanitized, so a sink may
// log or persist it without re-scrubbing.
//
// EventSink is the engine's output adapter boundary (the CLI implements it over a
// JSONL writer), so each method returns an error: a sink that fails to write or
// persist an event surfaces that failure to the engine, which can abort and
// classify it the way the terminal output path does. A nil EventSink disables
// event delivery. Experimental.
type EventSink interface {
	// OnRun reports the resolved run configuration.
	OnRun(ctx context.Context, rec RunRecord) error
	// OnSource reports the resolved source.
	OnSource(ctx context.Context, rec SourceRunRecord) error
	// OnRecord reports a per-object outcome.
	OnRecord(ctx context.Context, rec Record) error
	// OnWarning reports a non-fatal condition (e.g. the IfAbsent fallback notice).
	OnWarning(ctx context.Context, w Warning) error
	// OnError reports a per-object or run error with a redacted message.
	OnError(ctx context.Context, e ErrorEvent) error
	// OnSummary reports the terminal run summary.
	OnSummary(ctx context.Context, rec SummaryRecord) error
}
