package indexbuild

import "context"

const (
	EventTypeRunStart          = "run_start"
	EventTypeCrawlError        = "crawl_error"
	EventTypeSnapshotPublished = "snapshot_published"
)

// Event is the sanitized event payload delivered by the engine.
type Event struct {
	Type    string         `json:"type"`
	RunID   string         `json:"run_id"`
	Message string         `json:"message,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

// EventSink receives sanitized engine events. A sink may log or persist Event
// values without re-scrubbing. A nil sink disables event delivery.
type EventSink interface {
	OnEvent(ctx context.Context, event Event) error
}

func emitEvent(ctx context.Context, sink EventSink, event Event) error {
	if sink == nil {
		return nil
	}
	event.Message = sanitizeMessage(event.Message)
	event.Details = sanitizeDetails(event.Details)
	return sink.OnEvent(ctx, event)
}
