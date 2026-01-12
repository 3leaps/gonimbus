package indexstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// RunStatus represents the status of an IndexRun.
type RunStatus string

const (
	// RunStatusRunning indicates the run is currently in progress.
	RunStatusRunning = "running"
	// RunStatusSuccess indicates the run completed successfully.
	RunStatusSuccess = "success"
	// RunStatusPartial indicates the run completed with partial results.
	RunStatusPartial = "partial"
	// RunStatusFailed indicates the run failed.
	RunStatusFailed = "failed"
)

// IndexRun represents a single index build execution.
//
// Each IndexRun is scoped to an IndexSet and represents a discrete
// snapshot of the source at a specific point in time.
type IndexRun struct {
	RunID            string
	IndexSetID       string
	StartedAt        time.Time
	EndedAt          *time.Time
	AcquiredAt       time.Time
	SourceType       string
	SourceSnapshotAt *time.Time
	Status           RunStatus
}

// RunEvent represents an event during an IndexRun.
//
// Events provide structured diagnostic information, especially for partial runs.
type RunEvent struct {
	EventID       string
	RunID         string
	OccurredAt    time.Time
	EventType     string
	EventCategory string
	Detail        *string
	Key           *string
	Prefix        *string
	ErrorCode     *string
}

// EventCategory groups events by type.
type EventCategory string

const (
	// EventCategoryInfo for informational events.
	EventCategoryInfo = "info"
	// EventCategoryWarning for warning events.
	EventCategoryWarning = "warning"
	// EventCategoryError for error events.
	EventCategoryError = "error"
	// EventCategoryThrottle for rate-limiting events.
	EventCategoryThrottle = "throttle"
	// EventCategoryAccess for permission/access events.
	EventCategoryAccess = "access"
)

// EventType identifies specific event types.
type EventType string

const (
	EventTypePrefixSkipped EventType = "prefix_skipped"
	EventTypeAccessDenied  EventType = "access_denied"
	EventTypeRateLimited   EventType = "rate_limited"
	EventTypePartial       EventType = "partial_run"
	EventTypeUnknownError  EventType = "unknown_error"
	EventTypeObjectAdded   EventType = "object_added"
	EventTypeObjectDeleted EventType = "object_deleted"
	EventTypePrefixListed  EventType = "prefix_listed"
	EventTypeRunStarted    EventType = "run_started"
	EventTypeRunCompleted  EventType = "run_completed"
)

// CreateIndexRun creates a new IndexRun in running status.
func CreateIndexRun(ctx context.Context, db *sql.DB, indexSetID string, sourceType string) (*IndexRun, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	now := time.Now().UTC()
	runID := generateRunID()

	_, err := db.ExecContext(ctx,
		`INSERT INTO index_runs
		 (run_id, index_set_id, started_at, acquired_at, source_type, status)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		runID, indexSetID, now, now, sourceType, RunStatusRunning)

	if err != nil {
		return nil, fmt.Errorf("create index_run: %w", err)
	}

	return &IndexRun{
		RunID:      runID,
		IndexSetID: indexSetID,
		StartedAt:  now,
		AcquiredAt: now,
		SourceType: sourceType,
		Status:     RunStatusRunning,
	}, nil
}

// GetIndexRun retrieves an IndexRun by ID.
func GetIndexRun(ctx context.Context, db *sql.DB, runID string) (*IndexRun, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var ir IndexRun
	var endedAt sql.NullTime
	var snapshotAt sql.NullTime

	err := db.QueryRowContext(ctx,
		`SELECT run_id, index_set_id, started_at, ended_at, acquired_at,
		        source_type, source_snapshot_at, status
		 FROM index_runs WHERE run_id = ?`,
		runID).Scan(
		&ir.RunID, &ir.IndexSetID, &ir.StartedAt, &endedAt, &ir.AcquiredAt,
		&ir.SourceType, &snapshotAt, &ir.Status)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("index_run not found: %s", runID)
	}
	if err != nil {
		return nil, fmt.Errorf("get index_run: %w", err)
	}

	if endedAt.Valid {
		ir.EndedAt = &endedAt.Time
	}
	if snapshotAt.Valid {
		ir.SourceSnapshotAt = &snapshotAt.Time
	}

	return &ir, nil
}

// ListIndexRuns lists all runs for an IndexSet.
func ListIndexRuns(ctx context.Context, db *sql.DB, indexSetID string) ([]IndexRun, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	rows, err := db.QueryContext(ctx,
		`SELECT run_id, index_set_id, started_at, ended_at, acquired_at,
		        source_type, source_snapshot_at, status
		 FROM index_runs
		 WHERE index_set_id = ?
		 ORDER BY started_at DESC`,
		indexSetID)

	if err != nil {
		return nil, fmt.Errorf("list index_runs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var runs []IndexRun
	for rows.Next() {
		var ir IndexRun
		var endedAt sql.NullTime
		var snapshotAt sql.NullTime

		err := rows.Scan(
			&ir.RunID, &ir.IndexSetID, &ir.StartedAt, &endedAt, &ir.AcquiredAt,
			&ir.SourceType, &snapshotAt, &ir.Status)
		if err != nil {
			return nil, fmt.Errorf("scan index_run: %w", err)
		}

		if endedAt.Valid {
			ir.EndedAt = &endedAt.Time
		}
		if snapshotAt.Valid {
			ir.SourceSnapshotAt = &snapshotAt.Time
		}

		runs = append(runs, ir)
	}

	return runs, nil
}

// UpdateIndexRunStatus updates the status and end time of an IndexRun.
func UpdateIndexRunStatus(ctx context.Context, db *sql.DB, runID string, status RunStatus, snapshotAt *time.Time) error {
	if ctx == nil {
		ctx = context.Background()
	}

	now := time.Now().UTC()

	_, err := db.ExecContext(ctx,
		`UPDATE index_runs
		 SET status = ?, ended_at = ?, source_snapshot_at = ?
		 WHERE run_id = ?`,
		string(status), now, snapshotAt, runID)

	if err != nil {
		return fmt.Errorf("update index_run status: %w", err)
	}

	return nil
}

// RecordRunEvent records an event for an IndexRun.
func RecordRunEvent(ctx context.Context, db *sql.DB, event RunEvent) error {
	if ctx == nil {
		ctx = context.Background()
	}

	_, err := db.ExecContext(ctx,
		`INSERT INTO index_run_events
		 (event_id, run_id, occurred_at, event_type, event_category,
		  detail, key, prefix, error_code)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.EventID, event.RunID, event.OccurredAt,
		string(event.EventType), string(event.EventCategory),
		event.Detail, event.Key, event.Prefix, event.ErrorCode)

	if err != nil {
		return fmt.Errorf("record run event: %w", err)
	}

	return nil
}

// ListRunEvents retrieves events for a run.
func ListRunEvents(ctx context.Context, db *sql.DB, runID string, category *EventCategory) ([]RunEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var rows *sql.Rows
	var err error

	if category != nil {
		rows, err = db.QueryContext(ctx,
			`SELECT event_id, run_id, occurred_at, event_type, event_category,
			        detail, key, prefix, error_code
			 FROM index_run_events
			 WHERE run_id = ? AND event_category = ?
			 ORDER BY occurred_at ASC`,
			runID, string(*category))
	} else {
		rows, err = db.QueryContext(ctx,
			`SELECT event_id, run_id, occurred_at, event_type, event_category,
			        detail, key, prefix, error_code
			 FROM index_run_events
			 WHERE run_id = ?
			 ORDER BY occurred_at ASC`,
			runID)
	}

	if err != nil {
		return nil, fmt.Errorf("list run events: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var events []RunEvent
	for rows.Next() {
		var e RunEvent
		var detail, key, prefix, errorCode sql.NullString

		err := rows.Scan(
			&e.EventID, &e.RunID, &e.OccurredAt,
			&e.EventType, &e.EventCategory,
			&detail, &key, &prefix, &errorCode)
		if err != nil {
			return nil, fmt.Errorf("scan run event: %w", err)
		}

		if detail.Valid {
			e.Detail = &detail.String
		}
		if key.Valid {
			e.Key = &key.String
		}
		if prefix.Valid {
			e.Prefix = &prefix.String
		}
		if errorCode.Valid {
			e.ErrorCode = &errorCode.String
		}

		events = append(events, e)
	}

	return events, nil
}

// RecordPartialRun records a partial run event and updates run status.
func RecordPartialRun(ctx context.Context, db *sql.DB, runID string, reason string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	category := EventCategoryWarning
	eventType := EventTypePartial

	event := RunEvent{
		EventID:       generateEventID(),
		RunID:         runID,
		OccurredAt:    time.Now().UTC(),
		EventType:     string(eventType),
		EventCategory: string(category),
		Detail:        &reason,
	}

	if err := RecordRunEvent(ctx, db, event); err != nil {
		return err
	}

	return UpdateIndexRunStatus(ctx, db, runID, RunStatusPartial, nil)
}

// RecordThrottling records a rate-limiting event for a prefix.
func RecordThrottling(ctx context.Context, db *sql.DB, runID, prefix string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	event := RunEvent{
		EventID:       generateEventID(),
		RunID:         runID,
		OccurredAt:    time.Now().UTC(),
		EventType:     string(EventTypeRateLimited),
		EventCategory: string(EventCategoryThrottle),
		Prefix:        &prefix,
	}

	return RecordRunEvent(ctx, db, event)
}

// RecordAccessDenied records an access denied event for a key or prefix.
func RecordAccessDenied(ctx context.Context, db *sql.DB, runID string, key, prefix string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	event := RunEvent{
		EventID:       generateEventID(),
		RunID:         runID,
		OccurredAt:    time.Now().UTC(),
		EventType:     string(EventTypeAccessDenied),
		EventCategory: string(EventCategoryAccess),
		Key:           stringPtr(key),
		Prefix:        stringPtr(prefix),
		ErrorCode:     stringPtr("ACCESS_DENIED"),
	}

	return RecordRunEvent(ctx, db, event)
}

// generateRunID generates a unique run ID.
func generateRunID() string {
	return fmt.Sprintf("run_%d", time.Now().UnixNano())
}

// generateEventID generates a unique event ID.
func generateEventID() string {
	return fmt.Sprintf("evt_%d", time.Now().UnixNano())
}

func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
