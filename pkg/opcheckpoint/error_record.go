package opcheckpoint

import (
	"fmt"
	"time"
)

const ErrorRecordType = "gonimbus.operation.error.v1"

// ErrorRecord is the redacted structured runtime failure surface shared by
// resumable operations.
type ErrorRecord struct {
	Type string          `json:"type"`
	TS   string          `json:"ts"`
	Data ErrorRecordData `json:"data"`
}

type ErrorRecordData struct {
	Operation     string           `json:"operation"`
	RunID         string           `json:"run_id"`
	ErrorClass    ErrorClass       `json:"error_class"`
	Cause         *ErrorCause      `json:"cause,omitempty"`
	Progress      map[string]int64 `json:"progress,omitempty"`
	ResumeCommand string           `json:"resume_command,omitempty"`
}

// ErrorCause carries an additive, command-level classification of the root
// failure that caused a resumable operation checkpoint.
type ErrorCause struct {
	Code        string `json:"code"`
	Reason      string `json:"reason"`
	Message     string `json:"message"`
	Resumable   bool   `json:"resumable"`
	Disposition string `json:"disposition"`
}

func NewErrorRecord(operation, runID string, class ErrorClass, progress map[string]int64, at time.Time) (ErrorRecord, error) {
	return NewErrorRecordWithCause(operation, runID, class, nil, progress, at)
}

func NewErrorRecordWithCause(operation, runID string, class ErrorClass, cause *ErrorCause, progress map[string]int64, at time.Time) (ErrorRecord, error) {
	operation = cleanSegment(operation)
	runID = cleanSegment(runID)
	if operation == "" {
		return ErrorRecord{}, fmt.Errorf("operation is invalid")
	}
	if runID == "" {
		return ErrorRecord{}, fmt.Errorf("run_id is invalid")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	resumeCommand, err := ResumeCommand(operation, runID)
	if err != nil {
		return ErrorRecord{}, err
	}
	return ErrorRecord{
		Type: ErrorRecordType,
		TS:   at.UTC().Format(time.RFC3339Nano),
		Data: ErrorRecordData{
			Operation:     operation,
			RunID:         runID,
			ErrorClass:    class,
			Cause:         cause,
			Progress:      progress,
			ResumeCommand: resumeCommand,
		},
	}, nil
}

func ResumeCommand(operation, runID string) (string, error) {
	operation = cleanSegment(operation)
	runID = cleanSegment(runID)
	if operation == "" {
		return "", fmt.Errorf("operation is invalid")
	}
	if runID == "" {
		return "", fmt.Errorf("run_id is invalid")
	}
	switch operation {
	case "index-build":
		return "gonimbus index build --resume-run " + runID, nil
	case "index-enrich-with-head":
		return "gonimbus index enrich-with-head --resume-run " + runID, nil
	case "transfer-reflow":
		return "gonimbus transfer reflow --resume-run " + runID, nil
	default:
		return "", fmt.Errorf("unsupported resumable operation: %s", operation)
	}
}
