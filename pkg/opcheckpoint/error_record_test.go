package opcheckpoint

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewErrorRecordBuildsRedactedResumeHint(t *testing.T) {
	rec, err := NewErrorRecord(
		"transfer-reflow",
		"run_123",
		ErrorClassCredentialsRefreshFailed,
		map[string]int64{"objects_written": 7, "bytes_written": 4096},
		time.Date(2026, 1, 1, 1, 2, 3, 0, time.UTC),
	)
	require.NoError(t, err)
	require.Equal(t, ErrorRecordType, rec.Type)
	require.Equal(t, "2026-01-01T01:02:03Z", rec.TS)
	require.Equal(t, "gonimbus transfer reflow --resume-run run_123", rec.Data.ResumeCommand)

	data, err := json.Marshal(rec)
	require.NoError(t, err)
	require.NotContains(t, string(data), "checkpoint")
	require.NotContains(t, string(data), "source_key")
	require.NotContains(t, strings.ToLower(string(data)), "authtoken")
	require.NotContains(t, strings.ToLower(string(data)), "secret")
}

func TestNewErrorRecordWithCauseIncludesClassifiedCause(t *testing.T) {
	rec, err := NewErrorRecordWithCause(
		"transfer-reflow",
		"run_123",
		ErrorClassInterrupted,
		&ErrorCause{
			Code:        "TRANSIENT",
			Reason:      "transient.network",
			Message:     "context deadline exceeded",
			Resumable:   true,
			Disposition: "aborted_resumable_checkpoint",
		},
		map[string]int64{"errors": 3},
		time.Date(2026, 1, 1, 1, 2, 3, 0, time.UTC),
	)
	require.NoError(t, err)
	require.NotNil(t, rec.Data.Cause)
	require.Equal(t, "TRANSIENT", rec.Data.Cause.Code)
	require.Equal(t, "transient.network", rec.Data.Cause.Reason)
	require.True(t, rec.Data.Cause.Resumable)
	require.Equal(t, "aborted_resumable_checkpoint", rec.Data.Cause.Disposition)
	require.Equal(t, int64(3), rec.Data.Progress["errors"])
}

func TestResumeCommandRejectsUnknownOperationAndInvalidRunID(t *testing.T) {
	_, err := ResumeCommand("content-probe", "run_123")
	require.Error(t, err)

	_, err = ResumeCommand("index-build", "../run_123")
	require.Error(t, err)

	_, err = ResumeCommand("index-build", "run 123")
	require.Error(t, err)

	_, err = ResumeCommand("index-build", "run;rm")
	require.Error(t, err)

	_, err = NewErrorRecord("transfer reflow", "run_123", ErrorClassInterrupted, nil, time.Time{})
	require.Error(t, err)
}
