package jobregistry

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"
)

var (
	ErrJobNoPID      = errors.New("job has no pid recorded")
	ErrJobNotRunning = errors.New("job is not running")
	ErrInvalidSignal = errors.New("invalid stop signal")
)

type StopOptions struct {
	Signal       string
	WaitTimeout  time.Duration
	PollInterval time.Duration
}

type StopResult struct {
	JobID      string `json:"job_id"`
	Signal     string `json:"signal"`
	ForcedKill bool   `json:"forced_kill"`
	State      string `json:"state"`
}

func (s *Store) Stop(jobID string, opts StopOptions) (*StopResult, error) {
	if s == nil {
		return nil, fmt.Errorf("job registry store is nil")
	}
	sigStr := strings.TrimSpace(strings.ToLower(opts.Signal))
	if sigStr == "" {
		sigStr = "term"
	}
	var sig syscall.Signal
	switch sigStr {
	case "term":
		sig = syscall.SIGTERM
	case "kill":
		sig = syscall.SIGKILL
	default:
		return nil, fmt.Errorf("%w: %s", ErrInvalidSignal, sigStr)
	}

	rec, err := s.Get(jobID)
	if err != nil {
		return nil, err
	}
	if rec.PID <= 0 {
		return nil, ErrJobNoPID
	}
	if rec.State != JobStateRunning {
		return nil, fmt.Errorf("%w (state=%s)", ErrJobNotRunning, rec.State)
	}

	proc, err := os.FindProcess(rec.PID)
	if err != nil {
		return nil, fmt.Errorf("find process: %w", err)
	}

	now := time.Now().UTC()
	rec.State = JobStateStopping
	rec.LastHeartbeat = &now
	_ = s.Write(rec)

	if err := proc.Signal(sig); err != nil {
		return nil, fmt.Errorf("signal %s: %w", sigStr, err)
	}

	if sig == syscall.SIGKILL {
		return s.markStopped(rec, sigStr, true), nil
	}

	waitTimeout := opts.WaitTimeout
	if waitTimeout <= 0 {
		waitTimeout = 30 * time.Second
	}
	pollInterval := opts.PollInterval
	if pollInterval <= 0 {
		pollInterval = 250 * time.Millisecond
	}

	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if !isProcessAlive(rec.PID) {
			return s.markStopped(rec, sigStr, false), nil
		}
		time.Sleep(pollInterval)
	}

	_ = proc.Signal(syscall.SIGKILL)
	return s.markStopped(rec, sigStr, true), nil
}

func (s *Store) markStopped(rec *JobRecord, sigStr string, forced bool) *StopResult {
	now := time.Now().UTC()
	rec.State = JobStateStopped
	rec.EndedAt = &now
	rec.LastHeartbeat = &now
	_ = s.Write(rec)
	return &StopResult{
		JobID:      rec.JobID,
		Signal:     sigStr,
		ForcedKill: forced,
		State:      string(rec.State),
	}
}
