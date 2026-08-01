package jobregistry

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	afterJobDirBoundBeforeTempCreate   = func() {}
	afterRecordTempCreateBeforeReplace = func() {}
)

// Store persists and loads JobRecords from an on-disk directory.
//
// Directory layout:
//
//	<root>/<job_id>/job.json
//	<root>/<job_id>/stdout.log
//	<root>/<job_id>/stderr.log
//
// Root is expected to be under the app data dir.
type Store struct {
	root string
}

const maxStrictJobRecordBytes = 4 << 20

func NewStore(root string) *Store {
	root = strings.TrimSpace(root)
	if root == "" {
		return &Store{}
	}
	if abs, err := filepath.Abs(root); err == nil {
		root = abs
		if resolved, resolveErr := filepath.EvalSymlinks(root); resolveErr == nil {
			root = resolved
		} else if resolvedParent, parentErr := filepath.EvalSymlinks(filepath.Dir(root)); parentErr == nil {
			root = filepath.Join(resolvedParent, filepath.Base(root))
		}
	}
	return &Store{root: root}
}

func (s *Store) RootDir() string {
	return s.root
}

func (s *Store) JobDir(jobID string) string {
	if strings.TrimSpace(s.root) == "" || validateJobID(jobID) != nil {
		return ""
	}
	return filepath.Join(s.root, jobID)
}

func (s *Store) JobPath(jobID string) string {
	return filepath.Join(s.JobDir(jobID), "job.json")
}

func (s *Store) ensureRoot() error {
	if strings.TrimSpace(s.root) == "" {
		return fmt.Errorf("job registry root dir is empty")
	}
	return mkdirSecure(s.root)
}

func (s *Store) withStartLock(fn func() error) error {
	if err := s.ensureRoot(); err != nil {
		return err
	}
	lockPath := filepath.Join(s.root, ".start.lock")
	f, err := openFileNoFollow(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open job start lock: %w", err)
	}
	defer func() { _ = f.Close() }()
	if err := lockFileExclusive(f); err != nil {
		return fmt.Errorf("lock job starts: %w", err)
	}
	defer func() { _ = unlockFile(f) }()
	return fn()
}

func (s *Store) Write(record *JobRecord) error {
	if record == nil {
		return fmt.Errorf("job record is nil")
	}
	jobID := strings.TrimSpace(record.JobID)
	if jobID == "" {
		return fmt.Errorf("job_id is required")
	}
	if err := validateJobID(jobID); err != nil {
		return err
	}
	// Recovery fence + persist share the start lock so a concurrent
	// BeginStalledRecovery cannot race a read-check-write that misses the fence.
	// Internal recovery/heartbeat paths use writeRecord under the same lock and
	// must not call Write (non-reentrant flock).
	return s.withStartLock(func() error {
		if existing, err := s.getReadOnlyStrict(jobID); err == nil {
			if fenceErr := recoveryFenceViolation(existing, record); fenceErr != nil {
				return fenceErr
			}
		}
		return s.writeRecord(record)
	})
}

func mkdirSecure(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	if err := rejectFinalSymlink(path); err != nil {
		return err
	}
	return os.Chmod(path, 0o700) // #nosec G302 -- this is an owner-only directory and requires its execute bit.
}

func (s *Store) Get(jobID string) (*JobRecord, error) {
	if err := s.ensureRoot(); err != nil {
		return nil, err
	}
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return nil, fmt.Errorf("job_id is required")
	}
	if err := validateJobID(jobID); err != nil {
		return nil, err
	}
	f, err := openJobFileNoFollow(s.root, jobID, "job.json", os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(f)
	_ = f.Close()
	if err != nil {
		return nil, err
	}

	trimmed := strings.TrimSpace(string(b))
	if trimmed == "" {
		return nil, fmt.Errorf("job.json is empty")
	}

	var record JobRecord
	if err := json.Unmarshal([]byte(trimmed), &record); err != nil {
		return nil, fmt.Errorf("parse job.json: %w", err)
	}
	if record.JobID != jobID {
		return nil, fmt.Errorf("job record id %q does not match directory id %q", record.JobID, jobID)
	}

	// Zombie detection: if a job claims running but its pid is gone, mark unknown.
	if record.State == JobStateRunning && record.PID > 0 {
		if !isProcessAlive(record.PID) {
			record.State = JobStateUnknown
			now := time.Now().UTC()
			record.LastHeartbeat = &now
			_ = s.Write(&record)
		}
	}

	return &record, nil
}

// GetReadOnlyStrict returns one job record without demoting state, updating
// heartbeats, creating directories, or otherwise mutating the registry. Recovery
// planners must use this surface so a dead-PID "zombie" demotion cannot erase the
// terminal-contradiction evidence the plan is meant to judge.
func (s *Store) GetReadOnlyStrict(jobID string) (*JobRecord, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return nil, fmt.Errorf("job_id is required")
	}
	root := strings.TrimSpace(s.root)
	if root == "" {
		return nil, fmt.Errorf("job registry root dir is empty")
	}
	return s.getReadOnlyStrict(jobID)
}

// ListReadOnlyStrict returns a byte-preserving snapshot of every registry job.
// Unlike List, it never promotes zombie state or creates registry directories,
// and it rejects every unrecognized or unreadable entry instead of skipping it.
// Destructive planners use this surface so damaged registry state cannot make
// an active job disappear from safety checks.
func (s *Store) ListReadOnlyStrict() ([]JobRecord, error) {
	root := strings.TrimSpace(s.root)
	if root == "" {
		return nil, fmt.Errorf("job registry root dir is empty")
	}
	rootInfo, err := os.Lstat(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect jobs root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, fmt.Errorf("job registry root must be a real directory")
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read jobs root: %w", err)
	}
	out := make([]JobRecord, 0, len(entries))
	for _, entry := range entries {
		if entry.Name() == ".start.lock" {
			info, infoErr := os.Lstat(filepath.Join(root, entry.Name()))
			if infoErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				return nil, fmt.Errorf("invalid job registry start lock")
			}
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() {
			return nil, fmt.Errorf("unrecognized job registry entry %q", entry.Name())
		}
		if err := validateJobID(entry.Name()); err != nil {
			return nil, fmt.Errorf("unrecognized job registry entry %q: %w", entry.Name(), err)
		}
		record, err := s.getReadOnlyStrict(entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read job %s: %w", entry.Name(), err)
		}
		out = append(out, *record)
	}
	sort.Slice(out, func(i, j int) bool {
		return jobSortTime(out[i]).After(jobSortTime(out[j]))
	})
	return out, nil
}

func (s *Store) getReadOnlyStrict(jobID string) (*JobRecord, error) {
	if err := validateJobID(jobID); err != nil {
		return nil, err
	}
	f, err := openJobFileNoFollow(s.root, jobID, "job.json", os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	b, readErr := io.ReadAll(io.LimitReader(f, maxStrictJobRecordBytes+1))
	closeErr := f.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(b) > maxStrictJobRecordBytes {
		return nil, fmt.Errorf("job.json exceeds %d bytes", maxStrictJobRecordBytes)
	}
	trimmed := strings.TrimSpace(string(b))
	if trimmed == "" {
		return nil, fmt.Errorf("job.json is empty")
	}
	var record JobRecord
	if err := json.Unmarshal([]byte(trimmed), &record); err != nil {
		return nil, fmt.Errorf("parse job.json: %w", err)
	}
	if record.JobID != jobID {
		return nil, fmt.Errorf("job record id %q does not match directory id %q", record.JobID, jobID)
	}
	if err := validatePersistedJobState(record.State); err != nil {
		return nil, err
	}
	return &record, nil
}

func validatePersistedJobState(state JobState) error {
	switch state {
	case JobStateQueued,
		JobStateRunning,
		JobStateStopping,
		JobStateStopped,
		JobStateSuccess,
		JobStatePartial,
		JobStateFailed,
		JobStateUnknown:
		return nil
	default:
		return fmt.Errorf("unrecognized persisted job state %q", state)
	}
}

func (s *Store) List() ([]JobRecord, error) {
	if err := s.ensureRoot(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read jobs root: %w", err)
	}

	out := make([]JobRecord, 0, len(entries))
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() || validateJobID(entry.Name()) != nil {
			continue
		}
		jobID := entry.Name()
		r, err := s.Get(jobID)
		if err != nil {
			continue
		}
		out = append(out, *r)
	}

	sort.Slice(out, func(i, j int) bool {
		return jobSortTime(out[i]).After(jobSortTime(out[j]))
	})

	return out, nil
}

func validateJobID(jobID string) error {
	jobID = strings.TrimSpace(jobID)
	parsed, err := uuid.Parse(jobID)
	if err != nil || parsed.String() != jobID {
		return fmt.Errorf("invalid job_id: must be a canonical UUID")
	}
	return nil
}

func rejectFinalSymlink(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("job registry path uses symlink: %s", path)
	}
	if !info.IsDir() {
		return fmt.Errorf("job registry path is not a directory: %s", path)
	}
	return nil
}

// OpenLog creates or opens a registry-owned log without following a final
// symlink. The caller owns the returned handle.
func (s *Store) OpenLog(jobID, name string, truncate bool) (*os.File, error) {
	if err := validateJobID(jobID); err != nil {
		return nil, err
	}
	if name != "stdout.log" && name != "stderr.log" {
		return nil, fmt.Errorf("invalid job log name")
	}
	if err := s.ensureRoot(); err != nil {
		return nil, err
	}
	if err := ensureJobDirNoFollow(s.root, jobID); err != nil {
		return nil, err
	}
	flags := os.O_CREATE | os.O_WRONLY
	if truncate {
		flags |= os.O_TRUNC
	}
	return openJobFileNoFollow(s.root, jobID, name, flags, 0o600)
}

func newRecordTempName() (string, error) {
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate job record temp name: %w", err)
	}
	return "job.json.tmp." + hex.EncodeToString(random[:]), nil
}

// OpenLogRead opens a canonical registry-owned log without following a final
// symlink. Callers must not use persisted record paths as read authority.
func (s *Store) OpenLogRead(jobID, name string) (*os.File, error) {
	if err := s.ensureRoot(); err != nil {
		return nil, err
	}
	if err := validateJobID(jobID); err != nil {
		return nil, err
	}
	if name != "stdout.log" && name != "stderr.log" {
		return nil, fmt.Errorf("invalid job log name")
	}
	return openJobFileNoFollow(s.root, jobID, name, os.O_RDONLY, 0)
}

// ClaimQueued atomically validates and promotes one queued job to running.
func (s *Store) ClaimQueued(jobID string, pid int, validate func(*JobRecord) error) (*JobRecord, error) {
	var claimed *JobRecord
	err := s.withStartLock(func() error {
		// Strict read: Get may Write (zombie demotion) and would deadlock under
		// the start lock now that Write also takes the lock for fence CAS.
		rec, err := s.getReadOnlyStrict(jobID)
		if err != nil {
			return err
		}
		if rec.State != JobStateQueued {
			return fmt.Errorf("managed job %s is not queued", jobID)
		}
		failQueued := func(cause error) error {
			now := time.Now().UTC()
			rec.State = JobStateFailed
			rec.EndedAt = &now
			rec.EnqueueOwnerPID = 0
			rec.EnqueueExpiresAt = nil
			// writeRecord: already under withStartLock (Write would deadlock).
			if writeErr := s.writeRecord(rec); writeErr != nil {
				return fmt.Errorf("%v; persist failed claim: %w", cause, writeErr)
			}
			return cause
		}
		if rec.EnqueueExpiresAt != nil && time.Now().UTC().After(*rec.EnqueueExpiresAt) {
			return failQueued(fmt.Errorf("managed job %s enqueue ownership expired", jobID))
		}
		if validate != nil {
			if err := validate(rec); err != nil {
				return failQueued(err)
			}
		}
		now := time.Now().UTC()
		rec.State = JobStateRunning
		rec.PID = pid
		rec.StartedAt = &now
		rec.LastHeartbeat = &now
		rec.HeartbeatPersistError = ""
		// Capture OS process birth identity at claim. Missing identity is not a
		// claim failure — the recovery planner refuses indeterminate records —
		// but we never invent a token.
		if id := observeProcessIdentity(pid); id.Proven {
			ApplyProcessIdentity(rec, id)
		} else {
			rec.ProcessStartTimeUnixMS = nil
			rec.ProcessBootID = ""
			rec.ProcessTokenVersion = 0
			rec.ProcessStartTicks = 0
			rec.ProcessStartSec = 0
			rec.ProcessStartUsec = 0
			rec.ProcessFiletime = 0
		}
		rec.EnqueueOwnerPID = 0
		rec.EnqueueExpiresAt = nil
		if err := s.writeRecord(rec); err != nil {
			return err
		}
		claimed = rec
		return nil
	})
	return claimed, err
}

// TouchHeartbeat persists a heartbeat for a still-running job using a
// start-lock-serialized re-read so a concurrent stop/recovery transition is not
// overwritten from a stale in-memory snapshot. It refuses when the record is no
// longer running or the PID/birth identity no longer match the caller.
//
// recover-stalled recovery fencing will share this lock; terminal writers that must not be
// clobbered by a heartbeat should also enter withStartLock (or a future CAS).
func (s *Store) TouchHeartbeat(jobID string, expectedPID int, expectedStartMS *uint64, expectedBootID string) error {
	return s.withStartLock(func() error {
		rec, err := s.getReadOnlyStrict(jobID)
		if err != nil {
			return err
		}
		if rec.State != JobStateRunning {
			return fmt.Errorf("job %s is not running (state=%s)", jobID, rec.State)
		}
		if rec.PID != expectedPID || expectedPID <= 0 {
			return fmt.Errorf("job %s pid changed during heartbeat", jobID)
		}
		if expectedStartMS != nil {
			if rec.ProcessStartTimeUnixMS == nil || *rec.ProcessStartTimeUnixMS != *expectedStartMS {
				return fmt.Errorf("job %s process identity changed during heartbeat", jobID)
			}
		}
		if expectedBootID != "" && rec.ProcessBootID != expectedBootID {
			return fmt.Errorf("job %s process boot identity changed during heartbeat", jobID)
		}
		now := time.Now().UTC()
		rec.LastHeartbeat = &now
		rec.HeartbeatPersistError = ""
		return s.writeRecord(rec)
	})
}

// RecordHeartbeatPersistError records that a heartbeat write failed so planners
// can refuse to treat heartbeat age as stop authority.
func (s *Store) RecordHeartbeatPersistError(jobID string, persistErr error) error {
	if persistErr == nil {
		return nil
	}
	return s.withStartLock(func() error {
		rec, err := s.getReadOnlyStrict(jobID)
		if err != nil {
			return err
		}
		if rec.State != JobStateRunning {
			return nil
		}
		rec.HeartbeatPersistError = persistErr.Error()
		return s.writeRecord(rec)
	})
}

func jobSortTime(r JobRecord) time.Time {
	if r.StartedAt != nil {
		return r.StartedAt.UTC()
	}
	return r.CreatedAt.UTC()
}
