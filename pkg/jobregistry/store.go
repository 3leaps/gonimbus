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
	if err := s.ensureRoot(); err != nil {
		return err
	}

	b, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal job record: %w", err)
	}
	b = append(b, '\n')

	return writeJobRecordAtomic(s.root, jobID, b)
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
		rec, err := s.Get(jobID)
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
			if writeErr := s.Write(rec); writeErr != nil {
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
		rec.EnqueueOwnerPID = 0
		rec.EnqueueExpiresAt = nil
		if err := s.Write(rec); err != nil {
			return err
		}
		claimed = rec
		return nil
	})
	return claimed, err
}

func jobSortTime(r JobRecord) time.Time {
	if r.StartedAt != nil {
		return r.StartedAt.UTC()
	}
	return r.CreatedAt.UTC()
}
