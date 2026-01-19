package jobregistry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
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
	return &Store{root: strings.TrimSpace(root)}
}

func (s *Store) RootDir() string {
	return s.root
}

func (s *Store) JobDir(jobID string) string {
	return filepath.Join(s.root, jobID)
}

func (s *Store) JobPath(jobID string) string {
	return filepath.Join(s.JobDir(jobID), "job.json")
}

func (s *Store) ensureRoot() error {
	if strings.TrimSpace(s.root) == "" {
		return fmt.Errorf("job registry root dir is empty")
	}
	return os.MkdirAll(s.root, 0755)
}

func (s *Store) Write(record *JobRecord) error {
	if record == nil {
		return fmt.Errorf("job record is nil")
	}
	jobID := strings.TrimSpace(record.JobID)
	if jobID == "" {
		return fmt.Errorf("job_id is required")
	}
	if err := s.ensureRoot(); err != nil {
		return err
	}

	jobDir := s.JobDir(jobID)
	if err := os.MkdirAll(jobDir, 0755); err != nil {
		return fmt.Errorf("create job dir: %w", err)
	}

	b, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal job record: %w", err)
	}
	b = append(b, '\n')

	tmp, err := os.CreateTemp(jobDir, "job.json.tmp.*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp job file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp job file: %w", err)
	}

	finalPath := s.JobPath(jobID)
	if err := os.Rename(tmpName, finalPath); err != nil {
		return fmt.Errorf("rename job file: %w", err)
	}
	return nil
}

func (s *Store) Get(jobID string) (*JobRecord, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return nil, fmt.Errorf("job_id is required")
	}
	path := s.JobPath(jobID)
	b, err := os.ReadFile(path)
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
		if !entry.IsDir() {
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

func jobSortTime(r JobRecord) time.Time {
	if r.StartedAt != nil {
		return r.StartedAt.UTC()
	}
	return r.CreatedAt.UTC()
}

func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// signal 0 is supported on unix; it checks for existence without sending a signal.
	if err := p.Signal(os.Signal(syscall.Signal(0))); err != nil {
		return false
	}
	return true
}
