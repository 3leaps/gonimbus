package jobregistry

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Executor spawns and manages background jobs.
//
// v0.1.4 design: spawn a child process that runs `gonimbus index build` in
// managed mode, capturing stdout/stderr to per-job log files.
type Executor struct {
	store *Store
}

func NewExecutor(root string) *Executor {
	return &Executor{store: NewStore(root)}
}

func (e *Executor) Store() *Store {
	return e.store
}

func (e *Executor) StdoutPath(jobID string) string {
	return filepath.Join(e.store.JobDir(jobID), "stdout.log")
}

func (e *Executor) StderrPath(jobID string) string {
	return filepath.Join(e.store.JobDir(jobID), "stderr.log")
}

type BackgroundOptions struct {
	Dedupe   bool
	Since    string
	JobType  string
	Metadata map[string]string
}

// StartIndexBuildBackground spawns a managed child process running:
//
//	gonimbus index build --job <manifest> --_managed-job-id <job_id>
//
// It returns after the child successfully starts.
func (e *Executor) StartIndexBuildBackground(manifestPath string, name string, opts BackgroundOptions) (*JobRecord, error) {
	if e == nil || e.store == nil {
		return nil, fmt.Errorf("executor is not initialized")
	}

	jobID := uuid.New().String()
	jobDir := e.store.JobDir(jobID)
	if err := mkdirSecure(jobDir); err != nil {
		return nil, fmt.Errorf("create job dir: %w", err)
	}

	stdoutFile, err := os.OpenFile(e.StdoutPath(jobID), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create stdout log: %w", err)
	}
	stderrFile, err := os.OpenFile(e.StderrPath(jobID), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		_ = stdoutFile.Close()
		return nil, fmt.Errorf("create stderr log: %w", err)
	}

	exe, err := os.Executable()
	if err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()
		return nil, fmt.Errorf("resolve executable: %w", err)
	}

	absManifest, err := filepath.Abs(strings.TrimSpace(manifestPath))
	if err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()
		return nil, fmt.Errorf("resolve manifest path: %w", err)
	}
	if absManifest == "" {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()
		return nil, fmt.Errorf("manifest path is required")
	}
	if _, err := os.Stat(absManifest); err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()
		return nil, fmt.Errorf("manifest not found: %s", absManifest)
	}

	if opts.Dedupe {
		if existing, _ := e.store.List(); len(existing) > 0 {
			for _, j := range existing {
				if strings.TrimSpace(j.ManifestPath) == absManifest && j.State == JobStateRunning {
					_ = stdoutFile.Close()
					_ = stderrFile.Close()
					return nil, fmt.Errorf("duplicate running job exists: %s", j.JobID)
				}
			}
		}
	}

	args := []string{"index", "build", "--job", absManifest, "--_managed-job-id", jobID}
	if since := strings.TrimSpace(opts.Since); since != "" {
		args = append(args, "--since", since)
	}
	cmd := exec.Command(exe, args...)
	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile
	cmd.Env = os.Environ()

	if err := cmd.Start(); err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()
		return nil, fmt.Errorf("start managed index build: %w", err)
	}

	now := time.Now().UTC()
	rec := &JobRecord{
		JobID:         jobID,
		Type:          normalizeJobType(opts.JobType),
		Name:          strings.TrimSpace(name),
		State:         JobStateRunning,
		ManifestPath:  absManifest,
		PID:           cmd.Process.Pid,
		CreatedAt:     now,
		StartedAt:     &now,
		LastHeartbeat: func() *time.Time { t := now; return &t }(),
		StdoutPath:    e.StdoutPath(jobID),
		StderrPath:    e.StderrPath(jobID),
		Metadata:      indexBuildBackgroundMetadata(opts),
	}
	if err := e.store.Write(rec); err != nil {
		return nil, err
	}

	_ = stdoutFile.Close()
	_ = stderrFile.Close()

	return rec, nil
}

func normalizeJobType(jobType string) string {
	jobType = strings.TrimSpace(jobType)
	if jobType == "" {
		return JobTypeIndexBuild
	}
	return jobType
}

func cloneMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func indexBuildBackgroundMetadata(opts BackgroundOptions) map[string]string {
	out := cloneMetadata(opts.Metadata)
	if since := strings.TrimSpace(opts.Since); since != "" {
		if out == nil {
			out = map[string]string{}
		}
		out["since"] = since
	}
	return out
}
