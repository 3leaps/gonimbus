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
	store       *Store
	newCommand  func(string, ...string) *exec.Cmd
	afterQueued func(*JobRecord) error
}

const enqueueOwnershipTTL = 30 * time.Second

func NewExecutor(root string) *Executor {
	return &Executor{store: NewStore(root), newCommand: exec.Command}
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
	Dedupe     bool
	Since      string
	JobType    string
	Metadata   map[string]string
	Invocation *IndexBuildInvocation
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
	if err := validateBackgroundMetadata(opts.Metadata); err != nil {
		return nil, err
	}

	requestedInvocation := opts.Invocation
	if requestedInvocation == nil {
		requestedInvocation = &IndexBuildInvocation{
			SchemaVersion:     IndexBuildInvocationVersion,
			RequestedFormat:   "durable",
			EffectiveFormat:   "durable",
			Since:             strings.TrimSpace(opts.Since),
			ScopeWarnPrefixes: DefaultScopeWarnPrefixes,
			ScopeMaxPrefixes:  DefaultScopeMaxPrefixes,
		}
	}
	inv, fingerprint, err := PrepareIndexBuildInvocation(manifestPath, name, requestedInvocation)
	if err != nil {
		return nil, err
	}
	jobID := uuid.New().String()
	now := time.Now().UTC()
	enqueueExpiresAt := now.Add(enqueueOwnershipTTL)
	rec := &JobRecord{
		JobID:                 jobID,
		Type:                  normalizeJobType(opts.JobType),
		Name:                  inv.Name,
		State:                 JobStateQueued,
		ManifestPath:          inv.ManifestPath,
		CreatedAt:             now,
		EnqueueOwnerPID:       os.Getpid(),
		EnqueueExpiresAt:      &enqueueExpiresAt,
		StdoutPath:            e.StdoutPath(jobID),
		StderrPath:            e.StderrPath(jobID),
		Metadata:              indexBuildBackgroundMetadata(opts),
		Invocation:            inv,
		InvocationFingerprint: fingerprint,
	}
	if err := e.store.withStartLock(func() error {
		if err := recoverExpiredQueuedJobs(e.store, now); err != nil {
			return err
		}
		if opts.Dedupe {
			existing, err := e.store.List()
			if err != nil {
				return err
			}
			for _, j := range existing {
				if j.InvocationFingerprint == fingerprint && activeJobState(j.State) {
					return fmt.Errorf("duplicate running job exists: %s", j.JobID)
				}
			}
		}
		return e.store.Write(rec)
	}); err != nil {
		return nil, err
	}
	if e.afterQueued != nil {
		if err := e.afterQueued(rec); err != nil {
			return nil, err
		}
	}

	stdoutFile, err := e.store.OpenLog(jobID, "stdout.log", true)
	if err != nil {
		markJobStartFailed(e.store, rec)
		return nil, fmt.Errorf("create stdout log: %w", err)
	}
	stderrFile, err := e.store.OpenLog(jobID, "stderr.log", true)
	if err != nil {
		_ = stdoutFile.Close()
		markJobStartFailed(e.store, rec)
		return nil, fmt.Errorf("create stderr log: %w", err)
	}

	exe, err := os.Executable()
	if err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()
		markJobStartFailed(e.store, rec)
		return nil, fmt.Errorf("resolve executable: %w", err)
	}
	args := indexBuildInvocationArgs(*inv, jobID)
	newCommand := e.newCommand
	if newCommand == nil {
		newCommand = exec.Command
	}
	cmd := newCommand(exe, args...)
	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile
	if cmd.Env == nil {
		cmd.Env = os.Environ()
	}
	if inv.DataRoot != "" {
		cmd.Env = replaceEnv(cmd.Env, "GONIMBUS_DATA_DIR", inv.DataRoot)
	}

	if err := cmd.Start(); err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()
		markJobStartFailed(e.store, rec)
		return nil, fmt.Errorf("start managed index build: %w", err)
	}
	// The child owns queued -> running -> terminal transitions. The parent must
	// not overwrite a fast child after cmd.Start. PID is returned for immediate
	// operator feedback but is persisted by the child's claimed transition.
	rec.PID = cmd.Process.Pid
	go func() {
		_ = cmd.Wait()
	}()

	_ = stdoutFile.Close()
	_ = stderrFile.Close()

	return rec, nil
}

func activeJobState(state JobState) bool {
	return state == JobStateQueued || state == JobStateRunning || state == JobStateStopping
}

func recoverExpiredQueuedJobs(store *Store, now time.Time) error {
	jobs, err := store.List()
	if err != nil {
		return err
	}
	for i := range jobs {
		rec := &jobs[i]
		if rec.State != JobStateQueued || rec.EnqueueExpiresAt == nil || now.Before(*rec.EnqueueExpiresAt) {
			continue
		}
		rec.State = JobStateFailed
		rec.EndedAt = &now
		rec.EnqueueOwnerPID = 0
		rec.EnqueueExpiresAt = nil
		if err := store.Write(rec); err != nil {
			return fmt.Errorf("recover expired queued job %s: %w", rec.JobID, err)
		}
	}
	return nil
}

func replaceEnv(env []string, key, value string) []string {
	prefix := key + "="
	out := make([]string, 0, len(env)+1)
	for _, item := range env {
		if !strings.HasPrefix(item, prefix) {
			out = append(out, item)
		}
	}
	return append(out, prefix+value)
}

func markJobStartFailed(store *Store, rec *JobRecord) {
	if store == nil || rec == nil {
		return
	}
	rec.State = JobStateFailed
	ended := time.Now().UTC()
	rec.EndedAt = &ended
	_ = store.Write(rec)
}

func indexBuildInvocationArgs(inv IndexBuildInvocation, jobID string) []string {
	args := make([]string, 0, 32)
	if inv.ConfigPath != "" {
		args = append(args, "--config", inv.ConfigPath)
	}
	if inv.Verbose {
		args = append(args, "--verbose")
	}
	if inv.ReadOnly {
		args = append(args, "--readonly")
	}
	args = append(args, "index", "build", "--job", inv.ManifestPath, "--format", inv.EffectiveFormat, "--_managed-job-id", jobID)
	appendStringFlag := func(flag, value string) {
		if strings.TrimSpace(value) != "" {
			args = append(args, flag, value)
		}
	}
	appendStringFlag("--db", inv.DBPath)
	appendStringFlag("--since", inv.Since)
	appendStringFlag("--name", inv.Name)
	appendStringFlag("--storage-provider", inv.StorageProvider)
	appendStringFlag("--cloud-provider", inv.CloudProvider)
	appendStringFlag("--region-kind", inv.RegionKind)
	appendStringFlag("--region", inv.Region)
	appendStringFlag("--endpoint-host", inv.EndpointHost)
	args = append(args,
		"--scope-warn-prefixes", fmt.Sprintf("%d", inv.ScopeWarnPrefixes),
		"--scope-max-prefixes", fmt.Sprintf("%d", inv.ScopeMaxPrefixes),
	)
	return args
}

func normalizeJobType(jobType string) string {
	jobType = strings.TrimSpace(jobType)
	if jobType == "" {
		return JobTypeIndexBuild
	}
	return jobType
}

func validateBackgroundMetadata(metadata map[string]string) error {
	if len(metadata) != 0 {
		return fmt.Errorf("managed build metadata is not supported")
	}
	return nil
}

func indexBuildBackgroundMetadata(opts BackgroundOptions) map[string]string {
	var out map[string]string
	if since := strings.TrimSpace(opts.Since); since != "" {
		if out == nil {
			out = map[string]string{}
		}
		out["since"] = since
	}
	return out
}
