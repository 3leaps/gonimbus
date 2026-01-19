package jobregistry

import "time"

// JobState is the lifecycle state of a managed job.
//
// NOTE: These values are persisted in job.json and are part of the stable
// on-disk contract.
type JobState string

const (
	JobStateQueued   JobState = "queued"
	JobStateRunning  JobState = "running"
	JobStateStopping JobState = "stopping"
	JobStateStopped  JobState = "stopped"
	JobStateSuccess  JobState = "success"
	JobStatePartial  JobState = "partial"
	JobStateFailed   JobState = "failed"
	JobStateUnknown  JobState = "unknown"
)

// EffectiveIdentity is a minimal identity summary captured for operator clarity.
//
// This is intentionally shallow and string-only so the job registry stays
// stable even if deeper identity schemas evolve.
type EffectiveIdentity struct {
	StorageProvider string `json:"storage_provider,omitempty"`
	CloudProvider   string `json:"cloud_provider,omitempty"`
	RegionKind      string `json:"region_kind,omitempty"`
	Region          string `json:"region,omitempty"`
	EndpointHost    string `json:"endpoint_host,omitempty"`
}

// JobRecord is the persistent record written to job.json.
//
// The schema is designed for backward-compatible extension (additive fields).
type JobRecord struct {
	JobID        string    `json:"job_id"`
	Name         string    `json:"name,omitempty"`
	State        JobState  `json:"state"`
	ManifestPath string    `json:"manifest_path"`
	IndexDir     string    `json:"index_dir,omitempty"`
	IndexSetID   string    `json:"index_set_id,omitempty"`
	RunID        string    `json:"run_id,omitempty"`
	PID          int       `json:"pid,omitempty"`
	CreatedAt    time.Time `json:"created_at"`

	StartedAt     *time.Time         `json:"started_at,omitempty"`
	EndedAt       *time.Time         `json:"ended_at,omitempty"`
	LastHeartbeat *time.Time         `json:"last_heartbeat,omitempty"`
	Identity      *EffectiveIdentity `json:"effective_identity,omitempty"`
	StdoutPath    string             `json:"stdout_path,omitempty"`
	StderrPath    string             `json:"stderr_path,omitempty"`
}
