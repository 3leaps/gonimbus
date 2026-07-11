package jobregistry

import "time"

const (
	JobTypeIndexBuild           = "index.build"
	IndexBuildInvocationVersion = 1
)

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

// IndexBuildInvocation is the disclosure-minimal, versioned command contract
// shared by a background-build parent and its managed child. It intentionally
// contains only accepted build controls; credentials and provider endpoints are
// resolved by the child through the normal manifest/config credential chain.
type IndexBuildInvocation struct {
	SchemaVersion     int    `json:"schema_version"`
	ManifestPath      string `json:"manifest_path"`
	ManifestSHA256    string `json:"manifest_sha256"`
	RequestedFormat   string `json:"requested_format"`
	EffectiveFormat   string `json:"effective_format"`
	ConfigPath        string `json:"config_path,omitempty"`
	DataRoot          string `json:"data_root,omitempty"`
	Verbose           bool   `json:"verbose,omitempty"`
	ReadOnly          bool   `json:"readonly,omitempty"`
	DBPath            string `json:"db_path,omitempty"`
	Since             string `json:"since,omitempty"`
	Name              string `json:"name,omitempty"`
	StorageProvider   string `json:"storage_provider,omitempty"`
	CloudProvider     string `json:"cloud_provider,omitempty"`
	RegionKind        string `json:"region_kind,omitempty"`
	Region            string `json:"region,omitempty"`
	EndpointHost      string `json:"endpoint_host,omitempty"`
	ScopeWarnPrefixes int    `json:"scope_warn_prefixes"`
	ScopeMaxPrefixes  int    `json:"scope_max_prefixes"`
}

// BuildReceiptIdentity is the stable, metadata-only committed-artifact
// identity attached to a terminal managed job without parsing its logs.
type BuildReceiptIdentity struct {
	Type             string   `json:"type"`
	SchemaVersion    string   `json:"schema_version"`
	Status           string   `json:"status"`
	RequestedFormat  string   `json:"requested_format"`
	FormatsCommitted []string `json:"formats_committed"`
	IndexSetID       string   `json:"index_set_id"`
	RunID            string   `json:"run_id"`
	ScopeHash        string   `json:"scope_hash,omitempty"`
	ManifestSHA256   string   `json:"manifest_sha256,omitempty"`
}

// JobRecord is the persistent record written to job.json.
//
// The schema is designed for backward-compatible extension (additive fields).
type JobRecord struct {
	JobID            string     `json:"job_id"`
	Type             string     `json:"type,omitempty"`
	Name             string     `json:"name,omitempty"`
	State            JobState   `json:"state"`
	ManifestPath     string     `json:"manifest_path"`
	IndexDir         string     `json:"index_dir,omitempty"`
	IndexSetID       string     `json:"index_set_id,omitempty"`
	RunID            string     `json:"run_id,omitempty"`
	PID              int        `json:"pid,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	EnqueueOwnerPID  int        `json:"enqueue_owner_pid,omitempty"`
	EnqueueExpiresAt *time.Time `json:"enqueue_expires_at,omitempty"`

	StartedAt             *time.Time            `json:"started_at,omitempty"`
	EndedAt               *time.Time            `json:"ended_at,omitempty"`
	LastHeartbeat         *time.Time            `json:"last_heartbeat,omitempty"`
	Identity              *EffectiveIdentity    `json:"effective_identity,omitempty"`
	StdoutPath            string                `json:"stdout_path,omitempty"`
	StderrPath            string                `json:"stderr_path,omitempty"`
	Metadata              map[string]string     `json:"metadata,omitempty"`
	Invocation            *IndexBuildInvocation `json:"effective_invocation,omitempty"`
	InvocationFingerprint string                `json:"invocation_fingerprint,omitempty"`
	Receipt               *BuildReceiptIdentity `json:"terminal_receipt,omitempty"`
}
