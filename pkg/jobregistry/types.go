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

	StartedAt     *time.Time `json:"started_at,omitempty"`
	EndedAt       *time.Time `json:"ended_at,omitempty"`
	LastHeartbeat *time.Time `json:"last_heartbeat,omitempty"`
	// ProcessStartTimeUnixMS is the OS process birth token captured at claim.
	// Together with PID (and ProcessBootID when present) it is the durable
	// process identity used for PID-reuse protection. Job StartedAt is wall
	// clock for the job, not this token.
	ProcessStartTimeUnixMS *uint64 `json:"process_start_time_unix_ms,omitempty"`
	// ProcessBootID binds ProcessStartTimeUnixMS to a boot/session when the
	// platform exposes one (e.g. Linux boot_id). Empty when unavailable.
	ProcessBootID string `json:"process_boot_id,omitempty"`
	// HeartbeatPersistError records the last failed heartbeat write. A non-empty
	// value means heartbeat age must not authorize termination (writes were not
	// proven). Cleared on the next successful heartbeat persist.
	HeartbeatPersistError string `json:"heartbeat_persist_error,omitempty"`
	// RecoveryIntent is set when a stalled-recovery fence owns the job
	// (value "stalled"). While set with state stopping, heartbeat and child
	// terminal writers must not restore running or claim success.
	RecoveryIntent string `json:"recovery_intent,omitempty"`
	// RecoveryOwner is an opaque id for the recovery attempt that holds the
	// fence. Concurrent recoveries must not steal an active owner.
	RecoveryOwner string `json:"recovery_owner,omitempty"`
	// RecoverySignalOwner is the exclusive claim for bind/signal/finalize under
	// an active fence. Multiple callers may share RecoveryOwner (resume), but
	// only one may hold RecoverySignalOwner and perform the signal sequence.
	RecoverySignalOwner string `json:"recovery_signal_owner,omitempty"`
	// RecoverySignalClaimedAt is when RecoverySignalOwner was acquired.
	RecoverySignalClaimedAt *time.Time `json:"recovery_signal_claimed_at,omitempty"`
	// RecoverySignalClaimerPID is the OS PID of the recover process that holds
	// the signal claim. Used with claimer birth token to prove the claimer is gone.
	RecoverySignalClaimerPID int `json:"recovery_signal_claimer_pid,omitempty"`
	// RecoverySignalClaimerStartMS is legacy display ms of the claimer process.
	RecoverySignalClaimerStartMS *uint64 `json:"recovery_signal_claimer_start_ms,omitempty"`
	// Full versioned claimer birth token (same contract as managed process).
	RecoverySignalClaimerTokenVersion int    `json:"recovery_signal_claimer_token_version,omitempty"`
	RecoverySignalClaimerBootID       string `json:"recovery_signal_claimer_boot_id,omitempty"`
	RecoverySignalClaimerStartTicks   uint64 `json:"recovery_signal_claimer_start_ticks,omitempty"`
	RecoverySignalClaimerStartSec     int64  `json:"recovery_signal_claimer_start_sec,omitempty"`
	RecoverySignalClaimerStartUsec    int64  `json:"recovery_signal_claimer_start_usec,omitempty"`
	RecoverySignalClaimerFiletime     uint64 `json:"recovery_signal_claimer_filetime,omitempty"`
	// RecoveryStartedAt is when the fence was acquired.
	RecoveryStartedAt *time.Time `json:"recovery_started_at,omitempty"`
	// RecoveryPhase is the durable recovery state machine phase (entarch A+B).
	// Values: fenced, claimed, bound, term-intent, term-sent, kill-intent,
	// kill-sent, death-observed, lease-reconciled, finalized. Empty when idle.
	RecoveryPhase string `json:"recovery_phase,omitempty"`
	// RecoveryGeneration increments on each fence acquisition; phase CAS binds to it.
	RecoveryGeneration int64 `json:"recovery_generation,omitempty"`
	// RecoveryW2Receipt is the identity-bound lease reconciliation proof (E-R10-1).
	// Finalize-only resume trusts this receipt rather than re-probing the path.
	RecoveryW2Receipt *RecoveryW2Receipt `json:"recovery_w2_receipt,omitempty"`
	// RecoveryDeliverySignalled is durable proof that at least one stop syscall
	// was accepted under this generation (set only after accepted transport).
	RecoveryDeliverySignalled bool `json:"recovery_delivery_signalled,omitempty"`
	// RecoveryDeliveryForced is durable proof that a hard-stop/KILL was accepted.
	RecoveryDeliveryForced bool `json:"recovery_delivery_forced,omitempty"`
	// RecoveryBound* is the generation/attempt-bound target snapshot for replay.
	RecoveryBoundPID          int    `json:"recovery_bound_pid,omitempty"`
	RecoveryBoundTokenVersion int    `json:"recovery_bound_token_version,omitempty"`
	RecoveryBoundStartMS      uint64 `json:"recovery_bound_start_ms,omitempty"`
	RecoveryBoundBootID       string `json:"recovery_bound_boot_id,omitempty"`
	RecoveryBoundStartTicks   uint64 `json:"recovery_bound_start_ticks,omitempty"`
	RecoveryBoundStartSec     int64  `json:"recovery_bound_start_sec,omitempty"`
	RecoveryBoundStartUsec    int64  `json:"recovery_bound_start_usec,omitempty"`
	RecoveryBoundFiletime     uint64 `json:"recovery_bound_filetime,omitempty"`
	RecoveryBoundAttempt      string `json:"recovery_bound_attempt,omitempty"`
	RecoveryBoundFenceOwner   string `json:"recovery_bound_fence_owner,omitempty"`
	RecoveryBoundGeneration   int64  `json:"recovery_bound_generation,omitempty"`
	RecoveryBoundIndexSetID   string `json:"recovery_bound_index_set_id,omitempty"`
	RecoveryBoundJobID        string `json:"recovery_bound_job_id,omitempty"`
	// RecoveryAuthorityRoot is the canonical set-authority root bound at fence time.
	// W2 may only operate against this exact root (D-R15-02 / D-R17-03).
	RecoveryAuthorityRoot string `json:"recovery_authority_root,omitempty"`
	// RecoveryAuthorityDev/Ino bind directory identity of the authority root when
	// the platform exposes them (Unix). Used to detect symlink retarget/replace.
	RecoveryAuthorityDev uint64 `json:"recovery_authority_dev,omitempty"`
	RecoveryAuthorityIno uint64 `json:"recovery_authority_ino,omitempty"`
	// ProcessTokenVersion is the birth-token schema (1 = native fields authoritative).
	ProcessTokenVersion int `json:"process_token_version,omitempty"`
	// ProcessStartTicks is Linux /proc starttime ticks (authoritative when version=1).
	ProcessStartTicks uint64 `json:"process_start_ticks,omitempty"`
	// ProcessStartSec/Usec are Darwin kinfo starttime components.
	ProcessStartSec  int64 `json:"process_start_sec,omitempty"`
	ProcessStartUsec int64 `json:"process_start_usec,omitempty"`
	// ProcessFiletime is Windows FILETIME 100ns ticks since 1601 (authoritative).
	ProcessFiletime uint64 `json:"process_filetime,omitempty"`

	Identity              *EffectiveIdentity    `json:"effective_identity,omitempty"`
	StdoutPath            string                `json:"stdout_path,omitempty"`
	StderrPath            string                `json:"stderr_path,omitempty"`
	Metadata              map[string]string     `json:"metadata,omitempty"`
	Invocation            *IndexBuildInvocation `json:"effective_invocation,omitempty"`
	InvocationFingerprint string                `json:"invocation_fingerprint,omitempty"`
	Receipt               *BuildReceiptIdentity `json:"terminal_receipt,omitempty"`
}

// RecoveryIntentStalled is the durable fence label for managed stalled recovery.
const RecoveryIntentStalled = "stalled"

// ProcessTokenVersionV1 is the versioned native birth-token schema.
const ProcessTokenVersionV1 = 1

// RecoveryW2Receipt proves what lease reconciliation did for a generation/attempt.
type RecoveryW2Receipt struct {
	SchemaVersion int    `json:"schema_version"`
	Generation    int64  `json:"generation"`
	FenceOwner    string `json:"fence_owner"`
	// OriginAttempt is the attempt that produced the receipt (may differ from resume attempt).
	OriginAttempt string    `json:"origin_attempt"`
	JobID         string    `json:"job_id"`
	IndexSetID    string    `json:"index_set_id"`
	LeaseVerdict  string    `json:"lease_verdict"`
	Reclaimed     bool      `json:"reclaimed"`
	Signalled     bool      `json:"signalled"`
	ForcedKill    bool      `json:"forced_kill"`
	ReconciledAt  time.Time `json:"reconciled_at"`
	// LeasePath is the authority path when known (successor race control).
	LeasePath string `json:"lease_path,omitempty"`
	// LeaseDev/LeaseIno bind the old lease file identity when available.
	LeaseDev uint64 `json:"lease_dev,omitempty"`
	LeaseIno uint64 `json:"lease_ino,omitempty"`
}
