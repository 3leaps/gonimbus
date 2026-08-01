package indexcoord

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

// StalledPlanClass is a typed recovery-plan outcome. It is never a boolean: each
// class names a distinct authorization posture for a later mutating recover.
type StalledPlanClass string

const (
	// PlanHealthy: managed job is live under a matching identity with a fresh
	// heartbeat (or is still in a pre-lease phase with a fresh claim heartbeat).
	PlanHealthy StalledPlanClass = "healthy"
	// PlanSuspectHeartbeatOverdue: live matching identity, held lease, heartbeat
	// past grace — a candidate for a later confirm-gated recover. plan-stalled never
	// signals on this class.
	PlanSuspectHeartbeatOverdue StalledPlanClass = "suspect-heartbeat-overdue"
	// PlanTerminalContradiction: running/stopping record whose process is dead.
	// No signal; only re-probe + possible unheld reclaim in a later wave.
	PlanTerminalContradiction StalledPlanClass = "terminal-contradiction"
	// PlanIdentityMismatch: live PID but birth token does not match the record.
	PlanIdentityMismatch StalledPlanClass = "identity-mismatch"
	// PlanForegroundOrUnmatched: holder is not a durable managed job identity.
	PlanForegroundOrUnmatched StalledPlanClass = "foreground-or-unmatched"
	// PlanLeaseNotHeld: process identity may match, but the set-authority lease
	// is not held (nothing for process-stop to unlock).
	PlanLeaseNotHeld StalledPlanClass = "lease-not-held"
	// PlanInvalid: wrong type, unreadable record, or invalid lease residue.
	PlanInvalid StalledPlanClass = "invalid"
	// PlanIndeterminate: identity unproven, heartbeat writes failing, missing
	// index set correlation, or other incomplete evidence. Refuse, never signal.
	PlanIndeterminate StalledPlanClass = "indeterminate"
)

// ManagedHeartbeatInterval is the product heartbeat cadence. Grace floors are
// defined relative to this interval so a caller cannot shrink stop authority to
// an instant.
const ManagedHeartbeatInterval = 30 * time.Second

// DefaultStalledHeartbeatGrace is 3× the heartbeat interval. Callers may raise
// it; they may not lower it below MinStalledHeartbeatGrace.
const DefaultStalledHeartbeatGrace = 3 * ManagedHeartbeatInterval

// MinStalledHeartbeatGrace is the floor (2× heartbeat interval).
const MinStalledHeartbeatGrace = 2 * ManagedHeartbeatInterval

// StalledPlanOptions configures a read-only recovery plan.
type StalledPlanOptions struct {
	// AuthorityRoot is the set-authority directory used for the lease probe.
	// Required when the job has an IndexSetID; empty skips lease correlation and
	// yields indeterminate once identity otherwise looks stalled.
	AuthorityRoot string
	// Now is the observation clock. Zero means time.Now().UTC().
	Now time.Time
	// HeartbeatGrace is how long a live matching process may go without a
	// heartbeat before becoming suspect. Zero uses DefaultStalledHeartbeatGrace.
	// Values below MinStalledHeartbeatGrace are raised to the floor.
	HeartbeatGrace time.Duration
}

// StalledRecoveryPlan is the read-only judgment for one managed job. It never
// signals, reaps, or rewrites registry state.
type StalledRecoveryPlan struct {
	JobID             string           `json:"job_id"`
	Class             StalledPlanClass `json:"class"`
	Detail            string           `json:"detail,omitempty"`
	JobState          string           `json:"job_state,omitempty"`
	JobType           string           `json:"job_type,omitempty"`
	PID               int              `json:"pid,omitempty"`
	ProcessAlive      bool             `json:"process_alive"`
	RecordedIdentity  string           `json:"recorded_identity,omitempty"`
	ObservedIdentity  string           `json:"observed_identity,omitempty"`
	IndexSetID        string           `json:"index_set_id,omitempty"`
	LeaseVerdict      LeaseVerdict     `json:"lease_verdict,omitempty"`
	LeaseHolder       string           `json:"lease_holder,omitempty"`
	HeartbeatAge      string           `json:"heartbeat_age,omitempty"`
	HeartbeatGrace    string           `json:"heartbeat_grace,omitempty"`
	HeartbeatWriteErr string           `json:"heartbeat_persist_error,omitempty"`
	// SignalCandidate is true only when every gate for a later recover-stalled recover is
	// satisfied (suspect-heartbeat-overdue). plan-stalled never acts on it.
	SignalCandidate bool `json:"signal_candidate"`
	// MayReapUnheld is true for terminal-contradiction when the lease probe is
	// already unheld/missing — process-stop is not required.
	MayReapUnheld bool `json:"may_reap_unheld"`

	// Auth* is the exact plan-time record snapshot for fence CAS. Recover must
	// pass these unchanged into BeginStalledRecovery — never re-read them after
	// plan. Omitted from JSON (operator surface uses formatted fields above).
	AuthSnapshotOK            bool       `json:"-"`
	AuthState                 string     `json:"-"`
	AuthPID                   int        `json:"-"`
	AuthStartMS               uint64     `json:"-"`
	AuthBootID                string     `json:"-"`
	AuthTokenVersion          int        `json:"-"`
	AuthStartTicks            uint64     `json:"-"`
	AuthStartSec              int64      `json:"-"`
	AuthStartUsec             int64      `json:"-"`
	AuthFiletime              uint64     `json:"-"`
	AuthIndexSetID            string     `json:"-"`
	AuthLastHeartbeat         *time.Time `json:"-"`
	AuthHeartbeatPersistError string     `json:"-"`
	AuthRecoveryOwner         string     `json:"-"`
	AuthRecoveryIntent        string     `json:"-"`
}

// PlanManagedStalledRecovery classifies one job for stalled recovery without
// mutating registry or lease state. It is the plan-stalled microscope: typed outcomes
// only, zero signals.
func PlanManagedStalledRecovery(store *jobregistry.Store, jobID string, opts StalledPlanOptions) (StalledRecoveryPlan, error) {
	plan := StalledRecoveryPlan{JobID: strings.TrimSpace(jobID)}
	if store == nil {
		return plan, fmt.Errorf("job registry store is nil")
	}
	if plan.JobID == "" {
		return plan, fmt.Errorf("job_id is required")
	}

	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	grace := opts.HeartbeatGrace
	if grace <= 0 {
		grace = DefaultStalledHeartbeatGrace
	}
	if grace < MinStalledHeartbeatGrace {
		grace = MinStalledHeartbeatGrace
	}
	plan.HeartbeatGrace = grace.String()

	rec, err := store.GetReadOnlyStrict(plan.JobID)
	if err != nil {
		plan.Class = PlanInvalid
		plan.Detail = err.Error()
		return plan, nil
	}
	captureAuthSnapshot(&plan, rec)
	plan.JobState = string(rec.State)
	plan.JobType = rec.Type
	plan.PID = rec.PID
	plan.IndexSetID = strings.TrimSpace(rec.IndexSetID)
	plan.HeartbeatWriteErr = strings.TrimSpace(rec.HeartbeatPersistError)

	// Independent durable health authority: if the managed build could not
	// persist heartbeats and could not record that failure on job.json, a
	// sidecar marker makes age non-authorizing.
	if heartbeatUnhealthyMarkerPresent(store, plan.JobID) {
		plan.Class = PlanIndeterminate
		plan.Detail = "heartbeat unhealthy marker present; heartbeat age cannot authorize termination"
		return plan, nil
	}

	if rec.Type != "" && rec.Type != jobregistry.JobTypeIndexBuild {
		plan.Class = PlanInvalid
		plan.Detail = fmt.Sprintf("job type %q is not a managed index build", rec.Type)
		return plan, nil
	}

	switch rec.State {
	case jobregistry.JobStateRunning, jobregistry.JobStateStopping:
	default:
		plan.Class = PlanInvalid
		plan.Detail = fmt.Sprintf("job state %s is not a recovery target", rec.State)
		return plan, nil
	}

	recorded := jobregistry.ProcessIdentityFromRecord(rec)
	plan.RecordedIdentity = jobregistry.FormatProcessIdentity(recorded)

	// Typed liveness only (D-R7-02). Indeterminate/mismatch never become death authority.
	switch jobregistry.ClassifyProcess(recorded) {
	case jobregistry.LivenessGone:
		plan.ProcessAlive = false
		plan.Class = PlanTerminalContradiction
		plan.Detail = "running-state record whose process is proven gone; no signal — re-probe lease only"
		attachLease(&plan, opts.AuthorityRoot, plan.IndexSetID)
		if plan.LeaseVerdict == LeaseUnheld || plan.LeaseVerdict == LeaseMissing {
			plan.MayReapUnheld = true
		}
		return plan, nil
	case jobregistry.LivenessLiveMismatched:
		plan.ProcessAlive = true
		plan.Class = PlanIdentityMismatch
		plan.Detail = "live process does not match recorded birth identity (possible pid reuse)"
		return plan, nil
	case jobregistry.LivenessIndeterminate:
		plan.ProcessAlive = false
		plan.Class = PlanIndeterminate
		plan.Detail = "process liveness indeterminate; refuse death-side authority"
		return plan, nil
	default:
		plan.ProcessAlive = true
	}

	// Active recovery fence with a still-live process: resume candidate. Heartbeat
	// age is not authoritative here (fence acquisition refreshes LastHeartbeat).
	if rec.State == jobregistry.JobStateStopping && rec.RecoveryIntent == jobregistry.RecoveryIntentStalled {
		observed := jobregistry.ObserveProcessIdentity(rec.PID)
		plan.ObservedIdentity = jobregistry.FormatProcessIdentity(observed)
		if !recorded.Proven || !observed.Proven || !jobregistry.ProcessIdentityMatch(recorded, observed) {
			plan.Class = PlanIndeterminate
			plan.Detail = "recovery fence active but live process identity is unproven or mismatched"
			return plan, nil
		}
		if plan.IndexSetID == "" || strings.TrimSpace(opts.AuthorityRoot) == "" {
			plan.Class = PlanIndeterminate
			plan.Detail = "recovery fence active but lease correlation is incomplete"
			return plan, nil
		}
		lease, leaseErr := ProbeLease(opts.AuthorityRoot, plan.IndexSetID, nil)
		if leaseErr != nil && lease.Verdict == "" {
			plan.Class = PlanIndeterminate
			plan.Detail = fmt.Sprintf("lease probe infrastructure failure: %v", leaseErr)
			return plan, nil
		}
		plan.LeaseVerdict = lease.Verdict
		plan.LeaseHolder = lease.Holder
		switch lease.Verdict {
		case LeaseHeld:
			wantHolder := "index-build-" + plan.JobID
			holder := strings.TrimSpace(lease.Holder)
			if holder == "" {
				plan.Class = PlanIndeterminate
				plan.Detail = "active recovery fence with held lease but unreadable holder attribution; refuse signal"
				return plan, nil
			}
			if holder != wantHolder {
				plan.Class = PlanInvalid
				plan.Detail = fmt.Sprintf("active recovery fence but lease holder %q does not match job", holder)
				return plan, nil
			}
			plan.Class = PlanSuspectHeartbeatOverdue
			plan.Detail = "active recovery fence with live matching identity and held lease — resume candidate"
			plan.SignalCandidate = true
			return plan, nil
		case LeaseUnheld, LeaseMissing:
			plan.Class = PlanLeaseNotHeld
			plan.Detail = "active recovery fence but lease is not held — finalize without signal"
			return plan, nil
		case LeaseInvalid:
			plan.Class = PlanInvalid
			plan.Detail = "active recovery fence with invalid lease residue"
			return plan, nil
		default:
			plan.Class = PlanIndeterminate
			plan.Detail = fmt.Sprintf("unrecognized lease verdict %q under recovery fence", lease.Verdict)
			return plan, nil
		}
	}

	observed := jobregistry.ObserveProcessIdentity(rec.PID)
	plan.ObservedIdentity = jobregistry.FormatProcessIdentity(observed)
	if !recorded.Proven || !observed.Proven {
		plan.Class = PlanIndeterminate
		if !recorded.Proven {
			plan.Detail = "process birth identity missing from job record; refuse rather than signal by raw pid"
		} else {
			plan.Detail = "live process birth identity could not be proven on this platform"
		}
		return plan, nil
	}
	if !jobregistry.ProcessIdentityMatch(recorded, observed) {
		plan.Class = PlanIdentityMismatch
		plan.Detail = "live process does not match the recorded birth identity (possible pid reuse)"
		return plan, nil
	}

	if plan.HeartbeatWriteErr != "" {
		plan.Class = PlanIndeterminate
		plan.Detail = "heartbeat persistence has failed; heartbeat age cannot authorize termination"
		return plan, nil
	}

	hbAge, hbOK := heartbeatAge(rec, now)
	if hbOK {
		plan.HeartbeatAge = hbAge.String()
	}

	// Pre-authority phase: IndexSetID not yet persisted. A fresh heartbeat means
	// healthy; an overdue one without a lease correlation is indeterminate.
	if plan.IndexSetID == "" {
		if !hbOK || hbAge <= grace {
			plan.Class = PlanHealthy
			plan.Detail = "managed job is live with matching identity; index set not yet bound (pre-authority)"
			return plan, nil
		}
		plan.Class = PlanIndeterminate
		plan.Detail = "heartbeat overdue but index_set_id is not persisted; cannot correlate a held lease"
		return plan, nil
	}

	if strings.TrimSpace(opts.AuthorityRoot) == "" {
		plan.Class = PlanIndeterminate
		plan.Detail = "authority root not supplied; cannot probe the set-authority lease"
		return plan, nil
	}

	lease, leaseErr := ProbeLease(opts.AuthorityRoot, plan.IndexSetID, nil)
	if leaseErr != nil && lease.Verdict == "" {
		plan.Class = PlanIndeterminate
		plan.Detail = fmt.Sprintf("lease probe infrastructure failure: %v", leaseErr)
		return plan, nil
	}
	plan.LeaseVerdict = lease.Verdict
	plan.LeaseHolder = lease.Holder

	// Exact holder correlation: managed holders are index-build-<jobID>.
	// Unreadable holder text on a held lease cannot authorize stop (fail closed).
	wantHolder := "index-build-" + plan.JobID
	holder := strings.TrimSpace(lease.Holder)

	switch lease.Verdict {
	case LeaseHeld:
		if holder == "" {
			plan.Class = PlanIndeterminate
			plan.Detail = "lease is held but holder attribution is unreadable; refuse signal authorization"
			return plan, nil
		}
		if holder != wantHolder {
			if strings.HasPrefix(holder, "index-build-") {
				plan.Class = PlanInvalid
				plan.Detail = fmt.Sprintf("lease holder %q does not match job %s", holder, plan.JobID)
				return plan, nil
			}
			plan.Class = PlanForegroundOrUnmatched
			plan.Detail = "lease holder is not a durable managed job identity"
			return plan, nil
		}
		// continue
	case LeaseUnheld, LeaseMissing:
		plan.Class = PlanLeaseNotHeld
		plan.Detail = "process is live and matched but the set-authority lease is not held"
		return plan, nil
	case LeaseInvalid:
		plan.Class = PlanInvalid
		plan.Detail = "set-authority lease is invalid residue; preserve and report"
		if leaseErr != nil {
			plan.Detail = plan.Detail + ": " + leaseErr.Error()
		}
		return plan, nil
	default:
		plan.Class = PlanIndeterminate
		plan.Detail = fmt.Sprintf("unrecognized lease verdict %q", lease.Verdict)
		return plan, nil
	}

	if !hbOK {
		plan.Class = PlanIndeterminate
		plan.Detail = "job record has no last_heartbeat; cannot judge stall"
		return plan, nil
	}
	if hbAge <= grace {
		plan.Class = PlanHealthy
		plan.Detail = "managed job is live under matching identity with a held lease and fresh heartbeat"
		return plan, nil
	}

	plan.Class = PlanSuspectHeartbeatOverdue
	plan.Detail = "live matching identity, held lease, heartbeat past grace — candidate for confirm-gated recover (no signal in plan)"
	plan.SignalCandidate = true
	return plan, nil
}

func attachLease(plan *StalledRecoveryPlan, authorityRoot, indexSetID string) {
	if plan == nil || strings.TrimSpace(authorityRoot) == "" || strings.TrimSpace(indexSetID) == "" {
		return
	}
	lease, err := ProbeLease(authorityRoot, indexSetID, nil)
	if err != nil && lease.Verdict == "" {
		return
	}
	plan.LeaseVerdict = lease.Verdict
	plan.LeaseHolder = lease.Holder
}

func heartbeatAge(rec *jobregistry.JobRecord, now time.Time) (time.Duration, bool) {
	if rec == nil || rec.LastHeartbeat == nil || rec.LastHeartbeat.IsZero() {
		return 0, false
	}
	age := now.Sub(rec.LastHeartbeat.UTC())
	if age < 0 {
		age = 0
	}
	return age, true
}

// heartbeatUnhealthyMarker is the independent durable authority written when
// both TouchHeartbeat and RecordHeartbeatPersistError fail (see cmd heartbeat).
const heartbeatUnhealthyMarker = "heartbeat_unhealthy"

func heartbeatUnhealthyMarkerPresent(store *jobregistry.Store, jobID string) bool {
	if store == nil || strings.TrimSpace(jobID) == "" {
		return false
	}
	// JobDir is public on Store.
	_, err := os.Stat(filepath.Join(store.JobDir(jobID), heartbeatUnhealthyMarker))
	return err == nil
}

// captureAuthSnapshot freezes plan-time authorization fields for the act-path CAS.
func captureAuthSnapshot(plan *StalledRecoveryPlan, rec *jobregistry.JobRecord) {
	if plan == nil || rec == nil {
		return
	}
	plan.AuthSnapshotOK = true
	plan.AuthState = string(rec.State)
	plan.AuthPID = rec.PID
	if rec.ProcessStartTimeUnixMS != nil {
		plan.AuthStartMS = *rec.ProcessStartTimeUnixMS
	}
	plan.AuthBootID = rec.ProcessBootID
	plan.AuthTokenVersion = rec.ProcessTokenVersion
	plan.AuthStartTicks = rec.ProcessStartTicks
	plan.AuthStartSec = rec.ProcessStartSec
	plan.AuthStartUsec = rec.ProcessStartUsec
	plan.AuthFiletime = rec.ProcessFiletime
	plan.AuthIndexSetID = strings.TrimSpace(rec.IndexSetID)
	if rec.LastHeartbeat != nil {
		hb := rec.LastHeartbeat.UTC()
		plan.AuthLastHeartbeat = &hb
	}
	plan.AuthHeartbeatPersistError = rec.HeartbeatPersistError
	plan.AuthRecoveryOwner = rec.RecoveryOwner
	plan.AuthRecoveryIntent = rec.RecoveryIntent
}
