package indexcoord

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/3leaps/gonimbus/internal/procidentity"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

// RecoverStalledOutcome is a distinct non-boolean result class for mutation.
type RecoverStalledOutcome string

const (
	OutcomeDryRun         RecoverStalledOutcome = "dry-run"
	OutcomeRefused        RecoverStalledOutcome = "refused"
	OutcomeSignalled      RecoverStalledOutcome = "signalled-stopped"
	OutcomeReapedOnly     RecoverStalledOutcome = "reaped-unheld"
	OutcomeAlreadyStopped RecoverStalledOutcome = "already-stopped"
	OutcomeRecoveryFailed RecoverStalledOutcome = "recovery-failed"
	OutcomeLeaseStillHeld RecoverStalledOutcome = "lease-still-held"
	OutcomeInvalidResidue RecoverStalledOutcome = "invalid-residue"
	OutcomeNoop           RecoverStalledOutcome = "noop"
)

// RecoverStalledOptions configures a single-job recover-stalled mutation.
type RecoverStalledOptions struct {
	AuthorityRoot  string
	Confirm        bool // false = dry-run (byte-preserving plan only)
	WaitTimeout    time.Duration
	PollInterval   time.Duration
	HeartbeatGrace time.Duration
	// Now is ignored when Confirm is true. Mutation authorization always uses
	// the library's production clock so callers cannot forge stall age. Dry-run
	// / plan-only may still use Now for inspection (via PlanManagedStalledRecovery).
	Now time.Time
	// Owner is optional; empty mints a new recovery owner id.
	Owner string
	// Deadline, when non-zero, is the absolute monotonic attempt deadline
	// (D-R13-03). Zero means derive from WaitTimeout at attempt entry.
	// Tests may set an already-expired deadline to prove refuse paths.
	Deadline time.Time
}

// RecoverStalledResult is the library result of recover-stalled.
type RecoverStalledResult struct {
	JobID      string                `json:"job_id"`
	Outcome    RecoverStalledOutcome `json:"outcome"`
	Detail     string                `json:"detail,omitempty"`
	Plan       StalledRecoveryPlan   `json:"plan"`
	DryRun     bool                  `json:"dry_run"`
	Owner      string                `json:"recovery_owner,omitempty"`
	Signalled  bool                  `json:"signalled"`
	ForcedKill bool                  `json:"forced_kill"`
	Reclaimed  bool                  `json:"reclaimed"`
	JobState   string                `json:"job_state,omitempty"`
	LeaseAfter LeaseVerdict          `json:"lease_verdict_after,omitempty"`
}

// RecoverManagedStalled is the recover-stalled mutation: single managed job, confirm-gated,
// identity-bound signal, observe death, then W2 unheld reclaim. It never calls
// Store.Stop as the authority and never unlinks a held lease.
func RecoverManagedStalled(store *jobregistry.Store, jobID string, opts RecoverStalledOptions) (RecoverStalledResult, error) {
	res := RecoverStalledResult{JobID: strings.TrimSpace(jobID), DryRun: !opts.Confirm}
	if store == nil {
		return res, fmt.Errorf("job registry store is nil")
	}
	if res.JobID == "" {
		return res, fmt.Errorf("job_id is required")
	}

	// Dry-run may use caller Now for inspection. Mutation never does.
	planNow := opts.Now
	if opts.Confirm {
		planNow = time.Time{} // force Plan to use production clock
	}
	plan, err := PlanManagedStalledRecovery(store, res.JobID, StalledPlanOptions{
		AuthorityRoot:  opts.AuthorityRoot,
		Now:            planNow,
		HeartbeatGrace: opts.HeartbeatGrace,
	})
	if err != nil {
		return res, err
	}
	res.Plan = plan
	res.JobState = plan.JobState

	if !opts.Confirm {
		res.Outcome = OutcomeDryRun
		res.Detail = "dry-run: plan only; no signal, no fence, no reclaim"
		return res, nil
	}

	// Conservative second observation with production clock before any mutation.
	// Production floor is one managed heartbeat cadence; tests may use a shorter
	// delay only via testing.Testing() (production binaries always wait the floor).
	time.Sleep(secondObservationDelay())
	plan2, err := PlanManagedStalledRecovery(store, res.JobID, StalledPlanOptions{
		AuthorityRoot:  opts.AuthorityRoot,
		HeartbeatGrace: opts.HeartbeatGrace,
		// Now intentionally zero → production clock
	})
	if err != nil {
		return res, err
	}
	if plan.SignalCandidate != plan2.SignalCandidate ||
		plan.Class != plan2.Class ||
		!authHeartbeatEqual(plan.AuthLastHeartbeat, plan2.AuthLastHeartbeat) ||
		plan.AuthHeartbeatPersistError != plan2.AuthHeartbeatPersistError {
		res.Outcome = OutcomeRefused
		res.Detail = "second observation disagreed with first plan; refuse mutation"
		res.Signalled = false
		res.Plan = plan2
		return res, nil
	}
	// Act on the later production-clock plan snapshot.
	plan = plan2
	res.Plan = plan
	res.JobState = plan.JobState

	// Already terminal with no work.
	if plan.Class == PlanInvalid && strings.Contains(plan.Detail, "not a recovery target") {
		rec, getErr := store.GetReadOnlyStrict(res.JobID)
		if getErr == nil && rec.State == jobregistry.JobStateStopped {
			res.Outcome = OutcomeAlreadyStopped
			res.Detail = "job already stopped"
			res.JobState = string(rec.State)
			return res, nil
		}
		res.Outcome = OutcomeRefused
		res.Detail = plan.Detail
		return res, nil
	}

	// Durable crash-resume: if a fence is already active, bind to its owner
	// before any new owner minting. The planner classifies active fences as
	// suspect-heartbeat-overdue (resume candidate) or lease-not-held (finalize).
	if rec, getErr := store.GetReadOnlyStrict(res.JobID); getErr == nil {
		if rec.RecoveryIntent == jobregistry.RecoveryIntentStalled && rec.State == jobregistry.JobStateStopping {
			opts.Owner = rec.RecoveryOwner
			return recoverResumeOrSignal(store, res, plan, opts)
		}
	}

	switch plan.Class {
	case PlanHealthy, PlanIdentityMismatch, PlanForegroundOrUnmatched, PlanLeaseNotHeld, PlanIndeterminate, PlanInvalid:
		res.Outcome = OutcomeRefused
		res.Detail = plan.Detail
		if res.Detail == "" {
			res.Detail = string(plan.Class)
		}
		return res, nil
	case PlanTerminalContradiction:
		return recoverTerminalContradiction(store, res, plan, opts)
	case PlanSuspectHeartbeatOverdue:
		if !plan.SignalCandidate {
			res.Outcome = OutcomeRefused
			res.Detail = "plan is not a signal candidate"
			return res, nil
		}
		return recoverSignalCandidate(store, res, plan, opts)
	default:
		res.Outcome = OutcomeRefused
		res.Detail = fmt.Sprintf("unhandled plan class %s", plan.Class)
		return res, nil
	}
}

func recoverTerminalContradiction(store *jobregistry.Store, res RecoverStalledResult, plan StalledRecoveryPlan, opts RecoverStalledOptions) (RecoverStalledResult, error) {
	// Concurrent late callers may still hold a terminal-contradiction plan snapshot
	// after another recoverer already finalized. Observe live state first.
	if rec, getErr := store.GetReadOnlyStrict(res.JobID); getErr == nil && rec.State == jobregistry.JobStateStopped {
		res.Outcome = OutcomeAlreadyStopped
		res.Detail = "job already stopped"
		res.JobState = string(rec.State)
		return res, nil
	}
	// No signal. Only reclaim when already unheld/missing via W2.
	if !plan.MayReapUnheld {
		// Held or invalid while process is dead: preserve / report.
		if plan.LeaseVerdict == LeaseHeld {
			res.Outcome = OutcomeLeaseStillHeld
			res.Detail = "process is dead but lease probe is still held; refuse unlink (possible other holder)"
			return res, nil
		}
		if plan.LeaseVerdict == LeaseInvalid {
			res.Outcome = OutcomeInvalidResidue
			res.Detail = "process is dead and lease residue is invalid; preserve for operator recovery"
			return res, nil
		}
		res.Outcome = OutcomeRefused
		res.Detail = "terminal contradiction without a reclaimable unheld lease"
		return res, nil
	}
	reclaimed, verdict, err := reclaimIfUnheld(opts.AuthorityRoot, plan.IndexSetID)
	if err != nil {
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = err.Error()
		return res, nil
	}
	res.Reclaimed = reclaimed
	res.LeaseAfter = verdict
	// Best-effort: mark job stopped if still running/stopping/unknown.
	if rec, getErr := store.GetReadOnlyStrict(res.JobID); getErr == nil {
		switch rec.State {
		case jobregistry.JobStateRunning, jobregistry.JobStateStopping, jobregistry.JobStateUnknown:
			now := time.Now().UTC()
			rec.State = jobregistry.JobStateStopped
			rec.EndedAt = &now
			rec.RecoveryIntent = ""
			rec.RecoveryOwner = ""
			rec.RecoveryStartedAt = nil
			if rec.Metadata == nil {
				rec.Metadata = map[string]string{}
			}
			rec.Metadata["stalled_recovery"] = "terminal-contradiction-reaped"
			if werr := store.Write(rec); werr != nil {
				res.Outcome = OutcomeRecoveryFailed
				res.Detail = fmt.Sprintf("reclaim ok but job finalize failed: %v", werr)
				return res, nil
			}
			res.JobState = string(jobregistry.JobStateStopped)
		case jobregistry.JobStateStopped:
			// Another recoverer won the finalize race after our lease probe.
			res.Outcome = OutcomeAlreadyStopped
			res.Detail = "job already stopped"
			res.JobState = string(rec.State)
			return res, nil
		default:
			res.JobState = string(rec.State)
		}
	}
	res.Outcome = OutcomeReapedOnly
	res.Detail = "terminal contradiction: no signal; unheld lease reclaimed via existing W2 path"
	return res, nil
}

func recoverResumeOrSignal(store *jobregistry.Store, res RecoverStalledResult, plan StalledRecoveryPlan, opts RecoverStalledOptions) (RecoverStalledResult, error) {
	rec, err := store.GetReadOnlyStrict(res.JobID)
	if err != nil {
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = err.Error()
		return res, nil
	}
	if rec.RecoveryIntent == jobregistry.RecoveryIntentStalled && rec.State == jobregistry.JobStateStopping {
		// Resume: bind to durable owner; never mint a competing UUID.
		opts.Owner = rec.RecoveryOwner
		return recoverSignalCandidate(store, res, plan, opts)
	}
	res.Outcome = OutcomeRefused
	res.Detail = plan.Detail
	return res, nil
}

func recoverSignalCandidate(store *jobregistry.Store, res RecoverStalledResult, plan StalledRecoveryPlan, opts RecoverStalledOptions) (RecoverStalledResult, error) {
	// D-R13-03: establish absolute attempt deadline at entry (covers fence/bind/signal).
	waitTimeout := opts.WaitTimeout
	if waitTimeout <= 0 {
		waitTimeout = 30 * time.Second
	}
	if opts.Deadline.IsZero() {
		opts.Deadline = time.Now().Add(waitTimeout)
	}
	if !deadlineLive(opts.Deadline) {
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = "recovery deadline already expired at attempt entry"
		return res, nil
	}

	// Plan-time auth snapshot is mandatory for first fence CAS. Resume under an
	// active fence uses plan.AuthRecoveryOwner / already-owned Begin path.
	if !plan.AuthSnapshotOK {
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = "plan lacks authorization snapshot"
		return res, nil
	}
	if plan.AuthStartMS == 0 {
		res.Outcome = OutcomeRefused
		res.Detail = "process birth identity missing; refuse raw-pid stop"
		return res, nil
	}

	// Durable fence owner: prefer existing fence, then opts, then mint once.
	fenceOwner := strings.TrimSpace(opts.Owner)
	if plan.AuthRecoveryIntent == jobregistry.RecoveryIntentStalled && strings.TrimSpace(plan.AuthRecoveryOwner) != "" {
		fenceOwner = plan.AuthRecoveryOwner
	}
	if fenceOwner == "" {
		// Live record may already be fenced even if plan snapshot was pre-fence.
		if rec, err := store.GetReadOnlyStrict(res.JobID); err == nil {
			if rec.RecoveryIntent == jobregistry.RecoveryIntentStalled && rec.RecoveryOwner != "" {
				fenceOwner = rec.RecoveryOwner
			}
		}
	}
	if fenceOwner == "" {
		fenceOwner = uuid.NewString()
	}
	// Unique per invocation — concurrent resume cannot share signal phase.
	attemptID := uuid.NewString()
	res.Owner = fenceOwner

	// D-R11-04: platform + token destructive-capability preflight before any fence.
	// Resume under an already-active fence skips this only when mutation already occurred;
	// first fence and Darwin/legacy paths must refuse with byte-identical job state.
	if plan.AuthRecoveryIntent != jobregistry.RecoveryIntentStalled {
		if refuse, detail := destructiveRecoveryPreflight(store, res.JobID, plan); refuse {
			res.Outcome = OutcomeRefused
			res.Detail = detail
			res.Signalled = false
			return res, nil
		}
	}

	// CAS uses plan snapshot only (not a second read of heartbeat/identity).
	fence, err := store.BeginStalledRecovery(res.JobID, jobregistry.BeginStalledRecoveryOptions{
		Owner:                         fenceOwner,
		ExpectedPID:                   plan.AuthPID,
		ExpectedStartMS:               plan.AuthStartMS,
		ExpectedBootID:                plan.AuthBootID,
		ExpectedTokenVersion:          plan.AuthTokenVersion,
		ExpectedStartTicks:            plan.AuthStartTicks,
		ExpectedStartSec:              plan.AuthStartSec,
		ExpectedStartUsec:             plan.AuthStartUsec,
		ExpectedFiletime:              plan.AuthFiletime,
		ExpectedIndexSetID:            plan.AuthIndexSetID,
		MatchPlanSnapshot:             true, // required for new fence; ignored on already-owned resume
		ExpectedLastHeartbeat:         plan.AuthLastHeartbeat,
		ExpectedHeartbeatPersistError: plan.AuthHeartbeatPersistError,
		ExpectedAuthorityRoot:         opts.AuthorityRoot,
	})
	if err != nil {
		if errors.Is(err, jobregistry.ErrRecoveryAlreadyActive) ||
			errors.Is(err, jobregistry.ErrRecoveryPlanSnapshotMismatch) ||
			errors.Is(err, jobregistry.ErrRecoverySnapshotRequired) {
			res.Outcome = OutcomeRefused
			res.Detail = err.Error()
			res.Signalled = false
			return res, nil
		}
		// Already-owned path: Begin with MatchPlanSnapshot true fails on stopping
		// state before owner check... Actually already-owned is checked first.
		// New fence without running fails ErrJobNotRunning.
		if errors.Is(err, jobregistry.ErrJobNotRunning) && plan.AuthState == string(jobregistry.JobStateStopping) {
			// Retry Begin after binding fence owner from live record for pure resume.
			if rec, rerr := store.GetReadOnlyStrict(res.JobID); rerr == nil &&
				rec.RecoveryIntent == jobregistry.RecoveryIntentStalled && rec.RecoveryOwner != "" {
				fenceOwner = rec.RecoveryOwner
				res.Owner = fenceOwner
				fence, err = store.BeginStalledRecovery(res.JobID, jobregistry.BeginStalledRecoveryOptions{
					Owner:                         fenceOwner,
					ExpectedPID:                   plan.AuthPID,
					ExpectedStartMS:               plan.AuthStartMS,
					ExpectedBootID:                plan.AuthBootID,
					ExpectedTokenVersion:          plan.AuthTokenVersion,
					ExpectedStartTicks:            plan.AuthStartTicks,
					ExpectedStartSec:              plan.AuthStartSec,
					ExpectedStartUsec:             plan.AuthStartUsec,
					ExpectedFiletime:              plan.AuthFiletime,
					ExpectedIndexSetID:            plan.AuthIndexSetID,
					MatchPlanSnapshot:             true,
					ExpectedLastHeartbeat:         plan.AuthLastHeartbeat,
					ExpectedHeartbeatPersistError: plan.AuthHeartbeatPersistError,
					ExpectedAuthorityRoot:         opts.AuthorityRoot,
				})
			}
		}
		if err != nil {
			if errors.Is(err, jobregistry.ErrRecoveryAlreadyActive) ||
				errors.Is(err, jobregistry.ErrRecoveryPlanSnapshotMismatch) {
				res.Outcome = OutcomeRefused
				res.Detail = err.Error()
				res.Signalled = false
				return res, nil
			}
			// Concurrent winner already finalized → job stopped / no fence.
			if errors.Is(err, jobregistry.ErrJobNotRunning) {
				if rec, rerr := store.GetReadOnlyStrict(res.JobID); rerr == nil && rec.State == jobregistry.JobStateStopped {
					res.Outcome = OutcomeAlreadyStopped
					res.Detail = "job already stopped"
					res.JobState = string(rec.State)
					res.Signalled = false
					return res, nil
				}
				res.Outcome = OutcomeRefused
				res.Detail = err.Error()
				res.Signalled = false
				return res, nil
			}
			if o, d, ok := contentionOutcome(store, res.JobID, err, "acquire recovery fence: "); ok {
				res.Outcome, res.Detail = o, d
				res.Signalled = false
				return res, nil
			}
			res.Outcome = OutcomeRecoveryFailed
			res.Detail = fmt.Sprintf("acquire recovery fence: %v", err)
			return res, nil
		}
	}
	fenceOwner = fence.Owner
	res.Owner = fenceOwner
	// D-R16-02: active fence must already carry durable authority root (no repair).
	if err := store.CheckRecoveryAuthorityRoot(res.JobID, fenceOwner, opts.AuthorityRoot); err != nil {
		res.Outcome = OutcomeRefused
		res.Detail = err.Error()
		res.Signalled = false
		return res, nil
	}

	// Exclusive signal phase. Concurrent live claimers are busy. Orphan claimer
	// (claimer process typed Gone) may be replaced by store-verified takeover.
	// A dead managed target alone does not authorize displacing a live claimer.
	if err := store.ClaimStalledRecoverySignal(res.JobID, fenceOwner, attemptID); err != nil {
		if errors.Is(err, jobregistry.ErrRecoverySignalBusy) {
			// Store-verified takeover (claimer gone OR managed process gone).
			if err2 := store.ClaimStalledRecoverySignalVerifiedTakeover(res.JobID, fenceOwner, attemptID); err2 != nil {
				res.Outcome = OutcomeRefused
				res.Detail = err.Error()
				res.Signalled = false
				return res, nil
			}
			recAfter, rerr := store.GetReadOnlyStrict(res.JobID)
			if rerr != nil {
				_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, rerr.Error())
				res.Outcome = OutcomeRecoveryFailed
				res.Detail = rerr.Error()
				return res, nil
			}
			// Dead managed target: only typed Gone (not LiveMismatched/indeterminate).
			recordedAfter := jobregistry.ProcessIdentityFromRecord(recAfter)
			if jobregistry.ClassifyProcess(recordedAfter) == jobregistry.LivenessGone {
				return finishAfterDeath(store, res, recAfter, fenceOwner, attemptID, opts, false)
			}
			// Live or indeterminate: continue only if LiveMatching; mismatch/indet refuse.
			switch jobregistry.ClassifyProcess(recordedAfter) {
			case jobregistry.LivenessLiveMatching:
				// fall through to signal path
			default:
				_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, "managed process not live-matching after takeover")
				res.Outcome = OutcomeRefused
				res.Detail = "typed liveness is not live-matching after claim takeover; refuse signal"
				res.Signalled = false
				return res, nil
			}
		} else {
			// Winner already finalized (no active fence) is ordinary contention.
			if o, d, ok := contentionOutcome(store, res.JobID, err, "claim signal phase: "); ok {
				res.Outcome, res.Detail = o, d
				res.Signalled = false
				return res, nil
			}
			res.Outcome = OutcomeRecoveryFailed
			res.Detail = fmt.Sprintf("claim signal phase: %v", err)
			return res, nil
		}
	}
	// E4 test-only: durable exclusive claim acquired (hook nil in production).
	evidencePark("claimed")

	// Re-validate plan after fence. Authorization is enforced, not diagnostic.
	replan, err := PlanManagedStalledRecovery(store, res.JobID, StalledPlanOptions{
		AuthorityRoot:  opts.AuthorityRoot,
		Now:            opts.Now,
		HeartbeatGrace: opts.HeartbeatGrace,
	})
	if err != nil {
		_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, err.Error())
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = err.Error()
		return res, nil
	}
	res.Plan = replan

	rec2, err := store.GetReadOnlyStrict(res.JobID)
	if err != nil {
		_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, err.Error())
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = err.Error()
		return res, nil
	}
	if rec2.RecoveryOwner != fenceOwner || rec2.RecoveryIntent != jobregistry.RecoveryIntentStalled {
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = "recovery fence lost after acquire"
		return res, nil
	}
	if rec2.RecoverySignalOwner != attemptID {
		res.Outcome = OutcomeRefused
		res.Detail = "signal phase claim lost"
		res.Signalled = false
		return res, nil
	}

	// Hybrid resume: branch on durable phase before transport (E-R8-01 / E-R9).
	gen := rec2.RecoveryGeneration
	phase := rec2.RecoveryPhase
	switch phase {
	case jobregistry.RecoveryPhaseLeaseReconciled:
		// E-R9-02 / D-R11-01: finalize-only from durable receipt (provenance preserved).
		return finalizeFromReceipt(store, res, rec2, fenceOwner, attemptID, gen)
	case jobregistry.RecoveryPhaseDeathObserved:
		return finishAfterDeath(store, res, rec2, fenceOwner, attemptID, opts, false)
	case jobregistry.RecoveryPhaseFinalized:
		res.Outcome = OutcomeAlreadyStopped
		res.Detail = "recovery already finalized"
		return res, nil
	}

	// D-R13-02: complete bound snapshot required for transport/replay (fail closed).
	if !jobregistry.BoundTargetMatches(rec2) {
		_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, "bound target snapshot missing or mismatched")
		res.Outcome = OutcomeRefused
		res.Detail = "bound recovery target snapshot missing or mismatched; refuse transport"
		res.Signalled = false
		return res, nil
	}

	recorded := jobregistry.ProcessIdentityFromRecord(rec2)
	switch jobregistry.ClassifyProcess(recorded) {
	case jobregistry.LivenessGone:
		return finishAfterDeath(store, res, rec2, fenceOwner, attemptID, opts, false)
	case jobregistry.LivenessLiveMismatched:
		_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, "identity mismatch under fence")
		res.Outcome = OutcomeRefused
		res.Detail = "live process identity mismatch under fence; refuse death-side path without stable-handle proof"
		res.Signalled = false
		return res, nil
	case jobregistry.LivenessIndeterminate:
		_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, "liveness indeterminate under fence")
		res.Outcome = OutcomeRefused
		res.Detail = "process liveness indeterminate under fence; refuse signal and death-side path"
		res.Signalled = false
		return res, nil
	}
	if !recorded.Proven {
		_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, "birth identity missing under fence")
		res.Outcome = OutcomeRefused
		res.Detail = "birth identity missing under fence"
		return res, nil
	}

	if !replan.SignalCandidate && replan.Class != PlanTerminalContradiction {
		_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, "post-fence plan is not a signal candidate: "+string(replan.Class))
		res.Outcome = OutcomeRefused
		res.Detail = "post-fence plan is not a signal candidate: " + replan.Detail
		res.Signalled = false
		return res, nil
	}

	lease, leaseErr := ProbeLease(opts.AuthorityRoot, strings.TrimSpace(rec2.IndexSetID), nil)
	if leaseErr != nil && lease.Verdict == "" {
		_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, leaseErr.Error())
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = leaseErr.Error()
		return res, nil
	}
	if lease.Verdict == LeaseInvalid {
		res.Outcome = OutcomeInvalidResidue
		res.Detail = "lease invalid under fence; preserve residue; no signal"
		return res, nil
	}
	if lease.Verdict != LeaseHeld {
		// Live process + lease not held: refuse signal (including resume).
		_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, "lease not held under fence")
		res.Outcome = OutcomeRefused
		res.Detail = "lease not held under fence; refuse signal"
		res.Signalled = false
		return res, nil
	}

	holder := strings.TrimSpace(lease.Holder)
	wantHolder := "index-build-" + res.JobID
	if holder == "" || holder != wantHolder {
		_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, "lease holder missing or mismatch under fence")
		res.Outcome = OutcomeRefused
		res.Detail = fmt.Sprintf("lease holder %q does not authorize job under fence (want %q)", holder, wantHolder)
		res.Signalled = false
		return res, nil
	}

	from := phase
	// Early phases may bind; advanced phases keep durable phase for hybrid replay.
	switch phase {
	case "", jobregistry.RecoveryPhaseFenced, jobregistry.RecoveryPhaseClaimed:
		if err := store.AdvanceRecoveryPhase(res.JobID, jobregistry.AdvanceRecoveryPhaseOptions{
			FenceOwner: fenceOwner, AttemptID: attemptID, ExpectedGeneration: gen,
			FromPhase: jobregistry.RecoveryPhaseClaimed, ToPhase: jobregistry.RecoveryPhaseBound,
		}); err != nil {
			if phase != jobregistry.RecoveryPhaseBound {
				if o, d, ok := contentionOutcome(store, res.JobID, err, "phase bound: "); ok {
					res.Outcome, res.Detail = o, d
					res.Signalled = false
					return res, nil
				}
				_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, err.Error())
				res.Outcome = OutcomeRecoveryFailed
				res.Detail = fmt.Sprintf("phase bound: %v", err)
				return res, nil
			}
		}
		from = jobregistry.RecoveryPhaseBound
	case jobregistry.RecoveryPhaseBound,
		jobregistry.RecoveryPhaseTermIntent, jobregistry.RecoveryPhaseTermSent,
		jobregistry.RecoveryPhaseKillIntent, jobregistry.RecoveryPhaseKillSent:
		from = phase
	default:
		_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, "unhandled recovery phase: "+phase)
		res.Outcome = OutcomeRefused
		res.Detail = "unhandled recovery phase for resume: " + phase
		return res, nil
	}

	// D-R16-01: session owns Bind + deadline; no raw Target escape.
	deadline := attemptDeadline(opts, waitTimeout)
	sess, serr := store.OpenSignalSession(res.JobID, fenceOwner, attemptID, gen, deadline)
	if serr != nil {
		_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, serr.Error())
		res.Outcome = OutcomeRefused
		res.Detail = serr.Error()
		res.Signalled = false
		return res, nil
	}
	defer func() { _ = sess.Close() }()

	remain := func() time.Duration {
		d := time.Until(deadline)
		if d < 0 {
			return 0
		}
		return d
	}
	requireLiveDeadline := func(op string) error {
		if !deadlineLive(deadline) {
			return fmt.Errorf("recovery deadline expired before %s", op)
		}
		return nil
	}
	advance := func(to string) error {
		err := store.AdvanceRecoveryPhase(res.JobID, jobregistry.AdvanceRecoveryPhaseOptions{
			FenceOwner: fenceOwner, AttemptID: attemptID, ExpectedGeneration: gen,
			FromPhase: from, ToPhase: to,
		})
		if err == nil {
			from = to
		}
		return err
	}
	failDeadline := func(op string) (RecoverStalledResult, error) {
		_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, "deadline expired: "+op)
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = "recovery deadline expired before " + op + "; no post-deadline transport"
		return res, nil
	}
	handleDeliverErr := func(err error, op string) (RecoverStalledResult, error) {
		if err == nil {
			return res, nil
		}
		if errors.Is(err, procidentity.ErrIdentityLost) || errors.Is(err, procidentity.ErrAlreadyGone) {
			return finishAfterIdentityLostSession(store, res, rec2, fenceOwner, attemptID, opts, sess)
		}
		if errors.Is(err, jobregistry.ErrDeliveryPersistFailed) {
			res.Signalled = true
			if strings.Contains(op, "kill") {
				res.ForcedKill = true
			}
			res.Outcome = OutcomeRecoveryFailed
			res.Detail = err.Error()
			return res, nil
		}
		// Deadline expiry mid-deliver: refuse transport side-effects already handled.
		if strings.Contains(err.Error(), "deadline expired") {
			return failDeadline(op)
		}
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = fmt.Sprintf("%s: %v", op, err)
		return res, nil
	}

	// Hybrid: if already at term-sent/kill-sent, observe death first; replay if still live.
	if phase == jobregistry.RecoveryPhaseTermSent || phase == jobregistry.RecoveryPhaseKillSent {
		done, terr := sess.WaitTerminated(remain(), opts.PollInterval)
		if terr != nil {
			res.Outcome = OutcomeRecoveryFailed
			res.Detail = terr.Error()
			return res, nil
		}
		if done {
			if err := advance(jobregistry.RecoveryPhaseDeathObserved); err != nil {
				res.Outcome = OutcomeRecoveryFailed
				res.Detail = err.Error()
				return res, nil
			}
			return finishAfterDeath(store, res, rec2, fenceOwner, attemptID, opts, phase == jobregistry.RecoveryPhaseKillSent)
		}
		// Still live: E-R9-01 — explicit same-instance kill replay for kill-sent;
		// term-sent escalates to kill path below.
		if phase == jobregistry.RecoveryPhaseKillSent {
			if err := requireLiveDeadline("kill replay"); err != nil {
				return failDeadline("kill replay")
			}
			if err := sess.DeliverKill(); err != nil {
				return handleDeliverErr(err, "kill replay")
			}
			res.Signalled = true
			res.ForcedKill = true
			from = jobregistry.RecoveryPhaseKillSent
			// Stay at kill-sent (idempotent phase); wait again.
			done, err := sess.WaitTerminated(remain(), opts.PollInterval)
			if err != nil {
				res.Outcome = OutcomeRecoveryFailed
				res.Detail = err.Error()
				return res, nil
			}
			if !done {
				res.Outcome = OutcomeRecoveryFailed
				res.Detail = "termination not observed after kill replay; not claiming stopped"
				return res, nil
			}
			if err := advance(jobregistry.RecoveryPhaseDeathObserved); err != nil {
				res.Outcome = OutcomeRecoveryFailed
				res.Detail = err.Error()
				return res, nil
			}
			return finishAfterDeath(store, res, rec2, fenceOwner, attemptID, opts, true)
		}
	}

	// S1/S2/D-R8-03: delivery only through session-owned target (D-R15-01).
	if sess.HardStopOnly() || phase == jobregistry.RecoveryPhaseKillIntent {
		if err := requireLiveDeadline("hard stop"); err != nil {
			return failDeadline("hard stop")
		}
		if err := sess.DeliverKill(); err != nil {
			return handleDeliverErr(err, "hard stop")
		}
		res.Signalled = true
		res.ForcedKill = true
		from = jobregistry.RecoveryPhaseKillSent
		done, err := sess.WaitTerminated(remain(), opts.PollInterval)
		if err != nil {
			res.Outcome = OutcomeRecoveryFailed
			res.Detail = err.Error()
			return res, nil
		}
		if !done {
			res.Outcome = OutcomeRecoveryFailed
			res.Detail = "termination not observed after hard stop; not claiming stopped; not reclaiming"
			res.ForcedKill = true
			return res, nil
		}
		if err := advance(jobregistry.RecoveryPhaseDeathObserved); err != nil {
			res.Outcome = OutcomeRecoveryFailed
			res.Detail = err.Error()
			return res, nil
		}
		return finishAfterDeath(store, res, rec2, fenceOwner, attemptID, opts, true)
	}

	// Graceful TERM path (Unix) via SignalSession.
	if phase != jobregistry.RecoveryPhaseTermSent && phase != jobregistry.RecoveryPhaseKillIntent && phase != jobregistry.RecoveryPhaseKillSent {
		if err := requireLiveDeadline("signal term"); err != nil {
			return failDeadline("signal term")
		}
		if err := sess.DeliverTerm(); err != nil {
			return handleDeliverErr(err, "signal term")
		}
		res.Signalled = true
		from = jobregistry.RecoveryPhaseTermSent
	}
	done, err := sess.WaitTerminated(remain(), opts.PollInterval)
	if err != nil {
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = err.Error()
		return res, nil
	}
	forced := false
	if !done {
		if err := requireLiveDeadline("signal kill"); err != nil {
			return failDeadline("signal kill")
		}
		if err := sess.DeliverKill(); err != nil {
			out, e := handleDeliverErr(err, "signal kill")
			if out.Signalled {
				out.ForcedKill = true
			}
			return out, e
		}
		res.Signalled = true
		forced = true
		from = jobregistry.RecoveryPhaseKillSent
		done, err = sess.WaitTerminated(remain(), opts.PollInterval)
		if err != nil {
			res.ForcedKill = true
			res.Outcome = OutcomeRecoveryFailed
			res.Detail = err.Error()
			return res, nil
		}
		if !done {
			res.ForcedKill = true
			res.Outcome = OutcomeRecoveryFailed
			res.Detail = "termination not observed after kill; not claiming stopped; not reclaiming"
			return res, nil
		}
	}
	if err := advance(jobregistry.RecoveryPhaseDeathObserved); err != nil {
		res.ForcedKill = forced
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = err.Error()
		return res, nil
	}
	res.ForcedKill = forced
	return finishAfterDeath(store, res, rec2, fenceOwner, attemptID, opts, forced)
}

// finishAfterIdentityLostSession handles ErrIdentityLost without claiming a false signal delivery.
// Signalled is left as-is: true only if a prior syscall was accepted on this result.
func finishAfterIdentityLostSession(store *jobregistry.Store, res RecoverStalledResult, rec *jobregistry.JobRecord, fenceOwner, attemptID string, opts RecoverStalledOptions, sess *jobregistry.SignalSession) (RecoverStalledResult, error) {
	if sess != nil {
		done, err := sess.Terminated()
		if err == nil && done {
			return finishAfterDeath(store, res, rec, fenceOwner, attemptID, opts, false)
		}
	}
	_, _ = store.FailStalledRecovery(res.JobID, fenceOwner, attemptID, "identity lost before/during signal; death not independently proven")
	res.Outcome = OutcomeRefused
	res.Detail = "process identity lost; Signalled reflects only accepted syscalls; death not proven for finalize"
	if !res.Signalled {
		res.Signalled = false
	}
	return res, nil
}

// contentionOutcome maps exclusive-claim races to non-failure outcomes.
// Busy claim / not-active after another winner finalized are ordinary under
// concurrent recover; they must not surface as recovery-failed (E6).
func contentionOutcome(store *jobregistry.Store, jobID string, err error, prefix string) (RecoverStalledOutcome, string, bool) {
	if err == nil {
		return "", "", false
	}
	detail := err.Error()
	if prefix != "" {
		detail = prefix + err.Error()
	}
	if errors.Is(err, jobregistry.ErrRecoverySignalBusy) {
		if store != nil {
			if rec, gerr := store.GetReadOnlyStrict(jobID); gerr == nil && rec.State == jobregistry.JobStateStopped {
				return OutcomeAlreadyStopped, "job already stopped after concurrent recovery", true
			}
		}
		return OutcomeRefused, detail, true
	}
	if errors.Is(err, jobregistry.ErrRecoveryNotActive) {
		if store != nil {
			if rec, gerr := store.GetReadOnlyStrict(jobID); gerr == nil && rec.State == jobregistry.JobStateStopped {
				return OutcomeAlreadyStopped, "job already stopped", true
			}
		}
		return OutcomeRefused, detail, true
	}
	if errors.Is(err, jobregistry.ErrRecoveryNotOwned) {
		return OutcomeRefused, detail, true
	}
	return "", "", false
}

func finishAfterDeath(store *jobregistry.Store, res RecoverStalledResult, rec *jobregistry.JobRecord, fenceOwner, attemptID string, opts RecoverStalledOptions, forced bool) (RecoverStalledResult, error) {
	// E-R8-02: death-observed before any W2 mutation; held/invalid never finalize.
	cur, cerr := store.GetReadOnlyStrict(res.JobID)
	if cerr != nil {
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = cerr.Error()
		return res, nil
	}
	gen := cur.RecoveryGeneration
	phase := cur.RecoveryPhase

	// Already reconciled: finalize-only; provenance comes from durable receipt (D-R11-01).
	if phase == jobregistry.RecoveryPhaseLeaseReconciled {
		return finalizeFromReceipt(store, res, cur, fenceOwner, attemptID, gen)
	}
	// Another winner already finalized while we raced.
	if phase == jobregistry.RecoveryPhaseFinalized || cur.State == jobregistry.JobStateStopped {
		res.Outcome = OutcomeAlreadyStopped
		res.Detail = "job already stopped"
		res.JobState = string(cur.State)
		res.ForcedKill = forced
		return res, nil
	}

	// D-R13-02: bound snapshot required for death/W2 path (except finalize-only above).
	if !jobregistry.BoundTargetMatches(cur) {
		res.Outcome = OutcomeRefused
		res.Detail = "bound recovery target snapshot missing or mismatched at death/W2; refuse finalize"
		res.ForcedKill = forced
		return res, nil
	}

	if phase != jobregistry.RecoveryPhaseDeathObserved {
		if err := store.AdvanceRecoveryPhase(res.JobID, jobregistry.AdvanceRecoveryPhaseOptions{
			FenceOwner: fenceOwner, AttemptID: attemptID, ExpectedGeneration: gen,
			FromPhase: phase, ToPhase: jobregistry.RecoveryPhaseDeathObserved,
		}); err != nil {
			if o, d, ok := contentionOutcome(store, res.JobID, err, "phase death-observed: "); ok {
				res.Outcome, res.Detail = o, d
				res.ForcedKill = forced
				return res, nil
			}
			res.Outcome = OutcomeRecoveryFailed
			res.Detail = fmt.Sprintf("phase death-observed: %v", err)
			res.ForcedKill = forced
			return res, nil
		}
	}
	// E4 test-only: death-observed durably persisted (hook nil in production).
	evidencePark("death-observed")

	indexSetID := strings.TrimSpace(rec.IndexSetID)
	if indexSetID == "" {
		indexSetID = strings.TrimSpace(res.Plan.IndexSetID)
	}
	// Pre-check only: held/invalid/successor refuse without store W2 mutation.
	// Actual reclaim + receipt construction is exclusively via ReconcileStalledW2.
	lease, leaseErr := ProbeLease(opts.AuthorityRoot, indexSetID, nil)
	if leaseErr != nil && lease.Verdict == "" {
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = leaseErr.Error()
		res.ForcedKill = forced
		return res, nil
	}
	res.LeaseAfter = lease.Verdict
	switch lease.Verdict {
	case LeaseHeld:
		res.Outcome = OutcomeLeaseStillHeld
		res.Detail = "process terminated but lease still held; no unlink; not finalized"
		res.ForcedKill = forced
		return res, nil
	case LeaseInvalid:
		res.Outcome = OutcomeInvalidResidue
		res.Detail = "process terminated; invalid lease residue preserved; not finalized"
		res.ForcedKill = forced
		return res, nil
	case LeaseUnheld:
		wantHolder := "index-build-" + res.JobID
		holder := strings.TrimSpace(lease.Holder)
		if holder != "" && holder != wantHolder {
			res.Outcome = OutcomeRefused
			res.Detail = fmt.Sprintf("unheld lease holder %q does not match managed job %q; refuse unlink (possible successor)", holder, wantHolder)
			res.ForcedKill = forced
			return res, nil
		}
		if holder == "" {
			res.Outcome = OutcomeRefused
			res.Detail = "unheld lease has no holder attribution; refuse unlink without identity proof"
			res.ForcedKill = forced
			return res, nil
		}
	case LeaseMissing:
		// OK — W2 will record missing.
	default:
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = fmt.Sprintf("unrecognized lease verdict %q after death", lease.Verdict)
		return res, nil
	}

	// D-R15-02: store-owned W2 — authority/index/holder from locked record only.
	if err := store.ReconcileStalledW2(res.JobID, fenceOwner, attemptID, gen); err != nil {
		if errors.Is(err, jobregistry.ErrW2NotFinalizing) {
			if strings.Contains(err.Error(), "invalid") {
				res.Outcome = OutcomeInvalidResidue
			} else {
				res.Outcome = OutcomeLeaseStillHeld
			}
			res.Detail = err.Error()
			res.ForcedKill = forced
			return res, nil
		}
		if o, d, ok := contentionOutcome(store, res.JobID, err, "W2 reconcile: "); ok {
			res.Outcome, res.Detail = o, d
			res.ForcedKill = forced
			return res, nil
		}
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = fmt.Sprintf("W2 reconcile: %v", err)
		res.ForcedKill = forced
		return res, nil
	}
	// E4 test-only: W2 receipt durable (lease-reconciled). pre-finalize is the
	// same persisted phase — one durable crash window (not two).
	evidencePark("lease-reconciled")
	if after, aerr := store.GetReadOnlyStrict(res.JobID); aerr == nil && after.RecoveryW2Receipt != nil {
		res.Reclaimed = after.RecoveryW2Receipt.Reclaimed
		res.LeaseAfter = LeaseVerdict(after.RecoveryW2Receipt.LeaseVerdict)
		res.Signalled = after.RecoveryW2Receipt.Signalled
		forced = after.RecoveryW2Receipt.ForcedKill || forced
	}
	// Finalize derives forced/signal provenance from the durable receipt.
	if _, err := store.FinalizeStalledRecoveryGen(res.JobID, fenceOwner, attemptID, gen, forced); err != nil {
		if o, d, ok := contentionOutcome(store, res.JobID, err, "finalize stopped: "); ok {
			res.Outcome, res.Detail = o, d
			res.ForcedKill = forced
			return res, nil
		}
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = fmt.Sprintf("finalize stopped: %v", err)
		res.ForcedKill = forced
		return res, nil
	}
	// Prefer receipt-backed provenance after successful finalize.
	if after, aerr := store.GetReadOnlyStrict(res.JobID); aerr == nil && after.RecoveryW2Receipt != nil {
		// Receipt may still be present until a later rewrite; finalize keeps it.
		res.ForcedKill = after.RecoveryW2Receipt.ForcedKill || forced
		if after.RecoveryW2Receipt.Signalled {
			res.Signalled = true
		}
	} else {
		res.ForcedKill = forced
	}
	res.Outcome = OutcomeSignalled
	if !res.Signalled {
		res.Outcome = OutcomeReapedOnly
		res.Detail = "process already dead under fence; reclaimed unheld lease and finalized"
	} else {
		res.Detail = "identity-bound stop observed; unheld lease handled; job stopped"
	}
	res.JobState = string(jobregistry.JobStateStopped)
	return res, nil
}

// finalizeFromReceipt completes a lease-reconciled resume without re-running W2.
func finalizeFromReceipt(store *jobregistry.Store, res RecoverStalledResult, rec *jobregistry.JobRecord, fenceOwner, attemptID string, gen int64) (RecoverStalledResult, error) {
	// Late concurrent callers that observe a completed lineage must not report
	// a second reaped-only winner (Finalize is idempotent on stopped).
	if rec != nil && (rec.State == jobregistry.JobStateStopped || rec.RecoveryPhase == jobregistry.RecoveryPhaseFinalized || rec.RecoveryIntent == "") {
		res.Outcome = OutcomeAlreadyStopped
		res.Detail = "job already stopped"
		if rec != nil {
			res.JobState = string(rec.State)
		}
		return res, nil
	}
	forced := false
	if rec.RecoveryW2Receipt != nil {
		forced = rec.RecoveryW2Receipt.ForcedKill
		if rec.RecoveryW2Receipt.Signalled {
			res.Signalled = true
		}
		res.Reclaimed = rec.RecoveryW2Receipt.Reclaimed
		res.LeaseAfter = LeaseVerdict(rec.RecoveryW2Receipt.LeaseVerdict)
	}
	if _, err := store.FinalizeStalledRecoveryGen(res.JobID, fenceOwner, attemptID, gen, forced); err != nil {
		if o, d, ok := contentionOutcome(store, res.JobID, err, ""); ok {
			res.Outcome, res.Detail = o, d
			return res, nil
		}
		res.Outcome = OutcomeRecoveryFailed
		res.Detail = err.Error()
		return res, nil
	}
	res.ForcedKill = forced
	res.JobState = string(jobregistry.JobStateStopped)
	if res.Signalled {
		res.Outcome = OutcomeSignalled
		res.Detail = "resumed from lease-reconciled; finalized from receipt without re-running W2"
	} else {
		res.Outcome = OutcomeReapedOnly
		res.Detail = "resumed from lease-reconciled; finalized from receipt without re-running W2"
	}
	return res, nil
}

func authHeartbeatEqual(a, b *time.Time) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.UTC().Equal(b.UTC())
}

// secondObservationDelay is at least one managed heartbeat interval in production.
// Under go test, a short delay keeps unit tests practical; production binaries
// always observe for ManagedHeartbeatInterval.
func secondObservationDelay() time.Duration {
	if testing.Testing() {
		return 50 * time.Millisecond
	}
	return ManagedHeartbeatInterval
}

func attemptDeadline(opts RecoverStalledOptions, waitTimeout time.Duration) time.Time {
	if !opts.Deadline.IsZero() {
		return opts.Deadline
	}
	if waitTimeout <= 0 {
		waitTimeout = 30 * time.Second
	}
	return time.Now().Add(waitTimeout)
}

func deadlineLive(deadline time.Time) bool {
	if deadline.IsZero() {
		return true
	}
	return time.Until(deadline) > 0
}

func reclaimIfUnheld(authorityRoot, indexSetID string) (reclaimed bool, verdict LeaseVerdict, err error) {
	reclaimed, report, err := reclaimIfUnheldReport(authorityRoot, indexSetID)
	return reclaimed, report.Verdict, err
}

func reclaimIfUnheldReport(authorityRoot, indexSetID string) (reclaimed bool, report ReclaimReport, err error) {
	indexSetID = strings.TrimSpace(indexSetID)
	if strings.TrimSpace(authorityRoot) == "" || indexSetID == "" {
		return false, report, fmt.Errorf("authority root and index_set_id required for reclaim")
	}
	report, err = ReclaimUnheldLease(authorityRoot, indexSetID)
	if err != nil {
		if errors.Is(err, ErrLeaseHeld) {
			return false, report, nil
		}
		return false, report, err
	}
	return report.Reclaimed, report, nil
}

// destructiveRecoveryPreflight refuses fence acquisition when the platform cannot
// perform instance-stable destructive recovery or the recorded birth token is
// legacy/lossy (D-R11-04). Byte-preserving: no store mutation.
func destructiveRecoveryPreflight(store *jobregistry.Store, jobID string, plan StalledRecoveryPlan) (refuse bool, detail string) {
	if err := procidentity.CheckDestructiveRecoverySupported(); err != nil {
		return true, fmt.Sprintf("destructive recovery preflight: %v", err)
	}
	rec, err := store.GetReadOnlyStrict(jobID)
	if err != nil {
		return true, fmt.Sprintf("destructive recovery preflight: %v", err)
	}
	id := jobregistry.ProcessIdentityFromRecord(rec)
	if !id.Proven {
		return true, "destructive recovery preflight: birth identity unproven; refuse fence"
	}
	if id.TokenVersion < procidentity.TokenVersionV1 {
		return true, "destructive recovery preflight: legacy/lossy birth token; refuse fence without mutation"
	}
	// Plan snapshot drift vs live record identity must refuse before fence.
	if plan.AuthPID != 0 && plan.AuthPID != rec.PID {
		return true, "destructive recovery preflight: plan pid drifted before fence"
	}
	if plan.AuthStartMS != 0 && (rec.ProcessStartTimeUnixMS == nil || *rec.ProcessStartTimeUnixMS != plan.AuthStartMS) {
		return true, "destructive recovery preflight: plan birth token drifted before fence"
	}
	return false, ""
}
