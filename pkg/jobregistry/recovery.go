package jobregistry

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/internal/procidentity"
)

var (
	// ErrRecoveryFenced is returned when a write would overwrite an active
	// stalled-recovery fence (restore running or claim success/partial).
	ErrRecoveryFenced = errors.New("job is under stalled-recovery fence")
	// ErrRecoveryNotOwned is returned when a recovery action's owner does not
	// match the durable fence owner.
	ErrRecoveryNotOwned = errors.New("stalled-recovery fence owner mismatch")
	// ErrRecoveryNotActive is returned when a recovery action expects a fence
	// that is not present.
	ErrRecoveryNotActive = errors.New("no active stalled-recovery fence")
	// ErrRecoveryAlreadyActive is returned when BeginStalledRecovery finds an
	// existing fence held by another owner.
	ErrRecoveryAlreadyActive = errors.New("stalled-recovery fence already active")
	// ErrRecoveryPlanSnapshotMismatch is returned when plan-time authorization
	// fields (heartbeat, state, identity) no longer match at fence acquisition.
	ErrRecoveryPlanSnapshotMismatch = errors.New("recovery plan snapshot mismatch")
	// ErrRecoverySignalBusy is returned when another caller holds the exclusive
	// signal/finalize phase under an active fence.
	ErrRecoverySignalBusy = errors.New("stalled-recovery signal phase already claimed")
	// ErrRecoverySnapshotRequired is returned when a new fence is requested
	// without MatchPlanSnapshot (snapshot CAS is mandatory for first fence).
	ErrRecoverySnapshotRequired = errors.New("plan snapshot match is required to acquire a recovery fence")
)

// Recovery phase names (entarch hybrid A+B). Signal transport is at-least-once
// against the same instance-stable bind; phases prevent silent double-finalize.
const (
	RecoveryPhaseFenced          = "fenced"
	RecoveryPhaseClaimed         = "claimed"
	RecoveryPhaseBound           = "bound"
	RecoveryPhaseTermIntent      = "term-intent"
	RecoveryPhaseTermSent        = "term-sent"
	RecoveryPhaseKillIntent      = "kill-intent"
	RecoveryPhaseKillSent        = "kill-sent"
	RecoveryPhaseDeathObserved   = "death-observed"
	RecoveryPhaseLeaseReconciled = "lease-reconciled"
	RecoveryPhaseFinalized       = "finalized"
)

// BeginStalledRecoveryOptions gates acquisition of the recovery fence.
type BeginStalledRecoveryOptions struct {
	// Owner is an opaque recovery-attempt id. Empty mints a new UUID.
	Owner string
	// ExpectedPID must match the record (PID-reuse protection at fence time).
	ExpectedPID int
	// ExpectedStartMS is the recorded birth token; required and must match.
	ExpectedStartMS uint64
	// ExpectedBootID must match when non-empty on the record.
	ExpectedBootID string
	// Expected native token fields (D-R12-02) — plan snapshot must include them.
	ExpectedTokenVersion int
	ExpectedStartTicks   uint64
	ExpectedStartSec     int64
	ExpectedStartUsec    int64
	ExpectedFiletime     uint64
	// ExpectedIndexSetID must match the record when non-empty.
	ExpectedIndexSetID string
	// MatchPlanSnapshot, when true, requires the durable record to still match
	// the plan-time authorization snapshot (state running, heartbeat, heartbeat
	// write error) before a new fence is acquired. Same-owner resume skips this.
	MatchPlanSnapshot bool
	// ExpectedLastHeartbeat is compared when MatchPlanSnapshot is true.
	// Both nil means "record also has no last_heartbeat".
	ExpectedLastHeartbeat *time.Time
	// ExpectedHeartbeatPersistError is compared when MatchPlanSnapshot is true.
	ExpectedHeartbeatPersistError string
	// ExpectedAuthorityRoot is the set-authority root for later W2 (D-R15-02).
	// Required for a new fence; stored durably as RecoveryAuthorityRoot.
	ExpectedAuthorityRoot string
}

// BeginStalledRecoveryResult is the fenced record after a successful claim.
type BeginStalledRecoveryResult struct {
	Record *JobRecord
	Owner  string
	// AlreadyOwned is true when this owner already held the fence (idempotent
	// re-entry for crash resume).
	AlreadyOwned bool
}

// BeginStalledRecovery acquires a durable recovery fence under the start lock:
// state running → stopping with RecoveryIntent=stalled. It refuses identity or
// index-set drift and converges concurrent attempts onto one owner.
func (s *Store) BeginStalledRecovery(jobID string, opts BeginStalledRecoveryOptions) (*BeginStalledRecoveryResult, error) {
	if s == nil {
		return nil, fmt.Errorf("job registry store is nil")
	}
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return nil, fmt.Errorf("job_id is required")
	}
	if opts.ExpectedPID <= 0 || opts.ExpectedStartMS == 0 {
		return nil, fmt.Errorf("expected process birth identity is required to fence recovery")
	}
	owner := strings.TrimSpace(opts.Owner)
	if owner == "" {
		owner = uuid.NewString()
	}

	var out *BeginStalledRecoveryResult
	err := s.withStartLock(func() error {
		rec, err := s.getReadOnlyStrict(jobID)
		if err != nil {
			return err
		}
		// Idempotent resume for the same owner.
		if rec.RecoveryIntent == RecoveryIntentStalled && rec.State == JobStateStopping {
			if rec.RecoveryOwner == owner {
				// D-R16-02: active fence must already carry authority root; never repair.
				if strings.TrimSpace(rec.RecoveryAuthorityRoot) == "" {
					return fmt.Errorf("recovery authority root missing on active fence; refuse resume")
				}
				if ar := strings.TrimSpace(opts.ExpectedAuthorityRoot); ar != "" {
					canon, dev, ino, err := resolveAuthorityRootIdentity(ar)
					if err != nil {
						return fmt.Errorf("authority root identity on resume: %w", err)
					}
					if canon != rec.RecoveryAuthorityRoot ||
						(procidentity.FileIdentityRequired() && (dev != rec.RecoveryAuthorityDev || ino != rec.RecoveryAuthorityIno)) {
						return fmt.Errorf("recovery authority root identity mismatch on resume")
					}
				}
				out = &BeginStalledRecoveryResult{Record: rec, Owner: owner, AlreadyOwned: true}
				return nil
			}
			return fmt.Errorf("%w (owner=%s)", ErrRecoveryAlreadyActive, rec.RecoveryOwner)
		}
		if rec.State != JobStateRunning {
			return fmt.Errorf("%w (state=%s)", ErrJobNotRunning, rec.State)
		}
		// New fence acquisition always requires plan-time snapshot CAS.
		if !opts.MatchPlanSnapshot {
			return ErrRecoverySnapshotRequired
		}
		authRoot, authDev, authIno, aerr := resolveAuthorityRootIdentity(opts.ExpectedAuthorityRoot)
		if aerr != nil {
			return fmt.Errorf("authority root identity: %w", aerr)
		}
		if rec.Type != "" && rec.Type != JobTypeIndexBuild {
			return fmt.Errorf("job type %q is not a managed index build", rec.Type)
		}
		if err := matchPlanSnapshot(rec, opts); err != nil {
			return err
		}
		// D-R12-02 / D-R11-04: destructive capability + native token inside the
		// same start-lock mutation boundary (byte-identical refuse).
		if err := procidentity.CheckDestructiveRecoverySupported(); err != nil {
			return fmt.Errorf("destructive recovery preflight under lock: %w", err)
		}
		recorded := ProcessIdentityFromRecord(rec)
		if !recorded.Proven || recorded.TokenVersion < procidentity.TokenVersionV1 {
			return fmt.Errorf("%w: native v1 birth token required to fence recovery", ErrRecoveryPlanSnapshotMismatch)
		}
		// Live identity must still match at fence time.
		observed := observeProcessIdentity(rec.PID)
		if !ProcessIdentityMatch(recorded, observed) {
			return fmt.Errorf("live process birth identity does not match record at fence time")
		}

		// Do not refresh LastHeartbeat here: fence acquisition must not erase
		// the stall evidence the plan used, and must not race a fresh heartbeat
		// into an authorized kill by overwriting it with "now".
		now := time.Now().UTC()
		rec.State = JobStateStopping
		rec.RecoveryIntent = RecoveryIntentStalled
		rec.RecoveryOwner = owner
		rec.RecoverySignalOwner = "" // signal phase claimed separately
		rec.RecoveryStartedAt = &now
		rec.RecoveryGeneration++
		rec.RecoveryPhase = RecoveryPhaseFenced
		rec.RecoveryDeliverySignalled = false
		rec.RecoveryDeliveryForced = false
		rec.RecoveryW2Receipt = nil
		rec.RecoveryAuthorityRoot = authRoot
		rec.RecoveryAuthorityDev = authDev
		rec.RecoveryAuthorityIno = authIno
		clearBoundTarget(rec)
		if err := s.writeRecord(rec); err != nil {
			return err
		}
		out = &BeginStalledRecoveryResult{Record: rec, Owner: owner}
		return nil
	})
	return out, err
}

// ErrW2NotFinalizing means W2 observed held/invalid residue — no receipt written.
var ErrW2NotFinalizing = errors.New("W2 lease state is not finalizing")

// ReconcileStalledW2 is the store-owned W2 path (D-R15-02). Authority root,
// index set, and holder are taken only from the locked job record — callers
// cannot substitute empty/foreign coordinates.
func (s *Store) ReconcileStalledW2(jobID, fenceOwner, attemptID string, expectedGeneration int64) error {
	if s == nil {
		return fmt.Errorf("store is required")
	}
	if expectedGeneration <= 0 || strings.TrimSpace(attemptID) == "" || strings.TrimSpace(fenceOwner) == "" {
		return fmt.Errorf("generation, fence owner, and attempt id are required")
	}
	return s.withStartLock(func() error {
		rec, err := s.getReadOnlyStrict(jobID)
		if err != nil {
			return err
		}
		if rec.RecoveryIntent != RecoveryIntentStalled {
			return ErrRecoveryNotActive
		}
		if rec.RecoveryOwner != fenceOwner || rec.RecoverySignalOwner != attemptID {
			return ErrRecoveryNotOwned
		}
		if rec.RecoveryGeneration != expectedGeneration {
			return fmt.Errorf("recovery generation mismatch on W2 receipt")
		}
		if rec.RecoveryPhase != RecoveryPhaseDeathObserved {
			return fmt.Errorf("W2 receipt requires death-observed (have %q)", rec.RecoveryPhase)
		}
		if !BoundTargetMatches(rec) {
			return fmt.Errorf("bound target snapshot mismatch on W2")
		}
		authorityRoot := strings.TrimSpace(rec.RecoveryAuthorityRoot)
		indexSetID := strings.TrimSpace(rec.IndexSetID)
		if authorityRoot == "" || indexSetID == "" {
			return fmt.Errorf("recovery authority root and index_set_id required on record for W2")
		}
		// D-R17-03: revalidate canonical path + directory identity before probe/reclaim.
		live, dev, ino, rerr := resolveAuthorityRootIdentity(authorityRoot)
		if rerr != nil {
			return fmt.Errorf("authority root revalidate: %w", rerr)
		}
		if live != authorityRoot {
			return fmt.Errorf("authority root canonical path drifted")
		}
		if procidentity.FileIdentityRequired() && (dev != rec.RecoveryAuthorityDev || ino != rec.RecoveryAuthorityIno) {
			return fmt.Errorf("authority root directory identity changed (symlink retarget or replace)")
		}
		wantHolder := "index-build-" + rec.JobID
		obs, err := observeAndMaybeReclaimLease(authorityRoot, indexSetID, wantHolder)
		if err != nil {
			return err
		}
		v := strings.TrimSpace(obs.verdict)
		switch v {
		case string(indexsubstrate.LeaseHeld):
			return fmt.Errorf("%w: lease still held", ErrW2NotFinalizing)
		case string(indexsubstrate.LeaseInvalid):
			return fmt.Errorf("%w: lease invalid residue", ErrW2NotFinalizing)
		case string(indexsubstrate.LeaseMissing):
			if obs.reclaimed {
				return fmt.Errorf("W2 observation inconsistent: reclaimed with missing verdict")
			}
		case string(indexsubstrate.LeaseUnheld):
			if !obs.reclaimed {
				return fmt.Errorf("W2 observation inconsistent: unheld without reclaim in this call")
			}
			if strings.TrimSpace(obs.path) == "" {
				return fmt.Errorf("W2 reclaim requires lease path identity")
			}
			if obs.dev == 0 && obs.ino == 0 && procidentity.FileIdentityRequired() {
				return fmt.Errorf("W2 reclaim requires lease file identity (dev+ino)")
			}
		default:
			return fmt.Errorf("W2 observation unknown lease verdict %q", v)
		}
		signalled := rec.RecoveryDeliverySignalled
		forced := rec.RecoveryDeliveryForced
		if forced && !signalled {
			signalled = true
		}
		rec.RecoveryW2Receipt = &RecoveryW2Receipt{
			SchemaVersion: 1,
			Generation:    expectedGeneration,
			FenceOwner:    fenceOwner,
			OriginAttempt: attemptID,
			JobID:         rec.JobID,
			IndexSetID:    rec.IndexSetID,
			LeaseVerdict:  v,
			Reclaimed:     obs.reclaimed,
			Signalled:     signalled,
			ForcedKill:    forced,
			ReconciledAt:  time.Now().UTC(),
			LeasePath:     strings.TrimSpace(obs.path),
			LeaseDev:      obs.dev,
			LeaseIno:      obs.ino,
		}
		rec.RecoveryPhase = RecoveryPhaseLeaseReconciled
		return s.writeRecord(rec)
	})
}

type leaseObs struct {
	verdict   string
	path      string
	dev, ino  uint64
	holder    string
	reclaimed bool
}

func observeAndMaybeReclaimLease(authorityRoot, indexSetID, wantHolder string) (leaseObs, error) {
	lease, err := indexsubstrate.ProbeSetAuthorityLease(authorityRoot, indexSetID)
	if err != nil && lease.Verdict == "" {
		return leaseObs{}, err
	}
	obs := leaseObs{
		verdict: string(lease.Verdict),
		path:    lease.Path,
		holder:  lease.Holder,
	}
	if lease.Path != "" {
		obs.dev, obs.ino = procidentity.FileDevIno(lease.Path)
	}
	switch lease.Verdict {
	case indexsubstrate.LeaseMissing, indexsubstrate.LeaseHeld, indexsubstrate.LeaseInvalid:
		return obs, nil
	case indexsubstrate.LeaseUnheld:
		if wantHolder != "" {
			h := strings.TrimSpace(lease.Holder)
			if h == "" {
				return leaseObs{}, fmt.Errorf("unheld lease has no holder attribution")
			}
			if h != wantHolder {
				return leaseObs{}, fmt.Errorf("unheld lease holder %q does not match managed job %q", h, wantHolder)
			}
		}
		// Capture identity before unlink.
		path := lease.Path
		dev, ino := obs.dev, obs.ino
		res, rerr := indexsubstrate.ReclaimUnheldSetAuthorityLease(authorityRoot, indexSetID)
		if rerr != nil {
			if errors.Is(rerr, indexsubstrate.ErrSetAuthorityHeld) {
				return leaseObs{verdict: string(indexsubstrate.LeaseHeld), path: res.Path, holder: res.Holder}, nil
			}
			return leaseObs{}, rerr
		}
		if res.Path != "" {
			path = res.Path
		}
		return leaseObs{
			verdict:   string(indexsubstrate.LeaseUnheld),
			path:      path,
			dev:       dev,
			ino:       ino,
			holder:    res.Holder,
			reclaimed: res.Reclaimed,
		}, nil
	default:
		return leaseObs{}, fmt.Errorf("unrecognized lease verdict %q", lease.Verdict)
	}
}

// sessionNowForTest is package-private clock injection for same-package tests only.
// Production callers cannot override it via the public API (D-R17-01).
var sessionNowForTest func() time.Time

// SignalSession owns the exact instance-bound target from RecoveryBound* and
// couples transport with durable delivery marking (D-R15-01 / D-R16-01).
// The raw *procidentity.Target is never exposed — only non-destructive waits
// and coupled Deliver* are public.
type SignalSession struct {
	store    *Store
	jobID    string
	fence    string
	attempt  string
	gen      int64
	phase    string
	target   *procidentity.Target // unexported — no raw signal escape
	deadline time.Time            // mandatory absolute deadline
}

// OpenSignalSession validates fence/attempt/generation/bound/authority, Binds
// the exact bound identity, and returns a session for DeliverTerm/DeliverKill.
// deadline must be non-zero and still live (D-R17-01).
func (s *Store) OpenSignalSession(jobID, fenceOwner, attemptID string, expectedGeneration int64, deadline time.Time) (*SignalSession, error) {
	if s == nil {
		return nil, fmt.Errorf("store is required")
	}
	if deadline.IsZero() {
		return nil, fmt.Errorf("mandatory live recovery deadline is required")
	}
	if !sessionClockNow().Before(deadline) {
		return nil, fmt.Errorf("recovery deadline already expired at session open")
	}
	var sess *SignalSession
	var expected ProcessIdentity
	err := s.withStartLock(func() error {
		rec, err := s.getReadOnlyStrict(jobID)
		if err != nil {
			return err
		}
		if rec.RecoveryIntent != RecoveryIntentStalled {
			return ErrRecoveryNotActive
		}
		if rec.RecoveryOwner != fenceOwner || rec.RecoverySignalOwner != attemptID {
			return ErrRecoveryNotOwned
		}
		if rec.RecoveryGeneration != expectedGeneration {
			return fmt.Errorf("recovery generation mismatch on signal session")
		}
		if strings.TrimSpace(rec.RecoveryAuthorityRoot) == "" {
			return fmt.Errorf("recovery authority root missing on active fence; refuse session")
		}
		if !BoundTargetMatches(rec) {
			return fmt.Errorf("bound target snapshot missing or mismatched")
		}
		expected = identityFromBound(rec)
		sess = &SignalSession{
			store: s, jobID: jobID, fence: fenceOwner, attempt: attemptID,
			gen: expectedGeneration, phase: rec.RecoveryPhase,
			deadline: deadline,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	target, berr := procidentity.Bind(expected)
	if berr != nil {
		return nil, berr
	}
	sess.target = target
	return sess, nil
}

func sessionClockNow() time.Time {
	if sessionNowForTest != nil {
		return sessionNowForTest()
	}
	return time.Now()
}

// Close releases the owned target handle.
func (sess *SignalSession) Close() error {
	if sess == nil || sess.target == nil {
		return nil
	}
	err := sess.target.Close()
	sess.target = nil
	return err
}

// HardStopOnly reports whether the owned target is hard-stop-only.
func (sess *SignalSession) HardStopOnly() bool {
	if sess == nil || sess.target == nil {
		return false
	}
	return sess.target.HardStopOnly()
}

// Terminated reports whether the session-owned instance is gone.
func (sess *SignalSession) Terminated() (bool, error) {
	if sess == nil || sess.target == nil {
		return false, procidentity.ErrNotBound
	}
	return sess.target.Terminated()
}

// WaitTerminated waits for the session-owned instance to exit.
func (sess *SignalSession) WaitTerminated(timeout, poll time.Duration) (bool, error) {
	if sess == nil || sess.target == nil {
		return false, procidentity.ErrNotBound
	}
	return sess.target.WaitTerminated(timeout, poll)
}

func (sess *SignalSession) requireDeadline(op string) error {
	if sess == nil {
		return fmt.Errorf("signal session is nil")
	}
	if sess.deadline.IsZero() {
		return fmt.Errorf("session has no deadline")
	}
	if !sessionClockNow().Before(sess.deadline) {
		return fmt.Errorf("recovery deadline expired before %s", op)
	}
	return nil
}

// DeliverTerm signals TERM on the session-owned target then records term-sent.
func (sess *SignalSession) DeliverTerm() error {
	if sess == nil || sess.store == nil || sess.target == nil {
		return fmt.Errorf("signal session is nil")
	}
	if err := sess.requireDeadline("term-intent"); err != nil {
		return err
	}
	if err := sess.revalidateBoundBeforeTransport(); err != nil {
		return err
	}
	if err := sess.ensureIntent(RecoveryPhaseTermIntent); err != nil {
		return err
	}
	// Re-check after intent mutation — covers intent↔syscall expiry (D-R16-01).
	if err := sess.requireDeadline("signal term"); err != nil {
		return err
	}
	if err := sess.revalidateBoundBeforeTransport(); err != nil {
		return err
	}
	if err := sess.target.SignalTerm(); err != nil {
		return err
	}
	if err := sess.store.recordAcceptedSignalAndAdvance(sess.jobID, sess.fence, sess.attempt, sess.gen,
		RecoveryPhaseTermIntent, RecoveryPhaseTermSent, false); err != nil {
		return fmt.Errorf("%w: %v", ErrDeliveryPersistFailed, err)
	}
	sess.phase = RecoveryPhaseTermSent
	return nil
}

// DeliverKill signals KILL on the session-owned target then records kill-sent.
func (sess *SignalSession) DeliverKill() error {
	if sess == nil || sess.store == nil || sess.target == nil {
		return fmt.Errorf("signal session is nil")
	}
	if err := sess.requireDeadline("kill-intent"); err != nil {
		return err
	}
	if err := sess.revalidateBoundBeforeTransport(); err != nil {
		return err
	}
	from := RecoveryPhaseKillIntent
	if sess.phase == RecoveryPhaseKillSent {
		from = RecoveryPhaseKillSent
	} else if err := sess.ensureIntent(RecoveryPhaseKillIntent); err != nil {
		return err
	}
	if err := sess.requireDeadline("signal kill"); err != nil {
		return err
	}
	if err := sess.revalidateBoundBeforeTransport(); err != nil {
		return err
	}
	if err := sess.target.SignalKill(); err != nil {
		return err
	}
	if err := sess.store.recordAcceptedSignalAndAdvance(sess.jobID, sess.fence, sess.attempt, sess.gen,
		from, RecoveryPhaseKillSent, true); err != nil {
		return fmt.Errorf("%w: %v", ErrDeliveryPersistFailed, err)
	}
	sess.phase = RecoveryPhaseKillSent
	return nil
}

func identityFromBound(rec *JobRecord) ProcessIdentity {
	start := rec.RecoveryBoundStartMS
	return procidentity.FromRecordFull(
		rec.RecoveryBoundPID, &start, rec.RecoveryBoundBootID,
		rec.RecoveryBoundTokenVersion, rec.RecoveryBoundStartTicks,
		rec.RecoveryBoundStartSec, rec.RecoveryBoundStartUsec, rec.RecoveryBoundFiletime,
	)
}

// CheckRecoveryAuthorityRoot verifies the durable root on an active fence matches
// the caller's expected root identity. Missing root refuses (D-R16-02 / D-R17-03).
func (s *Store) CheckRecoveryAuthorityRoot(jobID, fenceOwner, expectedRoot string) error {
	if s == nil {
		return fmt.Errorf("store is required")
	}
	expectedRoot = strings.TrimSpace(expectedRoot)
	return s.withStartLock(func() error {
		rec, err := s.getReadOnlyStrict(jobID)
		if err != nil {
			return err
		}
		if rec.RecoveryIntent != RecoveryIntentStalled || rec.RecoveryOwner != fenceOwner {
			return ErrRecoveryNotOwned
		}
		have := strings.TrimSpace(rec.RecoveryAuthorityRoot)
		if have == "" {
			return fmt.Errorf("recovery authority root missing on active fence; refuse repair from caller input")
		}
		if expectedRoot == "" {
			return nil
		}
		canon, dev, ino, err := resolveAuthorityRootIdentity(expectedRoot)
		if err != nil {
			return fmt.Errorf("authority root identity: %w", err)
		}
		if canon != have {
			return fmt.Errorf("recovery authority root mismatch: have %q want %q", have, canon)
		}
		if procidentity.FileIdentityRequired() && (dev != rec.RecoveryAuthorityDev || ino != rec.RecoveryAuthorityIno) {
			return fmt.Errorf("recovery authority root directory identity mismatch")
		}
		return nil
	})
}

// resolveAuthorityRootIdentity returns a canonical absolute path and optional
// platform directory identity for an authority root (D-R17-03).
func resolveAuthorityRootIdentity(root string) (canon string, dev, ino uint64, err error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return "", 0, 0, fmt.Errorf("authority root is required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", 0, 0, err
	}
	// EvalSymlinks fails if the path does not exist; require a real directory.
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", 0, 0, err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", 0, 0, err
	}
	if !info.IsDir() {
		return "", 0, 0, fmt.Errorf("authority root is not a directory")
	}
	dev, ino = procidentity.FileDevIno(resolved)
	return resolved, dev, ino, nil
}

func (sess *SignalSession) revalidateBoundBeforeTransport() error {
	return sess.store.withStartLock(func() error {
		rec, err := sess.store.getReadOnlyStrict(sess.jobID)
		if err != nil {
			return err
		}
		if rec.RecoveryOwner != sess.fence || rec.RecoverySignalOwner != sess.attempt {
			return ErrRecoveryNotOwned
		}
		if rec.RecoveryGeneration != sess.gen {
			return fmt.Errorf("recovery generation mismatch before transport")
		}
		if !BoundTargetMatches(rec) {
			return fmt.Errorf("bound target snapshot missing or mismatched before transport")
		}
		// Target Expected must still equal durable bound identity.
		if sess.target == nil || !ProcessIdentityMatch(identityFromBound(rec), sess.target.Expected()) {
			return fmt.Errorf("session target does not match durable bound identity")
		}
		sess.phase = rec.RecoveryPhase
		return nil
	})
}

// ErrDeliveryPersistFailed means the OS signal was accepted but durable delivery
// mark/phase advance failed — callers must report Signalled=true and not finalize.
var ErrDeliveryPersistFailed = errors.New("signal accepted but delivery persistence failed")

func (sess *SignalSession) ensureIntent(to string) error {
	return sess.store.withStartLock(func() error {
		rec, err := sess.store.getReadOnlyStrict(sess.jobID)
		if err != nil {
			return err
		}
		if rec.RecoveryOwner != sess.fence || rec.RecoverySignalOwner != sess.attempt {
			return ErrRecoveryNotOwned
		}
		if rec.RecoveryGeneration != sess.gen {
			return fmt.Errorf("recovery generation mismatch")
		}
		if !BoundTargetMatches(rec) {
			return fmt.Errorf("bound target snapshot missing or mismatched")
		}
		cur := rec.RecoveryPhase
		if cur == to {
			sess.phase = cur
			return nil
		}
		// Already delivered the matching signal class: keep sent phase (replay paths).
		if to == RecoveryPhaseTermIntent && cur == RecoveryPhaseTermSent {
			sess.phase = cur
			return nil
		}
		if to == RecoveryPhaseKillIntent && cur == RecoveryPhaseKillSent {
			sess.phase = cur
			return nil
		}
		// Escalation: durable term-sent → kill-intent before KILL syscall (E3).
		// Do not treat term-sent as a no-op for kill-intent — that left from=kill-intent
		// while the record stayed term-sent and broke post-accept phase CAS.
		if !legalPhaseTransition(cur, to) {
			return fmt.Errorf("illegal recovery phase transition %q -> %q", cur, to)
		}
		rec.RecoveryPhase = to
		sess.phase = to
		return sess.store.writeRecord(rec)
	})
}

// recordAcceptedSignalAndAdvance is unexported — only SignalSession.Deliver* call it.
func (s *Store) recordAcceptedSignalAndAdvance(jobID, fenceOwner, attemptID string, expectedGeneration int64, fromPhase, toPhase string, forced bool) error {
	from := strings.TrimSpace(fromPhase)
	to := strings.TrimSpace(toPhase)
	if expectedGeneration <= 0 || strings.TrimSpace(fenceOwner) == "" || strings.TrimSpace(attemptID) == "" {
		return fmt.Errorf("generation, fence owner, and attempt id are required")
	}
	switch from {
	case RecoveryPhaseTermIntent, RecoveryPhaseKillIntent:
	case RecoveryPhaseKillSent:
		if to != RecoveryPhaseKillSent {
			return fmt.Errorf("kill-sent replay must stay at kill-sent")
		}
	default:
		return fmt.Errorf("accepted-signal advance requires intent phase (have %q)", from)
	}
	switch to {
	case RecoveryPhaseTermSent:
		if forced {
			return fmt.Errorf("term-sent cannot be forced")
		}
	case RecoveryPhaseKillSent:
		if !forced {
			return fmt.Errorf("kill-sent requires forced delivery")
		}
	default:
		return fmt.Errorf("accepted-signal advance requires sent phase (have %q)", to)
	}
	if from != RecoveryPhaseKillSent && !legalPhaseTransition(from, to) {
		return fmt.Errorf("illegal recovery phase transition %q -> %q", from, to)
	}
	return s.withStartLock(func() error {
		rec, err := s.getReadOnlyStrict(jobID)
		if err != nil {
			return err
		}
		if rec.RecoveryIntent != RecoveryIntentStalled {
			return ErrRecoveryNotActive
		}
		if rec.RecoveryOwner != fenceOwner || rec.RecoverySignalOwner != attemptID {
			return ErrRecoveryNotOwned
		}
		if rec.RecoveryGeneration != expectedGeneration {
			return fmt.Errorf("recovery generation mismatch on delivery mark")
		}
		if rec.RecoveryPhase != from {
			return fmt.Errorf("recovery phase mismatch: have %q want %q", rec.RecoveryPhase, from)
		}
		if !BoundTargetMatches(rec) {
			return fmt.Errorf("bound target snapshot mismatch on delivery")
		}
		rec.RecoveryDeliverySignalled = true
		if forced {
			rec.RecoveryDeliveryForced = true
		}
		rec.RecoveryPhase = to
		return s.writeRecord(rec)
	})
}

func phaseAtOrAfterTransport(phase string) bool {
	switch phase {
	case RecoveryPhaseTermIntent, RecoveryPhaseTermSent,
		RecoveryPhaseKillIntent, RecoveryPhaseKillSent,
		RecoveryPhaseDeathObserved:
		return true
	default:
		return false
	}
}

// AdvanceRecoveryPhase is a fail-closed generation/from→to CAS for recovery phases.
type AdvanceRecoveryPhaseOptions struct {
	FenceOwner         string
	AttemptID          string
	ExpectedGeneration int64
	FromPhase          string // empty allows any current phase (first transition after claim)
	ToPhase            string
}

// AdvanceRecoveryPhase persists a legal phase transition. Generation, from-phase,
// fence owner, and attempt owner are mandatory (E-R7-01 / D-R7-01).
func (s *Store) AdvanceRecoveryPhase(jobID string, opts AdvanceRecoveryPhaseOptions) error {
	if s == nil {
		return fmt.Errorf("job registry store is nil")
	}
	to := strings.TrimSpace(opts.ToPhase)
	from := strings.TrimSpace(opts.FromPhase)
	fenceOwner := strings.TrimSpace(opts.FenceOwner)
	attemptID := strings.TrimSpace(opts.AttemptID)
	if to == "" || from == "" || fenceOwner == "" || attemptID == "" {
		return fmt.Errorf("fence owner, attempt id, from phase, and to phase are required")
	}
	if opts.ExpectedGeneration <= 0 {
		return fmt.Errorf("expected generation must be positive")
	}
	if !legalPhaseTransition(from, to) {
		return fmt.Errorf("illegal recovery phase transition %q -> %q", from, to)
	}
	return s.withStartLock(func() error {
		rec, err := s.getReadOnlyStrict(jobID)
		if err != nil {
			return err
		}
		if rec.RecoveryIntent != RecoveryIntentStalled {
			return ErrRecoveryNotActive
		}
		if rec.RecoveryOwner != fenceOwner {
			return fmt.Errorf("%w (have %s)", ErrRecoveryNotOwned, rec.RecoveryOwner)
		}
		// Attempt ownership is required even when claim field was cleared incorrectly.
		if rec.RecoverySignalOwner != attemptID {
			return fmt.Errorf("%w (signal holder=%q want %q)", ErrRecoverySignalBusy, rec.RecoverySignalOwner, attemptID)
		}
		if rec.RecoveryGeneration != opts.ExpectedGeneration {
			return fmt.Errorf("recovery generation mismatch: have %d want %d", rec.RecoveryGeneration, opts.ExpectedGeneration)
		}
		if rec.RecoveryPhase != from {
			return fmt.Errorf("recovery phase mismatch: have %q want %q", rec.RecoveryPhase, from)
		}
		rec.RecoveryPhase = to
		return s.writeRecord(rec)
	})
}

func legalPhaseTransition(from, to string) bool {
	if from == to && from != "" {
		return true // idempotent re-entry
	}
	allowed := map[string][]string{
		RecoveryPhaseFenced:          {RecoveryPhaseClaimed},
		RecoveryPhaseClaimed:         {RecoveryPhaseBound, RecoveryPhaseDeathObserved}, // no-signal dead-target path
		RecoveryPhaseBound:           {RecoveryPhaseTermIntent, RecoveryPhaseKillIntent, RecoveryPhaseDeathObserved},
		RecoveryPhaseTermIntent:      {RecoveryPhaseTermSent, RecoveryPhaseDeathObserved},
		RecoveryPhaseTermSent:        {RecoveryPhaseKillIntent, RecoveryPhaseDeathObserved},
		RecoveryPhaseKillIntent:      {RecoveryPhaseKillSent, RecoveryPhaseDeathObserved},
		RecoveryPhaseKillSent:        {RecoveryPhaseDeathObserved},
		RecoveryPhaseDeathObserved:   {RecoveryPhaseLeaseReconciled},
		RecoveryPhaseLeaseReconciled: {RecoveryPhaseFinalized},
	}
	for _, next := range allowed[from] {
		if next == to {
			return true
		}
	}
	return false
}

func matchPlanSnapshot(rec *JobRecord, opts BeginStalledRecoveryOptions) error {
	if rec == nil {
		return fmt.Errorf("%w: nil record", ErrRecoveryPlanSnapshotMismatch)
	}
	if rec.State != JobStateRunning {
		return fmt.Errorf("%w: state=%s (expected running)", ErrRecoveryPlanSnapshotMismatch, rec.State)
	}
	if rec.PID != opts.ExpectedPID {
		return fmt.Errorf("%w: pid changed since plan", ErrRecoveryPlanSnapshotMismatch)
	}
	if rec.ProcessStartTimeUnixMS == nil || *rec.ProcessStartTimeUnixMS != opts.ExpectedStartMS {
		return fmt.Errorf("%w: process birth token changed since plan", ErrRecoveryPlanSnapshotMismatch)
	}
	if strings.TrimSpace(rec.ProcessBootID) != strings.TrimSpace(opts.ExpectedBootID) {
		return fmt.Errorf("%w: process boot identity changed since plan", ErrRecoveryPlanSnapshotMismatch)
	}
	// D-R12-02: exact native token in plan→fence CAS.
	if rec.ProcessTokenVersion != opts.ExpectedTokenVersion {
		return fmt.Errorf("%w: process token version changed since plan", ErrRecoveryPlanSnapshotMismatch)
	}
	if rec.ProcessStartTicks != opts.ExpectedStartTicks {
		return fmt.Errorf("%w: process start ticks changed since plan", ErrRecoveryPlanSnapshotMismatch)
	}
	if rec.ProcessStartSec != opts.ExpectedStartSec || rec.ProcessStartUsec != opts.ExpectedStartUsec {
		return fmt.Errorf("%w: process start sec/usec changed since plan", ErrRecoveryPlanSnapshotMismatch)
	}
	if rec.ProcessFiletime != opts.ExpectedFiletime {
		return fmt.Errorf("%w: process filetime changed since plan", ErrRecoveryPlanSnapshotMismatch)
	}
	if strings.TrimSpace(rec.IndexSetID) != strings.TrimSpace(opts.ExpectedIndexSetID) {
		return fmt.Errorf("%w: index_set_id changed since plan", ErrRecoveryPlanSnapshotMismatch)
	}
	if strings.TrimSpace(rec.HeartbeatPersistError) != strings.TrimSpace(opts.ExpectedHeartbeatPersistError) {
		return fmt.Errorf("%w: heartbeat_persist_error changed since plan", ErrRecoveryPlanSnapshotMismatch)
	}
	if !heartbeatEqual(rec.LastHeartbeat, opts.ExpectedLastHeartbeat) {
		return fmt.Errorf("%w: last_heartbeat changed since plan", ErrRecoveryPlanSnapshotMismatch)
	}
	return nil
}

func clearBoundTarget(rec *JobRecord) {
	if rec == nil {
		return
	}
	rec.RecoveryBoundPID = 0
	rec.RecoveryBoundTokenVersion = 0
	rec.RecoveryBoundStartMS = 0
	rec.RecoveryBoundBootID = ""
	rec.RecoveryBoundStartTicks = 0
	rec.RecoveryBoundStartSec = 0
	rec.RecoveryBoundStartUsec = 0
	rec.RecoveryBoundFiletime = 0
	rec.RecoveryBoundAttempt = ""
	rec.RecoveryBoundFenceOwner = ""
	rec.RecoveryBoundGeneration = 0
	rec.RecoveryBoundIndexSetID = ""
	rec.RecoveryBoundJobID = ""
}

func persistBoundTarget(rec *JobRecord, attemptID string) {
	if rec == nil {
		return
	}
	rec.RecoveryBoundPID = rec.PID
	rec.RecoveryBoundTokenVersion = rec.ProcessTokenVersion
	if rec.ProcessStartTimeUnixMS != nil {
		rec.RecoveryBoundStartMS = *rec.ProcessStartTimeUnixMS
	}
	rec.RecoveryBoundBootID = rec.ProcessBootID
	rec.RecoveryBoundStartTicks = rec.ProcessStartTicks
	rec.RecoveryBoundStartSec = rec.ProcessStartSec
	rec.RecoveryBoundStartUsec = rec.ProcessStartUsec
	rec.RecoveryBoundFiletime = rec.ProcessFiletime
	rec.RecoveryBoundAttempt = attemptID
	rec.RecoveryBoundFenceOwner = rec.RecoveryOwner
	rec.RecoveryBoundGeneration = rec.RecoveryGeneration
	rec.RecoveryBoundIndexSetID = rec.IndexSetID
	rec.RecoveryBoundJobID = rec.JobID
}

// BoundTargetMatches reports whether the durable bound snapshot is complete and
// still matches job/index/fence/attempt/generation + native token (D-R13-02).
// Missing snapshot fails closed (false).
func BoundTargetMatches(rec *JobRecord) bool {
	if rec == nil {
		return false
	}
	// Complete snapshot required — never "validate if present".
	if rec.RecoveryBoundPID <= 0 ||
		rec.RecoveryBoundTokenVersion < ProcessTokenVersionV1 ||
		rec.RecoveryBoundAttempt == "" ||
		rec.RecoveryBoundFenceOwner == "" ||
		rec.RecoveryBoundGeneration <= 0 ||
		rec.RecoveryBoundJobID == "" {
		return false
	}
	if rec.RecoveryBoundGeneration != rec.RecoveryGeneration {
		return false
	}
	if rec.RecoveryBoundFenceOwner != rec.RecoveryOwner {
		return false
	}
	if rec.RecoveryBoundAttempt != rec.RecoverySignalOwner {
		return false
	}
	if rec.RecoveryBoundJobID != rec.JobID {
		return false
	}
	if rec.RecoveryBoundPID != rec.PID {
		return false
	}
	if rec.RecoveryBoundTokenVersion != rec.ProcessTokenVersion {
		return false
	}
	var startMS uint64
	if rec.ProcessStartTimeUnixMS != nil {
		startMS = *rec.ProcessStartTimeUnixMS
	}
	if rec.RecoveryBoundStartMS != startMS {
		return false
	}
	if rec.RecoveryBoundBootID != rec.ProcessBootID {
		return false
	}
	if rec.RecoveryBoundStartTicks != rec.ProcessStartTicks {
		return false
	}
	if rec.RecoveryBoundStartSec != rec.ProcessStartSec || rec.RecoveryBoundStartUsec != rec.ProcessStartUsec {
		return false
	}
	if rec.RecoveryBoundFiletime != rec.ProcessFiletime {
		return false
	}
	if rec.RecoveryBoundIndexSetID != rec.IndexSetID {
		return false
	}
	return true
}

// claimSignalMode is internal; external packages cannot assert force takeover.
type claimSignalMode int

const (
	claimSignalNormal claimSignalMode = iota
	// claimSignalTakeoverVerified: store re-checks that the prior claimer
	// process is typed Gone (orphan claim). Managed-target death alone does not
	// authorize takeover while a live claimer holds the exclusive phase.
	// Callers cannot assert this without the store re-verifying under the start lock.
	claimSignalTakeoverVerified
)

// ClaimStalledRecoverySignal acquires the exclusive signal phase.
func (s *Store) ClaimStalledRecoverySignal(jobID, fenceOwner, attemptID string) error {
	return s.claimStalledRecoverySignal(jobID, fenceOwner, attemptID, claimSignalNormal)
}

// ClaimStalledRecoverySignalVerifiedTakeover replaces a busy claim only when the
// store verifies (under lock) that the prior claimer process is gone (orphan
// claim). Live concurrent claimers must receive ErrRecoverySignalBusy — a dead
// managed target alone does not authorize stealing an exclusive signal phase
// from a still-live claimer (that would stampede multi-winner finalize).
// No caller-asserted force flag.
func (s *Store) ClaimStalledRecoverySignalVerifiedTakeover(jobID, fenceOwner, attemptID string) error {
	return s.claimStalledRecoverySignal(jobID, fenceOwner, attemptID, claimSignalTakeoverVerified)
}

func (s *Store) claimStalledRecoverySignal(jobID, fenceOwner, attemptID string, mode claimSignalMode) error {
	if s == nil {
		return fmt.Errorf("job registry store is nil")
	}
	fenceOwner = strings.TrimSpace(fenceOwner)
	attemptID = strings.TrimSpace(attemptID)
	if fenceOwner == "" || attemptID == "" {
		return fmt.Errorf("fence owner and signal attempt id are required")
	}
	return s.withStartLock(func() error {
		rec, err := s.getReadOnlyStrict(jobID)
		if err != nil {
			return err
		}
		if rec.RecoveryIntent != RecoveryIntentStalled || rec.State != JobStateStopping {
			return ErrRecoveryNotActive
		}
		if rec.RecoveryOwner != fenceOwner {
			return fmt.Errorf("%w (have %s)", ErrRecoveryNotOwned, rec.RecoveryOwner)
		}
		if rec.RecoverySignalOwner != "" && rec.RecoverySignalOwner != attemptID {
			if mode != claimSignalTakeoverVerified || !signalClaimTakeoverAllowed(rec) {
				return fmt.Errorf("%w (holder=%s)", ErrRecoverySignalBusy, rec.RecoverySignalOwner)
			}
		}
		if rec.RecoverySignalOwner == attemptID {
			// Same-attempt re-entry: never invent a missing historical target (D-R14-02).
			if phaseAtOrAfterTransport(rec.RecoveryPhase) {
				if !BoundTargetMatches(rec) {
					return fmt.Errorf("bound target snapshot missing or mismatched on same-attempt reentry")
				}
				return nil
			}
			// Pre-transport: may establish bound once if empty.
			if rec.RecoveryBoundPID == 0 {
				persistBoundTarget(rec, attemptID)
			} else {
				rec.RecoveryBoundAttempt = attemptID
				rec.RecoveryBoundFenceOwner = fenceOwner
			}
			if !BoundTargetMatches(rec) {
				return fmt.Errorf("bound target snapshot invalid on same-attempt reentry")
			}
			return s.writeRecord(rec)
		}
		now := time.Now().UTC()
		rec.RecoverySignalOwner = attemptID
		rec.RecoverySignalClaimedAt = &now
		selfPID := os.Getpid()
		rec.RecoverySignalClaimerPID = selfPID
		if id := observeProcessIdentity(selfPID); id.Proven {
			start := id.StartTimeUnixMS
			rec.RecoverySignalClaimerStartMS = &start
			rec.RecoverySignalClaimerTokenVersion = id.TokenVersion
			rec.RecoverySignalClaimerBootID = id.BootID
			rec.RecoverySignalClaimerStartTicks = id.StartTicks
			rec.RecoverySignalClaimerStartSec = id.StartSec
			rec.RecoverySignalClaimerStartUsec = id.StartUsec
			rec.RecoverySignalClaimerFiletime = id.Filetime
		} else {
			rec.RecoverySignalClaimerStartMS = nil
			rec.RecoverySignalClaimerTokenVersion = 0
		}
		// D-R14-02: target identity is immutable for the generation after first transport.
		// Takeover may reassign attempt on an existing complete snapshot; never recreate it.
		if phaseAtOrAfterTransport(rec.RecoveryPhase) {
			// D-R15-03: incomplete historical bound (including missing fence owner)
			// refuses — never invent or fill fields after transport.
			if rec.RecoveryBoundPID == 0 || rec.RecoveryBoundFenceOwner == "" || rec.RecoveryBoundAttempt == "" {
				return fmt.Errorf("bound target snapshot incomplete at advanced phase; refuse inventing target")
			}
			// Preserve historical target fields; only rebind attempt to new claimer.
			rec.RecoveryBoundAttempt = attemptID
			if !BoundTargetMatches(rec) {
				return fmt.Errorf("bound target snapshot mismatched on takeover; refuse")
			}
		} else {
			// Pre-transport claim: establish bound once from current managed identity.
			persistBoundTarget(rec, attemptID)
			if !BoundTargetMatches(rec) {
				return fmt.Errorf("bound target snapshot incomplete after claim")
			}
		}
		// Preserve prior phase ambiguity (term-sent/kill-intent/…); only set
		// claimed when phase is still early (E-R7-01 / D-R6-02).
		switch rec.RecoveryPhase {
		case "", RecoveryPhaseFenced, RecoveryPhaseClaimed:
			rec.RecoveryPhase = RecoveryPhaseClaimed
		}
		return s.writeRecord(rec)
	})
}

// signalClaimTakeoverAllowed is true only when the store can prove the prior
// claim is orphaned (claimer process typed Gone). A dead managed target with a
// still-live claimer must keep the exclusive signal phase so concurrent
// recoverers converge on one winner lineage (E6 / exclusive claim).
func signalClaimTakeoverAllowed(rec *JobRecord) bool {
	return claimerProcessGone(rec)
}

// claimerProcessGone: only typed Gone authorizes takeover (E-R7-02).
func claimerProcessGone(rec *JobRecord) bool {
	if rec == nil || rec.RecoverySignalClaimerPID <= 0 {
		return false
	}
	expected := ProcessIdentity{
		PID:          rec.RecoverySignalClaimerPID,
		TokenVersion: rec.RecoverySignalClaimerTokenVersion,
		BootID:       rec.RecoverySignalClaimerBootID,
		StartTicks:   rec.RecoverySignalClaimerStartTicks,
		StartSec:     rec.RecoverySignalClaimerStartSec,
		StartUsec:    rec.RecoverySignalClaimerStartUsec,
		Filetime:     rec.RecoverySignalClaimerFiletime,
	}
	if rec.RecoverySignalClaimerStartMS != nil {
		expected.StartTimeUnixMS = *rec.RecoverySignalClaimerStartMS
	}
	if expected.TokenVersion >= procidentity.TokenVersionV1 || expected.StartTimeUnixMS != 0 {
		expected.Proven = true
	}
	return procidentity.Classify(expected) == procidentity.Gone
}

func heartbeatEqual(a, b *time.Time) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.UTC().Equal(b.UTC())
}

// FinalizeStalledRecoveryGen finalizes after lease-reconciled with mandatory generation.
func (s *Store) FinalizeStalledRecoveryGen(jobID, fenceOwner, attemptID string, expectedGeneration int64, forcedKill bool) (*JobRecord, error) {
	if s == nil {
		return nil, fmt.Errorf("job registry store is nil")
	}
	fenceOwner = strings.TrimSpace(fenceOwner)
	attemptID = strings.TrimSpace(attemptID)
	if fenceOwner == "" || attemptID == "" {
		return nil, fmt.Errorf("recovery owner and attempt id are required")
	}
	if expectedGeneration <= 0 {
		return nil, fmt.Errorf("expected generation must be positive")
	}
	var out *JobRecord
	err := s.withStartLock(func() error {
		rec, err := s.getReadOnlyStrict(jobID)
		if err != nil {
			return err
		}
		if rec.RecoveryIntent != RecoveryIntentStalled {
			// Idempotent: already finalized to stopped with no intent.
			if rec.State == JobStateStopped {
				out = rec
				return nil
			}
			return ErrRecoveryNotActive
		}
		if rec.RecoveryOwner != fenceOwner {
			return fmt.Errorf("%w (have %s)", ErrRecoveryNotOwned, rec.RecoveryOwner)
		}
		now := time.Now().UTC()
		// Only the exclusive signal-phase claimer may finalize.
		if rec.RecoverySignalOwner == "" || rec.RecoverySignalOwner != attemptID {
			return fmt.Errorf("%w (signal holder=%q attempt=%q)", ErrRecoverySignalBusy, rec.RecoverySignalOwner, attemptID)
		}
		if rec.RecoveryGeneration != expectedGeneration {
			return fmt.Errorf("recovery generation mismatch on finalize: have %d want %d", rec.RecoveryGeneration, expectedGeneration)
		}
		// E-R7-01: only lease-reconciled may finalize.
		if rec.RecoveryPhase != RecoveryPhaseLeaseReconciled {
			return fmt.Errorf("finalize requires phase %s (have %q)", RecoveryPhaseLeaseReconciled, rec.RecoveryPhase)
		}
		if rec.RecoveryW2Receipt == nil {
			return fmt.Errorf("finalize requires durable W2 receipt")
		}
		// Receipt is bound to generation+fence; origin attempt may differ after takeover.
		if rec.RecoveryW2Receipt.Generation != expectedGeneration || rec.RecoveryW2Receipt.FenceOwner != fenceOwner {
			return fmt.Errorf("W2 receipt does not match generation/fence")
		}
		if forcedKill {
			// Prefer durable forced provenance from receipt when set.
			if rec.RecoveryW2Receipt.ForcedKill {
				forcedKill = true
			}
		} else if rec.RecoveryW2Receipt.ForcedKill {
			forcedKill = true
		}
		rec.State = JobStateStopped
		rec.EndedAt = &now
		rec.LastHeartbeat = &now
		rec.RecoveryIntent = ""
		rec.RecoveryOwner = ""
		rec.RecoverySignalOwner = ""
		rec.RecoverySignalClaimedAt = nil
		rec.RecoverySignalClaimerPID = 0
		rec.RecoverySignalClaimerStartMS = nil
		rec.RecoveryStartedAt = nil
		rec.RecoveryPhase = RecoveryPhaseFinalized
		if rec.Metadata == nil {
			rec.Metadata = map[string]string{}
		}
		if forcedKill {
			rec.Metadata["stalled_recovery_forced_kill"] = "true"
		}
		rec.Metadata["stalled_recovery"] = "completed"
		if err := s.writeRecord(rec); err != nil {
			return err
		}
		out = rec
		return nil
	})
	return out, err
}

// FailStalledRecovery leaves an honest non-success result when recovery cannot
// prove termination. The signal claim is released so a crash-retry can re-claim.
func (s *Store) FailStalledRecovery(jobID, fenceOwner, attemptID, reason string) (*JobRecord, error) {
	if s == nil {
		return nil, fmt.Errorf("job registry store is nil")
	}
	fenceOwner = strings.TrimSpace(fenceOwner)
	attemptID = strings.TrimSpace(attemptID)
	if fenceOwner == "" {
		return nil, fmt.Errorf("recovery owner is required")
	}
	var out *JobRecord
	err := s.withStartLock(func() error {
		rec, err := s.getReadOnlyStrict(jobID)
		if err != nil {
			return err
		}
		if rec.RecoveryIntent != RecoveryIntentStalled {
			return ErrRecoveryNotActive
		}
		if rec.RecoveryOwner != fenceOwner {
			return fmt.Errorf("%w (have %s)", ErrRecoveryNotOwned, rec.RecoveryOwner)
		}
		// Only the signal claimer may fail and release (or fence owner if claim empty).
		if rec.RecoverySignalOwner != "" && rec.RecoverySignalOwner != attemptID {
			return fmt.Errorf("%w (signal holder=%s)", ErrRecoverySignalBusy, rec.RecoverySignalOwner)
		}
		now := time.Now().UTC()
		// Keep stopping so a retry can resume; do not claim stopped.
		// Release signal claim so another attempt can re-enter bind/signal.
		rec.State = JobStateStopping
		rec.LastHeartbeat = &now
		rec.RecoverySignalOwner = ""
		rec.RecoverySignalClaimedAt = nil
		rec.RecoverySignalClaimerPID = 0
		rec.RecoverySignalClaimerStartMS = nil
		if rec.Metadata == nil {
			rec.Metadata = map[string]string{}
		}
		rec.Metadata["stalled_recovery"] = "failed"
		if strings.TrimSpace(reason) != "" {
			rec.Metadata["stalled_recovery_error"] = reason
		}
		if err := s.writeRecord(rec); err != nil {
			return err
		}
		out = rec
		return nil
	})
	return out, err
}

// writeRecord is the unlocked persist path used under withStartLock.
func (s *Store) writeRecord(record *JobRecord) error {
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
	b, err := marshalJobRecord(record)
	if err != nil {
		return err
	}
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		lastErr = writeJobRecordAtomic(s.root, jobID, b)
		if lastErr == nil {
			return nil
		}
		if !isTransientRegistryIOError(lastErr) {
			return lastErr
		}
		time.Sleep(time.Duration(5*(attempt+1)) * time.Millisecond)
	}
	return lastErr
}

func marshalJobRecord(record *JobRecord) ([]byte, error) {
	b, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal job record: %w", err)
	}
	return append(b, '\n'), nil
}

// recoveryFenceViolation reports whether incoming would clobber an active fence.
// While a fence is active, only the fence owner may write (via Write). Foreign
// child terminal writes (failed/unknown/stopping without owner) are refused so
// they cannot erase RecoveryIntent/Owner. Owner finalize paths use writeRecord
// under withStartLock and bypass this check.
func recoveryFenceViolation(existing, incoming *JobRecord) error {
	if existing == nil || incoming == nil {
		return nil
	}
	if existing.RecoveryIntent != RecoveryIntentStalled {
		return nil
	}
	sameOwner := incoming.RecoveryOwner != "" && incoming.RecoveryOwner == existing.RecoveryOwner
	if !sameOwner {
		return fmt.Errorf("%w: non-owner cannot write while recovery fence is active", ErrRecoveryFenced)
	}
	// Owner may only progress the recovery state machine, never restore running
	// or claim success/partial through the generic Write path.
	switch incoming.State {
	case JobStateRunning:
		return fmt.Errorf("%w: cannot restore running under recovery fence", ErrRecoveryFenced)
	case JobStateSuccess, JobStatePartial:
		return fmt.Errorf("%w: cannot claim success under recovery fence", ErrRecoveryFenced)
	case JobStateStopped, JobStateStopping, JobStateFailed, JobStateUnknown:
		// Owner Write is allowed for these states only when fence fields stay put
		// (stopped finalize normally goes through FinalizeStalledRecovery).
		if incoming.RecoveryIntent != RecoveryIntentStalled || incoming.RecoveryOwner != existing.RecoveryOwner {
			return fmt.Errorf("%w: owner write cannot clear recovery fence fields", ErrRecoveryFenced)
		}
		return nil
	default:
		return fmt.Errorf("%w: owner cannot write state %s under recovery fence", ErrRecoveryFenced, incoming.State)
	}
}
