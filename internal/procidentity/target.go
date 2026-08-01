package procidentity

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrIdentityLost means the live process no longer matches the bound birth
	// token (exit + PID reuse, or observation failure).
	ErrIdentityLost = errors.New("process birth identity no longer matches")
	// ErrNotBound means the target was closed or never opened.
	ErrNotBound = errors.New("process target is not bound")
	// ErrUnsupportedPlatform means Bind cannot authorize destructive recovery
	// (no instance-stable signaling primitive — e.g. Darwin stalled-recovery path).
	ErrUnsupportedPlatform = errors.New("platform does not support instance-stable destructive recovery")
	// ErrAlreadyGone means the bound instance was already dead before a signal
	// could be delivered (e.g. pidfd ESRCH). Not a successful signal delivery.
	ErrAlreadyGone = errors.New("process instance already gone before signal delivery")
)

// Target is an identity-bound handle for signalling one process instance.
//
// On Linux it prefers a pidfd (kernel-bound to the process). On other platforms
// it re-validates the birth token immediately before every signal so a recycled
// PID cannot be targeted. Neither form is a raw "signal this number" API.
type Target struct {
	expected Identity
	impl     targetImpl
	closed   bool
}

type targetImpl interface {
	signal(sig signalKind) error
	// terminated reports whether the bound instance is gone (exited or identity
	// no longer matches). A mismatched live PID is treated as terminated for
	// the original instance.
	terminated() (bool, error)
	close() error
}

type signalKind int

const (
	sigTerm signalKind = iota
	sigKill
)

// Bind opens an identity-bound target for the expected process instance.
// It fails closed when identity cannot be proven or the live process does not
// match.
func Bind(expected Identity) (*Target, error) {
	if !expected.Proven || expected.PID <= 0 {
		return nil, fmt.Errorf("cannot bind unproven process identity: %s", Format(expected))
	}
	// stalled-recovery destructive recovery requires versioned native tokens (E-R6-02).
	if expected.TokenVersion < TokenVersionV1 {
		return nil, fmt.Errorf("cannot bind legacy/lossy birth token for destructive recovery (token_version=%d)", expected.TokenVersion)
	}
	observed := Observe(expected.PID)
	if !Match(expected, observed) {
		return nil, fmt.Errorf("%w: expected %s observed %s", ErrIdentityLost, Format(expected), Format(observed))
	}
	impl, err := openTarget(expected)
	if err != nil {
		return nil, err
	}
	return &Target{expected: expected, impl: impl}, nil
}

// SignalTerm delivers a graceful termination request to the bound instance.
func (t *Target) SignalTerm() error {
	if t == nil || t.closed || t.impl == nil {
		return ErrNotBound
	}
	return t.impl.signal(sigTerm)
}

// SignalKill delivers an uncatchable kill to the bound instance.
func (t *Target) SignalKill() error {
	if t == nil || t.closed || t.impl == nil {
		return ErrNotBound
	}
	return t.impl.signal(sigKill)
}

// Terminated reports whether the bound process instance is gone.
func (t *Target) Terminated() (bool, error) {
	if t == nil || t.closed || t.impl == nil {
		return false, ErrNotBound
	}
	return t.impl.terminated()
}

// WaitTerminated polls until the bound instance is gone or the timeout elapses.
// timeout == 0 means already expired (immediate return, no wait). timeout < 0
// uses the 30s default. Callers sharing a monotonic deadline must pass remain()
// which may be 0 at expiry (D-R11-03).
func (t *Target) WaitTerminated(timeout, poll time.Duration) (bool, error) {
	if timeout == 0 {
		done, err := t.Terminated()
		if err != nil {
			return false, err
		}
		return done, nil
	}
	if timeout < 0 {
		timeout = 30 * time.Second
	}
	if poll <= 0 {
		poll = 250 * time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	for {
		done, err := t.Terminated()
		if err != nil {
			return false, err
		}
		if done {
			return true, nil
		}
		if !time.Now().Before(deadline) {
			return false, nil
		}
		time.Sleep(poll)
	}
}

// Close releases platform resources. It is safe to call more than once.
func (t *Target) Close() error {
	if t == nil || t.closed {
		return nil
	}
	t.closed = true
	if t.impl == nil {
		return nil
	}
	return t.impl.close()
}

// Expected returns the birth identity this target was bound to.
func (t *Target) Expected() Identity {
	if t == nil {
		return Identity{}
	}
	return t.expected
}

// HardStopOnly reports whether this platform's stop primitive is a hard stop
// (e.g. Windows TerminateProcess) with no portable graceful TERM for arbitrary
// processes. Callers must report ForcedKill / hard-stop semantics and must not
// invent a TERM→grace→KILL story.
func (t *Target) HardStopOnly() bool {
	if t == nil || t.impl == nil {
		return false
	}
	type hardStop interface{ hardStopOnly() bool }
	if h, ok := t.impl.(hardStop); ok {
		return h.hardStopOnly()
	}
	return false
}

// CheckDestructiveRecoverySupported reports whether this GOOS can perform
// instance-stable destructive recovery (Bind + signal). Darwin and other
// unsupported platforms refuse before any registry fence (D-R11-04).
func CheckDestructiveRecoverySupported() error {
	return checkDestructiveRecoverySupported()
}
