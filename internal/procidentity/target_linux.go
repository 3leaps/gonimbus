//go:build linux

package procidentity

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func openTarget(expected Identity) (targetImpl, error) {
	fd, err := unix.PidfdOpen(expected.PID, 0)
	if err == nil {
		// Re-check birth token after open; pidfd is bound but we still want the
		// recorded token to match what we observed at bind time.
		observed := Observe(expected.PID)
		if !Match(expected, observed) {
			_ = unix.Close(fd)
			return nil, fmt.Errorf("%w after pidfd open", ErrIdentityLost)
		}
		return &pidfdTarget{fd: fd, expected: expected}, nil
	}
	// S4 / panel D2.1: destructive recovery on Linux requires pidfd. ENOSYS,
	// EPERM, resource errors, and any other failure fail closed — never
	// silently downgrade to revalidate-then-raw-PID kill.
	return nil, fmt.Errorf("pidfd required for destructive recovery on linux: %w", err)
}

type pidfdTarget struct {
	fd       int
	expected Identity
}

func (t *pidfdTarget) signal(sig signalKind) error {
	if t == nil || t.fd < 0 {
		return ErrNotBound
	}
	var s unix.Signal
	switch sig {
	case sigTerm:
		s = unix.SIGTERM
	case sigKill:
		s = unix.SIGKILL
	default:
		return fmt.Errorf("unsupported signal")
	}
	if err := unix.PidfdSendSignal(t.fd, s, nil, 0); err != nil {
		// E-R6-05: ESRCH means no signal was accepted — not success.
		if err == unix.ESRCH {
			return fmt.Errorf("%w: pidfd ESRCH", ErrAlreadyGone)
		}
		return fmt.Errorf("pidfd signal: %w", err)
	}
	return nil
}

func (t *pidfdTarget) terminated() (bool, error) {
	if t == nil || t.fd < 0 {
		return false, ErrNotBound
	}
	// Death proof comes from the stable pidfd, not a fresh raw-PID classifier.
	err := unix.PidfdSendSignal(t.fd, 0, nil, 0)
	if err == nil {
		return false, nil
	}
	if err == unix.ESRCH {
		return true, nil
	}
	return false, fmt.Errorf("pidfd liveness probe: %w", err)
}

func (t *pidfdTarget) close() error {
	if t == nil || t.fd < 0 {
		return nil
	}
	err := unix.Close(t.fd)
	t.fd = -1
	return err
}

func isAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return process.Signal(syscall.Signal(0)) == nil
}
