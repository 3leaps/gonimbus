//go:build linux

package procidentity

import (
	"fmt"
	"os"
	"syscall"
)

// revalidateTarget is the Linux fallback when pidfd is unavailable.
type revalidateTarget struct {
	expected Identity
}

func (t *revalidateTarget) signal(sig signalKind) error {
	if t == nil {
		return ErrNotBound
	}
	obs := Observe(t.expected.PID)
	if !Match(t.expected, obs) {
		return fmt.Errorf("%w: expected %s observed %s", ErrIdentityLost, Format(t.expected), Format(obs))
	}
	process, err := os.FindProcess(t.expected.PID)
	if err != nil {
		return fmt.Errorf("find process: %w", err)
	}
	var s os.Signal
	switch sig {
	case sigTerm:
		s = syscall.SIGTERM
	case sigKill:
		s = syscall.SIGKILL
	default:
		return fmt.Errorf("unsupported signal")
	}
	if err := process.Signal(s); err != nil {
		if err == os.ErrProcessDone {
			return nil
		}
		return err
	}
	return nil
}

func (t *revalidateTarget) terminated() (bool, error) {
	if t == nil {
		return false, ErrNotBound
	}
	switch Classify(t.expected) {
	case Gone, LiveMismatched:
		return true, nil
	case Indeterminate:
		return false, fmt.Errorf("process liveness indeterminate")
	default:
		return false, nil
	}
}

func (t *revalidateTarget) close() error { return nil }
