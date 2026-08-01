//go:build unix

package procidentity

import (
	"errors"
	"os"
	"runtime"
	"syscall"

	"github.com/3leaps/gonimbus/internal/procstate"
	"golang.org/x/sys/unix"
)

func classify(expected Identity) Liveness {
	if expected.PID <= 0 {
		return Gone
	}
	addr, indet := probeAddressable(expected.PID)
	if indet {
		return Indeterminate
	}
	if !addr {
		return Gone
	}
	if runtime.GOOS == "linux" {
		if state, ok := procstate.State("/proc", expected.PID); ok && procstate.IsTerminal(state) {
			return Gone
		}
	}
	if runtime.GOOS == "darwin" {
		if zombie, ok := darwinIsZombie(expected.PID); ok && zombie {
			return Gone
		}
	}
	if !expected.Proven || expected.StartTimeUnixMS == 0 {
		// Live PID without a usable birth token: cannot claim match or mismatch.
		return Indeterminate
	}
	obs := Observe(expected.PID)
	if !obs.Proven {
		return Indeterminate
	}
	if Match(expected, obs) {
		return LiveMatching
	}
	return LiveMismatched
}

func probeAddressable(pid int) (addressable bool, indeterminate bool) {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false, true
	}
	err = process.Signal(syscall.Signal(0))
	if err == nil {
		return true, false
	}
	if isNoSuchProcess(err) {
		return false, false
	}
	return false, true
}

func isNoSuchProcess(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrProcessDone) {
		return true
	}
	if errors.Is(err, unix.ESRCH) || errors.Is(err, syscall.ESRCH) {
		return true
	}
	var errno syscall.Errno
	if errors.As(err, &errno) && errno == syscall.ESRCH {
		return true
	}
	return false
}
