//go:build windows

package procidentity

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func openTarget(expected Identity) (targetImpl, error) {
	handle, err := windows.OpenProcess(
		windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.PROCESS_TERMINATE|windows.SYNCHRONIZE,
		false,
		uint32(expected.PID), // #nosec G115 -- positive Windows PIDs are DWORD values
	)
	if err != nil {
		return nil, fmt.Errorf("open process: %w", err)
	}
	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(handle, &creation, &exit, &kernel, &user); err != nil {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("process times: %w", err)
	}
	ft := uint64(creation.HighDateTime)<<32 | uint64(creation.LowDateTime)
	if expected.TokenVersion >= TokenVersionV1 {
		if ft == 0 || ft != expected.Filetime {
			_ = windows.CloseHandle(handle)
			return nil, fmt.Errorf("%w: handle filetime %d != expected %d", ErrIdentityLost, ft, expected.Filetime)
		}
	} else {
		startMS := filetimeToUnixMS(creation)
		if startMS == 0 || startMS != expected.StartTimeUnixMS {
			_ = windows.CloseHandle(handle)
			return nil, fmt.Errorf("%w: handle creation time %d != expected %d", ErrIdentityLost, startMS, expected.StartTimeUnixMS)
		}
	}
	return &windowsTarget{handle: handle, expected: expected}, nil
}

type windowsTarget struct {
	handle   windows.Handle
	expected Identity
}

func (t *windowsTarget) hardStopOnly() bool { return true }

func (t *windowsTarget) signal(sig signalKind) error {
	if t == nil || t.handle == 0 {
		return ErrNotBound
	}
	// Re-check creation time under the held handle.
	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(t.handle, &creation, &exit, &kernel, &user); err != nil {
		return fmt.Errorf("process times before signal: %w", err)
	}
	ft := uint64(creation.HighDateTime)<<32 | uint64(creation.LowDateTime)
	if t.expected.TokenVersion >= TokenVersionV1 {
		if ft != t.expected.Filetime {
			return ErrIdentityLost
		}
	} else if filetimeToUnixMS(creation) != t.expected.StartTimeUnixMS {
		return ErrIdentityLost
	}
	switch sig {
	case sigTerm, sigKill:
		// Windows has no portable SIGTERM for arbitrary processes; TerminateProcess
		// is the supported hard stop. Stalled recovery must report ForcedKill /
		// hard-stop honesty (never graceful TERM / forced_kill=false).
		if err := windows.TerminateProcess(t.handle, 1); err != nil {
			// Already-exited instance: second stop (escalation / replay) is not a
			// delivery failure — surface as already-gone for callers.
			var code uint32
			if e2 := windows.GetExitCodeProcess(t.handle, &code); e2 == nil {
				const stillActive = 259
				if code != stillActive {
					return fmt.Errorf("%w: process already exited", ErrAlreadyGone)
				}
			}
			return fmt.Errorf("terminate process: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported signal")
	}
}

func (t *windowsTarget) terminated() (bool, error) {
	if t == nil || t.handle == 0 {
		return false, ErrNotBound
	}
	var code uint32
	if err := windows.GetExitCodeProcess(t.handle, &code); err != nil {
		return false, err
	}
	const stillActive = 259
	return code != stillActive, nil
}

func (t *windowsTarget) close() error {
	if t == nil || t.handle == 0 {
		return nil
	}
	err := windows.CloseHandle(t.handle)
	t.handle = 0
	return err
}
