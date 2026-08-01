//go:build windows

package procidentity

import (
	"golang.org/x/sys/windows"
)

func classify(expected Identity) Liveness {
	if expected.PID <= 0 {
		return Gone
	}
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(expected.PID)) // #nosec G115
	if err != nil {
		// Access denied / not found: distinguish if possible.
		if err == windows.ERROR_INVALID_PARAMETER || err == windows.ERROR_INVALID_HANDLE {
			return Gone
		}
		// ERROR_ACCESS_DENIED and other failures are indeterminate.
		return Indeterminate
	}
	defer func() { _ = windows.CloseHandle(handle) }()

	var code uint32
	if err := windows.GetExitCodeProcess(handle, &code); err != nil {
		return Indeterminate
	}
	const stillActive = 259
	if code != stillActive {
		return Gone
	}
	if !expected.Proven || expected.StartTimeUnixMS == 0 {
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

func instanceGone(expected Identity) bool {
	return classify(expected) == Gone
}
