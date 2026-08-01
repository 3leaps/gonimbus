//go:build windows

package procidentity

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func observe(pid int) Identity {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid)) // #nosec G115
	if err != nil {
		return Identity{PID: pid, UnavailableReason: fmt.Sprintf("open process: %v", err)}
	}
	defer func() { _ = windows.CloseHandle(handle) }()

	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(handle, &creation, &exit, &kernel, &user); err != nil {
		return Identity{PID: pid, UnavailableReason: fmt.Sprintf("process times: %v", err)}
	}
	ft := uint64(creation.HighDateTime)<<32 | uint64(creation.LowDateTime)
	if ft == 0 {
		return Identity{PID: pid, UnavailableReason: "process creation time unavailable"}
	}
	return Identity{
		PID:             pid,
		StartTimeUnixMS: filetimeToUnixMS(creation), // display only
		TokenVersion:    TokenVersionV1,
		Filetime:        ft,
		Proven:          true,
	}
}

func filetimeToUnixMS(ft windows.Filetime) uint64 {
	const epochDiff = 116444736000000000
	n := uint64(ft.HighDateTime)<<32 | uint64(ft.LowDateTime)
	if n < epochDiff {
		return 0
	}
	return (n - epochDiff) / 10000
}
