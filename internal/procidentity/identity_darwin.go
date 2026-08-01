//go:build darwin

package procidentity

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func observe(pid int) Identity {
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return Identity{PID: pid, UnavailableReason: fmt.Sprintf("sysctl kinfo_proc: %v", err)}
	}
	if int(info.Proc.P_pid) != pid {
		return Identity{PID: pid, UnavailableReason: "kinfo_proc pid mismatch"}
	}
	sec := int64(info.Proc.P_starttime.Sec)
	usec := int64(info.Proc.P_starttime.Usec)
	if sec == 0 && usec == 0 {
		return Identity{PID: pid, UnavailableReason: "process start time unavailable"}
	}
	// #nosec G115 -- display-only ms derived from positive proc start time components
	startMS := uint64(sec)*1000 + uint64(usec)/1000
	return Identity{
		PID:             pid,
		StartTimeUnixMS: startMS,
		TokenVersion:    TokenVersionV1,
		StartSec:        sec,
		StartUsec:       usec,
		Proven:          true,
	}
}
