//go:build !linux && !darwin && !windows

package procidentity

func observe(pid int) Identity {
	return Identity{
		PID:               pid,
		UnavailableReason: "process birth identity unsupported on this platform",
	}
}
