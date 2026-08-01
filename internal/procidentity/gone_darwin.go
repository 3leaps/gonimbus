//go:build darwin

package procidentity

import "golang.org/x/sys/unix"

// darwinIsZombie reports whether pid is a zombie (SZOMB). ok is false when the
// process table entry cannot be read.
func darwinIsZombie(pid int) (zombie bool, ok bool) {
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return false, false
	}
	// SZOMB is 5 on Darwin (see sys/proc.h). SIDL=1, SRUN=2, SSLEEP=3, SSTOP=4, SZOMB=5.
	const szomb int8 = 5
	return info.Proc.P_stat == szomb, true
}
