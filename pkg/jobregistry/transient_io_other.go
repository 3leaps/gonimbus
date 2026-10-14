//go:build !windows

package jobregistry

import "syscall"

// platformErrno has nothing to map off Windows; errors carry syscall.Errno.
func platformErrno(error) (syscall.Errno, bool) { return 0, false }
