//go:build windows

package indexsubstrate

import (
	"errors"
	"syscall"
)

// mandatoryFileLocks reports whether this platform's file locks block other
// processes from reading, writing, or rebinding the locked range. LockFileEx is
// mandatory: the held range is inaccessible to every other handle, and a
// pathname whose file is open and locked cannot be replaced.
const mandatoryFileLocks = true

// Windows refusals produced by touching a range another handle holds locked, or
// by rebinding a pathname whose file is still open.
const (
	errorAccessDenied     syscall.Errno = 5
	errorSharingViolation syscall.Errno = 32
	errorLockViolation    syscall.Errno = 33
)

// isLockedRangeError reports whether err is one of those refusals specifically,
// so a fixture cannot mistake an unrelated failure for expected lock semantics.
func isLockedRangeError(err error) bool {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return false
	}
	switch errno {
	case errorAccessDenied, errorSharingViolation, errorLockViolation:
		return true
	}
	return false
}
