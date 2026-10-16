//go:build windows

package jobregistry

import (
	"errors"
	"syscall"

	"golang.org/x/sys/windows"
)

// platformErrno maps an NTSTATUS from the handle-relative registry operations
// to its Win32 error, which is what the transient classification is keyed on.
func platformErrno(err error) (syscall.Errno, bool) {
	var status windows.NTStatus
	if !errors.As(err, &status) {
		return 0, false
	}
	return status.Errno(), true
}
