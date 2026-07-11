//go:build windows

package indexsubstrate

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// errLockWouldBlock is returned by lockFileExclusive when the lock is held.
var errLockWouldBlock = errors.New("lock would block")

func lockFileExclusive(f *os.File) error {
	if f == nil {
		return fmt.Errorf("nil file")
	}
	// Lock the first byte of the file exclusively, non-blocking.
	var ol windows.Overlapped
	err := windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		&ol,
	)
	if err != nil {
		// ERROR_LOCK_VIOLATION / ERROR_IO_PENDING variants surface as locked.
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) || errors.Is(err, windows.ERROR_IO_PENDING) {
			return errLockWouldBlock
		}
		// Some Windows builds return syscall.Errno(33) ERROR_LOCK_VIOLATION.
		var errno windows.Errno
		if errors.As(err, &errno) && (errno == windows.ERROR_LOCK_VIOLATION || errno == 33) {
			return errLockWouldBlock
		}
		return err
	}
	return nil
}

func unlockFile(f *os.File) error {
	if f == nil {
		return nil
	}
	var ol windows.Overlapped
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &ol)
}
