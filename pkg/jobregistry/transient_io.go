package jobregistry

import (
	"errors"
	"io/fs"
	"strings"
	"syscall"
)

// isTransientRegistryIOError reports I/O failures that are often temporary under
// concurrent open handles (especially Windows atomic job.json replace while a
// reader holds the file with share-delete semantics).
func isTransientRegistryIOError(err error) bool {
	if err == nil {
		return false
	}
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) && pathErr.Err != nil {
		err = pathErr.Err
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case 5, 32, 33: // ERROR_ACCESS_DENIED, ERROR_SHARING_VIOLATION, ERROR_LOCK_VIOLATION
			return true
		case syscall.EAGAIN, syscall.EBUSY:
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "sharing violation") ||
		strings.Contains(msg, "being used by another process") ||
		strings.Contains(msg, "resource deadlock") ||
		strings.Contains(msg, "locked")
}
