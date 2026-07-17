//go:build !windows

package indexreader

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// createDirectoryAt creates (or, when exclusive is false, reuses) the named
// child directory relative to the retained parent handle, then opens it
// no-follow relative to the same handle. The child pathname is never rewalked
// from the root, so a parent substituted after binding cannot redirect the
// creation into a foreign tree: the child lands under the originally bound
// directory or the call fails.
func createDirectoryAt(parent *os.File, name, path string, exclusive bool) (*os.File, error) {
	parentFd := int(parent.Fd()) // #nosec G115 -- native descriptors fit int
	if err := unix.Mkdirat(parentFd, name, 0o700); err != nil {
		if exclusive || !errors.Is(err, unix.EEXIST) {
			return nil, err
		}
	}
	fd, err := unix.Openat(parentFd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path) // #nosec G115 -- unix.Openat returns a nonnegative descriptor
	if file == nil {
		_ = unix.Close(fd)
		return nil, os.ErrInvalid
	}
	return file, nil
}

// createFileExclusiveAt exclusively creates the named regular file relative to
// the retained parent handle with owner-only permissions, never following a
// planted symlink and never rewalking the parent pathname.
func createFileExclusiveAt(parent *os.File, name, path string) (*os.File, error) {
	parentFd := int(parent.Fd()) // #nosec G115 -- native descriptors fit int
	fd, err := unix.Openat(parentFd, name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path) // #nosec G115 -- unix.Openat returns a nonnegative descriptor
	if file == nil {
		_ = unix.Close(fd)
		return nil, os.ErrInvalid
	}
	return file, nil
}
