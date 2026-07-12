//go:build !windows

package indexstore

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func openBoundSQLiteSnapshotFile(path string) (*os.File, string, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, "", err
	}
	file := os.NewFile(uintptr(fd), path) // #nosec G115 -- unix.Open returns a nonnegative descriptor that fits uintptr
	if file == nil {
		_ = unix.Close(fd)
		return nil, "", fmt.Errorf("create bound file handle")
	}
	boundInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, "", err
	}
	namedInfo, err := os.Lstat(path)
	if err != nil {
		_ = file.Close()
		return nil, "", err
	}
	if namedInfo.Mode()&os.ModeSymlink != 0 || !namedInfo.Mode().IsRegular() || !os.SameFile(boundInfo, namedInfo) {
		_ = file.Close()
		return nil, "", fmt.Errorf("index store binding changed before inspection")
	}
	return file, fmt.Sprintf("/dev/fd/%d", fd), nil
}
