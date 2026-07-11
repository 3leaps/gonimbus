//go:build unix

package indexsubstrate

import (
	"errors"
	"fmt"
	"math"
	"os"

	"golang.org/x/sys/unix"
)

// errLockWouldBlock is returned by lockFileExclusive when the lock is held.
var errLockWouldBlock = errors.New("lock would block")

func fileFD(f *os.File) (int, error) {
	if f == nil {
		return 0, fmt.Errorf("nil file")
	}
	fd := f.Fd()
	if fd > uintptr(math.MaxInt) {
		return 0, fmt.Errorf("file descriptor does not fit in int")
	}
	return int(fd), nil // #nosec G115 -- overflow checked above
}

func lockFileExclusive(f *os.File) error {
	fd, err := fileFD(f)
	if err != nil {
		return err
	}
	err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
	if err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
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
	fd, err := fileFD(f)
	if err != nil {
		return err
	}
	return unix.Flock(fd, unix.LOCK_UN)
}
