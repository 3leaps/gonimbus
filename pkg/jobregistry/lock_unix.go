//go:build unix

package jobregistry

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func openFileNoFollow(path string, flags int, mode os.FileMode) (*os.File, error) {
	fd, err := unix.Open(path, flags|unix.O_NOFOLLOW|unix.O_CLOEXEC, uint32(mode.Perm()))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil // #nosec G115 -- unix.Open returns a nonnegative native descriptor on success.
}

func openJobFileNoFollow(root, jobID, name string, flags int, mode os.FileMode) (*os.File, error) {
	rootFD, jobFD, err := openBoundJobDir(root, jobID, false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Close(rootFD) }()
	defer func() { _ = unix.Close(jobFD) }()
	fd, err := unix.Openat(jobFD, name, flags|unix.O_NOFOLLOW|unix.O_CLOEXEC, uint32(mode.Perm()))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), filepath.Join(root, jobID, name)), nil // #nosec G115 -- unix.Openat returns a nonnegative native descriptor on success.
}

func ensureJobDirNoFollow(root, jobID string) error {
	rootFD, jobFD, err := openBoundJobDir(root, jobID, true)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(rootFD) }()
	defer func() { _ = unix.Close(jobFD) }()
	return nil
}

func writeJobRecordAtomic(root, jobID string, data []byte) error {
	rootFD, jobFD, err := openBoundJobDir(root, jobID, true)
	if err != nil {
		return fmt.Errorf("bind job directory: %w", err)
	}
	defer func() { _ = unix.Close(rootFD) }()
	defer func() { _ = unix.Close(jobFD) }()
	afterJobDirBoundBeforeTempCreate()
	if err := verifyBoundJobDir(rootFD, jobFD, jobID); err != nil {
		return err
	}
	tmpName, err := newRecordTempName()
	if err != nil {
		return err
	}
	fd, err := unix.Openat(jobFD, tmpName, unix.O_CREAT|unix.O_EXCL|unix.O_WRONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return fmt.Errorf("create bound temp job record: %w", err)
	}
	defer func() { _ = unix.Unlinkat(jobFD, tmpName, 0) }()
	tmp := os.NewFile(uintptr(fd), tmpName) // #nosec G115 -- unix.Openat returns a nonnegative native descriptor on success.
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp job record: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp job record: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp job record: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp job record: %w", err)
	}
	afterRecordTempCreateBeforeReplace()
	if err := verifyBoundJobDir(rootFD, jobFD, jobID); err != nil {
		return err
	}
	if err := unix.Renameat(jobFD, tmpName, jobFD, "job.json"); err != nil {
		return fmt.Errorf("replace bound job record: %w", err)
	}
	if err := unix.Fsync(jobFD); err != nil {
		return fmt.Errorf("sync bound job directory: %w", err)
	}
	return verifyBoundJobDir(rootFD, jobFD, jobID)
}

func openBoundJobDir(root, jobID string, create bool) (int, int, error) {
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, -1, err
	}
	if create {
		if err := unix.Mkdirat(rootFD, jobID, 0o700); err != nil && err != unix.EEXIST {
			_ = unix.Close(rootFD)
			return -1, -1, err
		}
	}
	jobFD, err := unix.Openat(rootFD, jobID, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = unix.Close(rootFD)
		return -1, -1, err
	}
	if create {
		if err := unix.Fchmod(jobFD, 0o700); err != nil {
			_ = unix.Close(jobFD)
			_ = unix.Close(rootFD)
			return -1, -1, err
		}
	}
	if err := verifyBoundJobDir(rootFD, jobFD, jobID); err != nil {
		_ = unix.Close(jobFD)
		_ = unix.Close(rootFD)
		return -1, -1, err
	}
	return rootFD, jobFD, nil
}

func verifyBoundJobDir(rootFD, jobFD int, jobID string) error {
	var bound, named unix.Stat_t
	if err := unix.Fstat(jobFD, &bound); err != nil {
		return err
	}
	if err := unix.Fstatat(rootFD, jobID, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if named.Mode&unix.S_IFMT != unix.S_IFDIR || bound.Dev != named.Dev || bound.Ino != named.Ino {
		return fmt.Errorf("job directory binding changed during registry mutation")
	}
	return nil
}

func lockFileExclusive(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_EX) // #nosec G115 -- file descriptors fit the platform int.
}

func unlockFile(f *os.File) error {
	if f == nil {
		return nil
	}
	return unix.Flock(int(f.Fd()), unix.LOCK_UN) // #nosec G115 -- file descriptors fit the platform int.
}
