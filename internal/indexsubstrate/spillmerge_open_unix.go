//go:build unix

package indexsubstrate

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// openRegularNoFollow opens path for reading only if it is a regular file and
// not a symlink (O_NOFOLLOW). Same-open posture for sealed journals.
func openRegularNoFollow(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if !st.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("path is not a regular file")
	}
	return f, nil
}

// openDirNoFollow opens a directory without following a final symlink component.
func openDirNoFollow(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

// unlinkChildrenAt removes all directory entries via the bound directory FD.
func unlinkChildrenAt(dir *os.File) error {
	if dir == nil {
		return fmt.Errorf("nil directory")
	}
	fd, err := fileFD(dir)
	if err != nil {
		return err
	}
	// Rewind and read names through the FD (not the path).
	if _, err := dir.Seek(0, 0); err != nil {
		// directories may not support Seek; ignore and use Readdirnames
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	var first error
	for _, name := range names {
		if name == "." || name == ".." {
			continue
		}
		// Try as non-dir first, then as dir.
		if err := unix.Unlinkat(fd, name, 0); err != nil {
			if err2 := unix.Unlinkat(fd, name, unix.AT_REMOVEDIR); err2 != nil {
				if first == nil {
					first = fmt.Errorf("unlinkat %s: %v / %v", name, err, err2)
				}
			}
		}
	}
	return first
}

// rmdirAt removes an empty directory name under parentDirFD.
func rmdirAt(parentDir *os.File, name string) error {
	fd, err := fileFD(parentDir)
	if err != nil {
		return err
	}
	return unix.Unlinkat(fd, name, unix.AT_REMOVEDIR)
}

// openParentDirNoFollow opens the parent directory of path with O_NOFOLLOW.
func openParentDirNoFollow(path string) (*os.File, string, error) {
	parent := filepath.Dir(path)
	base := filepath.Base(path)
	d, err := openDirNoFollow(parent)
	if err != nil {
		return nil, "", err
	}
	return d, base, nil
}
