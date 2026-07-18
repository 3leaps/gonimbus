//go:build windows

package indexsubstrate

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/windows"
)

// openRegularNoFollow opens path for reading and refuses reparse points
// atomically via FILE_FLAG_OPEN_REPARSE_POINT + attribute check.
func openRegularNoFollow(path string) (*os.File, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, err
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		_ = windows.CloseHandle(handle)
		return nil, err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("refusing reparse-point path")
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("path is not a regular file")
	}
	return os.NewFile(uintptr(handle), path), nil
}

func openDirNoFollow(path string) (*os.File, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, err
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		_ = windows.CloseHandle(handle)
		return nil, err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("refusing reparse-point path")
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("path is not a directory")
	}
	return os.NewFile(uintptr(handle), path), nil
}

func unlinkChildrenAt(dir *os.File) error {
	if dir == nil {
		return fmt.Errorf("nil directory")
	}
	// Windows (spill/merge source trust model): names come from the no-follow-opened
	// directory handle, but child deletion uses paths derived from dir.Name()
	// — not FILE_DISPOSITION / exact-handle APIs. This is best-effort under an
	// exclusive SpillRoot; it is NOT claimed FD-relative against hostile
	// concurrent namespace mutation. See durable-spill-merge.md.
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	base := dir.Name()
	var first error
	for _, name := range names {
		if name == "." || name == ".." {
			continue
		}
		p := filepath.Join(base, name)
		if err := os.RemoveAll(p); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func rmdirAt(parentDir *os.File, name string) error {
	// Empty-directory remove only — never recursive by parent path alone.
	return os.Remove(filepath.Join(parentDir.Name(), name))
}

func openParentDirNoFollow(path string) (*os.File, string, error) {
	parent := filepath.Dir(path)
	base := filepath.Base(path)
	d, err := openDirNoFollow(parent)
	if err != nil {
		return nil, "", err
	}
	return d, base, nil
}

var _ = syscall.EINVAL
