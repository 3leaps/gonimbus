//go:build windows

package indexstore

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func openBoundSQLiteSnapshotFile(path string) (*os.File, string, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, "", err
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, "", err
	}
	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.CloseHandle(handle)
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
	// The restrictive share mode keeps this pathname bound against replacement
	// while SQLite opens its own read-only handle.
	return file, path, nil
}
