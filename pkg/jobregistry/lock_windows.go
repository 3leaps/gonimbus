//go:build windows

package jobregistry

import (
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	// Directory-specific aliases from WinNT.h are not exported by x/sys.
	fileAddSubdirectoryAccess = 0x00000004
	fileDeleteChildAccess     = 0x00000040
)

func openFileNoFollow(path string, flags int, mode os.FileMode) (*os.File, error) {
	_ = mode
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	access := uint32(windows.GENERIC_READ)
	if flags&(os.O_WRONLY|os.O_RDWR) != 0 {
		access = windows.GENERIC_WRITE
		if flags&os.O_RDWR != 0 {
			access |= windows.GENERIC_READ
		}
	}
	disposition := uint32(windows.OPEN_EXISTING)
	switch {
	case flags&os.O_CREATE != 0 && flags&os.O_TRUNC != 0:
		disposition = windows.CREATE_ALWAYS
	case flags&os.O_CREATE != 0 && flags&os.O_EXCL != 0:
		disposition = windows.CREATE_NEW
	case flags&os.O_CREATE != 0:
		disposition = windows.OPEN_ALWAYS
	case flags&os.O_TRUNC != 0:
		disposition = windows.TRUNCATE_EXISTING
	}
	handle, err := windows.CreateFile(
		pathPtr,
		access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		disposition,
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
	return os.NewFile(uintptr(handle), path), nil
}

func openJobFileNoFollow(root, jobID, name string, flags int, mode os.FileMode) (*os.File, error) {
	_ = mode
	rootHandle, err := openDirectoryHandleNoFollow(root, false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = windows.CloseHandle(rootHandle) }()
	jobFlags := os.O_RDONLY
	if flags&(os.O_CREATE|os.O_WRONLY|os.O_RDWR|os.O_TRUNC) != 0 {
		jobFlags = os.O_RDWR
	}
	jobHandle, err := openRelativeHandleNoFollow(rootHandle, jobID, jobFlags, true, false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = windows.CloseHandle(jobHandle) }()
	fileHandle, err := openRelativeHandleNoFollow(jobHandle, name, flags, false, false)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fileHandle), filepath.Join(root, jobID, name)), nil
}

func openDirectoryHandleNoFollow(path string, mutation bool) (windows.Handle, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	access := uint32(windows.FILE_LIST_DIRECTORY | windows.FILE_TRAVERSE | windows.FILE_READ_ATTRIBUTES | windows.SYNCHRONIZE)
	if mutation {
		access |= fileAddSubdirectoryAccess
	}
	handle, err := windows.CreateFile(
		pathPtr,
		access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return 0, err
	}
	if err := rejectReparseHandle(handle, true); err != nil {
		_ = windows.CloseHandle(handle)
		return 0, err
	}
	return handle, nil
}

func openRelativeHandleNoFollow(parent windows.Handle, name string, flags int, directory, deleteAccess bool) (windows.Handle, error) {
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return 0, err
	}
	attrs := &windows.OBJECT_ATTRIBUTES{
		RootDirectory: parent,
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE,
	}
	attrs.Length = uint32(unsafe.Sizeof(*attrs))
	access := uint32(windows.FILE_GENERIC_READ | windows.SYNCHRONIZE)
	if flags&(os.O_WRONLY|os.O_RDWR) != 0 {
		access = windows.FILE_GENERIC_WRITE | windows.SYNCHRONIZE
		if flags&os.O_RDWR != 0 {
			access |= windows.FILE_GENERIC_READ
		}
	}
	if deleteAccess {
		access |= windows.DELETE
	}
	if directory && flags&(os.O_WRONLY|os.O_RDWR) != 0 {
		access |= fileDeleteChildAccess
	}
	disposition := uint32(windows.FILE_OPEN)
	switch {
	case flags&os.O_CREATE != 0 && flags&os.O_TRUNC != 0:
		disposition = windows.FILE_OVERWRITE_IF
	case flags&os.O_CREATE != 0 && flags&os.O_EXCL != 0:
		disposition = windows.FILE_CREATE
	case flags&os.O_CREATE != 0:
		disposition = windows.FILE_OPEN_IF
	case flags&os.O_TRUNC != 0:
		disposition = windows.FILE_OVERWRITE
	}
	options := uint32(windows.FILE_OPEN_REPARSE_POINT | windows.FILE_SYNCHRONOUS_IO_NONALERT)
	if directory {
		options |= windows.FILE_DIRECTORY_FILE
	} else {
		options |= windows.FILE_NON_DIRECTORY_FILE
	}
	var handle windows.Handle
	var iosb windows.IO_STATUS_BLOCK
	allocationSize := int64(0)
	err = windows.NtCreateFile(
		&handle,
		access,
		attrs,
		&iosb,
		&allocationSize,
		0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		disposition,
		options,
		0,
		0,
	)
	if err != nil {
		return 0, err
	}
	if err := rejectReparseHandle(handle, directory); err != nil {
		_ = windows.CloseHandle(handle)
		return 0, err
	}
	return handle, nil
}

func ensureJobDirNoFollow(root, jobID string) error {
	rootHandle, jobHandle, err := openBoundJobDirectoryWindows(root, jobID, true)
	if err != nil {
		return err
	}
	_ = windows.CloseHandle(jobHandle)
	_ = windows.CloseHandle(rootHandle)
	return nil
}

func writeJobRecordAtomic(root, jobID string, data []byte) error {
	rootHandle, jobHandle, err := openBoundJobDirectoryWindows(root, jobID, true)
	if err != nil {
		return fmt.Errorf("bind job directory: %w", err)
	}
	defer func() { _ = windows.CloseHandle(rootHandle) }()
	defer func() { _ = windows.CloseHandle(jobHandle) }()
	afterJobDirBoundBeforeTempCreate()
	if err := verifyBoundJobDirectoryWindows(rootHandle, jobHandle, jobID); err != nil {
		return err
	}
	tmpName, err := newRecordTempName()
	if err != nil {
		return err
	}
	tmpHandle, err := openRelativeHandleNoFollow(jobHandle, tmpName, os.O_CREATE|os.O_EXCL|os.O_RDWR, false, true)
	if err != nil {
		return fmt.Errorf("create bound temp job record: %w", err)
	}
	tmp := os.NewFile(uintptr(tmpHandle), tmpName)
	defer func() {
		_ = tmp.Close()
		_ = deleteRelativeFileWindows(jobHandle, tmpName)
	}()
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write temp job record: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temp job record: %w", err)
	}
	afterRecordTempCreateBeforeReplace()
	if err := verifyBoundJobDirectoryWindows(rootHandle, jobHandle, jobID); err != nil {
		return err
	}
	if err := renameRelativeFileWindows(windows.Handle(tmp.Fd()), jobHandle, "job.json"); err != nil {
		return fmt.Errorf("replace bound job record: %w", err)
	}
	return verifyBoundJobDirectoryWindows(rootHandle, jobHandle, jobID)
}

func openBoundJobDirectoryWindows(root, jobID string, create bool) (windows.Handle, windows.Handle, error) {
	rootHandle, err := openDirectoryHandleNoFollow(root, create)
	if err != nil {
		return 0, 0, err
	}
	flags := os.O_RDONLY
	if create {
		flags = os.O_CREATE | os.O_RDWR
	}
	jobHandle, err := openRelativeHandleNoFollow(rootHandle, jobID, flags, true, false)
	if err != nil {
		_ = windows.CloseHandle(rootHandle)
		return 0, 0, err
	}
	if err := verifyBoundJobDirectoryWindows(rootHandle, jobHandle, jobID); err != nil {
		_ = windows.CloseHandle(jobHandle)
		_ = windows.CloseHandle(rootHandle)
		return 0, 0, err
	}
	return rootHandle, jobHandle, nil
}

func verifyBoundJobDirectoryWindows(rootHandle, jobHandle windows.Handle, jobID string) error {
	namedHandle, err := openRelativeHandleNoFollow(rootHandle, jobID, os.O_RDONLY, true, false)
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(namedHandle) }()
	var bound, named windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(jobHandle, &bound); err != nil {
		return err
	}
	if err := windows.GetFileInformationByHandle(namedHandle, &named); err != nil {
		return err
	}
	if bound.VolumeSerialNumber != named.VolumeSerialNumber || bound.FileIndexHigh != named.FileIndexHigh || bound.FileIndexLow != named.FileIndexLow {
		return fmt.Errorf("job directory binding changed during registry mutation")
	}
	return nil
}

type fileRenameInformation struct {
	ReplaceIfExists uint32
	RootDirectory   windows.Handle
	FileNameLength  uint32
	FileName        [1]uint16
}

func renameRelativeFileWindows(fileHandle, jobHandle windows.Handle, target string) error {
	targetUTF16, err := windows.UTF16FromString(target)
	if err != nil {
		return err
	}
	nameBytes := (len(targetUTF16) - 1) * 2
	var header fileRenameInformation
	bufferSize := int(unsafe.Offsetof(header.FileName)) + nameBytes
	buffer := make([]byte, bufferSize)
	info := (*fileRenameInformation)(unsafe.Pointer(&buffer[0]))
	info.ReplaceIfExists = windows.FILE_RENAME_REPLACE_IF_EXISTS | windows.FILE_RENAME_POSIX_SEMANTICS
	info.RootDirectory = jobHandle
	info.FileNameLength = uint32(nameBytes)
	copy((*[windows.MAX_LONG_PATH]uint16)(unsafe.Pointer(&info.FileName[0]))[:nameBytes/2:nameBytes/2], targetUTF16[:len(targetUTF16)-1])
	var iosb windows.IO_STATUS_BLOCK
	return windows.NtSetInformationFile(fileHandle, &iosb, &buffer[0], uint32(bufferSize), windows.FileRenameInformation)
}

func deleteRelativeFileWindows(jobHandle windows.Handle, name string) error {
	handle, err := openRelativeHandleNoFollow(jobHandle, name, os.O_RDWR, false, true)
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(handle) }()
	info := struct{ DeleteFile byte }{DeleteFile: 1}
	var iosb windows.IO_STATUS_BLOCK
	return windows.NtSetInformationFile(handle, &iosb, (*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)), windows.FileDispositionInformation)
}

func rejectReparseHandle(handle windows.Handle, requireDirectory bool) error {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return fmt.Errorf("refusing reparse-point path")
	}
	if requireDirectory && info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 {
		return fmt.Errorf("job registry path is not a directory")
	}
	return nil
}

func lockFileExclusive(f *os.File) error {
	if f == nil {
		return fmt.Errorf("nil file")
	}
	var ol windows.Overlapped
	return windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, &ol)
}

func unlockFile(f *os.File) error {
	if f == nil {
		return nil
	}
	var ol windows.Overlapped
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &ol)
}
